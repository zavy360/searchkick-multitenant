module Searchkick::MultiTenant
  class PartialReindexError < Error
    attr_reader :failed_tenants

    def initialize(failed_tenants)
      @failed_tenants = failed_tenants
      super("Reindex incomplete - #{failed_tenants.size} tenant(s) did not complete: #{failed_tenants.join(", ")}. " \
        "Index was NOT promoted. Re-run with resume: true to retry only the remaining tenants, " \
        "or force_promote_incomplete: true to promote anyway (not recommended).")
    end
  end

  # Reindexes every tenant into the SAME new physical index and only
  # promotes (alias-swaps) once every tenant has completed — the
  # requirement that ruled out Searchkick's stock `Model.reindex`, which
  # promotes after a single, single-scope import.
  #
  # Progress is checkpointed in a Redis set (one member per tenant still
  # outstanding), mirroring the SADD/SREM/SCARD pattern Searchkick's own
  # async batch tracking already uses (relation_indexer.rb) — a `resume`
  # run re-attaches to the same not-yet-promoted index and only retries
  # tenants still in that set, instead of starting over.
  class TenantReindexer
    RETRIES = 3
    # a full reindex bulk-imports every tenant before promoting — throttle
    # refreshes during that import (ES/OpenSearch refresh is the expensive
    # part of heavy bulk indexing), then restore to whatever the model is
    # actually configured with (falls back to Searchkick's own "1s") on
    # promote, same as passing refresh_interval: "30s" to stock Searchkick's
    # full_reindex. Pass refresh_interval: nil explicitly to opt out.
    DEFAULT_REFRESH_INTERVAL = "30s"
    # same reasoning as DEFAULT_REFRESH_INTERVAL: a full reindex bulk-writes
    # every tenant, and every replica has to independently apply every one
    # of those writes — 0 replicas during the import means only the primary
    # shard does that work. Restored to whatever the model is actually
    # configured with on promote (stock Searchkick has no equivalent of its
    # own update_refresh_interval for replicas, so this gem manages it
    # directly — see restore_replicas). Pass replicas: nil to opt out.
    DEFAULT_REPLICAS_DURING_REINDEX = 0

    def self.call(model, **options)
      new(model, **options).call
    end

    def initialize(model, mode: :inline, retain: false, job_options: nil, resume: false, force_promote_incomplete: false, scope: nil, wait: nil, refresh_interval: DEFAULT_REFRESH_INTERVAL, replicas: DEFAULT_REPLICAS_DURING_REINDEX)
      raise Error, "Searchkick.redis not set" unless Searchkick.redis
      raise ArgumentError, "wait only available in :async mode" if !wait.nil? && mode != :async

      @model = model
      @mode = mode
      @retain = retain
      @job_options = job_options
      @resume = resume
      @force_promote_incomplete = force_promote_incomplete
      @scope = scope
      @wait = wait
      @refresh_interval = refresh_interval
      @replicas = replicas
      @index = model.searchkick_index
    end

    def call
      new_index = resolve_index
      alias_existed = @index.alias_exists?
      tenants_key = "searchkick:multitenant:reindex:#{new_index.name}:tenants"
      seed_checkpoint(tenants_key) unless @resume
      uuid = new_index.uuid

      # no prior serving index to protect — same bootstrap edge case
      # Searchkick's own full_reindex carries (index.rb: promote before
      # import when no alias exists yet)
      unless alias_existed
        @index.delete if @index.exists?
        @index.promote(new_index.name, update_refresh_interval: !@refresh_interval.nil?)
        restore_replicas(new_index)
      end

      if @mode == :async
        # each pending tenant's import runs as its OWN background job
        # (TenantImportJob) instead of inline here — that's what gives
        # concurrency across tenants: Sidekiq's own worker pool already
        # checks a DB connection out and back in correctly per job, so N
        # tenants' find_in_batches walks (relation_indexer_ext.rb) run
        # genuinely in parallel without this gem hand-rolling connection
        # pool lifecycle management. See TenantImportJob's own comment for
        # why that matters and what the first (thread-based) attempt at
        # this got wrong.
        enqueue_pending_tenants(new_index, tenants_key)

        unless @wait
          # every tenant's import is enqueued, but none has necessarily
          # run yet, let alone the batches within it — promoting here
          # could promote a near-empty index. Leave it unpromoted and let
          # the caller finish later — poll `Searchkick.reindex_status`
          # directly, or call this again with `resume: true, wait: true`.
          return {index_name: new_index.name, incomplete_tenants: pending_tenants(tenants_key), promoted: false}
        end

        log "Created index: #{new_index.name}"
        log "Jobs queued. Waiting for tenants..."
        wait_for_tenants(tenants_key)
        log "Tenants done. Waiting for batches..."
        wait_for_batches(new_index)
      else
        pending_tenants(tenants_key).each do |tenant|
          begin
            import_tenant(new_index, tenant)
            remove_from_checkpoint(tenants_key, tenant)
          rescue => e
            Searchkick.warn("[searchkick-multitenant] tenant #{tenant.inspect} failed to reindex into #{new_index.name}: #{e.message}")
          end
        end
      end

      remaining = pending_tenants(tenants_key)
      raise PartialReindexError, remaining if remaining.any? && !@force_promote_incomplete

      if alias_existed
        # Index#check_uuid does exactly this, but it's a protected method —
        # inlined rather than reaching past that with `send`
        if uuid != new_index.uuid
          raise Error, "Safety check failed - only run one Model.reindex/TenantReindexer per model at a time"
        end

        log "Jobs complete. Promoting..." if @mode == :async && @wait
        @index.promote(new_index.name, update_refresh_interval: !@refresh_interval.nil?)
        restore_replicas(new_index)
      end
      @index.clean_indices unless @retain
      log "SUCCESS!" if @mode == :async && @wait

      {index_name: new_index.name, incomplete_tenants: remaining, promoted: true}
    end

    # imports a single tenant into the CURRENTLY promoted index (no new
    # index, no promotion) — for backfilling a tenant onboarded after a
    # full reindex already ran, without needing to redo the whole thing.
    def self.reindex_tenant(model, tenant, mode: :inline, scope: nil)
      index = model.searchkick_index
      model.searchkick_tenant_scope(tenant) do |relation|
        index.import_scope(relation, mode: mode, full: false, scope: scope)
      end
    end

    # public (not private): TenantImportJob calls both of these directly —
    # each background job reconstructs a plain TenantReindexer(mode: :async,
    # job_options:, scope:) for the one tenant it's responsible for and
    # drives it through the exact same retry/around_reindex_read logic
    # `call`'s own (mode: :inline) loop uses, rather than duplicating it.
    def import_tenant(new_index, tenant)
      attempts = 0
      begin
        attempts += 1
        # around_reindex_read wraps the OUTSIDE of searchkick_tenant_scope,
        # not the inside: for schema-based (Apartment-style) tenancy, the
        # tenant switch sets search_path on whatever connection is checked
        # out at that moment. If a read-replica role were selected *after*
        # the switch, it'd select a different connection that never got the
        # switch applied — silently reading the wrong tenant's schema (or
        # none). Selecting the role first means the switch lands on the
        # connection that role actually resolves to.
        Searchkick::MultiTenant.config.around_reindex_read.call do
          @model.searchkick_tenant_scope(tenant) do |relation|
            new_index.import_scope(relation, mode: @mode, full: true, job_options: @job_options, scope: @scope, tenant: tenant)
          end
        end
      rescue => e
        retry if attempts < RETRIES
        raise e
      end
    end

    def remove_from_checkpoint(tenants_key, tenant)
      Searchkick.with_redis { |r| r.call("SREM", tenants_key, tenant) }
    end

    private

    def resolve_index
      if @resume
        index_name = @index.all_indices.sort.last
        raise Error, "No index to resume" unless index_name

        Searchkick::Index.new(index_name, @model.searchkick_options)
      else
        @index.clean_indices unless @retain
        index_options = @model.searchkick_index_options
        index_options.deep_merge!(settings: {index: {refresh_interval: @refresh_interval}}) if @refresh_interval
        index_options.deep_merge!(settings: {index: {number_of_replicas: @replicas}}) unless @replicas.nil?
        @index.create_index(index_options: index_options)
      end
    end

    # stock Searchkick's promote only knows how to restore refresh_interval
    # (update_refresh_interval:) — no equivalent exists for replicas, so
    # this reads the model's actually-configured value straight off @index
    # (the model's live, unmodified searchkick_options — same source stock
    # Searchkick's own refresh_interval restore reads from) the same way,
    # falling back to 1 (Elasticsearch/OpenSearch's own out-of-the-box
    # default) when the model doesn't configure one explicitly, mirroring
    # that same "1s" fallback precedent for refresh_interval.
    def restore_replicas(index)
      return if @replicas.nil?

      configured = @index.options.dig(:settings, :index, :number_of_replicas) || 1
      index.update_settings(index: {number_of_replicas: configured})
    end

    def seed_checkpoint(tenants_key)
      tenants = []
      Searchkick::MultiTenant.each_tenant { |tenant| tenants << tenant }
      Searchkick.with_redis { |r| r.call("SADD", tenants_key, tenants) } if tenants.any?
    end

    def pending_tenants(tenants_key)
      Searchkick.with_redis { |r| r.call("SMEMBERS", tenants_key) } || []
    end

    def enqueue_pending_tenants(new_index, tenants_key)
      pending_tenants(tenants_key).each do |tenant|
        TenantImportJob.set(**(@job_options || {})).perform_later(
          class_name: @model.name,
          tenant: tenant,
          new_index_name: new_index.name,
          tenants_key: tenants_key,
          mode: @mode,
          job_options: @job_options,
          scope: @scope
        )
      end
    end

    def wait_for_tenants(tenants_key)
      loop do
        remaining = pending_tenants(tenants_key)
        break if remaining.empty?
        log "Tenants left: #{remaining.size}"
        sleep 3
      end
    end

    def wait_for_batches(new_index)
      loop do
        status = Searchkick.reindex_status(new_index.name)
        break if status[:completed]
        log "Batches left: #{status[:batches_left]}"
        sleep 3
      end
    end

    # plain `puts` is not enough here: this runs inside a long-lived
    # background job, and Ruby's stdout is block-buffered (not
    # line-buffered) whenever it isn't a TTY — the normal case for a
    # worker process whose output is redirected to a file or piped to a
    # log collector. Without an explicit flush, these lines can sit in an
    # internal buffer for the entire multi-hour duration of a large
    # reindex — or be lost outright if the worker restarts — even though
    # the reindex itself is progressing correctly the whole time.
    def log(message)
      puts message
      $stdout.flush
    end
  end
end
