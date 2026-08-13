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

    def self.call(model, **options)
      new(model, **options).call
    end

    def initialize(model, mode: :inline, retain: false, job_options: nil, resume: false, force_promote_incomplete: false, scope: nil, wait: nil, refresh_interval: nil)
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
      end

      pending_tenants(tenants_key).each do |tenant|
        begin
          import_tenant(new_index, tenant)
          remove_from_checkpoint(tenants_key, tenant)
        rescue => e
          Searchkick.warn("[searchkick-multitenant] tenant #{tenant.inspect} failed to reindex into #{new_index.name}: #{e.message}")
        end
      end

      remaining = pending_tenants(tenants_key)
      raise PartialReindexError, remaining if remaining.any? && !@force_promote_incomplete

      # :async without wait: every tenant's import jobs are enqueued (they
      # all target new_index, so Searchkick's own batches_key — keyed only
      # by index name — aggregates completion across every tenant for
      # free), but they haven't necessarily run yet. Promoting here would
      # promote a possibly-empty index. Matches Searchkick's own full_reindex:
      # leave it unpromoted and let the caller finish the job later — poll
      # `Searchkick.reindex_status(new_index.name)` directly, or call this
      # again with `resume: true, wait: true` to block until done and promote.
      if @mode == :async && !@wait
        return {index_name: new_index.name, incomplete_tenants: remaining, promoted: false}
      end

      wait_for_batches(new_index) if @mode == :async && @wait

      if alias_existed
        # Index#check_uuid does exactly this, but it's a protected method —
        # inlined rather than reaching past that with `send`
        if uuid != new_index.uuid
          raise Error, "Safety check failed - only run one Model.reindex/TenantReindexer per model at a time"
        end

        @index.promote(new_index.name, update_refresh_interval: !@refresh_interval.nil?)
      end
      @index.clean_indices unless @retain

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
        @index.create_index(index_options: index_options)
      end
    end

    def seed_checkpoint(tenants_key)
      tenants = []
      Searchkick::MultiTenant.each_tenant { |tenant| tenants << tenant }
      Searchkick.with_redis { |r| r.call("SADD", tenants_key, tenants) } if tenants.any?
    end

    def pending_tenants(tenants_key)
      Searchkick.with_redis { |r| r.call("SMEMBERS", tenants_key) } || []
    end

    def remove_from_checkpoint(tenants_key, tenant)
      Searchkick.with_redis { |r| r.call("SREM", tenants_key, tenant) }
    end

    def import_tenant(new_index, tenant)
      attempts = 0
      begin
        attempts += 1
        @model.searchkick_tenant_scope(tenant) do |relation|
          new_index.import_scope(relation, mode: @mode, full: true, job_options: @job_options, scope: @scope, tenant: tenant)
        end
      rescue => e
        retry if attempts < RETRIES
        raise e
      end
    end

    def wait_for_batches(new_index)
      loop do
        sleep 3
        status = Searchkick.reindex_status(new_index.name)
        break if status[:completed]
      end
    end
  end
end
