module Searchkick::MultiTenant
  class Configuration
    attr_accessor :current_tenant, :each_tenant, :routing, :batch_job_timeout, :batch_job_max_attempts, :use_reader_replica_for_reindex, :enabled
    attr_writer :around_reindex_read

    def initialize
      @current_tenant = -> { raise Error, "Searchkick::MultiTenant.configure { |c| c.current_tenant = -> { ... } } is not set" }
      @each_tenant = ->(&block) { raise Error, "Searchkick::MultiTenant.configure { |c| c.each_tenant = ->(&block) { ... } } is not set" }
      @routing = false
      # false makes every patch a no-op — searchkick_multitenant can be
      # called on models and deployed with zero behavior change, then
      # flipped on later without a code deploy (e.g. from an ENV var or
      # feature flag) once you're ready to actually migrate.
      @enabled = true
      @batch_job_timeout = 300 # seconds a single BulkReindexJob attempt may run before it's timed out
      @batch_job_max_attempts = 5 # attempts before we give up on a batch and unblock batches_left
      # set true to auto-read from a configured :reading role during full
      # reindexing (see around_reindex_read below) — falls back to the
      # primary if no reading role is actually registered, so it's safe to
      # leave on even where one isn't configured.
      @use_reader_replica_for_reindex = false
    end

    # Wraps ONLY TenantReindexer's full-reindex reads (tenant_reindexer.rb's
    # import_tenant, and the BulkReindexJob batches it spawns for :async
    # mode) — never single-record callback reads (ReindexV2Job), where
    # replica lag could make a just-saved record look deleted, and never
    # plain inline/:queue reindexing outside a full reindex.
    #
    # Defaults to auto-detecting a configured :reading role
    # (ActiveRecord::Base.connects_to database: { reading: ... }) at call
    # time, gated behind use_reader_replica_for_reindex, and reading from it
    # when present. Assign this directly for full control (a differently
    # named role, custom logic) — an explicit assignment always wins over
    # the auto-detecting default.
    #
    # The wrap sits OUTSIDE searchkick_tenant_scope at both call sites, not
    # inside: for schema-based (Apartment-style) tenancy the tenant switch
    # applies (e.g. `SET search_path`) to whatever connection is checked
    # out at that moment, so the replica role must be selected first, or
    # the switch lands on the writer connection instead of the reader one.
    def around_reindex_read
      @around_reindex_read ||= ->(&block) {
        if use_reader_replica_for_reindex && Searchkick::MultiTenant.reader_replica_configured?
          ActiveRecord::Base.connected_to(role: :reading, &block)
        else
          block.call
        end
      }
    end
  end
end
