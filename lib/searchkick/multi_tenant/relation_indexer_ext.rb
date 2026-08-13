module Searchkick
  module MultiTenant
    # RelationIndexer#full_reindex_async numbers each batch starting from 1
    # independently on every call — harmless for a single-tenant reindex,
    # but TenantReindexer calls it once PER TENANT against the SAME shared
    # index, so two tenants' "batch 1" collide as the exact same member in
    # Searchkick's own batches_key Redis set (keyed only by index name, by
    # design — so completion aggregates across every tenant for free).
    # Whichever tenant's batch completes first removes that shared member,
    # making `Searchkick.reindex_status`/`batches_left` report done while
    # the other tenant's same-numbered batch is still running — silently
    # under-counting outstanding work, so `wait: true` (or anything else
    # polling reindex_status) can return before every tenant's data has
    # actually landed.
    #
    # Fix: qualify batch_id with whichever tenant this call is for, passed
    # explicitly as a `tenant:` keyword argument (from TenantReindexer, all
    # the way down through Index#import_scope's **options passthrough) and
    # stashed as a plain instance variable on `self` — not Thread.current.
    # `Index#relation_indexer` memoizes one RelationIndexer per Index
    # object, reused across every tenant's `import_scope` call in
    # TenantReindexer's sequential (never concurrent) per-tenant loop, so
    # `@multitenant_tenant` set at the top of one `reindex` call is exactly
    # as safe as `full_reindex_async`'s own local `batch_id` variable: it's
    # read later in the same synchronous call chain, on the same object,
    # before the next tenant's call ever touches it again.
    module RelationIndexerTenantExt
      def reindex(relation, mode:, method_name: nil, ignore_missing: nil, full: false, resume: false, scope: nil, job_options: nil, tenant: nil)
        @multitenant_tenant = tenant
        super(relation, mode: mode, method_name: method_name, ignore_missing: ignore_missing, full: full, resume: resume, scope: scope, job_options: job_options)
      ensure
        @multitenant_tenant = nil
      end

      def batch_job(class_name, batch_id, job_options, **options)
        batch_id = "#{@multitenant_tenant}::#{batch_id}" if @multitenant_tenant
        super(class_name, batch_id, job_options, **options)
      end
    end
  end
end

Searchkick::RelationIndexer.prepend(Searchkick::MultiTenant::RelationIndexerTenantExt)
