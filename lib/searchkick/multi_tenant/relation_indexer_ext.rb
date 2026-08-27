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
        if @multitenant_tenant
          batch_id = "#{@multitenant_tenant}::#{batch_id}"
          # explicit, not ambient: this loop never touches "current tenant"
          # state for row-based tenancy, so BulkReindexJobExt can't rely on
          # CapturesTenantAtEnqueue's ambient capture here — see async_job_ext.rb
          options = options.merge(multitenant_tenant: @multitenant_tenant)
        end
        super(class_name, batch_id, job_options, **options)
      end

      # RelationIndexer#full_reindex_async's numeric-primary-key branch
      # computes batch COUNT from (max_id - min_id) / batch_size — assuming
      # ids in that range are packed densely. True for schema-based tenancy
      # (each tenant has its own table, its own tight id sequence). False
      # for row-based tenancy on a shared/global table: every tenant draws
      # from the SAME id sequence, so one tenant's min/max id can span
      # nearly the entire table even though that tenant owns a tiny
      # fraction of the rows in between. The result: massively inflated
      # batch counts (seen in production: ~35x), where most batches carry
      # a min_id..max_id window that belongs mostly to OTHER tenants and
      # matches few or zero of this tenant's own rows once
      # searchkick_tenant_scope's WHERE is applied.
      #
      # First fix attempt walked actual rows (find_in_batches, record_ids:
      # per batch) — correct, but turned batch enumeration into O(row
      # count) synchronous work PER TENANT, so TenantReindexer's per-tenant
      # loop went from "all tenants' batches queued almost instantly" (the
      # old min/max cost) to "tenant B doesn't even start being enumerated
      # until tenant A's entire row set has been walked" — trading a
      # correctness bug for a concurrency regression.
      #
      # This is the actual fix: COUNT is the same O(1) query cost as the
      # old MIN/MAX, but the batch count it produces is correct regardless
      # of id density. Each batch then carries offset:/limit: instead of a
      # min_id/max_id window or a pre-enumerated id list — BulkReindexJobExt
      # applies the tenant scope + offset/limit itself when the job actually
      # runs, so no row enumeration happens here at all.
      #
      # offset/limit is re-evaluated against ORDER BY id at execution time
      # rather than pinned like min_id/max_id or record_ids, but this is
      # still safe against concurrent writes during the reindex: a delete
      # shifts every later row's *position* down by one uniformly, and
      # since these windows partition by position (not id value), the
      # union of fixed-size windows computed from the original count still
      # covers every surviving row exactly once — no duplicates, no skips,
      # the last batch (or few) just does less work or nothing.
      #
      # The one case that isn't automatically covered: a row inserted after
      # this COUNT snapshot lands past whatever the snapshot covered, so a
      # *fixed*-size last batch wouldn't reach it. Solved by not fixing the
      # last batch's size at all — offset:, no limit:, so it just takes
      # whatever remains in the scope from its offset onward at execution
      # time, snapshot-inserts included.
      def full_reindex_async(relation, job_options: nil)
        return super unless @multitenant_tenant

        class_name = relation.searchkick_options[:class_name]
        count = relation.count
        return if count.zero?

        batches_count = (count / batch_size.to_f).ceil
        batches_count.times do |i|
          last_batch = i == batches_count - 1
          batch_job(class_name, i + 1, job_options, offset: i * batch_size, limit: last_batch ? nil : batch_size)
        end
      end
    end
  end
end

Searchkick::RelationIndexer.prepend(Searchkick::MultiTenant::RelationIndexerTenantExt)
