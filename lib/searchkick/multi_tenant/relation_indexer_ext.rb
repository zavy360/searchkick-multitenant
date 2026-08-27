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
      # Fix: min_id/max_id windows still work fine — they're a compact way
      # to describe a batch — as long as they're derived from what this
      # TENANT'S rows actually are, not assumed from arithmetic on the
      # whole table's id range. find_in_batches already walks the (tenant-
      # scoped) relation correctly — it's what Rails itself uses for batch
      # processing, ordered by primary key by default, no manual ordering
      # needed here. For each real chunk it yields, min_id/max_id are just
      # that chunk's own smallest/largest id — since those ids come from
      # THIS tenant's actual rows, `tenant_scope AND id BETWEEN` matches
      # that chunk exactly, no other row of this tenant falls in that span.
      #
      # Also handles non-numeric (e.g. UUID) primary keys — stock
      # Searchkick's own min/max branch requires Numeric ids because it
      # does arithmetic ((max-min)/batch_size); comparing real boundary
      # values with BETWEEN needs no arithmetic, just orderability, which
      # every primary key type has.
      #
      # find_in_batches itself carries this caveat: a row inserted after a
      # chunk's boundary is captured won't retroactively be picked up by
      # this run, same as Rails' own batch processing. Closed for free for
      # the LAST batch specifically: it's marked last: true (BulkReindexJobExt
      # then treats its range as open-ended, min_id and up), so it picks up
      # whatever's actually in the tenant's scope by the time it executes,
      # snapshot-inserts included.
      #
      # find_in_batches doesn't tell you in advance whether the chunk it
      # just handed you is the last one — a chunk's size only proves "last"
      # when it's smaller than batch_size; an exact-multiple total makes
      # the true last chunk indistinguishable from a middle one by size
      # alone. So: hold each batch back one iteration before dispatching
      # it. By the time we dispatch batch N, we already know whether batch
      # N+1 exists (we're either mid-loop, or the loop just ended) — no
      # ambiguity, no extra queries, just one small pending batch (a few
      # ids, not real records) held in memory at a time.
      def full_reindex_async(relation, job_options: nil)
        return super unless @multitenant_tenant

        class_name = relation.searchkick_options[:class_name]
        batch_id = 1
        pending_batch = nil

        relation.find_in_batches(batch_size: batch_size) do |batch|
          dispatch_pending_batch(class_name, job_options, pending_batch, last: false) if pending_batch
          # first/last, not ids.min/ids.max: find_in_batches already yields
          # this chunk in the exact order Postgres's own ORDER BY primary_key
          # produced, so these ARE the DB's min/max already. Recomputing via
          # Ruby's own comparison isn't guaranteed to agree with Postgres's
          # for non-integer keys (collation-dependent for text/uuid columns,
          # and case-sensitive in a way that doesn't track UUID magnitude).
          pending_batch = {batch_id: batch_id, min_id: batch.first.id, max_id: batch.last.id}
          batch_id += 1
        end
        dispatch_pending_batch(class_name, job_options, pending_batch, last: true) if pending_batch
      end

      def dispatch_pending_batch(class_name, job_options, pending_batch, last:)
        batch_job(class_name, pending_batch[:batch_id], job_options, min_id: pending_batch[:min_id], max_id: pending_batch[:max_id], last: last)
      end
    end
  end
end

Searchkick::RelationIndexer.prepend(Searchkick::MultiTenant::RelationIndexerTenantExt)
