module Searchkick
  module MultiTenant
    # ReindexV2Job/BulkReindexJob (the `callbacks: :async` path, and
    # TenantReindexer's own `mode: :async`) are shared, tenant-agnostic
    # Searchkick job classes. Unlike :queue mode (queue_ext.rb), nothing in
    # their payload carries a tenant identifier by default, and that's a
    # real correctness gap, not just a missing nicety:
    #
    # 1. Schema-based tenancy: a job can execute with the wrong (or no)
    #    schema active in a reused worker thread. If the target record is
    #    "missing" under that wrong schema, RecordIndexer#reindex_items
    #    treats it as deleted and constructs a *fake* record just to
    #    compute its composite id for the delete call
    #    (RecordIndexer#construct_record — id set, nothing else). That fake
    #    record's `searchkick_tenant` then resolves to whatever tenant
    #    happens to be ambient — not the tenant the job was actually about
    #    — so the delete can target and remove a DIFFERENT tenant's valid
    #    document.
    # 2. Row-based tenancy: the fake record has no tenant column value at
    #    all (it's a bare `klass.new`, not built from a `.where(tenant_column:
    #    ...)`-scoped relation), so the composite id is wrong outright and
    #    the real orphaned document is silently never cleaned up.
    #
    # Fix: capture the ambient tenant at enqueue time — `serialize` runs
    # synchronously inside `perform_later`, in whatever context actually
    # initiated the job — and thread it through `searchkick_tenant_scope`
    # instead of the bare unscoped class, so both the DB fetch AND the
    # constructed delete record resolve against the correct tenant.
    #
    # This ambient-current_tenant capture is correct for ReindexV2Job and
    # for BulkReindexJob's plain callback-triggered path (a normal app save
    # runs inside whatever tenant context the app is actually in). It is
    # NOT correct for BulkReindexJob when enqueued from TenantReindexer's
    # own per-tenant loop (relation_indexer_ext.rb's full_reindex_async
    # path): that loop never touches "current tenant" ambient state at all
    # for row-based tenancy (tenant_scope is a pure `.where(...)` filter,
    # no side effect), so ambient capture there would just grab whatever's
    # leftover from outside the loop entirely — not the tenant actually
    # being imported. For that path, relation_indexer_ext.rb passes the
    # real tenant through as an explicit `multitenant_tenant:` argument on
    # the job itself, which BulkReindexJobExt prefers over this ambient one.
    module CapturesTenantAtEnqueue
      def serialize
        tenant =
          begin
            Searchkick::MultiTenant.current_tenant
          rescue StandardError
            nil
          end
        super.merge("multitenant_tenant" => tenant)
      end

      def deserialize(job_data)
        super
        @multitenant_tenant = job_data["multitenant_tenant"]
      end
    end

    module ReindexV2JobExt
      def perform(class_name, id, method_name = nil, routing: nil, index_name: nil, ignore_missing: nil)
        model = Searchkick.load_model(class_name, allow_child: true)
        return super unless Searchkick::MultiTenant.enabled_for?(model)

        index = model.searchkick_index(name: index_name)
        items = [{id: id, routing: routing}]
        model.searchkick_tenant_scope(@multitenant_tenant) do |relation|
          Searchkick::RecordIndexer.new(index).reindex_items(relation, items, method_name: method_name, ignore_missing: ignore_missing, single: true)
        end
      end
    end

    module BulkReindexJobExt
      # multitenant_tenant: is a keyword this gem adds, not part of stock
      # Searchkick — present (non-nil) only when relation_indexer_ext.rb's
      # batch_job explicitly set it for a TenantReindexer-driven import.
      # Falls back to the ambient-captured @multitenant_tenant (from
      # CapturesTenantAtEnqueue) for the plain callback-triggered path.
      def perform(class_name:, record_ids: nil, index_name: nil, method_name: nil, batch_id: nil, min_id: nil, max_id: nil, ignore_missing: nil, multitenant_tenant: nil)
        model = Searchkick.load_model(class_name)
        unless Searchkick::MultiTenant.enabled_for?(model)
          return super(class_name: class_name, record_ids: record_ids, index_name: index_name, method_name: method_name, batch_id: batch_id, min_id: min_id, max_id: max_id, ignore_missing: ignore_missing)
        end

        index = model.searchkick_index(name: index_name)
        ids = record_ids || (min_id..max_id)
        tenant = multitenant_tenant || @multitenant_tenant

        model.searchkick_tenant_scope(tenant) do |relation|
          relation = Searchkick.load_records(relation, ids)
          relation = relation.search_import if relation.respond_to?(:search_import)
          Searchkick::RecordIndexer.new(index).reindex(relation, mode: :inline, method_name: method_name, ignore_missing: ignore_missing, full: false)
        end
        Searchkick::RelationIndexer.new(index).batch_completed(batch_id) if batch_id
      end
    end
  end
end

Searchkick::ReindexV2Job.prepend(Searchkick::MultiTenant::CapturesTenantAtEnqueue)
Searchkick::ReindexV2Job.prepend(Searchkick::MultiTenant::ReindexV2JobExt)
Searchkick::BulkReindexJob.prepend(Searchkick::MultiTenant::CapturesTenantAtEnqueue)
Searchkick::BulkReindexJob.prepend(Searchkick::MultiTenant::BulkReindexJobExt)
