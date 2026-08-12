module Searchkick::MultiTenant
  # the `:queue` reindex mode LPUSHes onto one Redis list shared by every
  # tenant (keyed only by index name) — the one queue-mode path that's
  # genuinely multi-tenant per list, so it needs a 3rd "tenant" pipe field
  # threaded through push -> reserve -> per-tenant job dispatch -> DB scope.
  module ReindexQueueExt
    def push_records(records)
      return super if records.empty? || !Searchkick::MultiTenant.enabled_for?(records.first.class)

      record_ids = records.map do |record|
        routing = record.search_routing if record.respond_to?(:search_routing)
        Searchkick::MultiTenant.build_queue_item(record.id.to_s, record.searchkick_tenant.to_s, routing)
      end

      push(record_ids)
    end
  end

  # groups a drained (potentially mixed-tenant) batch by tenant before
  # dispatching, so each ProcessBatchJob only ever carries one tenant.
  module ProcessQueueJobExt
    def perform(class_name:, index_name: nil, inline: false, job_options: nil)
      model = Searchkick.load_model(class_name)
      return super unless Searchkick::MultiTenant.enabled_for?(model)

      index = model.searchkick_index(name: index_name)
      limit = model.searchkick_options[:batch_size] || 1000
      job_options = (model.searchkick_options[:job_options] || {}).merge(job_options || {})

      loop do
        record_ids = index.reindex_queue.reserve(limit: limit)
        if record_ids.any?
          record_ids.uniq.group_by { |r| Searchkick::MultiTenant.split_queue_item(r)[:tenant] }.each do |tenant, ids|
            batch_options = {class_name: class_name, record_ids: ids, index_name: index_name}
            if inline
              Searchkick::ProcessBatchJob.new.perform(**batch_options)
            else
              Searchkick::ProcessBatchJob.set(job_options).perform_later(**batch_options)
            end
          end
        end
        break unless record_ids.size == limit
      end
    end
  end

  # a batch job is always single-tenant by construction once it's gone
  # through ProcessQueueJobExt's grouping above, so it's safe to switch into
  # that one tenant before the relation gets scoped and records loaded.
  module ProcessBatchJobExt
    def perform(class_name:, record_ids:, index_name: nil)
      model = Searchkick.load_model(class_name)
      return super unless Searchkick::MultiTenant.enabled_for?(model)

      index = model.searchkick_index(name: index_name)
      parsed = record_ids.map { |r| Searchkick::MultiTenant.split_queue_item(r) }
      tenant = parsed.map { |p| p[:tenant] }.compact.first
      items = parsed.map { |p| {id: p[:id], routing: p[:routing]} }

      model.searchkick_tenant_scope(tenant) do |relation|
        Searchkick::RecordIndexer.new(index).reindex_items(relation, items, method_name: nil, ignore_missing: nil)
      end
    end
  end
end

Searchkick::ReindexQueue.prepend(Searchkick::MultiTenant::ReindexQueueExt)
Searchkick::ProcessQueueJob.prepend(Searchkick::MultiTenant::ProcessQueueJobExt)
Searchkick::ProcessBatchJob.prepend(Searchkick::MultiTenant::ProcessBatchJobExt)
