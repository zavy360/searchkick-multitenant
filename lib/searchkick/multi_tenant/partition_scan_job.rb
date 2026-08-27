module Searchkick
  module MultiTenant
    # For row-based tenancy on a shared table: TenantImportJob's per-tenant
    # `WHERE tenant_column = ? ORDER BY id` query needs a composite
    # (tenant_column, id) index to run efficiently. Without one, Postgres
    # re-filters and re-sorts that tenant's entire remaining matching row
    # set on every page — keyset's `id > last_seen` doesn't let it skip
    # that without an index already ordered within the tenant filter, so
    # it's the same quadratic-ish cost offset/limit had, just less obvious.
    # Confirmed in production: a single page query took ~1 minute on a
    # table with no such composite index available (and none could be
    # added).
    #
    # This sidesteps the problem instead of trying to make that query
    # faster: never filter by tenant at the DB level at all. Walk a slice
    # of the table in plain `id` order — needs only the primary key index,
    # always available, never needs a composite — and bucket each row into
    # its own tenant's batch in Ruby as it streams by, flushing a
    # BulkReindexJob once a tenant's bucket reaches batch_size.
    #
    # TenantReindexer(global_scan: true) dispatches a small, FIXED number
    # of these (one per id-range partition) instead of one per tenant —
    # unlike TenantImportJob, worker-pool saturation from enumeration alone
    # no longer scales with tenant count.
    #
    # Trade-off versus TenantImportJob's per-tenant scan: a tenant's rows
    # can span multiple partitions, so there's no single well-defined
    # "last batch" to mark open-ended the way relation_indexer_ext.rb's
    # find_in_batches-per-tenant path does — a row inserted after this
    # partition's scan passed it isn't retroactively picked up by this
    # run, same limitation Rails' own find_in_batches documents.
    class PartitionScanJob < Searchkick.parent_job.constantize
      queue_as { Searchkick.queue_name }

      def perform(class_name:, new_index_name:, partitions_key:, partition_id:, min_id:, max_id:, job_options: nil)
        model = Searchkick.load_model(class_name)
        new_index = Searchkick::Index.new(new_index_name, model.searchkick_options)
        primary_key = model.primary_key
        batch_size = new_index.send(:relation_indexer).send(:batch_size)

        buckets = Hash.new { |h, k| h[k] = [] }
        batch_numbers = Hash.new(0)

        Searchkick::MultiTenant.config.around_reindex_read.call do
          model.all.where(primary_key => min_id..max_id).reorder(primary_key).find_in_batches(batch_size: 1000) do |chunk|
            chunk.each do |record|
              tenant = record.searchkick_tenant
              bucket = buckets[tenant]
              bucket << record.id
              next if bucket.size < batch_size

              batch_numbers[tenant] += 1
              enqueue_batch(class_name, new_index, tenant, batch_numbers[tenant], bucket.dup, job_options)
              bucket.clear
            end
          end
        end

        buckets.each do |tenant, bucket|
          next if bucket.empty?

          batch_numbers[tenant] += 1
          enqueue_batch(class_name, new_index, tenant, batch_numbers[tenant], bucket, job_options)
        end

        Searchkick.with_redis { |r| r.call("SREM", partitions_key, partition_id) }
      end

      private

      # min_id/max_id are the bucket's first/last accumulated id, not
      # Ruby's own .min/.max — the bucket fills in the same order the
      # (id-ordered) outer scan encounters rows, so it's already sorted;
      # recomputing via Ruby's own comparison isn't guaranteed to agree
      # with Postgres's for non-integer keys (collation-dependent for
      # text/uuid columns).
      def enqueue_batch(class_name, index, tenant, batch_number, ids, job_options)
        batch_id = "#{tenant}::#{batch_number}"
        Searchkick.with_redis { |r| r.call("SADD", "searchkick:reindex:#{index.name}:batches", [batch_id]) }
        Searchkick::BulkReindexJob.set(**(job_options || {})).perform_later(
          class_name: class_name,
          index_name: index.name,
          batch_id: batch_id,
          min_id: ids.first,
          max_id: ids.last,
          multitenant_tenant: tenant
        )
      end
    end
  end
end
