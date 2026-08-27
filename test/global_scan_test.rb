require_relative "row_based_test_helper"
require "active_job/test_helper"

# global_scan: true replaces per-tenant TenantImportJobs with a small,
# fixed number of PartitionScanJobs that walk id ranges of the WHOLE table
# (no tenant filter, so no composite index needed) and bucket rows by
# tenant in Ruby. See PartitionScanJob's own comment for the full "why".
class GlobalScanTest < Minitest::Test
  include ActiveJob::TestHelper

  def teardown
    Ticket.destroy_all
    Ticket.searchkick_index.refresh
  end

  def with_stubbed_batch_size(value)
    original = Searchkick::RelationIndexer.instance_method(:batch_size)
    Searchkick::RelationIndexer.send(:define_method, :batch_size) { value }
    yield
  ensure
    Searchkick::RelationIndexer.send(:define_method, :batch_size, original)
  end

  def test_global_scan_correctly_buckets_interleaved_tenants_by_id_range
    clear_enqueued_jobs
    expected = {"acme" => [], "globex" => []}
    6.times do |i|
      tenant = i.even? ? "acme" : "globex"
      as_tenant(tenant) do
        ticket = Ticket.create!(name: "#{tenant}-#{i}", account_id: tenant)
        expected[tenant] << ticket.id
      end
    end

    with_stubbed_batch_size(2) do
      Searchkick::MultiTenant::TenantReindexer.call(Ticket, mode: :async, global_scan: true, partitions: 3)
      perform_enqueued_jobs # runs PartitionScanJobs -> enqueues BulkReindexJobs
      perform_enqueued_jobs # runs the BulkReindexJobs themselves
    end

    Ticket.searchkick_index.refresh
    doc_ids = Searchkick.client.search(index: Ticket.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    expected_ids = (expected["acme"].map { |id| "acme::#{id}" } + expected["globex"].map { |id| "globex::#{id}" }).sort
    assert_equal expected_ids, doc_ids
  end

  def test_global_scan_requires_numeric_primary_key
    as_tenant("acme") { Ticket.create!(name: "not-a-number-id", account_id: "acme") }

    # stub AFTER creating the row — create! itself relies on AR's real
    # notion of the primary key, so only the seed_partitions call below
    # should see the fake (non-numeric) one
    original_pk = Ticket.method(:primary_key)
    Ticket.define_singleton_method(:primary_key) { "name" } # a real, non-numeric column

    error = assert_raises(Searchkick::MultiTenant::Error) do
      Searchkick::MultiTenant::TenantReindexer.call(Ticket, mode: :async, global_scan: true)
    end
    assert_match(/numeric primary key/, error.message)
  ensure
    Ticket.define_singleton_method(:primary_key, original_pk) if original_pk
  end

  def test_partial_reindex_error_uses_partition_wording_for_global_scan
    error = Searchkick::MultiTenant::PartialReindexError.new(["0:1:500", "1:501:1000"], unit: "partition")
    assert_match(/2 partition\(s\) did not complete/, error.message)
    assert_equal ["0:1:500", "1:501:1000"], error.failed_tenants
  end

  # resume: true must re-parse whatever partition ranges are still in the
  # checkpoint set from the ORIGINAL seed, not recompute new ones — new
  # rows added between the failed run and the resume must not shift a
  # retried partition's boundaries.
  def test_resume_reuses_exact_partition_ranges_instead_of_recomputing_them
    as_tenant("acme") { Ticket.create!(name: "A1", account_id: "acme") }
    index = Ticket.searchkick_index
    checkpoint_key = "searchkick:multitenant:reindex:#{index.name}:partitions"
    Searchkick.with_redis { |r| r.call("SADD", checkpoint_key, ["0:999:999999"]) } # deliberately not the real id span

    reindexer = Searchkick::MultiTenant::TenantReindexer.new(Ticket, mode: :async, global_scan: true, resume: true)
    reindexer.send(:enqueue_partitions, index, checkpoint_key)

    job = enqueued_jobs.find { |j| j[:job] == Searchkick::MultiTenant::PartitionScanJob }
    assert_equal 999, job[:args].first["min_id"]
    assert_equal 999999, job[:args].first["max_id"]
  ensure
    Searchkick.with_redis { |r| r.call("DEL", checkpoint_key) } if checkpoint_key
  end
end
