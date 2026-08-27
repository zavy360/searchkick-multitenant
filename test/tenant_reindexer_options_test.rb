require_relative "row_based_test_helper"
require "active_job/test_helper"

class TenantReindexerOptionsTest < Minitest::Test
  include ActiveJob::TestHelper

  def teardown
    Ticket.destroy_all
    Ticket.searchkick_index.refresh
  end

  def current_alias_target
    Searchkick.client.indices.get_alias(name: Ticket.searchkick_index.name).keys.first
  end

  def test_scope_option_limits_which_records_are_reindexed
    as_tenant("acme") do
      Ticket.create!(name: "Acme Active", account_id: "acme", archived: false)
      Ticket.create!(name: "Acme Archived", account_id: "acme", archived: true)
    end
    as_tenant("globex") do
      Ticket.create!(name: "Globex Active", account_id: "globex", archived: false)
      Ticket.create!(name: "Globex Archived", account_id: "globex", archived: true)
    end

    Ticket.reindex(scope: :active)
    Ticket.searchkick_index.refresh

    names = Searchkick.client.search(index: Ticket.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_source"]["name"] }.sort
    assert_equal ["Acme Active", "Globex Active"], names
  end

  # refresh_interval: is deliberately transient — Searchkick's own promote
  # (update_refresh_interval: true) resets it back to the model's
  # steady-state setting once the index goes live, same as stock
  # Model.reindex. So the only place to observe the requested value is on
  # the new (not-yet-promoted) index itself, mid-flight.
  def test_refresh_interval_option_is_applied_to_the_new_index_before_promotion
    reindexer = Searchkick::MultiTenant::TenantReindexer.new(Ticket, refresh_interval: "30s")
    new_index = reindexer.send(:resolve_index)

    settings = Searchkick.client.indices.get_settings(index: new_index.name)
    assert_equal "30s", settings.values.first["settings"]["index"]["refresh_interval"]
  ensure
    new_index&.delete
  end

  # a full reindex bulk-imports every tenant into the new index before
  # promoting — without throttling, ES/OpenSearch refreshes on every bulk
  # write the whole time, which is the expensive part of heavy indexing.
  def test_refresh_interval_defaults_to_30s_for_a_full_reindex
    reindexer = Searchkick::MultiTenant::TenantReindexer.new(Ticket)
    new_index = reindexer.send(:resolve_index)

    settings = Searchkick.client.indices.get_settings(index: new_index.name)
    assert_equal "30s", settings.values.first["settings"]["index"]["refresh_interval"]
  ensure
    new_index&.delete
  end

  def test_refresh_interval_is_restored_to_the_configured_value_on_promotion
    Searchkick::MultiTenant::TenantReindexer.call(Ticket)

    assert_equal "1s", Ticket.searchkick_index.refresh_interval, "Ticket doesn't configure its own refresh_interval, so promotion should fall back to Searchkick's stock default, not stay at the 30s used mid-reindex"
  end

  # a full reindex bulk-writes every tenant into the new index before
  # promoting — every replica would otherwise have to independently apply
  # every one of those writes too, for no benefit until the index is live.
  def test_replicas_defaults_to_0_for_a_full_reindex
    reindexer = Searchkick::MultiTenant::TenantReindexer.new(Ticket)
    new_index = reindexer.send(:resolve_index)

    settings = Searchkick.client.indices.get_settings(index: new_index.name)
    assert_equal "0", settings.values.first["settings"]["index"]["number_of_replicas"]
  ensure
    new_index&.delete
  end

  def test_replicas_is_restored_to_the_configured_value_on_promotion
    Searchkick::MultiTenant::TenantReindexer.call(Ticket)

    settings = Searchkick.client.indices.get_settings(index: Ticket.searchkick_index.name)
    assert_equal "1", settings.values.first["settings"]["index"]["number_of_replicas"],
      "Ticket doesn't configure its own replicas, so promotion should fall back to 1 (Elasticsearch/OpenSearch's own default), not stay at the 0 used mid-reindex"
  end

  def test_async_without_wait_defers_promotion_until_jobs_actually_run
    as_tenant("acme") { Ticket.create!(name: "Acme Ticket", account_id: "acme") }
    as_tenant("globex") { Ticket.create!(name: "Globex Ticket", account_id: "globex") }

    before_index = current_alias_target

    result = Ticket.reindex(mode: :async)
    refute result[:promoted], "must not promote before the async jobs have actually imported anything"
    assert_equal before_index, current_alias_target, "old index must still be serving reads"

    # simulate the background worker actually running the enqueued jobs
    perform_enqueued_jobs

    result = Searchkick::MultiTenant::TenantReindexer.call(Ticket, resume: true, mode: :async, wait: true)
    assert result[:promoted]
    refute_equal before_index, current_alias_target, "must now point at the fully-imported index"

    Ticket.searchkick_index.refresh
    names = Searchkick.client.search(index: Ticket.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_source"]["name"] }.sort
    assert_equal ["Acme Ticket", "Globex Ticket"], names
  end

  # regression test for a real production bug: RelationIndexer#full_reindex_async
  # (stock Searchkick) derives batch COUNT from (tenant's max_id - tenant's
  # min_id) / batch_size, assuming ids in that span are packed densely.
  # True for schema-based tenancy (each tenant has its own table/id
  # sequence). False for row-based tenancy on this shared `tickets` table —
  # tenants interleave in the same id sequence, so one tenant's min/max id
  # can span nearly the whole table even though it owns few of the rows in
  # between, wildly inflating the batch count (seen in prod: ~35x).
  #
  # 6 rows interleaved acme/globex/acme/globex/acme/globex means acme's own
  # ids span 1..5 despite acme owning only 3 rows — at batch_size 2 the old
  # id-span math would derive 3 batches (ceil(5/2)) from that span; correct
  # batching (derived from acme's own actual ids: 1, 3, 5) must derive 2.
  def test_row_based_batch_count_matches_actual_rows_not_id_span
    clear_enqueued_jobs
    acme_ids = []
    as_tenant("acme") { acme_ids << Ticket.create!(name: "A1", account_id: "acme").id }
    as_tenant("globex") { Ticket.create!(name: "G1", account_id: "globex") }
    as_tenant("acme") { acme_ids << Ticket.create!(name: "A2", account_id: "acme").id }
    as_tenant("globex") { Ticket.create!(name: "G2", account_id: "globex") }
    as_tenant("acme") { acme_ids << Ticket.create!(name: "A3", account_id: "acme").id }
    as_tenant("globex") { Ticket.create!(name: "G3", account_id: "globex") }

    with_stubbed_batch_size(2) do
      Searchkick::MultiTenant::TenantReindexer.call(Ticket, mode: :async)
    end

    bulk_jobs = enqueued_jobs.select { |j| j[:job] == Searchkick::BulkReindexJob }
    assert_equal 4, bulk_jobs.size, "3 rows per tenant at batch_size 2 should be 2 batches per tenant (4 total), not batches derived from each tenant's full id span"

    acme_jobs = bulk_jobs.select { |j| j[:args].first["batch_id"].start_with?("acme::") }.sort_by { |j| j[:args].first["min_id"] }
    assert_equal [{"min_id" => acme_ids[0], "max_id" => acme_ids[1], "last" => false}, {"min_id" => acme_ids[2], "max_id" => acme_ids[2], "last" => true}],
      acme_jobs.map { |j| j[:args].first.slice("min_id", "max_id", "last") },
      "min_id/max_id must come from acme's own actual ids per find_in_batches chunk, not arithmetic on the shared table's id range — and only the true last batch is marked last: true"
  end

  # last: true (BulkReindexJobExt then queries min_id and up, ignoring
  # max_id) is what lets a tenant's last batch pick up a row inserted into
  # its scope after full_reindex_async's find_in_batches enumeration
  # reached it, instead of silently excluding anything past that batch's
  # snapshot max_id.
  def test_last_batch_picks_up_rows_inserted_after_it_was_enumerated
    clear_enqueued_jobs
    as_tenant("acme") { Ticket.create!(name: "A1", account_id: "acme") }

    with_stubbed_batch_size(2) do
      Searchkick::MultiTenant::TenantReindexer.call(Ticket, mode: :async)
    end

    # simulates a row landing in this tenant's scope after
    # full_reindex_async already enumerated (and dispatched) its batch
    as_tenant("acme") { Ticket.create!(name: "A2 (late)", account_id: "acme") }

    perform_enqueued_jobs
    Ticket.searchkick_index.refresh

    names = Searchkick.client.search(index: Ticket.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_source"]["name"] }.sort
    assert_equal ["A1", "A2 (late)"], names
  end

  private

  # RelationIndexer#batch_size is a private, per-instance-memoized method —
  # swap it on the class for the duration of the test rather than adding a
  # searchkick batch_size: config just for this one test.
  def with_stubbed_batch_size(value)
    original = Searchkick::RelationIndexer.instance_method(:batch_size)
    Searchkick::RelationIndexer.send(:define_method, :batch_size) { value }
    yield
  ensure
    Searchkick::RelationIndexer.send(:define_method, :batch_size, original)
  end
end
