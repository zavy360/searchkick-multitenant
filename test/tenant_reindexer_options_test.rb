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
end
