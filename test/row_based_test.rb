require_relative "row_based_test_helper"

class RowBasedTest < Minitest::Test
  def setup
    Ticket.create!(name: "Acme Login Issue", account_id: "acme")
    Ticket.create!(name: "Globex Login Issue", account_id: "globex")
    Ticket.searchkick_index.refresh
  end

  def teardown
    Ticket.destroy_all # not delete_all — that skips after_commit callbacks and would leave stale ES docs behind
    Ticket.searchkick_index.refresh
  end

  def test_composite_id_includes_tenant_even_without_collision
    doc_ids = Searchkick.client.search(index: Ticket.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    assert_equal 2, doc_ids.size
    assert doc_ids.any? { |id| id.match?(/\Aacme::\d+\z/) }
    assert doc_ids.any? { |id| id.match?(/\Aglobex::\d+\z/) }
  end

  def test_tenant_scope_filters_by_row_column
    as_tenant("acme") do
      results = Ticket.search("login", load: false)
      assert_equal ["Acme Login Issue"], results.map { |r| r["name"] }
    end
  end

  def test_tenant_scope_hydrates_correct_record
    as_tenant("globex") do
      records = Ticket.search("login").to_a
      assert_equal ["Globex Login Issue"], records.map(&:name)
    end
  end

  def test_without_tenant_scope_sees_both
    as_tenant("acme") do
      names = Searchkick.without_tenant_scope { Searchkick.search("login", model: Ticket, load: false) }.map { |r| r["name"] }
      assert_equal ["Acme Login Issue", "Globex Login Issue"], names.sort
    end
  end
end
