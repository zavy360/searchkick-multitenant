require_relative "test_helper"

class MultitenantTest < Minitest::Test
  def setup
    as_tenant("acme") { Product.create!(id: 1, name: "Acme Widget") }
    as_tenant("globex") { Product.create!(id: 1, name: "Globex Widget") }
    Product.searchkick_index.refresh
  end

  def teardown
    as_tenant("acme") { Product.delete_all }
    as_tenant("globex") { Product.delete_all }
  end

  def test_composite_id_avoids_collision
    doc_ids = Searchkick.client.search(index: Product.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    assert_equal ["acme::1", "globex::1"], doc_ids
  end

  def test_tenant_scoped_search_returns_only_own_records
    as_tenant("acme") do
      results = Product.search("widget", load: false)
      assert_equal ["Acme Widget"], results.map { |r| r["name"] }
    end

    as_tenant("globex") do
      results = Product.search("widget", load: false)
      assert_equal ["Globex Widget"], results.map { |r| r["name"] }
    end
  end

  def test_tenant_scoped_search_hydrates_correct_ar_record
    as_tenant("acme") do
      records = Product.search("widget").to_a
      assert_equal 1, records.size
      assert_equal "Acme Widget", records.first.name
      assert_equal 1, records.first.id
    end

    as_tenant("globex") do
      records = Product.search("widget").to_a
      assert_equal 1, records.size
      assert_equal "Globex Widget", records.first.name
    end
  end

  def test_cross_tenant_search_is_blocked_by_default
    as_tenant("acme") do
      refute_includes Product.search("globex", load: false).map { |r| r["name"] }, "Globex Widget"
    end
  end

  def test_without_tenant_scope_escape_hatch_sees_everything
    as_tenant("acme") do
      names = Searchkick.without_tenant_scope { Searchkick.search("widget", model: Product, load: false) }.map { |r| r["name"] }
      assert_equal ["Acme Widget", "Globex Widget"], names.sort
    end
  end
end
