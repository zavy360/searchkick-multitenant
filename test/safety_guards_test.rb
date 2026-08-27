require_relative "test_helper"

class SafetyGuardsTest < Minitest::Test
  def setup
    as_tenant("acme") { Product.create!(id: 1, name: "Acme Widget") }
    as_tenant("globex") { Product.create!(id: 1, name: "Globex Widget") }
    Product.searchkick_index.refresh
  end

  def teardown
    as_tenant("acme") { Product.delete_all }
    as_tenant("globex") { Product.delete_all }
  end

  def test_bare_reindex_covers_every_tenant_and_promotes
    as_tenant("acme") do
      result = Product.reindex
      assert_empty result[:incomplete_tenants]
    end
    Product.searchkick_index.refresh

    doc_ids = Searchkick.client.search(index: Product.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    assert_equal ["acme::1", "globex::1"], doc_ids, "Model.reindex must cover every tenant, not just the one ambient at call time"
  end

  def test_bare_reindex_defaults_refresh_interval_to_30s
    captured = nil
    original = Searchkick::MultiTenant::TenantReindexer.method(:call)
    Searchkick::MultiTenant::TenantReindexer.define_singleton_method(:call) do |model, **opts|
      captured = opts
      {index_name: model.searchkick_index.name, incomplete_tenants: [], promoted: true}
    end
    begin
      Product.reindex
    ensure
      Searchkick::MultiTenant::TenantReindexer.define_singleton_method(:call, original)
    end

    assert_equal Searchkick::MultiTenant::TenantReindexer::DEFAULT_REFRESH_INTERVAL, captured[:refresh_interval]
    assert_equal Searchkick::MultiTenant::TenantReindexer::DEFAULT_REPLICAS_DURING_REINDEX, captured[:replicas]
  end

  # regression check: FullReindexGuard#full_reindex's disabled-model branch
  # explicitly lists which keywords it forwards to stock Searchkick's
  # full_reindex rather than using a bare `super` — stock has no replicas:
  # keyword at all, so a bare super forwarding it would raise
  # "ArgumentError: unknown keyword: :replicas" for every disabled-model
  # reindex the moment replicas: got a real default value.
  def test_bare_reindex_does_not_raise_when_globally_disabled
    Searchkick::MultiTenant.configure { |c| c.enabled = false }

    as_tenant("acme") { Product.reindex }
  ensure
    Searchkick::MultiTenant.configure { |c| c.enabled = true }
    # stock full_reindex (the path just exercised) promotes a single-tenant,
    # non-composite-id index over the shared alias — put it back the way
    # every other test in this file expects to find it
    Searchkick::MultiTenant::TenantReindexer.call(Product)
  end

  def test_reindex_with_unmappable_option_raises
    assert_raises(Searchkick::MultiTenant::Error) { Product.reindex(scope: :some_scope) }
  end

  def test_rewhere_reapplies_tenant_scope
    as_tenant("acme") do
      names = Product.search("*", load: false).rewhere(name: "Globex Widget").map { |r| r["name"] }
      assert_empty names, "rewhere must not be able to search into another tenant's data"
    end
  end

  def test_except_where_reapplies_tenant_scope
    as_tenant("acme") do
      names = Product.search("*", load: false).except(:where).map { |r| r["name"] }
      assert_equal ["Acme Widget"], names, "except(:where) must not drop tenant scoping"
    end
  end

  def test_suggest_option_is_rejected
    as_tenant("acme") do
      assert_raises(Searchkick::MultiTenant::Error) { Product.search("widget", suggest: true, load: false) }
    end
  end

  def test_aggregating_on_tenant_field_is_rejected
    as_tenant("acme") do
      assert_raises(Searchkick::MultiTenant::Error) { Product.search("*", aggs: [:tenant], load: false) }
    end
  end

  def test_aggregating_on_other_field_is_unaffected
    as_tenant("acme") do
      results = Product.search("*", aggs: [:name], load: false)
      assert results.aggs.key?("name")
    end
  end
end
