require_relative "test_helper"

class TenantReindexerTest < Minitest::Test
  def setup
    as_tenant("acme") { Product.create!(id: 1, name: "Acme Widget") }
    as_tenant("globex") { Product.create!(id: 1, name: "Globex Widget") }
    Product.searchkick_index.refresh
    @promoted_index_name = Product.searchkick_index.all_indices.first || current_alias_target
  end

  def teardown
    Thread.current[:fail_tenant] = nil
    as_tenant("acme") { Product.delete_all }
    as_tenant("globex") { Product.delete_all }
    # clean up any stray un-promoted indices left by a failed run
    Product.searchkick_index.clean_indices
  end

  def current_alias_target
    Searchkick.client.indices.get_alias(name: Product.searchkick_index.name).keys.first
  end

  def test_does_not_promote_when_a_tenant_fails
    before = current_alias_target
    Thread.current[:fail_tenant] = "globex"

    error = assert_raises(Searchkick::MultiTenant::PartialReindexError) do
      Searchkick::MultiTenant::TenantReindexer.call(Product)
    end
    assert_equal ["globex"], error.failed_tenants

    assert_equal before, current_alias_target, "alias must still point at the old index"
  end

  def test_resume_promotes_once_the_failed_tenant_succeeds
    Thread.current[:fail_tenant] = "globex"
    assert_raises(Searchkick::MultiTenant::PartialReindexError) do
      Searchkick::MultiTenant::TenantReindexer.call(Product)
    end
    before = current_alias_target

    Thread.current[:fail_tenant] = nil
    result = Searchkick::MultiTenant::TenantReindexer.call(Product, resume: true)

    assert_empty result[:incomplete_tenants]
    refute_equal before, current_alias_target, "alias must now point at the new, fully-reindexed index"

    Product.searchkick_index.refresh
    doc_ids = Searchkick.client.search(index: Product.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    assert_equal ["acme::1", "globex::1"], doc_ids
  end

  def test_full_success_promotes_immediately
    result = Searchkick::MultiTenant::TenantReindexer.call(Product)
    assert_empty result[:incomplete_tenants]

    Product.searchkick_index.refresh
    doc_ids = Searchkick.client.search(index: Product.searchkick_index.name, body: {query: {match_all: {}}})["hits"]["hits"].map { |h| h["_id"] }.sort
    assert_equal ["acme::1", "globex::1"], doc_ids
  end

  # around_reindex_read (e.g. a read-replica role switch) must wrap OUTSIDE
  # searchkick_tenant_scope's own switch — for schema-based tenancy the
  # switch applies to whichever connection is checked out *at that moment*,
  # so selecting a different connection role from inside the switch would
  # leave that connection never having had the switch applied to it.
  def test_around_reindex_read_wraps_outside_the_tenant_switch
    order = []
    Searchkick::MultiTenant.configure do |c|
      c.around_reindex_read = ->(&block) {
        order << Thread.current[:current_tenant]
        block.call
      }
    end

    Searchkick::MultiTenant::TenantReindexer.call(Product)

    assert_equal [nil, nil], order, "hook must observe the pre-switch tenant (nil) for both tenants, never the tenant about to be switched to"
  ensure
    Searchkick::MultiTenant.configure { |c| c.around_reindex_read = ->(&block) { block.call } }
  end
end
