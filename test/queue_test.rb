require_relative "test_helper"

class QueueTest < Minitest::Test
  def setup
    Product.searchkick_index.reindex_queue.clear

    @acme = as_tenant("acme") { Product.create!(id: 1, name: "Acme Widget") }
    @globex = as_tenant("globex") { Product.create!(id: 1, name: "Globex Widget") }
    Product.searchkick_index.refresh
  end

  def teardown
    as_tenant("acme") { Product.delete_all }
    as_tenant("globex") { Product.delete_all }
    Product.searchkick_index.reindex_queue.clear
  end

  def test_mixed_tenant_batch_is_split_and_scoped_per_tenant
    # simulate two tenants' records landing in the same shared Redis queue
    # (wrapped in a valid tenant context purely so AR can resolve Product's
    # attribute methods against *some* real table — both tenants' tables
    # share the same columns, so which one is active here doesn't matter)
    as_tenant("acme") { Product.searchkick_index.reindex_queue.push_records([@acme, @globex]) }

    # rename so a naive (non-tenant-aware) DB fetch would find the wrong
    # row if tenant splitting/switching weren't happening
    as_tenant("acme") { Product.find(1).update_column(:name, "Acme Widget Renamed") }
    Product.searchkick_index.refresh

    Searchkick::ProcessQueueJob.new.perform(class_name: "Product", index_name: Product.searchkick_index.name, inline: true)
    Product.searchkick_index.refresh

    doc = Searchkick.client.get(index: Product.searchkick_index.name, id: "acme::1")
    assert_equal "Acme Widget Renamed", doc["_source"]["name"]

    doc = Searchkick.client.get(index: Product.searchkick_index.name, id: "globex::1")
    assert_equal "Globex Widget", doc["_source"]["name"]
  end
end
