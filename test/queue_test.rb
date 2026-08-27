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

  # `Searchkick.callbacks(:queue)` forces the real after_commit-triggered
  # `reindex` (not a manual push_records call) through the :queue path, so
  # this proves callbacks: :queue is actually wired end to end, not just
  # that ReindexQueue/ProcessQueueJob work in isolation.
  def test_callbacks_queue_mode_pushes_on_save_and_processes_per_tenant
    Searchkick.callbacks(:queue) do
      as_tenant("acme") { Product.create!(id: 2, name: "Acme Gadget") }
      as_tenant("globex") { Product.create!(id: 2, name: "Globex Gadget") }
    end

    assert_equal 2, Product.searchkick_index.reindex_queue.length

    Searchkick::ProcessQueueJob.new.perform(class_name: "Product", index_name: Product.searchkick_index.name, inline: true)
    Product.searchkick_index.refresh

    doc = Searchkick.client.get(index: Product.searchkick_index.name, id: "acme::2")
    assert_equal "Acme Gadget", doc["_source"]["name"]

    doc = Searchkick.client.get(index: Product.searchkick_index.name, id: "globex::2")
    assert_equal "Globex Gadget", doc["_source"]["name"]
  end

  # the queue mode analogue of async_job_test's destroy scenario: a missing
  # record makes RecordIndexer construct a fake record just to compute the
  # delete's composite id, so this proves ProcessBatchJobExt's per-item
  # tenant (captured at push time, not ambient at process time) keeps that
  # delete scoped to the right tenant's document only.
  def test_callbacks_queue_mode_destroy_only_removes_correct_tenants_document
    as_tenant("acme") { Product.create!(id: 3, name: "Acme Temp") }
    as_tenant("globex") { Product.create!(id: 3, name: "Globex Temp") }
    Product.searchkick_index.refresh

    Searchkick.callbacks(:queue) { as_tenant("acme") { Product.find(3).destroy } }
    Searchkick::ProcessQueueJob.new.perform(class_name: "Product", index_name: Product.searchkick_index.name, inline: true)
    Product.searchkick_index.refresh

    refute Searchkick.client.exists(index: Product.searchkick_index.name, id: "acme::3")
    assert Searchkick.client.exists(index: Product.searchkick_index.name, id: "globex::3"), "globex's same-id record must survive acme's queue-mode destroy"
  end
end
