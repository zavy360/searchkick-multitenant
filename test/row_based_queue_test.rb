require_relative "row_based_test_helper"

# :queue mode against row-based tenancy specifically: unlike Product's
# per-tenant tables (schema-based), Ticket is one shared table with a
# `account_id` column, so ProcessBatchJobExt's `model.searchkick_tenant_scope(tenant)`
# doing `where(account_id: tenant)` is the only thing standing between one
# tenant's queue-mode job and another tenant's row.
class RowBasedQueueTest < Minitest::Test
  def teardown
    Ticket.destroy_all
    Ticket.searchkick_index.reindex_queue.clear
    Ticket.searchkick_index.refresh
  end

  def test_queue_mode_update_only_touches_the_correct_tenants_row
    acme = as_tenant("acme") { Ticket.create!(name: "Acme Ticket", account_id: "acme") }
    globex = as_tenant("globex") { Ticket.create!(name: "Globex Ticket", account_id: "globex") }
    Ticket.searchkick_index.refresh

    Searchkick.callbacks(:queue) { as_tenant("acme") { acme.update!(name: "Acme Ticket Renamed") } }
    Searchkick::ProcessQueueJob.new.perform(class_name: "Ticket", index_name: Ticket.searchkick_index.name, inline: true)
    Ticket.searchkick_index.refresh

    doc = Searchkick.client.get(index: Ticket.searchkick_index.name, id: "acme::#{acme.id}")
    assert_equal "Acme Ticket Renamed", doc["_source"]["name"]

    doc = Searchkick.client.get(index: Ticket.searchkick_index.name, id: "globex::#{globex.id}")
    assert_equal "Globex Ticket", doc["_source"]["name"]
  end

  def test_queue_mode_destroy_never_removes_another_tenants_row
    acme = as_tenant("acme") { Ticket.create!(name: "Acme Row", account_id: "acme") }
    globex = as_tenant("globex") { Ticket.create!(name: "Globex Row", account_id: "globex") }
    Ticket.searchkick_index.refresh

    Searchkick.callbacks(:queue) { as_tenant("acme") { acme.destroy } }
    Searchkick::ProcessQueueJob.new.perform(class_name: "Ticket", index_name: Ticket.searchkick_index.name, inline: true)
    Ticket.searchkick_index.refresh

    refute Searchkick.client.exists(index: Ticket.searchkick_index.name, id: "acme::#{acme.id}")
    assert Searchkick.client.exists(index: Ticket.searchkick_index.name, id: "globex::#{globex.id}"), "globex's row must survive acme's queue-mode destroy"
  end
end
