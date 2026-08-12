require_relative "test_helper"
require "active_job/test_helper"

class AsyncJobTest < Minitest::Test
  include ActiveJob::TestHelper

  def teardown
    as_tenant("acme") { Article.delete_all }
    as_tenant("globex") { Article.delete_all }
    Article.searchkick_index.refresh
  end

  def test_single_record_async_reindex_tags_correct_tenant
    as_tenant("acme") { Article.create!(id: 1, title: "Acme Doc") }
    perform_enqueued_jobs
    Article.searchkick_index.refresh

    doc = Searchkick.client.get(index: Article.searchkick_index.name, id: "acme::1")
    assert_equal "Acme Doc", doc["_source"]["title"]
  end

  # the actual bug this guards against: a ReindexV2Job is enqueued while
  # tenant A is ambient (correctly captured at enqueue time via `serialize`),
  # but by the time it *executes* — as would happen in a worker that has
  # since moved on to a different tenant's job on the same thread — the
  # ambient tenant has drifted to B. Without capturing tenant at enqueue
  # time, the destroy-reindex job would resolve A's deleted record's
  # composite id using whatever's ambient *now* (B), and delete B's valid
  # document instead of A's.
  def test_destroy_reindex_uses_enqueue_time_tenant_not_execution_time_ambient
    as_tenant("acme") { Article.create!(id: 1, title: "Acme Doc") }
    as_tenant("globex") { Article.create!(id: 1, title: "Globex Doc") }
    perform_enqueued_jobs
    Article.searchkick_index.refresh

    assert Searchkick.client.exists(index: Article.searchkick_index.name, id: "acme::1")
    assert Searchkick.client.exists(index: Article.searchkick_index.name, id: "globex::1")

    as_tenant("acme") { Article.find(1).destroy } # enqueues a destroy-reindex job, tenant "acme" captured now

    # simulate the job executing later on a worker thread that has since
    # drifted to a different tenant's context — bypass as_tenant/switch_tenant_tables
    # (which would also reset AR schema caches) and just flip the raw ambient value
    Thread.current[:current_tenant] = "globex"
    begin
      perform_enqueued_jobs
    ensure
      Thread.current[:current_tenant] = "acme" # restore before teardown's as_tenant calls run
    end
    Article.searchkick_index.refresh

    refute Searchkick.client.exists(index: Article.searchkick_index.name, id: "acme::1"), "acme's deleted record should be removed from the index"
    assert Searchkick.client.exists(index: Article.searchkick_index.name, id: "globex::1"), "globex's still-valid record must survive a same-id delete for a different tenant"
  end
end
