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
      # nil, not "acme" — teardown's own as_tenant calls switch explicitly
      # regardless, and leaving this at "acme" leaked into whichever test
      # ran next in the same thread (minitest runs a class's tests
      # sequentially, not one thread per test)
      Thread.current[:current_tenant] = nil
    end
    Article.searchkick_index.refresh

    refute Searchkick.client.exists(index: Article.searchkick_index.name, id: "acme::1"), "acme's deleted record should be removed from the index"
    assert Searchkick.client.exists(index: Article.searchkick_index.name, id: "globex::1"), "globex's still-valid record must survive a same-id delete for a different tenant"
  end

  # regression coverage for the "Batches left" stuck-forever bug: a batch
  # that keeps failing must eventually unblock batches_left (after
  # batch_job_max_attempts) instead of leaving it stuck for however long
  # Sidekiq's own retry backoff runs (or forever, if the job dies).
  def test_batch_gives_up_and_unblocks_batches_left_after_max_attempts
    index = Article.searchkick_index
    relation_indexer = Searchkick::RelationIndexer.new(index)
    Searchkick.with_redis { |r| r.call("SADD", "searchkick:reindex:#{index.name}:batches", ["1"]) }

    Searchkick::MultiTenant.configure { |c| c.batch_job_max_attempts = 2 }

    with_stubbed_new(Searchkick::RecordIndexer, ->(*) { raise "boom" }) do
      2.times do
        assert_raises(RuntimeError) do
          Searchkick::BulkReindexJob.new.perform(class_name: "Article", index_name: index.name, batch_id: "1", record_ids: [1], multitenant_tenant: "acme")
        end
      end
    end

    assert_equal 0, relation_indexer.batches_left, "batch should be unblocked after max_attempts failed attempts"
  ensure
    Searchkick::MultiTenant.configure { |c| c.batch_job_max_attempts = 5 }
  end

  def test_batch_job_times_out_instead_of_hanging_forever
    index = Article.searchkick_index
    Searchkick.with_redis { |r| r.call("SADD", "searchkick:reindex:#{index.name}:batches", ["1"]) }

    Searchkick::MultiTenant.configure do |c|
      c.batch_job_timeout = 0.01
      c.batch_job_max_attempts = 1
    end

    with_stubbed_new(Searchkick::RecordIndexer, ->(*) { sleep 1 }) do
      assert_raises(Timeout::Error) do
        Searchkick::BulkReindexJob.new.perform(class_name: "Article", index_name: index.name, batch_id: "1", record_ids: [1], multitenant_tenant: "acme")
      end
    end

    assert_equal 0, Searchkick::RelationIndexer.new(index).batches_left, "timed-out batch should still give up and unblock after max_attempts"
  ensure
    Searchkick::MultiTenant.configure do |c|
      c.batch_job_timeout = 300
      c.batch_job_max_attempts = 5
    end
  end

  # around_reindex_read (e.g. a read-replica role switch) must wrap OUTSIDE
  # searchkick_tenant_scope's own switch — for schema-based tenancy the
  # switch applies to whichever connection is checked out *at that moment*,
  # so selecting a different connection role from inside the switch would
  # leave that connection never having had the switch applied to it.
  def test_around_reindex_read_wraps_outside_the_tenant_switch
    as_tenant("acme") { Article.create!(id: 1, title: "Acme Doc") }
    index = Article.searchkick_index

    seen_tenant = :not_called
    Searchkick::MultiTenant.configure do |c|
      c.around_reindex_read = ->(&block) {
        seen_tenant = Thread.current[:current_tenant]
        block.call
      }
    end

    Searchkick::BulkReindexJob.new.perform(class_name: "Article", index_name: index.name, batch_id: "1", record_ids: [1], multitenant_tenant: "acme")

    refute_equal "acme", seen_tenant, "hook fired after the tenant switch instead of before it"
  ensure
    Searchkick::MultiTenant.configure { |c| c.around_reindex_read = ->(&block) { block.call } }
  end

  private

  # minitest 6 dropped Mock/Object#stub, and this is the only place we need
  # it — swap the singleton method directly instead of adding a mocking gem.
  def with_stubbed_new(klass, replacement)
    original = klass.method(:new)
    klass.define_singleton_method(:new) { |*args| replacement.call(*args) }
    yield
  ensure
    klass.define_singleton_method(:new, original)
  end
end
