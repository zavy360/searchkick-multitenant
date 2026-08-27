require_relative "test_helper"

class ConfigurationTest < Minitest::Test
  def teardown
    Searchkick::MultiTenant.configure do |c|
      c.use_reader_replica_for_reindex = false
      c.around_reindex_read = nil # falls back to the auto-detecting default
      c.enabled = true
    end
  end

  def test_reader_replica_configured_is_false_without_a_reading_role
    refute Searchkick::MultiTenant.reader_replica_configured?
  end

  def test_around_reindex_read_falls_back_to_primary_when_no_reader_is_configured
    Searchkick::MultiTenant.configure { |c| c.use_reader_replica_for_reindex = true }

    called = false
    Searchkick::MultiTenant.config.around_reindex_read.call { called = true }

    assert called, "block must still run even when use_reader_replica_for_reindex is on but no reader is registered"
  end

  def test_around_reindex_read_uses_the_reading_role_when_one_is_detected
    Searchkick::MultiTenant.configure { |c| c.use_reader_replica_for_reindex = true }

    with_stubbed_reader_replica_configured(true) do
      connected_to_role = nil
      with_stubbed_connected_to(->(**kwargs, &blk) { connected_to_role = kwargs[:role]; blk.call }) do
        Searchkick::MultiTenant.config.around_reindex_read.call {}
      end

      assert_equal :reading, connected_to_role
    end
  end

  def test_explicit_around_reindex_read_overrides_the_auto_detecting_default
    Searchkick::MultiTenant.configure { |c| c.use_reader_replica_for_reindex = true }
    custom_called = false
    Searchkick::MultiTenant.configure { |c| c.around_reindex_read = ->(&block) { custom_called = true; block.call } }

    ran = false
    Searchkick::MultiTenant.config.around_reindex_read.call { ran = true }

    assert custom_called, "explicit around_reindex_read must win over use_reader_replica_for_reindex's default"
    assert ran
  end

  def test_enabled_for_returns_false_when_globally_disabled
    assert Searchkick::MultiTenant.enabled_for?(Product), "sanity check: enabled by default"

    Searchkick::MultiTenant.configure { |c| c.enabled = false }

    refute Searchkick::MultiTenant.enabled_for?(Product)
  end

  # every patched method in the gem checks enabled_for? before doing
  # anything multitenant-specific — spot-check the two RecordData behaviors
  # (composite _id, tenant field in _source) that would otherwise mix
  # tenants' documents together in the shared index if left patched.
  def test_record_data_is_unpatched_when_globally_disabled
    as_tenant("acme") do
      product = Product.new(id: 1, name: "Widget")
      data = Searchkick::RecordData.new(Product.searchkick_index, product)

      Searchkick::MultiTenant.configure { |c| c.enabled = false }
      assert_equal 1, data.search_id
      refute data.send(:search_data).key?(:tenant)

      Searchkick::MultiTenant.configure { |c| c.enabled = true }
      assert_equal "acme::1", data.search_id
      assert_equal "acme", data.send(:search_data)[:tenant]
    end
  end

  private

  def with_stubbed_reader_replica_configured(value)
    original = Searchkick::MultiTenant.method(:reader_replica_configured?)
    Searchkick::MultiTenant.define_singleton_method(:reader_replica_configured?) { value }
    yield
  ensure
    Searchkick::MultiTenant.define_singleton_method(:reader_replica_configured?, original)
  end

  def with_stubbed_connected_to(replacement)
    original = ActiveRecord::Base.method(:connected_to)
    ActiveRecord::Base.define_singleton_method(:connected_to) { |*args, **kwargs, &blk| replacement.call(*args, **kwargs, &blk) }
    yield
  ensure
    ActiveRecord::Base.define_singleton_method(:connected_to, original)
  end
end
