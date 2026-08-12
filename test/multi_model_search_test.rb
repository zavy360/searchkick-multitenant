require_relative "test_helper"

class MultiModelSearchTest < Minitest::Test
  def setup
    as_tenant("acme") do
      Product.create!(id: 1, name: "Acme Gizmo")
      Category.create!(id: 1, name: "Acme Gadgets")
    end
    as_tenant("globex") do
      Product.create!(id: 1, name: "Globex Gizmo")
      Category.create!(id: 1, name: "Globex Gadgets")
    end
    Product.searchkick_index.refresh
    Category.searchkick_index.refresh
  end

  def teardown
    as_tenant("acme") { Product.delete_all; Category.delete_all }
    as_tenant("globex") { Product.delete_all; Category.delete_all }
  end

  def test_multi_model_search_stays_tenant_scoped
    as_tenant("acme") do
      names = Searchkick.search("*", models: [Product, Category], load: false).map { |r| r["name"] }
      assert_equal ["Acme Gadgets", "Acme Gizmo"], names.sort
    end

    as_tenant("globex") do
      names = Searchkick.search("*", models: [Product, Category], load: false).map { |r| r["name"] }
      assert_equal ["Globex Gadgets", "Globex Gizmo"], names.sort
    end
  end

  def test_multi_model_search_hydrates_correct_records
    as_tenant("acme") do
      records = Searchkick.search("*", models: [Product, Category]).to_a
      assert_equal ["Acme Gadgets", "Acme Gizmo"], records.map(&:name).sort
      assert records.all? { |r| r.id == 1 } # same PK across both tenants' rows — proves no cross-tenant mixup
    end
  end

  def test_mixed_tenant_and_non_tenant_models_raises
    non_tenant_klass = Class.new(ActiveRecord::Base) do
      def self.table_name = "products_acme" # reuse an existing table, content doesn't matter here
    end

    assert_raises(Searchkick::MultiTenant::Error) do
      Searchkick.search("x", models: [Product, non_tenant_klass], load: false)
    end
  end
end
