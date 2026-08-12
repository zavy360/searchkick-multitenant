require "bundler/setup"
require "active_record"
require "active_job"
require "opensearch-ruby"
require "redis-client"
require "searchkick/multi_tenant"

Searchkick.redis = RedisClient.new
require "minitest/autorun"

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord::Schema.define do
  create_table :products_acme do |t|
    t.string :name
  end
  create_table :products_globex do |t|
    t.string :name
  end
  create_table :categories_acme do |t|
    t.string :name
  end
  create_table :categories_globex do |t|
    t.string :name
  end
  create_table :articles_acme do |t|
    t.string :title
  end
  create_table :articles_globex do |t|
    t.string :title
  end
end

ActiveJob::Base.queue_adapter = :test

# simulates Apartment-style schema-per-tenant switching: which physical
# table a query hits is driven by the ambient tenant, not a column on the
# row — this is the scenario where two tenants can legitimately have a
# record with the same primary key (id=1 in each tenant's own table), which
# is exactly the collision the composite ES _id scheme has to survive.
#
# shared by both models below (searchkick_tenant_scope is bound to whichever
# model class it's installed on via define_singleton_method, so `self.all`
# resolves per-model even though the switching logic is identical).
def switch_tenant_tables(tenant)
  previous = Thread.current[:current_tenant]
  Thread.current[:current_tenant] = tenant
  # table_name is overridden dynamically below; bust AR's memoized
  # Arel::Table *and* predicate_builder (which embeds its own reference to
  # the old arel_table, so clearing @arel_table alone isn't enough)
  TENANT_MODELS.each do |model|
    model.reset_column_information
    model.instance_variable_set(:@arel_table, nil)
    model.instance_variable_set(:@predicate_builder, nil)
  end
  raise "boom (injected failure for #{tenant})" if Thread.current[:fail_tenant] == tenant

  yield
ensure
  Thread.current[:current_tenant] = previous
  TENANT_MODELS.each do |model|
    model.reset_column_information
    model.instance_variable_set(:@arel_table, nil)
    model.instance_variable_set(:@predicate_builder, nil)
  end
end

TENANT_SCOPE = lambda do |tenant, &block|
  switch_tenant_tables(tenant) { block.call(all) }
end

class Product < ActiveRecord::Base
  searchkick callbacks: :inline
  searchkick_multitenant tenant_scope: TENANT_SCOPE
  # tenant: left unconfigured — defaults to the ambient current_tenant,
  # correct here since there's no column, the schema itself is the tenant

  def self.table_name
    "products_#{Thread.current[:current_tenant]}"
  end
end

# second tenant-enabled model, to prove multi-model search (Searchkick.search
# with models: [Product, Category]) stays tenant-scoped across both.
class Category < ActiveRecord::Base
  searchkick callbacks: :inline
  searchkick_multitenant tenant_scope: TENANT_SCOPE

  def self.table_name
    "categories_#{Thread.current[:current_tenant]}"
  end
end

# :async model — separate from Product/Category (both :inline) so async
# job tests don't interact with the inline callback tests.
class Article < ActiveRecord::Base
  searchkick callbacks: :async
  searchkick_multitenant tenant_scope: TENANT_SCOPE

  def self.table_name
    "articles_#{Thread.current[:current_tenant]}"
  end
end

TENANT_MODELS = [Product, Category, Article].freeze

Searchkick::MultiTenant.configure do |c|
  c.current_tenant = -> { Thread.current[:current_tenant] }
  c.each_tenant = ->(&block) { %w[acme globex].each(&block) }
  c.routing = true
end

def as_tenant(tenant, &block)
  switch_tenant_tables(tenant, &block)
end

[Product, Category, Article].each(&:reindex) # transparently delegates to TenantReindexer for tenant-enabled models

Minitest.after_run do
  Product.searchkick_index.delete if Product.searchkick_index.exists?
  Category.searchkick_index.delete if Category.searchkick_index.exists?
  Article.searchkick_index.delete if Article.searchkick_index.exists?
end
