require "bundler/setup"
require "active_record"
require "active_job"
require "opensearch-ruby"
require "redis-client"
require "searchkick/multi_tenant"

Searchkick.redis = RedisClient.new
require "minitest/autorun"

ActiveJob::Base.queue_adapter = :test

ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
ActiveRecord::Schema.define do
  create_table :tickets do |t|
    t.string :name
    t.string :account_id
    t.boolean :archived, default: false
  end
end

# row-based tenancy: one shared table, tenant column on the row, no schema
# switch needed — tenant_scope is just a plain `.where(...)`.
class Ticket < ActiveRecord::Base
  searchkick callbacks: :inline
  searchkick_multitenant tenant: :account_id, tenant_scope: ->(tenant, &block) { block.call(where(account_id: tenant)) }

  scope :active, -> { where(archived: false) }
end

Searchkick::MultiTenant.configure do |c|
  c.current_tenant = -> { Thread.current[:current_tenant] }
  c.each_tenant = ->(&block) { %w[acme globex].each(&block) }
  c.routing = true
end

def as_tenant(tenant)
  previous = Thread.current[:current_tenant]
  Thread.current[:current_tenant] = tenant
  yield
ensure
  Thread.current[:current_tenant] = previous
end

Ticket.reindex # transparently delegates to TenantReindexer for tenant-enabled models

Minitest.after_run do
  Ticket.searchkick_index.delete if Ticket.searchkick_index.exists?
end
