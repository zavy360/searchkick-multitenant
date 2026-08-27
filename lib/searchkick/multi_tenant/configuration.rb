module Searchkick::MultiTenant
  class Configuration
    attr_accessor :current_tenant, :each_tenant, :routing, :batch_job_timeout, :batch_job_max_attempts

    def initialize
      @current_tenant = -> { raise Error, "Searchkick::MultiTenant.configure { |c| c.current_tenant = -> { ... } } is not set" }
      @each_tenant = ->(&block) { raise Error, "Searchkick::MultiTenant.configure { |c| c.each_tenant = ->(&block) { ... } } is not set" }
      @routing = false
      @batch_job_timeout = 300 # seconds a single BulkReindexJob attempt may run before it's timed out
      @batch_job_max_attempts = 5 # attempts before we give up on a batch and unblock batches_left
    end
  end
end
