module Searchkick::MultiTenant
  class Configuration
    attr_accessor :current_tenant, :each_tenant, :routing

    def initialize
      @current_tenant = -> { raise Error, "Searchkick::MultiTenant.configure { |c| c.current_tenant = -> { ... } } is not set" }
      @each_tenant = ->(&block) { raise Error, "Searchkick::MultiTenant.configure { |c| c.each_tenant = ->(&block) { ... } } is not set" }
      @routing = false
    end
  end
end
