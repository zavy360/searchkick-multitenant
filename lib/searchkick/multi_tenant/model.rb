module Searchkick::MultiTenant
  # prepended (not included) so `super` reaches whatever `search_data` the
  # model itself defines, whether that's the user's own override or
  # Searchkick's generated default — a plain `include`/`define_method` on
  # the class would instead sit *below* or clobber that method.
  module SearchDataExt
    def search_data
      return super unless Searchkick::MultiTenant.enabled_for?(self.class)

      super.merge(Searchkick::MultiTenant.tenant_field(self.class) => searchkick_tenant)
    end
  end

  module Model
    # Follows Searchkick's own convention for per-model customization
    # (`index_name:`, `search_document_id`, `should_index?`): pass a proc,
    # pass a symbol naming a method, or just define the method directly.
    # Every form resolves to one real, class- or instance-bound method
    # (`searchkick_tenant_scope` / `searchkick_tenant`) via
    # define_singleton_method/define_method — never instance_exec, which
    # can't also accept a runtime block (only one block slot per call).
    #
    # tenant_scope: (tenant, &block) -> class method. Owns building AND
    # yielding a scoped relation for `tenant` — for schema-based tenancy,
    # wraps the switch and yields `all`; for row-based, just yields
    # `where(...)`. No built-in default: this is the one thing that used
    # to be a `tenant_column` shortcut, deliberately removed in favor of
    # always being explicit (it's one line either way).
    #
    # tenant: (no args) -> instance method, "what tenant does this record
    # belong to". Defaults to the ambient current_tenant when not
    # configured — correct by construction for schema-based tenancy (a
    # record can only have been loaded/created while switched into its own
    # tenant), and something row-based models override with a column name.
    def searchkick_multitenant(field: :tenant, tenant_scope: nil, tenant: nil)
      raise Error, "call `searchkick` before `searchkick_multitenant`" unless respond_to?(:searchkick_index)

      cattr_accessor :searchkick_multitenant_options, instance_accessor: false
      self.searchkick_multitenant_options = {field: field}

      resolve_tenant_hook(:searchkick_tenant_scope, tenant_scope, singleton: true)
      resolve_tenant_hook(:searchkick_tenant, tenant, singleton: false, default: -> { Searchkick::MultiTenant.current_tenant })

      prepend SearchDataExt
    end

    def searchkick_multitenant?
      respond_to?(:searchkick_multitenant_options) && !searchkick_multitenant_options.nil?
    end

    private

    def resolve_tenant_hook(name, config, singleton:, default: nil)
      definer = singleton ? :define_singleton_method : :define_method

      case config
      when Symbol
        send(definer, name) { |*args, &blk| send(config, *args, &blk) }
      when Proc
        send(definer, name, &config)
      else
        send(definer, name, &default) if default
      end
    end
  end
end

ActiveSupport.on_load(:active_record) do
  extend Searchkick::MultiTenant::Model
end
