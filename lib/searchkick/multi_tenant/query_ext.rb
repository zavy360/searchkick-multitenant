module Searchkick::MultiTenant
  module QueryExt
    private

    # Searchkick maps a where-key "id" to `_id`, which for tenant-enabled
    # models is the composite "tenant::id" — so `where(id: [1, 2])` would
    # match nothing. Point it at the real id in _source instead. Upstream
    # recurses back through this method for _or/_and/_not/or and agg
    # filters, so nested clauses and chained Relation#where are covered too.
    # An explicit `_id:` key is left alone as the raw ES escape hatch.
    def where_filters(where)
      return super unless where.is_a?(Hash) && tenant_query?

      super(where.transform_keys { |k| k.to_s == "id" ? Searchkick::MultiTenant::RECORD_ID_FIELD : k })
    end

    def tenant_query?
      models = klass ? [klass] : Array(options[:models])
      models.any? && models.all? { |m| Searchkick::MultiTenant.enabled_for?(m) }
    end
  end
end

Searchkick::Query.prepend(Searchkick::MultiTenant::QueryExt)
