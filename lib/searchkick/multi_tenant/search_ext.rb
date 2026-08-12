module Searchkick::MultiTenant
  module SearchExt
    def search(term = "*", model: nil, **options, &block)
      options = options.dup
      skip = options.delete(:skip_tenant_scope) || Thread.current[:searchkick_multitenant_skip]

      models = model ? [model] : Array(options[:models])

      if models.any? && !skip
        enabled = models.select { |m| Searchkick::MultiTenant.enabled_for?(m) }

        if enabled.size == models.size
          fields = enabled.map { |m| Searchkick::MultiTenant.tenant_field(m) }.uniq
          raise Searchkick::MultiTenant::Error, "Cannot auto-scope a multi-model search where models use different tenant fields (#{fields.join(", ")}) — pass `where:` explicitly." if fields.size > 1

          field = fields.first

          # Searchkick's suggest: builds the ES `suggest` section independently
          # of the query's where:/filter context (query.rb's set_suggestions
          # never touches `where`) — it would surface terms from every
          # tenant's documents regardless of the where: filter injected below.
          # There's no per-tenant suggester context plumbed by Searchkick to
          # scope this correctly, so refuse rather than silently leak.
          if options[:suggest]
            raise Searchkick::MultiTenant::Error, "`suggest:` is not tenant-scoped — Searchkick builds ES suggestions independently of the where: filter, so it would surface terms from every tenant. Pass `skip_tenant_scope: true` only if this search is intentionally cross-tenant."
          end

          # smart_aggs (on by default) deliberately excludes a field's own
          # condition from *that field's* aggregation bucket, for full facet
          # counts. If the aggregated field is the tenant field itself, that
          # exclusion drops the tenant filter from the bucket, leaking other
          # tenants' bucket keys/counts even though `hits` stays correctly
          # scoped. Aggregating any other field is unaffected and safe.
          if options[:aggs] && options.fetch(:smart_aggs, true) != false
            agg_keys = (options[:aggs].is_a?(Hash) ? options[:aggs].keys : Array(options[:aggs])).map(&:to_sym)
            if agg_keys.include?(field.to_sym)
              raise Searchkick::MultiTenant::Error, "Aggregating on the tenant field (#{field}) leaks other tenants' bucket counts, since smart_aggs excludes that field's own condition from its bucket. Pass `smart_aggs: false` if this is intentional."
            end
          end

          tenant = Searchkick::MultiTenant.current_tenant
          options[:where] = (options[:where] || {}).merge(field => tenant)
          options[:routing] = tenant.to_s if enabled.all? { |m| Searchkick::MultiTenant.routing?(m) }
        elsif enabled.any?
          # some but not all models in this search are tenant-enabled — a
          # flat `where:` can't scope only some of them, and silently
          # searching the enabled ones unscoped would leak across tenants.
          raise Searchkick::MultiTenant::Error, "Searching a mix of tenant-enabled (#{enabled.map(&:name).join(", ")}) and non-tenant models in one call isn't auto-scoped — pass `where:` explicitly or `skip_tenant_scope: true`."
        end
      end

      super(term, model: model, **options, &block)
    end

    # explicit escape hatch for cross-tenant/admin search, e.g. a superadmin
    # dashboard — mirrors Searchkick's own `disable_callbacks` thread-local
    # pattern (lib/searchkick.rb) rather than a global config flag.
    def without_tenant_scope
      previous = Thread.current[:searchkick_multitenant_skip]
      Thread.current[:searchkick_multitenant_skip] = true
      yield
    ensure
      Thread.current[:searchkick_multitenant_skip] = previous
    end
  end
end

Searchkick.singleton_class.prepend(Searchkick::MultiTenant::SearchExt)
