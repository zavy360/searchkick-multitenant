module Searchkick::MultiTenant
  module ResultsExt
    # strip the tenant prefix before it becomes a DB primary-key lookup, and
    # load each tenant's hits through that model's `searchkick_tenant_scope`
    # — which owns establishing tenant context *and* the base relation as
    # one atomic step, so there's no seam left where scoping order can be
    # gotten wrong (the previous with_tenant+tenant_scope split had exactly
    # that bug: `super`'s first line is `Searchkick.scope(records)`, which
    # raises if `records` is already a Relation — precisely what a
    # row-based filter produces).
    def results_query(records, hits)
      return super unless Searchkick::MultiTenant.enabled_for?(records)

      hits.group_by { |hit| Searchkick::MultiTenant.split_composite_id(hit["_id"]).first }.flat_map do |tenant, tenant_hits|
        records.searchkick_tenant_scope(tenant) do |relation|
          real_ids = tenant_hits.map { |hit| Searchkick::MultiTenant.split_composite_id(hit["_id"]).last }

          if options[:includes] || options[:model_includes]
            included_relations = []
            combine_includes(included_relations, options[:includes])
            combine_includes(included_relations, options[:model_includes][records]) if options[:model_includes]
            relation = relation.includes(included_relations)
          end

          relation = options[:scope_results].call(relation) if options[:scope_results]

          Searchkick.load_records(relation, real_ids).to_a
        end
      end
    end

    # only the options[:load] branch needs a tenant-aware rewrite: it's the
    # one place a loaded record (keyed by its real DB id) gets matched back
    # up against a raw ES hit (keyed by the composite _id). Re-key by the
    # full composite id instead of the real id so the existing lookup at
    # `results[hit["_index"]][hit["_id"].to_s]` keeps working unmodified.
    #
    # works across multiple models in one search (Searchkick.search(models:
    # [A, B])) exactly like upstream does: each index still resolves to its
    # own model(s) via @klass or index_mapping, we just handle each
    # resolved model's tenant-enablement independently rather than assuming
    # a single search-wide @klass.
    def with_hit_and_missing_records
      return super unless options[:load]

      @with_hit_and_missing_records ||= tenant_hit_and_missing_records
    end

    private

    def tenant_hit_and_missing_records
      missing_records = []
      results = {}

      grouped_hits = hits.group_by { |hit| hit["_index"] }

      index_models = {}
      grouped_hits.each do |index, _|
        models =
          if @klass
            [@klass]
          else
            index_alias = index.split("_")[0..-2].join("_")
            Array((options[:index_mapping] || {})[index_alias])
          end
        raise Searchkick::Error, "Unknown model for index: #{index}. Pass the `models` option to the search method." unless models.any?

        index_models[index] = models
      end

      grouped_hits.each do |index, index_hits|
        results[index] = {}
        index_models[index].each do |model|
          results[index].merge!(model_hits_by_key(model, index_hits))
        end
      end

      built =
        hits.map do |hit|
          result = results[hit["_index"]][hit["_id"].to_s]
          if result && !(options[:load].is_a?(Hash) && options[:load][:dumpable])
            if (hit["highlight"] || options[:highlight]) && !result.respond_to?(:search_highlights)
              highlights = hit_highlights(hit)
              result.define_singleton_method(:search_highlights) { highlights }
            end
          end
          [result, hit]
        end.select do |result, hit|
          unless result
            models = index_models[hit["_index"]]
            missing_records << {id: hit["_id"], model: models.size == 1 ? models.first : models}
          end
          result
        end

      [built, missing_records]
    end

    # non-tenant model: identical to upstream — key by the real DB id,
    # since hit["_id"] already *is* the real id for this model.
    #
    # tenant-enabled model: key by the full composite hit["_id"] instead,
    # since results_query (above) strips the tenant prefix before querying
    # and returns records keyed by their real id — re-keying here by the
    # original composite id is what lets the unchanged lookup below
    # (`results[hit["_index"]][hit["_id"].to_s]`) find it.
    def model_hits_by_key(model, index_hits)
      return results_query(model, index_hits).to_a.index_by { |r| r.id.to_s } unless Searchkick::MultiTenant.enabled_for?(model)

      by_real_id = results_query(model, index_hits).to_a.index_by { |r| r.id.to_s }
      index_hits.each_with_object({}) do |hit, memo|
        real_id = Searchkick::MultiTenant.split_composite_id(hit["_id"]).last
        memo[hit["_id"].to_s] = by_real_id[real_id] if by_real_id[real_id]
      end
    end
  end
end

Searchkick::Results.prepend(Searchkick::MultiTenant::ResultsExt)
