module Searchkick
  module MultiTenant
    # Index#full_reindex (bare `Model.reindex`, `Model.reindex(full: true)`,
    # and both `rake searchkick:reindex`/`searchkick:reindex:all` tasks) is a
    # real, unconditional data-loss footgun on a tenant-enabled model: it
    # scopes to whatever's ambient right now (`Searchkick.scope(model).all`
    # — one tenant's schema, for schema-based tenancy), builds a brand-new
    # index from just that, and promotes over the shared index once done —
    # deleting every other tenant's documents the moment the old index is
    # cleaned up.
    #
    # Rather than raise and push a different API onto every caller (`Model.
    # reindex` is what apps, gems, and Searchkick's own rake tasks already
    # call by convention), redirect transparently to TenantReindexer, which
    # loops every tenant into the same new index before promoting — so the
    # standard interface does the safe thing automatically.
    module FullReindexGuard
      # options TenantReindexer has no equivalent for — rather than silently
      # ignore them (diverging from what the caller explicitly asked for),
      # fail loudly and point at TenantReindexer directly for full control.
      UNMAPPABLE = [:import, :refresh_interval, :scope, :wait].freeze

      def full_reindex(relation, import: true, resume: false, retain: false, mode: nil, refresh_interval: nil, scope: nil, wait: nil, job_options: nil)
        model = relation.respond_to?(:searchkick_klass) ? relation.searchkick_klass : relation.klass
        return super unless Searchkick::MultiTenant.enabled_for?(model)

        raise ArgumentError, "Full reindex does not support :queue mode - use :async mode instead" if mode == :queue

        given = {import: import, refresh_interval: refresh_interval, scope: scope, wait: wait}
        unsupported = given.select { |k, v| k == :import ? v == false : !v.nil? }
        if unsupported.any?
          raise Searchkick::MultiTenant::Error,
            "#{model.name}.reindex(#{unsupported.map { |k, v| "#{k}: #{v.inspect}" }.join(", ")}) isn't supported for tenant-enabled models — " \
            "call Searchkick::MultiTenant::TenantReindexer.call(#{model.name}) directly for full control."
        end

        Searchkick::MultiTenant::TenantReindexer.call(model, mode: mode || :inline, retain: retain, job_options: job_options, resume: resume)
      end
    end
  end
end

Searchkick::Index.prepend(Searchkick::MultiTenant::FullReindexGuard)
