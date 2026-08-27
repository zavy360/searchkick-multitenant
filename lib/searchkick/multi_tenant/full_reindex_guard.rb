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
    # standard interface does the safe thing automatically. `mode:`,
    # `retain:`, `job_options:`, `resume:`, `scope:`, `wait:`,
    # `refresh_interval:`, `replicas:`, `global_scan:`, and `partitions:`
    # all map onto TenantReindexer options with the same meaning as stock
    # Searchkick (stock itself has none of `replicas:`/`global_scan:`/
    # `partitions:` — see the explicit super call below for why that
    # matters).
    module FullReindexGuard
      def full_reindex(relation, import: true, resume: false, retain: false, mode: nil, refresh_interval: Searchkick::MultiTenant::TenantReindexer::DEFAULT_REFRESH_INTERVAL, replicas: Searchkick::MultiTenant::TenantReindexer::DEFAULT_REPLICAS_DURING_REINDEX, global_scan: false, partitions: Searchkick::MultiTenant::TenantReindexer::DEFAULT_PARTITIONS, scope: nil, wait: nil, job_options: nil)
        model = relation.respond_to?(:searchkick_klass) ? relation.searchkick_klass : relation.klass
        # explicit, not bare `super` — a bare super forwards every one of
        # this method's keywords by name, including replicas:/global_scan:/
        # partitions:, which stock Searchkick's full_reindex doesn't declare
        # at all and would raise ArgumentError: unknown keyword on for every
        # disabled-model call
        unless Searchkick::MultiTenant.enabled_for?(model)
          return super(relation, import: import, resume: resume, retain: retain, mode: mode, refresh_interval: refresh_interval, scope: scope, wait: wait, job_options: job_options)
        end

        raise ArgumentError, "wait only available in :async mode" if !wait.nil? && mode != :async
        raise ArgumentError, "Full reindex does not support :queue mode - use :async mode instead" if mode == :queue

        if import == false
          raise Searchkick::MultiTenant::Error,
            "#{model.name}.reindex(import: false) isn't supported for tenant-enabled models — " \
            "call Searchkick::MultiTenant::TenantReindexer.call(#{model.name}) directly for full control."
        end

        Searchkick::MultiTenant::TenantReindexer.call(
          model,
          mode: mode || :inline,
          retain: retain,
          job_options: job_options,
          resume: resume,
          scope: scope,
          wait: wait,
          refresh_interval: refresh_interval,
          replicas: replicas,
          global_scan: global_scan,
          partitions: partitions
        )
      end
    end
  end
end

Searchkick::Index.prepend(Searchkick::MultiTenant::FullReindexGuard)
