module Searchkick
  module MultiTenant
    # Searchkick.search's prepend (search_ext.rb) injects the tenant filter
    # into the options hash before Relation.new runs — but `rewhere!`
    # unconditionally replaces @options[:where] wholesale (relation.rb:644-648),
    # and `only`/`except` (relation.rb:650-656) reconstruct via a *direct*
    # `Relation.new(@model, @term, **options)` call that never goes back
    # through Searchkick.search at all. Any of the three silently drops the
    # tenant filter with no re-injection. Reapply it here using the same
    # `where!` (not rewhere!) so it composes safely (merge or _and-wrap)
    # rather than risking another wholesale replace.
    module RelationExt
      def rewhere!(value)
        super
        reapply_tenant_scope!
        self
      end

      def only(*keys)
        reapply_tenant_scope(super)
      end

      def except(*keys)
        reapply_tenant_scope(super)
      end

      private

      def reapply_tenant_scope!
        return if Thread.current[:searchkick_multitenant_skip]
        return unless Searchkick::MultiTenant.enabled_for?(model)

        where!(Searchkick::MultiTenant.tenant_field(model) => Searchkick::MultiTenant.current_tenant)
      end

      def reapply_tenant_scope(relation)
        return relation if Thread.current[:searchkick_multitenant_skip]
        return relation unless Searchkick::MultiTenant.enabled_for?(relation.model)

        relation.send(:where!, Searchkick::MultiTenant.tenant_field(relation.model) => Searchkick::MultiTenant.current_tenant)
        relation
      end
    end
  end
end

Searchkick::Relation.prepend(Searchkick::MultiTenant::RelationExt)
