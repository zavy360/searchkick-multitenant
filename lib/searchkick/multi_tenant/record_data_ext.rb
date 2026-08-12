module Searchkick::MultiTenant
  module RecordDataExt
    # composite id so two tenants' records never collide in the shared index
    def search_id
      return super unless Searchkick::MultiTenant.enabled_for?(record.class)

      Searchkick::MultiTenant.composite_id(record.searchkick_tenant, super)
    end

    def record_data
      data = super
      if Searchkick::MultiTenant.enabled_for?(record.class) && Searchkick::MultiTenant.routing?(record.class)
        data[:routing] = record.searchkick_tenant.to_s
      end
      data
    end
  end
end

Searchkick::RecordData.prepend(Searchkick::MultiTenant::RecordDataExt)
