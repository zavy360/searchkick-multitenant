require "searchkick"
require "searchkick/multi_tenant/version"
require "searchkick/multi_tenant/configuration"

module Searchkick::MultiTenant
  class Error < StandardError; end

  class << self
    def configure
      yield config
    end

    def config
      @config ||= Configuration.new
    end

    def current_tenant
      config.current_tenant.call
    end

    def each_tenant(&block)
      config.each_tenant.call(&block)
    end

    def enabled_for?(klass)
      klass.respond_to?(:searchkick_multitenant?) && klass.searchkick_multitenant?
    end

    def tenant_field(klass)
      klass.searchkick_multitenant_options.fetch(:field)
    end

    def routing?(_klass)
      config.routing
    end

    # "::" instead of "_" — tenant identifiers commonly contain underscores
    # (e.g. schema names), so a rarer delimiter avoids ambiguous splits.
    # Split on the id's own value, not on hit["_source"], since _source can
    # be absent when a query restricts returned fields but _id is always
    # present on every hit.
    ID_DELIMITER = "::"

    def composite_id(tenant, id)
      "#{tenant}#{ID_DELIMITER}#{id}"
    end

    def split_composite_id(composite_id)
      composite_id.to_s.split(ID_DELIMITER, 2)
    end

    # matches Searchkick::ReindexQueue's own private escape/split — pipe
    # escaped with a double pipe. We add a `tenant` field so a mixed-tenant
    # Redis queue can be split by tenant before any DB work runs against
    # it. tenant goes right after id (always present, so it's never the
    # empty middle field) and optional routing stays last, exactly where
    # Searchkick's own encoding already conditionally omits it — an empty
    # *middle* field would join adjacent to the next delimiter as "||",
    # indistinguishable from an escaped pipe.
    def escape_queue_field(value)
      value.to_s.gsub("|", "||")
    end

    def build_queue_item(id, tenant, routing)
      parts = [escape_queue_field(id), escape_queue_field(tenant)]
      parts << escape_queue_field(routing) if routing
      parts.join("|")
    end

    def split_queue_item(raw)
      parts = raw.split(/(?<!\|)\|(?!\|)/).map { |v| v.gsub("||", "|") }
      {id: parts[0], tenant: parts[1], routing: parts[2]}
    end
  end
end

require "searchkick/multi_tenant/model"
require "searchkick/multi_tenant/record_data_ext"
require "searchkick/multi_tenant/results_ext"
require "searchkick/multi_tenant/search_ext"
require "searchkick/multi_tenant/relation_ext"
require "searchkick/multi_tenant/queue_ext"
require "searchkick/multi_tenant/async_job_ext"
require "searchkick/multi_tenant/relation_indexer_ext"
require "searchkick/multi_tenant/full_reindex_guard"
require "searchkick/multi_tenant/tenant_reindexer"
