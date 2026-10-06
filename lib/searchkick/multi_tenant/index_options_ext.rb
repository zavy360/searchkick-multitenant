module Searchkick::MultiTenant
  module IndexOptionsExt
    # explicit keyword mapping for the real-id field, so `where(id:)` keeps
    # `_id`'s string semantics. Not left to dynamic mapping: custom
    # `mappings:` without `merge_mappings` drops Searchkick's string ->
    # keyword dynamic template, and an integer id would map as `long`.
    # `||=` so a model's own mapping for the field still wins.
    def index_options
      result = super
      return result unless Searchkick::MultiTenant.enabled_for?(options[:class_name]&.safe_constantize)

      (result[:mappings][:properties] ||= {})[Searchkick::MultiTenant::RECORD_ID_FIELD] ||= {type: "keyword"}
      result
    end
  end
end

Searchkick::IndexOptions.prepend(Searchkick::MultiTenant::IndexOptionsExt)
