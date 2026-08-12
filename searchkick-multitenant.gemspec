require_relative "lib/searchkick/multi_tenant/version"

Gem::Specification.new do |spec|
  spec.name          = "searchkick-multitenant"
  spec.version       = Searchkick::MultiTenant::VERSION
  spec.summary       = "Shared-index multitenancy for Searchkick"
  spec.homepage      = "https://github.com/ankane/searchkick" # placeholder
  spec.license       = "MIT"

  spec.author        = "Victor Rudolfsson"
  spec.email         = "victor@zavy.com"

  spec.files         = Dir["*.{md,txt}", "{lib}/**/*"]
  spec.require_path  = "lib"

  spec.required_ruby_version = ">= 3.2"

  spec.add_dependency "searchkick", ">= 5.4", "< 7"
end
