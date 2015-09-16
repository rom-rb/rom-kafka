# encoding: utf-8
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "rom/kafka/version"

Gem::Specification.new do |gem|

  gem.name             = "rom-kafka"
  gem.version          = ROM::Kafka::VERSION.dup
  gem.author           = ["Andrew Kozin"]
  gem.email            = ["andrew.kozin@gmail.com"]
  gem.summary          = "Kafka support for Ruby Object Mapper"
  gem.description      = gem.summary
  gem.homepage         = "https://rom-rb.org"
  gem.license          = "MIT"

  gem.files            = `git ls-files -z`.split("\x0")
  gem.executables      = gem.files.grep(%r{^bin/}) { |f| File.basename(f) }
  gem.test_files       = gem.files.grep(%r{^(test|spec|features)/})
  gem.extra_rdoc_files = Dir["README.md", "LICENSE"]
  gem.require_paths    = ["lib"]

  gem.required_ruby_version = "~> 1.9", ">= 1.9.3"

  gem.add_runtime_dependency "rom", "~> 0.9", ">= 0.9.1"
  gem.add_runtime_dependency "poseidon", "~> 0.0", ">= 0.0.5"
  gem.add_runtime_dependency "attributes_dsl", "~> 0.0", ">= 0.0.2"

  gem.add_development_dependency "hexx-rspec", "~> 0.5"
  gem.add_development_dependency "inflecto", "~> 0.0", ">= 0.0.2"
  gem.add_development_dependency "timecop", "~> 0.8"

end # Gem::Specification
