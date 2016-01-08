Gem::Specification.new do |gem|
  gem.name             = "rom-kafka"
  gem.version          = "0.0.1"
  gem.author           = ["Andrew Kozin"]
  gem.email            = ["andrew.kozin@gmail.com"]
  gem.summary          = "Kafka support for Ruby Object Mapper"
  gem.description      = gem.summary
  gem.homepage         = "https://rom-rb.org"
  gem.license          = "MIT"

  gem.files            = `git ls-files`.split($INPUT_RECORD_SEPARATOR)
  gem.executables      = gem.files.grep(%r{^bin/}) { |f| File.basename(f) }
  gem.test_files       = gem.files.grep(/^spec/)
  gem.extra_rdoc_files = Dir["README.md", "LICENSE", "CHANGELOG.md"]
  gem.require_paths    = ["lib"]

  gem.required_ruby_version = "~> 1.9", ">= 1.9.3"

  gem.add_runtime_dependency "rom", "~> 1.0"
  gem.add_runtime_dependency "poseidon", "~> 0.0", ">= 0.0.5"
  gem.add_runtime_dependency "attributes_dsl", ">= 0.0.2"

  gem.add_development_dependency "hexx-rspec", "~> 0.5"
  gem.add_development_dependency "inflecto", "~> 0.0", ">= 0.0.2"
  gem.add_development_dependency "timecop", "~> 0.8"
end
