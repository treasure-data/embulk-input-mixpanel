
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-mixpanel"
  spec.version       = "0.0.1"
  spec.authors       = ["yoshihara"]
  spec.summary       = "Mixpanel input plugin for Embulk"
  spec.description   = "Loads records from Mixpanel."
  spec.email         = ["h.yoshihara@everyleaf.com", "k@uu59.org"]
  spec.licenses      = ["Apache2"]
  spec.homepage      = "https://github.com/treasure-data/embulk-input-mixpanel"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'embulk', ['>= 0.6.12', '< 1.0']
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'test-unit-rr'
end