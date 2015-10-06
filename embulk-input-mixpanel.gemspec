
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-mixpanel"
  spec.version       = "0.3.2"
  spec.authors       = ["yoshihara", "uu59"]
  spec.summary       = "Mixpanel input plugin for Embulk"
  spec.description   = "Loads records from Mixpanel."
  spec.email         = ["h.yoshihara@everyleaf.com", "k@uu59.org"]
  spec.licenses      = ["Apache2"]
  spec.homepage      = "https://github.com/treasure-data/embulk-input-mixpanel"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'httpclient'
  spec.add_dependency 'tzinfo'
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
  spec.add_development_dependency 'embulk', ['>= 0.6.16', '< 1.0']
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'test-unit-rr'
  spec.add_development_dependency 'codeclimate-test-reporter'
  spec.add_development_dependency 'everyleaf-embulk_helper'
end
