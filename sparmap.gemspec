# frozen_string_literal: true

require_relative "lib/sparmap/version"

Gem::Specification.new do |spec|
  spec.name          = "sparmap"
  spec.version       = Sparmap::VERSION
  spec.authors       = ["Giuliano Mega"]
  spec.email         = ["giuliano.mega@gmail.com"]
  spec.summary       = "A Simple PARallel Map for Ruby"
  spec.description   = <<-eof
    A Simple PARallel MAP for Ruby. Based on the Python Pool API, implements an unordered map operation 
    which runs on threads (thus, good for I/O bound workloads only).
  eof
  spec.homepage      = "https://github.com/gmega/sparmap-ruby"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.6.3")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.require_paths = ["lib"]

  spec.add_dependency "concurrent-ruby", "~> 1.1.8"
end
