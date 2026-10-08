# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name = "riverqueue-rails"
  s.version = "0.13.0"
  s.summary = "Active Job and Rails integration for River."
  s.authors = ["Blake Gentry", "Brandur Leach"]
  s.files = Dir.glob("lib/**/*") + ["README.md"]
  s.homepage = "https://riverqueue.com"
  s.license = "MPL-2.0"
  s.required_ruby_version = ">= 3.2"
  s.add_dependency "activejob", ">= 7.2", "< 8.2"
  s.add_dependency "railties", ">= 7.2", "< 8.2"
  s.add_dependency "riverqueue-activerecord", "= #{s.version}"
end
