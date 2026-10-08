# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name = "riverqueue-activerecord"
  s.version = "0.13.0"
  s.summary = "ActiveRecord PostgreSQL and SQLite driver for the River Ruby gem."
  s.description = "ActiveRecord PostgreSQL and SQLite driver for inserting and working River jobs in Ruby."
  s.authors = ["Blake Gentry", "Brandur Leach"]
  s.email = "brandur@brandur.org"
  s.files = Dir.glob("lib/**/*")
  s.homepage = "https://riverqueue.com"
  s.license = "MPL-2.0"
  s.required_ruby_version = ">= 3.2"
  # The stupid version bounds are used to silence Ruby's extremely obnoxious warnings.
  s.add_dependency "activerecord", "> 0", "< 1000"
  s.add_dependency "activesupport", "> 0", "< 1000" # required for ActiveRecord to load properly
  s.add_dependency "riverqueue", "= #{s.version}"
end
