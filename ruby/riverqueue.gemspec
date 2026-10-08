# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name = "riverqueue"
  s.version = "0.13.0"
  s.summary = "A fast, reliable job queue for Ruby backed by PostgreSQL or SQLite."
  s.description = "Insert and work River jobs in Ruby using the same schema and state machine as River's Go client. Use with riverqueue-activerecord or riverqueue-sequel."
  s.authors = ["Blake Gentry", "Brandur Leach"]
  s.email = "brandur@brandur.org"
  s.files = Dir.glob("{exe,lib,migration,sig}/**/*") + ["CHANGELOG.md", "LICENSE", "README.md", "docs/README.md", "docs/conformance.md", "docs/migrations.md", "docs/testing.md", "docs/workers.md"]
  s.bindir = "exe"
  s.executables = ["river"]
  s.homepage = "https://riverqueue.com"
  s.license = "MPL-2.0"
  s.required_ruby_version = ">= 3.2"
  s.require_path = %(lib)
  s.metadata = {
    "bug_tracker_uri" => "https://github.com/riverqueue/river/issues",
    "changelog_uri" => "https://github.com/riverqueue/river/blob/master/ruby/CHANGELOG.md",
    "rubygems_mfa_required" => "true",
    "source_code_uri" => "https://github.com/riverqueue/river/tree/master/ruby"
  }

  # Standard-library components distributed as gems on modern Ruby.
  s.add_dependency "logger", "> 0", "< 1000"
  s.add_dependency "optparse", "> 0", "< 1000"
  s.add_dependency "securerandom", "> 0", "< 1000"
  s.add_dependency "timeout", "> 0", "< 1000"
end
