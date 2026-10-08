# frozen_string_literal: true

require_relative "support/test_database"
require "debug"

ENV["RIVERQUEUE_ROOT_TEST_SUITE"] = "1"

# Only show coverage information if running the entire suite.
if RSpec.configuration.files_to_run.length > 1
  require "simplecov"
  SimpleCov.start do
    enable_coverage :branch
    command_name RiverTestDatabase::BACKEND
    minimum_coverage branch: 100, line: 100 if RiverTestDatabase::BACKEND == "all"

    # Drivers have their own spec suite where they're covered 100.0%, but
    # they're not fully covered from this top level test suite.
    add_filter("driver/riverqueue-sequel/")
    add_filter("driver/riverqueue-activerecord/")
    add_filter("/spec/")
  end
end

require "riverqueue"
