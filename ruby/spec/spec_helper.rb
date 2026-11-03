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
    # A full run covers both backends without results cached by earlier runs.
    merging false if RiverTestDatabase::BACKEND == "all"

    # Drivers have their own spec suite where they're covered 100.0%, but
    # they're not fully covered from this top level test suite.
    skip("driver/riverqueue-sequel/")
    skip("driver/riverqueue-activerecord/")
    skip("/spec/")
  end
end

require "riverqueue"
