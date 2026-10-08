# frozen_string_literal: true

require_relative "../../../spec/support/test_database"
require "sequel"
require "securerandom"
require_relative "../../../spec/support/river_sqlite_schema_fixture"

SEQUEL_TEST_SCHEMA = "river_sequel_test_#{SecureRandom.hex(8)}"

DB = if RiverTestDatabase.enabled?(:postgres)
  begin
    Sequel.connect(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test", search_path: SEQUEL_TEST_SCHEMA)
  rescue => e
    raise if ENV["CI"] == "true" || ENV["RIVER_REQUIRE_DATABASES"] == "1"

    warn "PostgreSQL not available, skipping PostgreSQL tests: #{e.message}"
    nil
  end
end

SQLITE_DB = if RiverTestDatabase.enabled?(:sqlite)
  begin
    require "sqlite3"
    Sequel.sqlite.tap do |db|
      db.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) }
    end
  rescue LoadError
    raise if ENV["CI"] == "true" || ENV["RIVER_REQUIRE_DATABASES"] == "1"

    warn "sqlite3 gem not available, skipping SQLite tests"
    nil
  end
end

def test_transaction
  DB.transaction do
    yield
    raise Sequel::Rollback
  end
end

def sqlite_test_transaction
  SQLITE_DB.transaction do
    yield
    raise Sequel::Rollback
  end
end

def available_test_database
  DB || SQLITE_DB
end

def available_test_transaction(&)
  if DB
    test_transaction(&)
  elsif SQLITE_DB
    sqlite_test_transaction(&)
  else
    skip "PostgreSQL and SQLite are unavailable"
  end
end

unless ENV["RIVERQUEUE_ROOT_TEST_SUITE"]
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
    enable_coverage :branch
    command_name RiverTestDatabase::BACKEND
    minimum_coverage branch: 100, line: 100 if RiverTestDatabase::BACKEND == "all"
  end
end

require "riverqueue"
require "riverqueue-sequel"

if DB
  # An empty, suite-local schema makes rollback sufficient between examples.
  # Deleting and rolling back shared tables repeatedly scans any developer data.
  DB.run("CREATE SCHEMA #{SEQUEL_TEST_SCHEMA}")
  at_exit do
    DB.run("DROP SCHEMA #{SEQUEL_TEST_SCHEMA} CASCADE")
  ensure
    DB.disconnect
  end
  River::Migrator.new(River::Driver::Sequel.new(DB)).migrate
end
