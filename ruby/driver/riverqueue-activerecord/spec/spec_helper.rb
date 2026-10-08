# frozen_string_literal: true

require_relative "../../../spec/support/test_database"
require "active_record"
require "debug"
require "securerandom"
require_relative "../../../spec/support/river_sqlite_schema_fixture"

POSTGRES_TEST_CONFIG = if RiverTestDatabase.enabled?(:postgres)
  begin
    ActiveRecord::Base.establish_connection(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test")
    ActiveRecord::Base.connection.execute("SELECT 1")
    ActiveRecord::Base.connection_db_config.configuration_hash.merge(schema_search_path: "river_activerecord_test_#{SecureRandom.hex(8)}")
  rescue => e
    raise if ENV["CI"] == "true" || ENV["RIVER_REQUIRE_DATABASES"] == "1"

    warn "Postgres not available, skipping Postgres tests: #{e.message}"
    nil
  end
end

PG_AVAILABLE = !POSTGRES_TEST_CONFIG.nil?

def test_transaction
  ActiveRecord::Base.transaction do
    yield
    raise ActiveRecord::Rollback
  end
end

def switch_to_sqlite!
  ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
  RiverSQLiteSchemaFixture.load(ActiveRecord::Base.connection.raw_connection)
end

def switch_to_postgres!
  ActiveRecord::Base.establish_connection(POSTGRES_TEST_CONFIG)
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
require "riverqueue-activerecord"

if PG_AVAILABLE
  # Keep rollback-wrapped examples independent of data in the developer's
  # database, without repeatedly deleting and restoring it for every example.
  switch_to_postgres!
  schema = POSTGRES_TEST_CONFIG.fetch(:schema_search_path)
  ActiveRecord::Base.connection.execute("CREATE SCHEMA #{schema}")
  at_exit do
    switch_to_postgres!
    ActiveRecord::Base.connection.execute("DROP SCHEMA #{schema} CASCADE")
  ensure
    ActiveRecord::Base.connection_pool.disconnect!
  end
  River::Migrator.new(River::Driver::ActiveRecord.new).migrate
end

# Client helpers restore this connection after each isolated SQLite database.
switch_to_sqlite! unless PG_AVAILABLE
