# frozen_string_literal: true

require "securerandom"
require "tmpdir"
require_relative "river_test_schema"

# Client tests need committed data and independent connections, unlike the
# rollback-wrapped driver contracts. Never run their workers against public
# tables: migrate an empty disposable schema with the bundled canonical SQL.
module ClientTestDatabase
  def self.with_active_record(adapter, migrate: true, pg_catalog_last: false)
    original = ActiveRecord::Base.connection_db_config.configuration_hash
    if adapter == :postgres
      ActiveRecord::Base.establish_connection(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test")
      schema = "river_client_test_#{SecureRandom.hex(8)}"
      config = ActiveRecord::Base.connection_db_config.configuration_hash
      search_path = pg_catalog_last ? "#{schema},pg_catalog" : "#{schema},public"
      ActiveRecord::Base.establish_connection(config.merge(pool: 20, schema_search_path: search_path))
      # Create and drop through the test pool to avoid opening separate admin
      # connections for every example. Postgres permits a not-yet-created
      # schema in search_path.
      ActiveRecord::Base.connection.execute("CREATE SCHEMA #{schema}")
      schema_created = true
      driver = River::Driver::ActiveRecord.new
      RiverTestSchema.load(driver) if migrate

      yield driver
    else
      Dir.mktmpdir("river-client-test-") do |directory|
        ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: File.join(directory, "river.sqlite3"), pool: 20, timeout: 5_000)
        begin
          driver = River::Driver::ActiveRecord.new
          RiverTestSchema.load(driver) if migrate

          yield driver
        ensure
          ActiveRecord::Base.connection_pool.disconnect!
        end
      end
    end
  ensure
    begin
      ActiveRecord::Base.connection.execute("DROP SCHEMA #{schema} CASCADE") if schema_created
    ensure
      ActiveRecord::Base.establish_connection(original)
    end
  end

  def self.with_sequel(adapter, migrate: true, pg_catalog_last: false)
    if adapter == :postgres
      schema = "river_client_test_#{SecureRandom.hex(8)}"
      search_path = pg_catalog_last ? "#{schema},pg_catalog" : "#{schema},public"
      database = Sequel.connect(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test", max_connections: 20, search_path: search_path)
      database.run("CREATE SCHEMA #{schema}")
      schema_created = true
      driver = River::Driver::Sequel.new(database)
      RiverTestSchema.load(driver) if migrate

      yield driver
    else
      Dir.mktmpdir("river-client-test-") do |directory|
        database = Sequel.sqlite(File.join(directory, "river.sqlite3"), max_connections: 4, timeout: 5_000)
        begin
          driver = River::Driver::Sequel.new(database)
          RiverTestSchema.load(driver) if migrate

          yield driver
        ensure
          database.disconnect
        end
      end
    end
  ensure
    begin
      database.run("DROP SCHEMA #{schema} CASCADE") if schema_created
    ensure
      database&.disconnect
    end
  end
end
