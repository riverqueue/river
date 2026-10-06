# frozen_string_literal: true

require "tmpdir"
require "securerandom"

# Creates a second isolated database/schema without changing Base's connection.
module ConnectionClassTestDatabase
  def self.with_class(connection_class, backend)
    Dir.mktmpdir("river-connection-class-") do |directory|
      if backend == :postgres
        schema = "river_connection_test_#{SecureRandom.hex(8)}"
        ActiveRecord::Base.connection.execute("CREATE SCHEMA #{schema}")
        config = ActiveRecord::Base.connection_db_config.configuration_hash.merge(pool: 20, schema_search_path: schema)
      else
        config = {adapter: "sqlite3", database: File.join(directory, "river.sqlite3"), pool: 20, timeout: 5_000}
      end

      connection_class.establish_connection(config)
      yield River::Driver::ActiveRecord.new(connection_class: connection_class)
    ensure
      connection_class.remove_connection
      ActiveRecord::Base.connection.execute("DROP SCHEMA #{schema} CASCADE") if schema
    end
  end
end
