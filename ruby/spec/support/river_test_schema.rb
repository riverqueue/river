# frozen_string_literal: true

# Only for empty, disposable test databases. Migration tests use the real
# migrator; other integration tests don't need its per-version history queries
# and advisory locks. Replay the same SQL, without maintaining a schema copy.
module RiverTestSchema
  def self.load(driver)
    migrations = River::Migrator.new(driver).migrations
    sql = migrations.map do |migration|
      # PostgreSQL must commit new enum values before later migrations use them.
      "BEGIN;\n#{migration.sql_up}\n;COMMIT;"
    end.join("\n")
    versions = migrations.map { |migration| "('main', #{migration.version})" }.join(", ")
    sql << "\nBEGIN; INSERT INTO /* TEMPLATE: schema */river_migration (line, version) VALUES #{versions}; COMMIT;"

    driver.migration_connection do |connection|
      if driver.migration_backend == :postgresql
        schema = connection.quote_ident(connection.exec("SELECT current_schema()").getvalue(0, 0))
        execute = ->(statement) { connection.exec(statement) }
        sql = sql.gsub("/* TEMPLATE: schema */", "#{schema}.")
      else
        execute = ->(statement) { connection.execute_batch(statement) }
        sql = sql.gsub("/* TEMPLATE: schema */", "")
      end

      loaded = false
      begin
        execute.call(sql)
        loaded = true
      ensure
        execute.call("ROLLBACK") unless loaded
      end
    end
  end
end
