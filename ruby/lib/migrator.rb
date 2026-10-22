# frozen_string_literal: true

module River
  # Runs River's canonical SQL migrations, committing each version separately.
  # Use a dedicated migration connection and stop workers before downgrading.
  class Migrator
    Migration = Data.define(:version, :name, :sql_up, :sql_down)
    Status = Data.define(:version, :name, :applied)

    attr_reader :migrations

    # Creates a migrator for an installed River driver. Postgres schemas must
    # already exist and use simple SQL identifiers; SQLite uses its main schema.
    # The optional migrations_path is a root containing backend/line/*.sql.
    def initialize(driver, line: "main", migrations_path: File.expand_path("../migration", __dir__.to_s), schema: nil)
      @backend = driver.migration_backend
      @driver = driver
      raise ArgumentError, "unsupported migration backend" unless [:mysql, :postgresql, :sqlite].include?(@backend)
      raise ArgumentError, "invalid migration line" unless line.match?(/\A[a-z][a-z0-9_]*\z/)
      raise ArgumentError, "SQLite does not support a migration schema option" if @backend == :sqlite && schema

      @line = line
      @mutex = Mutex.new
      @requested_schema = schema

      directory = File.join(migrations_path, @backend.to_s, line)
      @migrations = Dir.glob(File.join(directory, "*.up.sql")).sort.map do |path|
        version, name = File.basename(path).delete_suffix(".up.sql").split("_", 2)
        Migration.new(Integer(version.to_s, 10), name, File.read(path).freeze, File.read(path.sub(/\.up\.sql\z/, ".down.sql")).freeze)
      end.freeze
      unless @migrations.any? && @migrations.map(&:version) == (1..@migrations.length).to_a
        raise ArgumentError, "migration files must contain contiguous versions starting at 1"
      end
    end

    # Applies pending migrations up (all by default) or down (one by default).
    # target is the version to end at; target: 0 removes the migration line.
    # An up target that is already applied is a no-op.
    # steps limits the number applied. dry_run returns the plan without writes.
    # Returns Migration objects for the versions applied or planned.
    def migrate(direction: :up, dry_run: false, steps: nil, target: nil)
      raise ArgumentError, "direction must be up or down" unless [:up, :down].include?(direction)
      raise ArgumentError, "steps must be positive" if steps && (!steps.is_a?(Integer) || steps <= 0)
      raise ArgumentError, "target must be a bundled version or zero" if target && (!target.is_a?(Integer) || !(0..migrations.length).cover?(target))

      session(lock: !dry_run) do
        existing = existing_versions
        current = existing.last || 0
        destination = target || ((direction == :up) ? migrations.length : 0)
        if direction == :down && destination > current
          raise ArgumentError, "target is in the opposite direction"
        end

        plan = if direction == :up
          migrations.select { |migration| migration.version > current && migration.version <= destination }
        else
          migrations.reverse.select { |migration| migration.version <= current && migration.version > destination }
        end
        limit = steps || ((direction == :down && target.nil?) ? 1 : plan.length)
        plan = plan.first(limit)

        unless dry_run
          plan.each do |migration|
            execute((@backend == :sqlite) ? "BEGIN IMMEDIATE" : "BEGIN") unless @backend == :mysql
            committed = false
            begin
              raise River::Error, "migration state changed concurrently; rerun migration" unless existing_versions == existing

              if direction == :down && @line == "main" && line_column?
                raise River::Error, "remove non-main migration lines before downgrading main" if query("SELECT version FROM #{table} WHERE line <> 'main'").any?
              end

              sql = (direction == :up) ? migration.sql_up : migration.sql_down
              execute(sql.gsub("/* TEMPLATE: schema */", schema_prefix))

              if direction == :up
                columns, values = line_column? ? ["line, version", "'#{@line}', #{migration.version}"] : ["version", migration.version.to_s]
                execute("INSERT INTO #{table} (#{columns}) VALUES (#{values})")
                existing += [migration.version]
              else
                unless @line == "main" && migration.version == 1
                  filter = line_column? ? " AND line = '#{@line}'" : ""
                  execute("DELETE FROM #{table} WHERE version = #{migration.version}#{filter}")
                end

                existing -= [migration.version]
              end

              execute("COMMIT") unless @backend == :mysql
              committed = true
            ensure
              execute("ROLLBACK") unless committed || @backend == :mysql
            end
          end
        end

        plan
      end
    end

    # Returns the bundled versions with their database-applied status.
    def status
      session do
        existing = existing_versions
        migrations.map { |migration| Status.new(migration.version, migration.name, existing.include?(migration.version)) }
      end
    end

    private def execute(sql)
      if @backend == :postgresql
        @connection.exec(sql)
      elsif @backend == :mysql
        # MySQL DDL commits implicitly. Run statements separately without
        # requiring MULTI_STATEMENTS on the application's connection pool.
        mysql_statements(sql).each { |statement| @connection.query(statement) }
      else
        @connection.execute_batch(sql)
      end
    end

    private def existing_versions
      exists = if @backend == :postgresql
        query("SELECT to_regclass('#{table}') AS name").first.fetch("name")
      elsif @backend == :mysql
        query("SELECT table_name FROM information_schema.tables WHERE table_schema = '#{@schema}' AND table_name = 'river_migration'").any?
      else
        query("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'river_migration'").any?
      end
      if @line != "main"
        unless exists && line_column? && query("SELECT version FROM #{table} WHERE line = 'main' AND version >= 7").any?
          raise River::Error, "migrate main to version 7 or later before migrating #{@line}"
        end
      end

      return [] unless exists

      filter = line_column? ? " WHERE line = '#{@line}'" : ""
      versions = query("SELECT version FROM #{table}#{filter} ORDER BY version").map { |row| row.fetch("version").to_i }
      unless versions == (1..versions.length).to_a && versions.all? { |version| version <= migrations.length }
        raise River::Error, "database migration history is incomplete or newer than this gem"
      end

      versions
    end

    private def line_column?
      if [:mysql, :postgresql].include?(@backend)
        query("SELECT column_name FROM information_schema.columns WHERE table_schema = '#{@schema}' AND table_name = 'river_migration' AND column_name = 'line'").any?
      else
        query("PRAGMA table_info(river_migration)").any? { |row| row.fetch("name") == "line" }
      end
    end

    private def query(sql)
      if @backend == :postgresql
        @connection.exec(sql).to_a
      elsif @backend == :mysql
        @connection.query(sql, as: :hash, symbolize_keys: false).to_a
      else
        rows = @connection.execute2(sql)
        columns = rows.shift
        rows.map { |row| row.is_a?(Hash) ? row : columns.zip(row).to_h }
      end
    end

    private def session(lock: false)
      @mutex.synchronize do
        @driver.migration_connection do |connection|
          @connection = connection
          @schema = if @backend == :postgresql
            @requested_schema || query("SELECT current_schema() AS name").first.fetch("name")
          elsif @backend == :mysql
            @requested_schema || query("SELECT DATABASE() AS name").first.fetch("name")
          end
          if @backend == :mysql
            raise ArgumentError, "schema must be a simple SQL identifier" unless @schema&.match?(/\A[a-zA-Z_][a-zA-Z0-9_]{0,45}\z/)

            if lock
              locked = query("SELECT GET_LOCK('river_migrate:#{@schema}', 0) AS locked").first.fetch("locked")
              raise River::Error, "another migrator holds the database lock" unless locked == 1
            end
          end
          if @backend == :postgresql
            raise ArgumentError, "schema must be a simple SQL identifier" unless @schema&.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)

            if lock
              locked = query("SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext('river_migrate:#{@schema}')) AS locked").first.fetch("locked")
              raise River::Error, "another Ruby migrator holds the schema lock" unless [true, "t"].include?(locked)
            end
          end

          begin
            yield
          ensure
            if @backend == :postgresql && lock
              query("SELECT pg_advisory_unlock(hashtext(current_database()), hashtext('river_migrate:#{@schema}'))")
            elsif @backend == :mysql && lock
              query("SELECT RELEASE_LOCK('river_migrate:#{@schema}')")
            end
          end
        end
      end
    end

    private def table
      "#{schema_prefix}river_migration"
    end

    private def schema_prefix
      return "" unless @schema

      (@backend == :mysql) ? "`#{@schema}`." : %("#{@schema}".)
    end

    private def mysql_statements(sql)
      statements = [] #: Array[String]
      buffer = +""
      # Quoted text and comments are tokens so their semicolons cannot split
      # statements. River migrations contain plain SQL, not DELIMITER commands.
      sql.scan(/'(?:''|\\.|[^'\\])*'|"(?:""|\\.|[^"\\])*"|`(?:``|[^`])*`|--[^\n]*|\#[^\n]*|\/\*.*?\*\/|;|./m) do |token|
        token = token.to_s
        if token.start_with?("--", "#", "/*")
          buffer << " "
        elsif token == ";"
          statements << buffer.strip unless buffer.strip.empty?
          buffer = +""
        else
          buffer << token
        end
      end
      statements << buffer.strip unless buffer.strip.empty?
      statements
    end
  end
end
