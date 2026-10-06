# frozen_string_literal: true

module River
  # Runs River's canonical SQL migrations, committing each version separately.
  # Use a dedicated migration connection and stop workers before downgrading.
  class Migrator
    Migration = Data.define(:version, :name, :sql_up, :sql_down)
    Status = Data.define(:version, :name, :applied)

    attr_reader :migrations

    # Creates a migrator for an installed River driver. PostgreSQL schemas must
    # already exist and use simple SQL identifiers; SQLite uses its main schema.
    # The optional migrations_path is a root containing backend/line/*.sql.
    def initialize(driver, line: "main", migrations_path: File.expand_path("../migration", __dir__.to_s), schema: nil)
      @backend = driver.migration_backend
      @driver = driver
      raise ArgumentError, "unsupported migration backend" unless [:postgresql, :sqlite].include?(@backend)
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
            execute((@backend == :sqlite) ? "BEGIN IMMEDIATE" : "BEGIN")
            committed = false
            begin
              raise River::Error, "migration state changed concurrently; rerun migration" unless existing_versions == existing

              if direction == :down && @line == "main" && line_column?
                raise River::Error, "remove non-main migration lines before downgrading main" if query("SELECT version FROM #{table} WHERE line <> 'main'").any?
              end

              sql = (direction == :up) ? migration.sql_up : migration.sql_down
              execute(sql.gsub("/* TEMPLATE: schema */", @schema ? %("#{@schema}".) : ""))

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

              execute("COMMIT")
              committed = true
            ensure
              execute("ROLLBACK") unless committed
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
      else
        @connection.execute_batch(sql)
      end
    end

    private def existing_versions
      exists = if @backend == :postgresql
        query("SELECT to_regclass('#{table}') AS name").first.fetch("name")
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
      if @backend == :postgresql
        query("SELECT column_name FROM information_schema.columns WHERE table_schema = '#{@schema}' AND table_name = 'river_migration' AND column_name = 'line'").any?
      else
        query("PRAGMA table_info(river_migration)").any? { |row| row.fetch("name") == "line" }
      end
    end

    private def query(sql)
      if @backend == :postgresql
        @connection.exec(sql).to_a
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
            end
          end
        end
      end
    end

    private def table
      @schema ? %("#{@schema}".river_migration) : "river_migration"
    end
  end
end
