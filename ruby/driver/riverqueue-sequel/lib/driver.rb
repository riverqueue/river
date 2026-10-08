# frozen_string_literal: true

require "securerandom"

module River::Driver
  # Provides a Sequel driver for River supporting Postgres, YugabyteDB, and SQLite.
  #
  # Used in conjunction with a River client like:
  #
  #   DB = Sequel.connect("postgres://...")
  #   client = River::Client.new(River::Driver::Sequel.new(DB))
  #
  # Or with SQLite:
  #
  #   DB = Sequel.connect("sqlite://path/to/river.db")
  #   client = River::Client.new(River::Driver::Sequel.new(DB))
  #
  class Sequel
    include River::Driver::Runtime

    # Creates a driver backed by a connected Sequel::Database for Postgres or
    # SQLite.
    def initialize(db)
      @db = db
      @postgres_capabilities_cache = PostgresCapabilities::Cache.new
      @is_sqlite = (db.database_type == :sqlite)

      unless @is_sqlite
        db.extension(:pg_array)
        db.extension(:pg_json)
      end
    end

    def job_get_by_id(id)
      if @is_sqlite
        row = sqlite_job_rows("WHERE id = ? LIMIT 1", id).first
        row ? sqlite_to_job_row_from_raw(row) : nil
      else
        row = @db[:river_job].where(id: id).first
        row ? to_job_row(row) : nil
      end
    end

    def job_insert(insert_params)
      job_insert_many([insert_params]).first
    end

    def job_insert_many(insert_params_array)
      return [] if insert_params_array.empty?

      return sqlite_job_insert_many(insert_params_array) if @is_sqlite

      transaction do
        results = postgres_job_insert_many(insert_params_array)
        postgres_notify_insert(insert_params_array)
        results
      end
    end

    def job_list(params = :all)
      return super unless params == :all

      runtime_job_rows("ORDER BY id")
    end

    def rollback_exception
      ::Sequel::Rollback
    end

    # Backend used by River::Migrator.
    def migration_backend = @is_sqlite ? :sqlite : :postgresql

    # Pins a raw connection for the complete migration operation.
    def migration_connection
      @db.synchronize do |connection|
        raise River::Error, "migrations cannot run inside an application transaction" if @db.in_transaction?

        yield connection
      end
    end

    # Runs the block in a Sequel transaction or savepoint and returns the
    # block's result.
    def transaction
      block_error = nil
      @db.transaction(savepoint: true) do
        yield
      rescue ArgumentError => error
        block_error = error
        raise
      end
    rescue ::Sequel::DatabaseError => error
      # SQLite treats ArgumentError as an adapter error, even when it came
      # from an application callback. Preserve that error after rollback.
      raise block_error if block_error && error.wrapped_exception.equal?(block_error)

      raise
    end

    SQLITE_CONFLICT_WHERE = <<~SQL.chomp.freeze
      unique_key IS NOT NULL
          AND unique_states IS NOT NULL
          AND CASE state
            WHEN 'available' THEN unique_states & (1 << 0)
            WHEN 'cancelled' THEN unique_states & (1 << 1)
            WHEN 'completed' THEN unique_states & (1 << 2)
            WHEN 'discarded' THEN unique_states & (1 << 3)
            WHEN 'pending'   THEN unique_states & (1 << 4)
            WHEN 'retryable' THEN unique_states & (1 << 5)
            WHEN 'running'   THEN unique_states & (1 << 6)
            WHEN 'scheduled' THEN unique_states & (1 << 7)
            ELSE 0
          END >= 1
    SQL

    # SQLite 3.45+ may store JSON as binary JSONB. Always project JSON columns
    # through json() so this driver can read both the current JSONB format and
    # the text JSON used by River migrations through version 006. Cast times to
    # text so Sequel doesn't interpret timezone-less SQLite timestamps in the
    # process timezone. Flag 10 validates JSONB contents, not just its header,
    # so damaged blobs reach the row decoder without aborting the SQL query.
    SQLITE_JOB_COLUMNS = <<~SQL.chomp.freeze
      id,
      CASE WHEN json_valid(args, 10) THEN json(args) ELSE args END AS args,
      attempt,
      CAST(attempted_at AS text) AS attempted_at,
      CASE WHEN json_valid(attempted_by, 10) THEN json(attempted_by) ELSE attempted_by END AS attempted_by,
      CAST(created_at AS text) AS created_at,
      CASE WHEN json_valid(errors, 10) THEN json(errors) ELSE errors END AS errors,
      CAST(finalized_at AS text) AS finalized_at,
      kind,
      max_attempts,
      CASE WHEN json_valid(metadata, 10) THEN json(metadata) ELSE metadata END AS metadata,
      priority,
      queue,
      state,
      CAST(scheduled_at AS text) AS scheduled_at,
      CASE WHEN json_valid(tags, 10) THEN json(tags) ELSE tags END AS tags,
      unique_key,
      unique_states
    SQL

    UNIQUE_INSERT_METADATA_KEY = "river:unique_nonce"

    private_constant :SQLITE_CONFLICT_WHERE, :SQLITE_JOB_COLUMNS, :UNIQUE_INSERT_METADATA_KEY

    private def format_time(time)
      time.getutc.round(3).strftime("%Y-%m-%d %H:%M:%S.%3N")
    end

    private def postgres_insert_params_to_hash(insert_params, nonce)
      metadata = insert_params.metadata || {}
      metadata = metadata.merge(UNIQUE_INSERT_METADATA_KEY => nonce) if nonce
      {
        args: insert_params.encoded_args,
        kind: insert_params.kind,
        max_attempts: insert_params.max_attempts,
        metadata: ::Sequel.pg_jsonb(metadata),
        priority: insert_params.priority,
        queue: insert_params.queue,
        scheduled_at: insert_params.scheduled_at,
        state: insert_params.state,
        tags: ::Sequel.pg_array(insert_params.tags || [], :text),
        unique_key: insert_params.unique_key ? ::Sequel.blob(insert_params.unique_key) : nil,
        unique_states: insert_params.unique_states
      }
    end

    private def postgres_job_insert_many(insert_params_array)
      capabilities = postgres_capabilities
      nonce = SecureRandom.hex(8) if capabilities.unique_insert_mode == :metadata_nonce
      @db[:river_job]
        .insert_conflict(
          conflict_where: ::Sequel.lit(
            "unique_key IS NOT NULL AND unique_states IS NOT NULL AND river_job_state_in_bitmask(unique_states, state)"
          ),
          target: [:unique_key],
          update: {kind: ::Sequel[:river_job][:kind]}
        )
        .returning(::Sequel.lit("*, #{capabilities.unique_insert_sql} AS unique_skipped_as_duplicate"))
        .multi_insert(insert_params_array.map { |p| postgres_insert_params_to_hash(p, nonce) })
        .map do |row|
          job = to_job_row(row)
          [job, nonce ? job.metadata[UNIQUE_INSERT_METADATA_KEY] != nonce : row[:unique_skipped_as_duplicate]]
        end
    end

    private def postgres_to_job_row(river_job)
      runtime_decode_postgres_job(river_job)
    end

    private def runtime_connection_pool
      @db
    end

    private def runtime_execute(sql)
      @db.run(sql)
    end

    private def runtime_job_list_without_params
      job_list(:all)
    end

    private def runtime_job_rows(suffix)
      if @is_sqlite
        sqlite_job_rows(suffix).map { |row| sqlite_to_job_row_from_raw(row) }
      else
        @db.fetch("SELECT * FROM river_job #{suffix}").map { |row| to_job_row(row) }
      end
    end

    private def runtime_postgres?
      !@is_sqlite
    end

    private def runtime_query_rows(sql)
      @db.fetch(sql).all
    end

    private def runtime_quote(value)
      @db.literal(value)
    end

    private def runtime_unique_violation_class
      ::Sequel::UniqueConstraintViolation
    end

    private def runtime_value(row, key)
      row[key] || row[key.to_s]
    end

    private def sqlite_insert_params_to_hash(insert_params, nonce)
      {
        args: JSON.parse(insert_params.encoded_args),
        kind: insert_params.kind,
        max_attempts: insert_params.max_attempts,
        metadata: {UNIQUE_INSERT_METADATA_KEY => nonce},
        priority: insert_params.priority,
        queue: insert_params.queue,
        scheduled_at: insert_params.scheduled_at ? format_time(insert_params.scheduled_at) : nil,
        state: insert_params.state,
        tags: insert_params.tags || [],
        unique_key: insert_params.unique_key&.unpack1("H*"),
        unique_states: insert_params.unique_states&.to_i(2)
      }.tap { |values| values[:metadata] = (insert_params.metadata || {}).merge(values[:metadata]) }
    end

    # River's current SQLite driver uses json_each to make a batch a single,
    # atomic statement. The JSON columns are converted to SQLite JSONB here,
    # matching migration 007 and newer River databases.
    private def sqlite_job_insert_many(insert_params_array)
      nonces = sqlite_insert_nonces(insert_params_array)
      @db.transaction(savepoint: true) do
        jobs = insert_params_array.zip(nonces).map { |param, nonce| sqlite_insert_params_to_hash(param, nonce) }
        inserted_nonces = nonces.to_h { |nonce| [nonce, true] }

        sql = <<~SQL
          INSERT INTO river_job (
            args,
            created_at,
            kind,
            max_attempts,
            metadata,
            priority,
            queue,
            scheduled_at,
            state,
            tags,
            unique_key,
            unique_states
          )
          SELECT
            jsonb(json_extract(value, '$.args')),
            datetime('now', 'subsec'),
            cast(json_extract(value, '$.kind') AS text),
            cast(json_extract(value, '$.max_attempts') AS integer),
            jsonb(json_extract(value, '$.metadata')),
            cast(json_extract(value, '$.priority') AS integer),
            cast(json_extract(value, '$.queue') AS text),
            coalesce(cast(json_extract(value, '$.scheduled_at') AS text), datetime('now', 'subsec')),
            cast(json_extract(value, '$.state') AS text),
            jsonb(json_extract(value, '$.tags')),
            CASE
              WHEN length(cast(json_extract(value, '$.unique_key') AS text)) = 0 THEN NULL
              ELSE unhex(cast(json_extract(value, '$.unique_key') AS text))
            END,
            nullif(cast(json_extract(value, '$.unique_states') AS integer), 0)
          FROM json_each(cast(? AS blob))
          WHERE true
          ON CONFLICT (unique_key) WHERE #{SQLITE_CONFLICT_WHERE}
          DO UPDATE SET kind = river_job.kind
          RETURNING #{SQLITE_JOB_COLUMNS}
        SQL

        rows = @db.fetch(sql, JSON.generate(jobs)).all
        sqlite_notify_insert(insert_params_array)

        rows.map do |row|
          metadata = JSON.parse(row[:metadata])
          [sqlite_to_job_row_from_raw(row), !inserted_nonces.key?(metadata[UNIQUE_INSERT_METADATA_KEY])]
        end
      end
    end

    private def sqlite_job_rows(suffix, *binds)
      @db.fetch("SELECT #{SQLITE_JOB_COLUMNS} FROM river_job #{suffix}", *binds).all
    end

    private def sqlite_notify_insert(insert_params_array)
      queues = insert_params_array
        .select { |param| param.state == ::River::JOB_STATE_AVAILABLE }
        .map(&:queue)
        .uniq
      return if queues.empty?

      @db[:river_notification].multi_insert(queues.map do |queue|
        {payload: JSON.generate({queue: queue}), topic: "river_insert"}
      end)
    end

    private def sqlite_to_job_row_from_raw(row)
      runtime_decode_sqlite_job(row)
    end

    private def to_job_row(river_job)
      if @is_sqlite
        row = sqlite_job_rows("WHERE id = ? LIMIT 1", river_job[:id]).first
        sqlite_to_job_row_from_raw(row)
      else
        postgres_to_job_row(river_job)
      end
    end
  end
end
