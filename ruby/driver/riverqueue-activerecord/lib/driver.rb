# frozen_string_literal: true

require "securerandom"

module River::Driver
  # Provides an ActiveRecord driver for River supporting Postgres, YugabyteDB,
  # and SQLite.
  #
  # Used in conjunction with a River client like:
  #
  #   ActiveRecord::Base.establish_connection("postgres://...")
  #   client = River::Client.new(River::Driver::ActiveRecord.new)
  #
  class ActiveRecord
    include River::Driver::Runtime

    # Connection class whose pool and transaction context this driver uses.
    attr_reader :connection_class

    # Uses an established Postgres or SQLite connection. The class must be
    # ActiveRecord::Base or an abstract Active Record class. Routing follows its
    # current role/shard; configure consumers explicitly for each database.
    def initialize(connection_class: ::ActiveRecord::Base)
      unless connection_class.is_a?(Class) && connection_class <= ::ActiveRecord::Base
        raise ArgumentError, "connection_class must be an Active Record class"
      end

      unless connection_class == ::ActiveRecord::Base || connection_class.abstract_class?
        raise ArgumentError, "connection_class must be abstract"
      end

      @connection_class = connection_class
      @postgres_capabilities_cache = PostgresCapabilities::Cache.new
      @is_sqlite = connection_class.connection.adapter_name.downcase.include?("sqlite")
      # Do not inherit application scopes, callbacks, or schema caches. Each
      # driver owns its model, but all operations use the selected class's pool.
      @job_model = Class.new(::ActiveRecord::Base) do
        self.table_name = "river_job"
        define_singleton_method(:connection_pool) { connection_class.connection_pool }

        def self.dangerous_attribute_method?(method_name)
          return false if method_name == "errors"

          super
        end

        def errors = {}
      end
    end

    def job_get_by_id(id)
      if @is_sqlite
        row = sqlite_job_rows("WHERE id = ? LIMIT 1", [id]).first
        row ? sqlite_to_job_row_from_raw(row) : nil
      else
        row = @job_model.find_by(id: id)
        row ? to_job_row_from_model(row) : nil
      end
    end

    def job_insert(insert_params)
      job_insert_many([insert_params]).first
    end

    def job_insert_many(insert_params_many)
      return [] if insert_params_many.empty?

      return sqlite_job_insert_many(insert_params_many) if @is_sqlite

      transaction do
        results = postgres_job_insert_many(insert_params_many)
        postgres_notify_insert(insert_params_many)
        results
      end
    end

    def job_list(params = :all)
      return super unless params == :all

      runtime_job_rows("ORDER BY id")
    end

    def rollback_exception
      ::ActiveRecord::Rollback
    end

    # Backend used by River::Migrator.
    def migration_backend = @is_sqlite ? :sqlite : :postgresql

    # Pins a raw connection for migration SQL and refreshes cached model columns.
    def migration_connection
      @connection_class.connection_pool.with_connection do |connection|
        raise River::Error, "migrations cannot run inside an application transaction" if connection.transaction_open?

        begin
          yield connection.raw_connection
        ensure
          @job_model.reset_column_information
        end
      end
    end

    # Runs the block in a new Active Record transaction or savepoint and returns
    # the block's result.
    def transaction(&)
      @connection_class.transaction(requires_new: true, &)
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
    # text so ActiveRecord doesn't interpret timezone-less SQLite timestamps in
    # the process timezone. Flag 10 validates JSONB contents, not just its header,
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

    private def notification_query(sql)
      @connection_class.connection_pool.with_connection { runtime_query_rows(sql) }
    end

    private def postgres_insert_params_to_hash(insert_params, nonce)
      metadata = insert_params.metadata || {}
      metadata = metadata.merge(UNIQUE_INSERT_METADATA_KEY => nonce) if nonce
      {
        args: JSON.parse(insert_params.encoded_args),
        kind: insert_params.kind,
        max_attempts: insert_params.max_attempts,
        metadata: metadata,
        priority: insert_params.priority,
        queue: insert_params.queue,
        scheduled_at: insert_params.scheduled_at,
        state: insert_params.state,
        tags: insert_params.tags || [],
        unique_key: insert_params.unique_key,
        unique_states: insert_params.unique_states
      }
    end

    private def postgres_job_insert_many(insert_params_many)
      capabilities = postgres_capabilities
      nonce = SecureRandom.hex(8) if capabilities.unique_insert_mode == :metadata_nonce
      res = @job_model.upsert_all(
        insert_params_many.map { |param| postgres_insert_params_to_hash(param, nonce) },
        on_duplicate: Arel.sql("kind = river_job.kind"),
        returning: Arel.sql("*, #{capabilities.unique_insert_sql} AS unique_skipped_as_duplicate"),

        # It'd be nice to specify this as `(kind, unique_key) WHERE unique_key
        # IS NOT NULL` like we do elsewhere, but in its pure ingenuity, fucking
        # ActiveRecord tries to look up a unique index instead of letting
        # Postgres handle that, and of course it doesn't support a `WHERE`
        # clause. The workaround is to target the index name instead of columns.
        unique_by: "river_job_unique_idx"
      )
      postgres_to_insert_results(res, nonce)
    end

    private def postgres_to_insert_results(res, nonce)
      res.rows.map do |row|
        job, duplicate = postgres_to_job_row_from_raw(row, res.columns, res.column_types)
        [job, nonce ? job.metadata[UNIQUE_INSERT_METADATA_KEY] != nonce : duplicate]
      end
    end

    private def postgres_to_job_row_from_model(river_job)
      # Read attributes directly because Active Record shadows the errors column.
      runtime_decode_postgres_job(river_job.attributes)
    end

    # Upserts bypass model casting; normalize their values before shared decoding.
    private def postgres_to_job_row_from_raw(row, columns, column_types)
      river_job = {}

      row.each_with_index do |val, i|
        river_job[columns[i]] = column_types[i].deserialize(val)
      end

      [runtime_decode_postgres_job(river_job), river_job["unique_skipped_as_duplicate"]]
    end

    private def runtime_connection_pool
      @connection_class.connection_pool
    end

    private def runtime_execute(sql)
      @connection_class.connection.execute(sql)
    end

    private def runtime_job_list_without_params
      job_list(:all)
    end

    private def runtime_job_rows(suffix)
      if @is_sqlite
        sqlite_job_rows(suffix).map { |row| sqlite_to_job_row_from_raw(row) }
      else
        @job_model.find_by_sql("SELECT * FROM river_job #{suffix}").map { |row| to_job_row_from_model(row) }
      end
    end

    private def runtime_notification_connection
      params, schema = @connection_class.connection_pool.with_connection do |connection|
        [connection.raw_connection.conninfo_hash, connection.select_value("SELECT current_schema()")]
      end
      params.compact!
      params[:connect_timeout] = "5" unless params[:connect_timeout].to_i.positive?
      [::PG.connect(params), schema]
    end

    private def runtime_postgres?
      !@is_sqlite
    end

    private def runtime_query_rows(sql)
      if @is_sqlite
        @connection_class.connection.raw_connection.execute(sql).to_a
      else
        @connection_class.connection.select_all(sql).to_a
      end
    end

    private def runtime_quote(value)
      @connection_class.connection.quote(value)
    end

    private def runtime_unique_violation_class
      ::ActiveRecord::RecordNotUnique
    end

    private def runtime_value(row, key)
      row[key.to_s] || row[key]
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
    private def sqlite_job_insert_many(insert_params_many)
      nonces = sqlite_insert_nonces(insert_params_many)
      @connection_class.transaction(requires_new: true) do
        jobs = insert_params_many.zip(nonces).map { |param, nonce| sqlite_insert_params_to_hash(param, nonce) }
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

        rows = @connection_class.connection.raw_connection.execute(sql, [JSON.generate(jobs)])
        sqlite_notify_insert(insert_params_many)

        rows.map do |row|
          metadata = JSON.parse(row["metadata"])
          [sqlite_to_job_row_from_raw(row), !inserted_nonces.key?(metadata[UNIQUE_INSERT_METADATA_KEY])]
        end
      end
    end

    private def sqlite_job_rows(suffix, binds = [])
      sql = "SELECT #{SQLITE_JOB_COLUMNS} FROM river_job #{suffix}"
      @connection_class.connection.raw_connection.execute(sql, binds)
    end

    private def sqlite_notify_insert(insert_params_many)
      queues = insert_params_many
        .select { |param| param.state == ::River::JOB_STATE_AVAILABLE }
        .map(&:queue)
        .uniq
      return if queues.empty?

      notifications = queues.map do |queue|
        {payload: JSON.generate({queue: queue}), topic: "river_insert"}
      end

      @connection_class.connection.raw_connection.execute(<<~SQL, [JSON.generate(notifications)])
        INSERT INTO river_notification (payload, topic)
        SELECT
          json_extract(value, '$.payload'),
          json_extract(value, '$.topic')
        FROM json_each(cast(? AS blob))
      SQL
    end

    private def sqlite_to_job_row_from_model(river_job)
      row = sqlite_job_rows("WHERE id = ? LIMIT 1", [river_job.id]).first
      sqlite_to_job_row_from_raw(row)
    end

    private def sqlite_to_job_row_from_raw(row)
      runtime_decode_sqlite_job(row)
    end

    private def to_job_row_from_model(river_job)
      @is_sqlite ? sqlite_to_job_row_from_model(river_job) : postgres_to_job_row_from_model(river_job)
    end
  end
end
