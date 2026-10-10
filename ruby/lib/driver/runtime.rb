# frozen_string_literal: true

require "time"

module River::Driver
  # Database operations used by River's worker runtime. Driver gems implement a
  # small set of raw SQL primitives and include this module so the state machine
  # stays identical across ActiveRecord and Sequel. Those private primitives
  # provide execution, quoting, row access, and connection management inside
  # driver modules; callers use the driver's public operations and Ruby values.
  module Runtime
    def init_driver
      postgres_capabilities if runtime_postgres?
    end

    # Returns the UTC timestamp that writing and reading +time+ preserves.
    def normalize_time(time)
      runtime_postgres? ? time.getutc.floor(6) : time.getutc.round(3)
    end

    def postgres_capabilities
      @postgres_capabilities_cache.fetch(runtime_connection_pool) do
        row = runtime_query_rows(<<~SQL).first
          SELECT version()::text AS product,
                 current_setting('server_version_num')::int AS version_num,
                 coalesce(current_setting('yb_enable_listen_notify', true), 'off')::boolean AS yb_listen_notify_enabled
        SQL
        PostgresCapabilities.new(
          product: runtime_value(row, :product),
          version_num: runtime_value(row, :version_num),
          yb_listen_notify_enabled: runtime_value(row, :yb_listen_notify_enabled) == true
        )
      end
    end

    # Notifications and rows must commit together. The insertion drivers call
    # this inside their transaction; Postgres defers delivery until commit.
    private def postgres_notify_insert(params)
      queues = params.select { |param| param.state == River::JOB_STATE_AVAILABLE }.map(&:queue).uniq
      return if queues.empty?
      return unless postgres_capabilities.supports_listen_notify

      payloads = queues.map { |queue| "(#{runtime_quote(JSON.generate(queue: queue))})" }.join(",")
      runtime_execute("SELECT pg_notify(current_schema() || '.river_insert', payload) FROM (VALUES #{payloads}) AS notifications(payload)")
    end

    def job_cancel(id, now: Time.now.utc)
      original_id = Integer(id)
      transaction do
        updated_id = runtime_returning_ids(<<~SQL).first
          UPDATE river_job
          SET state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
              finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE #{runtime_time(now)} END,
              metadata = #{runtime_merge_metadata("cancel_attempted_at" => now.iso8601(6))}
          WHERE id = #{original_id}
            AND state NOT IN ('cancelled', 'completed', 'discarded')
            AND finalized_at IS NULL
          RETURNING id
        SQL
        row = job_get_by_id(updated_id || original_id)
        # Other engines interrupt running workers through this notification;
        # the durable marker remains Ruby's polling fallback.
        runtime_notify("river_control", action: "cancel", job_id: row.id, queue: row.queue) if updated_id
        row
      end
    end

    def job_claim(id:, attempted_by:, allow_scheduled: false, now: Time.now.utc)
      predicate = "id = #{Integer(id)} AND state IN ('available', 'retryable', 'scheduled')"
      predicate += " AND scheduled_at <= #{runtime_time(now)}" unless allow_scheduled

      runtime_claim_jobs(predicate, 1, attempted_by, now).first
    end

    # The cancellation predicate must be part of the completion update so it is
    # checked against the row we lock, including concurrent cancellations.
    # :cancelled asks the runtime to apply its normal cancellation/error hooks.
    def job_complete(id:, finalized_at:, expected_job: nil, metadata: nil, now: Time.now.utc)
      id = Integer(id)
      predicate = "id = #{id}"
      predicate += " AND #{runtime_job_attempt_predicate(expected_job)}" if expected_job
      assignments = ["state = 'completed'", "finalized_at = #{runtime_time(finalized_at)}"]
      assignments << "metadata = #{runtime_merge_metadata(metadata)}" unless metadata.nil? || metadata.empty?

      completed = runtime_returning_jobs(runtime_update_running_job(id, assignments, expected_job: expected_job, uncancelled: true)).first
      return completed if completed

      :cancelled if runtime_returning_ids("SELECT id FROM river_job WHERE #{predicate} AND (#{runtime_cancel_attempted})").any?
    end

    def job_delete(id)
      existing = job_get_by_id(id)
      return nil unless existing
      return existing if existing.state == River::JOB_STATE_RUNNING

      deleted_id = runtime_returning_ids("DELETE FROM river_job WHERE id = #{Integer(id)} AND state != 'running' RETURNING id").first
      deleted_id ? existing : job_get_by_id(id)
    end

    def job_delete_finalized(retention:, now: Time.now.utc, max: 1_000)
      clauses = retention.filter_map do |state, seconds|
        next unless seconds

        "(state = #{runtime_quote(state.to_s)} AND finalized_at < #{runtime_time(now - seconds)})"
      end

      return 0 if clauses.empty?

      ids = runtime_query_rows(<<~SQL).map { |row| runtime_value(row, :id).to_i }
        SELECT id FROM river_job WHERE #{clauses.join(" OR ")} ORDER BY id LIMIT #{Integer(max)}
      SQL
      return 0 if ids.empty?

      runtime_returning_ids(<<~SQL).length
        DELETE FROM river_job
        WHERE id IN (#{ids.join(",")}) AND (#{clauses.join(" OR ")})
        RETURNING id
      SQL
    end

    # A finalization hook's deletion must honor the same cancellation marker as
    # completion. Return :cancelled so the runtime can record the cancellation.
    def job_delete_if_running(id)
      id = Integer(id)
      deleted = runtime_returning_ids("#{runtime_delete_running_job(id, uncancelled: true)} RETURNING river_job.id").any?
      return true if deleted
      return :cancelled if job_get_cancelled_ids([id]).include?(id)

      false
    end

    def job_delete_many(params)
      transaction do
        jobs = runtime_job_list(params, for_delete: true)
        next [] if jobs.empty?

        ids = jobs.map(&:id)
        deleted = runtime_returning_ids(<<~SQL)
          DELETE FROM river_job
          WHERE id IN (#{ids.join(",")}) AND state != 'running'
          RETURNING id
        SQL
        jobs.select { |job| deleted.include?(job.id) }
      end
    end

    def job_get_available(queue:, max:, attempted_by:, kinds: nil, now: Time.now.utc)
      return [] if kinds&.empty?

      predicate = "state = 'available' AND queue = #{runtime_quote(queue)} AND scheduled_at <= #{runtime_time(now)}"
      predicate += " AND #{runtime_in_clause("kind", kinds)}" if kinds
      runtime_claim_jobs(predicate, max, attempted_by, now)
    end

    def job_get_cancelled_ids(ids)
      return [] if ids.empty?

      runtime_query_rows("SELECT id FROM river_job WHERE id IN (#{ids.map { |id| Integer(id) }.join(",")}) AND (#{runtime_cancel_attempted}) ORDER BY id")
        .map { |row| runtime_value(row, :id).to_i }
    end

    def job_list(params = nil)
      params ||= River::JobListParams.new
      return runtime_job_list_without_params if params == :all

      runtime_job_list(params)
    end

    # When supplied, expected_job prevents metadata from crossing into another
    # attempt or state. Keep the update and returned snapshot in one transaction.
    def job_metadata_merge(id, metadata, expected_job = nil)
      predicate = "id = #{Integer(id)}"
      if expected_job
        predicate += " AND #{runtime_job_attempt_predicate(expected_job)}"
        predicate += " AND state = #{runtime_quote(expected_job.state)}"
      end

      transaction do
        updated_id = runtime_returning_ids(<<~SQL).first
          UPDATE river_job
          SET metadata = #{runtime_merge_metadata(metadata)}
          WHERE #{predicate}
          RETURNING id
        SQL
        updated_id ? job_get_by_id(updated_id) : nil
      end
    end

    def job_rescue_stuck(horizon:, retry_policy:, now: Time.now.utc, max: 1_000, logger: nil, rescue_if: nil)
      max = Integer(max)
      transaction do
        # Lock each candidate until its transition finishes. Continue past jobs
        # that are still within their timeouts without consuming the rescue limit.
        after_id = rescued = 0
        lock = runtime_postgres? ? "FOR UPDATE SKIP LOCKED" : ""
        while rescued < max
          ids = runtime_returning_ids(<<~SQL)
            SELECT id FROM river_job
            WHERE state = 'running' AND attempted_at < #{runtime_time(horizon)} AND id > #{after_id}
            ORDER BY id LIMIT #{max - rescued} #{lock}
          SQL
          break if ids.empty?

          after_id = ids.last
          ids.each do |id|
            job = runtime_read_job(id)
            cancelled = job.metadata.key?("cancel_attempted_at")
            next if !cancelled && rescue_if && !rescue_if.call(job, now)
            final = cancelled || job.attempt >= job.max_attempts
            state = if cancelled
              River::JOB_STATE_CANCELLED
            elsif final
              River::JOB_STATE_DISCARDED
            else
              River::JOB_STATE_RETRYABLE
            end
            error = River::AttemptError.new(at: now, attempt: job.attempt, error: "Stuck job rescued by River", trace: "")
            rescue_count = job.metadata["river:rescue_count"]
            rescue_count = case rescue_count
            when Integer then rescue_count
            when Float then rescue_count.finite? ? rescue_count.to_i : 0
            else 0
            end
            job_set_state_if_running(
              id: job.id,
              error: error,
              finalized_at: final ? now : nil,
              metadata: {"river:rescue_count" => rescue_count + 1},
              now: now,
              scheduled_at: final ? nil : runtime_rescue_retry(job, error, retry_policy, now, logger),
              state: state
            )
            rescued += 1
          end
        end

        rescued
      end
    end

    def job_retry(id, now: Time.now.utc)
      updated_id, job = transaction do
        # Check the counter in the write itself so a concurrent claim cannot
        # advance it between validation and the update.
        updated_id = runtime_returning_ids(<<~SQL).first
          UPDATE river_job
          SET state = 'available',
              max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END,
              metadata = #{runtime_postgres? ? "metadata - 'cancel_attempted_at'" : "jsonb_remove(metadata, '$.cancel_attempted_at')"},
              finalized_at = NULL,
              scheduled_at = #{runtime_time(now)}
          WHERE id = #{Integer(id)}
            AND state != 'running'
            AND (state != 'available' OR scheduled_at > #{runtime_time(now)})
            AND attempt < #{River::MAX_ATTEMPTS_LIMIT}
          RETURNING id
        SQL
        job = job_get_by_id(updated_id || id)
        runtime_notify("river_insert", queue: job.queue) if updated_id && job
        [updated_id, job]
      end
      if !updated_id && job && job.state != "running" && (job.state != "available" || job.scheduled_at > now) && job.attempt >= River::MAX_ATTEMPTS_LIMIT
        raise ArgumentError, "cannot retry a job with #{River::MAX_ATTEMPTS_LIMIT} or more attempts"
      end
      job
    end

    # Makes due scheduled and retryable jobs available. A job is discarded
    # instead if another row already holds its unique key, or if an earlier
    # job in the same pass shares that key. The queries find these conflicts
    # before they write, so the pass never needs a savepoint to recover from a
    # failed write.
    def job_schedule(now: Time.now.utc, max: 1_000)
      transaction do
        queues = runtime_postgres? ? runtime_schedule_postgres(now, Integer(max)) : runtime_schedule_sqlite(now, Integer(max))
        # Notify each changed queue, including queues with only discarded
        # jobs, as Go does.
        queues.uniq.each { |queue| runtime_notify("river_insert", queue: queue) }
        queues.length
      end
    end

    def job_set_state_if_running(id:, state:, now: Time.now.utc, attempt: nil,
      error: nil, finalized_at: nil, metadata: nil, scheduled_at: nil)
      state = state.to_s
      # Cancellation wins over every attempt outcome, including exhausted
      # retries, direct completion, and transitions requested by extensions.
      cancel_path = runtime_cancel_attempted

      assignments = [] #: Array[String]
      assignments << "attempt = CASE WHEN NOT (#{cancel_path}) THEN #{Integer(attempt)} ELSE attempt END" unless attempt.nil?
      assignments << "errors = #{runtime_append_error(error)}" if error

      assignments << "finalized_at = CASE WHEN #{cancel_path} THEN #{runtime_time(now)} ELSE #{runtime_nullable_time(finalized_at)} END"
      assignments << "metadata = #{runtime_merge_metadata(metadata)}" unless metadata.nil? || metadata.empty?
      assignments << "scheduled_at = CASE WHEN NOT (#{cancel_path}) THEN #{runtime_time(scheduled_at)} ELSE scheduled_at END" if scheduled_at

      assignments << "state = CASE WHEN #{cancel_path} THEN 'cancelled' ELSE #{runtime_state(state)} END"

      runtime_returning_jobs(runtime_update_running_job(id, assignments)).first
    end

    def job_update(id, params)
      assignments = params.each.map do |field, value|
        raise ArgumentError, "unknown update field: #{field}" unless River::JobUpdateParams.method_defined?(field)

        "#{field} = #{runtime_update_value(field, value)}"
      end

      return job_get_by_id(id) if assignments.empty?

      updated_id = runtime_returning_ids(<<~SQL).first
        UPDATE river_job SET #{assignments.join(", ")}
        WHERE id = #{Integer(id)}
        RETURNING id
      SQL
      updated_id ? job_get_by_id(updated_id) : nil
    end

    def leader_acquire(id, ttl: 30, now: Time.now.utc)
      transaction do
        runtime_execute("DELETE FROM river_leader WHERE expires_at < #{runtime_time(now)}")
        runtime_execute(<<~SQL)
          INSERT INTO river_leader (leader_id, elected_at, expires_at)
          VALUES (#{runtime_quote(id)}, #{runtime_time(now)}, #{runtime_time(now + ttl)})
          ON CONFLICT (name) DO NOTHING
        SQL
        runtime_query_rows("SELECT leader_id FROM river_leader WHERE leader_id = #{runtime_quote(id)}").any?
      end
    end

    def leader_release(id)
      transaction do
        deleted = runtime_query_rows("DELETE FROM river_leader WHERE leader_id = #{runtime_quote(id)} RETURNING leader_id")
        runtime_notify("river_leadership", action: "resigned", leader_id: id) unless deleted.empty?
      end
    end

    def leader_renew(id, ttl: 30, now: Time.now.utc)
      runtime_query_rows(<<~SQL).any?
        UPDATE river_leader SET expires_at = #{runtime_time(now + ttl)}
        WHERE leader_id = #{runtime_quote(id)} AND expires_at >= #{runtime_time(now)}
        RETURNING leader_id
      SQL
    end

    # Bounded batches release SQLite's writer lock between maintenance passes.
    def notification_delete_before(horizon:, max: 10_000)
      return 0 if runtime_postgres?

      runtime_returning_ids(<<~SQL).length
        DELETE FROM river_notification
        WHERE id IN (
          SELECT id FROM river_notification
          WHERE created_at < #{runtime_time(horizon)}
          ORDER BY created_at, id LIMIT #{Integer(max)}
        )
        RETURNING id
      SQL
    end

    # Opens a receiver independent of application transactions. Postgres uses
    # one dedicated connection; SQLite keeps a cursor in its shared outbox.
    def notification_listener
      return NotificationListener::SQLite.new(method(:notification_query)) unless runtime_postgres?
      return unless postgres_capabilities.supports_listen_notify

      connection, schema = runtime_notification_connection
      NotificationListener::Postgres.new(connection, schema)
    end

    def queue_get(name)
      row = runtime_query_rows("SELECT #{runtime_queue_columns} FROM river_queue WHERE name = #{runtime_quote(name)}").first
      runtime_queue_from_row(row)
    end

    def queue_list(max: 100)
      runtime_query_rows("SELECT #{runtime_queue_columns} FROM river_queue ORDER BY name LIMIT #{Integer(max)}")
        .map { |row| runtime_queue_from_row(row) }
    end

    def queue_pause(name, now: Time.now.utc)
      transaction do
        filter = (name == "*") ? "true" : "name = #{runtime_quote(name)}"
        changed = runtime_query_rows(<<~SQL)
          UPDATE river_queue
          SET paused_at = CASE WHEN paused_at IS NULL THEN #{runtime_time(now)} ELSE paused_at END,
              updated_at = CASE WHEN paused_at IS NULL THEN #{runtime_time(now)} ELSE updated_at END
          WHERE #{filter}
          RETURNING #{runtime_queue_columns}
        SQL
        runtime_notify("river_control", action: "pause", queue: name) unless changed.empty?
        changed.map { |row| runtime_queue_from_row(row) }
      end
    end

    def queue_resume(name, now: Time.now.utc)
      transaction do
        filter = (name == "*") ? "true" : "name = #{runtime_quote(name)}"
        changed = runtime_query_rows(<<~SQL)
          UPDATE river_queue
          SET updated_at = CASE WHEN paused_at IS NOT NULL THEN #{runtime_time(now)} ELSE updated_at END,
              paused_at = NULL
          WHERE #{filter}
          RETURNING #{runtime_queue_columns}
        SQL
        runtime_notify("river_control", action: "resume", queue: name) unless changed.empty?
        changed.map { |row| runtime_queue_from_row(row) }
      end
    end

    def queue_update(name, metadata:, now: Time.now.utc)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      transaction do
        row = runtime_query_rows(<<~SQL).first
          UPDATE river_queue SET metadata = #{runtime_json(metadata)}, updated_at = #{runtime_time(now)}
          WHERE name = #{runtime_quote(name)} RETURNING name
        SQL
        if row
          runtime_notify("river_control", action: "metadata_changed", queue: name, metadata: metadata)
          queue_get(name)
        end
      end
    end

    def queue_upsert(name, metadata: {}, now: Time.now.utc)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      runtime_execute(<<~SQL)
        INSERT INTO river_queue (name, created_at, metadata, updated_at)
        VALUES (#{runtime_quote(name)}, #{runtime_time(now)}, #{runtime_json(metadata)}, #{runtime_time(now)})
        ON CONFLICT (name) DO UPDATE SET updated_at = excluded.updated_at
      SQL
      queue_get(name)
    end

    # Delivery follows the caller's transaction, just like job insertion.
    def request_resign
      transaction { runtime_notify("river_leadership", action: "request_resign", leader_id: "") }
      nil
    end

    private def notification_query(sql)
      runtime_query_rows(sql)
    end

    # Smallest timestamp increment preserved by this driver, in seconds.
    def time_precision
      runtime_postgres? ? Rational(1, 1_000_000) : Rational(1, 1_000)
    end

    private def runtime_append_error(error)
      value = error.respond_to?(:to_h) ? error.to_h : error
      if runtime_postgres?
        "array_append(errors, #{runtime_json(value)})"
      else
        encoded = "json(#{runtime_quote(JSON.generate(value))})"
        # Damaged JSONB may contain invalid UTF-8. Preserve its bytes as hex
        # rather than creating an unreadable JSON string by casting it to text.
        <<~SQL
          CASE WHEN NOT json_valid(errors, 10) AND errors IS NOT NULL THEN
                 jsonb(json_array(CASE WHEN typeof(errors) = 'blob' THEN 'invalid JSONB: ' || hex(errors) ELSE errors END, #{encoded}))
               WHEN coalesce(json_type(errors), 'array') <> 'array' THEN jsonb(json_array(json(errors), #{encoded}))
               ELSE jsonb(json_insert(json(coalesce(errors, jsonb('[]'))), '$[#]', #{encoded})) END
        SQL
      end
    end

    private def runtime_cancel_attempted
      runtime_postgres? ? "metadata ? 'cancel_attempted_at'" : "(CASE WHEN json_valid(metadata, 10) THEN metadata -> 'cancel_attempted_at' END) IS NOT NULL"
    end

    # One statement claims the jobs and returns their rows, like Go's
    # JobGetAvailable. The fetch subquery locks rows it then rejects, including
    # jobs other producers already claimed, until the statement commits.
    private def runtime_claim_jobs(predicate, max, attempted_by, now)
      attempted_by_sql = if runtime_postgres?
        "array_append(CASE WHEN cardinality(attempted_by) >= 100 THEN attempted_by[(cardinality(attempted_by) - 98):] ELSE attempted_by END, #{runtime_quote(attempted_by)})"
      else
        # Preserve corrupt history so decoding can fail this attempt instead
        # of silently repairing it or aborting the entire claim batch. Trim
        # to the newest 99 entries before appending, preserving JSON types.
        <<~SQL
          CASE WHEN NOT json_valid(attempted_by, 10) AND attempted_by IS NOT NULL THEN attempted_by
               ELSE jsonb(json_insert(json(
                 CASE WHEN json_array_length(attempted_by) >= 100 THEN (
                   SELECT json_group_array(json(value)) FROM (
                     SELECT attempted_by -> ('$[' || key || ']') AS value FROM json_each(attempted_by)
                     WHERE key >= json_array_length(attempted_by) - 99 ORDER BY key
                   )
                 ) ELSE coalesce(attempted_by, jsonb('[]')) END
               ), '$[#]', #{runtime_quote(attempted_by)})) END
        SQL
      end

      lock_clause = runtime_postgres? ? "FOR UPDATE SKIP LOCKED" : ""
      # An administratively requeued job may already be at the counter limit.
      # Saturate instead of letting that row abort the entire claim batch.
      runtime_returning_jobs(<<~SQL)
        UPDATE river_job
        SET attempt = CASE WHEN attempt < #{River::MAX_ATTEMPTS_LIMIT} THEN attempt + 1 ELSE attempt END,
            attempted_at = #{runtime_time(now)},
            attempted_by = #{attempted_by_sql},
            state = 'running'
        WHERE id IN (
          SELECT id FROM river_job
          WHERE #{predicate}
          ORDER BY priority ASC, scheduled_at ASC, id ASC
          LIMIT #{Integer(max)}
          #{lock_clause}
        )
      SQL
    end

    # A row that can't be decoded comes back with its decode error attached,
    # so one corrupt job fails only its own attempt.
    private def runtime_decoded_job
      yield
    rescue River::JobRowDecodeError => error
      error.job
    end

    private def runtime_read_job(id)
      runtime_decoded_job { job_get_by_id(id) }
    end

    private def runtime_decode_postgres_job(row)
      row = row.transform_keys(&:to_sym)
      decoder = JobRowDecoder.new
      decoder.finish(River::JobRow.new(
        id: row[:id], args: decoder.decoded(:args, row[:args], default: {}),
        attempt: row[:attempt], attempted_at: row[:attempted_at]&.getutc,
        attempted_by: decoder.decoded(:attempted_by, row[:attempted_by], type: Array, strings: true),
        created_at: row[:created_at].getutc,
        errors: row[:errors]&.map { |error| JobRowDecoder.attempt_error(error) },
        finalized_at: row[:finalized_at]&.getutc, kind: row[:kind], max_attempts: row[:max_attempts],
        metadata: decoder.decoded(:metadata, row[:metadata], type: Hash, default: {}),
        priority: row[:priority], queue: row[:queue], scheduled_at: row[:scheduled_at].getutc,
        state: row[:state], tags: decoder.decoded(:tags, row[:tags], type: Array, strings: true),
        unique_key: row[:unique_key]&.to_s,
        unique_states: row[:unique_states] ? River::UniqueBitmask.to_states(row[:unique_states].to_i(2)) : nil
      ))
    end

    private def runtime_decode_sqlite_job(row)
      row = row.transform_keys(&:to_sym)
      decoder = JobRowDecoder.new
      errors = decoder.json(:errors, row[:errors], type: Array, default: [])
      decoder.finish(River::JobRow.new(
        id: row[:id], args: decoder.json(:args, row[:args], default: {}),
        attempt: row[:attempt], attempted_at: runtime_parse_time(row[:attempted_at]),
        attempted_by: decoder.json(:attempted_by, row[:attempted_by], type: Array, strings: true),
        created_at: runtime_parse_time(row[:created_at]),
        errors: errors.map { |error| JobRowDecoder.attempt_error(error) },
        finalized_at: runtime_parse_time(row[:finalized_at]), kind: row[:kind], max_attempts: row[:max_attempts],
        metadata: decoder.json(:metadata, row[:metadata], type: Hash, default: {}),
        priority: row[:priority], queue: row[:queue], scheduled_at: runtime_parse_time(row[:scheduled_at]),
        state: row[:state], tags: decoder.json(:tags, row[:tags], type: Array, strings: true),
        unique_key: row[:unique_key]&.to_s,
        unique_states: row[:unique_states] ? River::UniqueBitmask.to_states(row[:unique_states]) : nil
      ))
    end

    private def runtime_in_clause(column, values)
      "#{column} IN (#{values.map { |value| runtime_quote(value) }.join(",")})"
    end

    private def runtime_notify(topic, payload)
      encoded = runtime_quote(JSON.generate(payload))
      if runtime_postgres?
        return unless postgres_capabilities.supports_listen_notify

        runtime_execute("SELECT pg_notify(current_schema() || '.' || #{runtime_quote(topic)}, #{encoded})")
      else
        runtime_execute("INSERT INTO river_notification (topic, payload) VALUES (#{runtime_quote(topic)}, #{encoded})")
      end
    end

    private def sqlite_insert_nonces(params)
      keys = {} #: Hash[String, bool]
      params.map do |param|
        key = param.unique_key
        if key && !key.empty? && River::UniqueBitmask.to_states((param.unique_states || "0").to_i(2)).include?(param.state)
          # Postgres rejects batches that upsert the same indexed row twice.
          raise ArgumentError, "unique key appears more than once in batch" if keys[key]

          keys[key] = true
        end
        SecureRandom.hex(8)
      end
    end

    private def runtime_cursor_clause(params)
      cursor = params.after
      comparison = (params.sort_order == :asc) ? ">" : "<"
      id_clause = "id #{comparison} #{Integer(cursor.id)}"
      return id_clause if params.sort_by == :id

      column = params.sort_by
      return "(#{column} IS NULL AND #{id_clause})" if cursor.value.nil?

      value = runtime_time(cursor.value)
      "(#{column} IS NULL OR #{column} #{comparison} #{value} OR (#{column} = #{value} AND #{id_clause}))"
    end

    private def runtime_job_attempt_predicate(job)
      "attempt = #{Integer(job.attempt)} AND attempted_at IS NOT DISTINCT FROM #{runtime_nullable_time(job.attempted_at)}"
    end

    private def runtime_job_list(params, for_delete: false)
      clauses = [] #: Array[String]
      clauses << "state != 'running'" if for_delete
      clauses << runtime_cursor_clause(params) if params.after
      clauses << "id #{(params.sort_order == :asc) ? ">" : "<"} #{Integer(params.after_id)}" if params.after_id
      clauses << runtime_in_clause("id", params.ids.map { |value| Integer(value) }) if params.ids&.any?
      clauses << runtime_in_clause("kind", params.kinds) if params.kinds&.any?
      clauses << runtime_in_clause("priority", params.priorities.map { |value| Integer(value) }) if params.priorities&.any?
      clauses << runtime_in_clause("queue", params.queues) if params.queues&.any?
      clauses << runtime_in_clause("state", params.states) if params.states&.any?

      finalized = params.sort_by == :finalized_at && params.states&.length == 1 &&
        %w[cancelled completed discarded].include?(params.states.first)
      # Schemas require finalized timestamps for terminal states. Spell this out
      # so Postgres can use the partial (state, finalized_at) index.
      clauses << "finalized_at IS NOT NULL" if finalized
      # Explicit NULLS LAST prevents a backward index scan, even when there are
      # no nulls. Only request it for timestamps that can actually be null.
      null_order = (params.sort_by == :finalized_at && !finalized) ? " NULLS LAST" : ""

      params.metadata&.each { |key, value| clauses << runtime_metadata_equals(key, value) }
      Array(params.tags_all).each { |tag| clauses << runtime_tag_contains(tag) }
      if params.tags_any&.any?
        clauses << "(" + params.tags_any.map { |tag| runtime_tag_contains(tag) }.join(" OR ") + ")"
      end

      where = clauses.empty? ? "" : "WHERE #{clauses.join(" AND ")}"
      runtime_job_rows(<<~SQL)
        #{where}
        ORDER BY #{params.sort_by} #{params.sort_order.to_s.upcase}#{null_order}, id #{params.sort_order.to_s.upcase}
        LIMIT #{params.limit}
        #{"FOR UPDATE SKIP LOCKED" if for_delete && runtime_postgres?}
      SQL
    end

    private def runtime_json(value)
      encoded = value.is_a?(String) ? value : JSON.generate(value)
      runtime_postgres? ? "#{runtime_quote(encoded)}::jsonb" : "jsonb(#{runtime_quote(encoded)})"
    end

    # Temporary fallback: when true, transaction helpers open a savepoint
    # inside an open transaction instead of joining it.
    private def runtime_legacy_subtransactions?
      %w[1 true].include?(ENV.fetch("RIVER_USE_LEGACY_SUBTRANSACTIONS", ""))
    end

    private def runtime_merge_metadata(metadata)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      if runtime_postgres?
        "metadata || #{runtime_json(metadata)}"
      else
        # Keep the SQL shape fixed regardless of the number of updated keys.
        # -> preserves JSON booleans and large numeric tokens during aggregation.
        merged = <<~SQL.strip
          (WITH updates(doc) AS (SELECT #{runtime_json(metadata)})
              SELECT CASE WHEN json_type(metadata) = 'object' THEN (
                  SELECT jsonb_group_object(key, jsonb(doc -> fullkey))
                  FROM (
                      SELECT key, fullkey, metadata AS doc FROM json_each(metadata)
                      WHERE key NOT IN (SELECT key FROM updates, json_each(doc))
                      UNION ALL
                      SELECT key, fullkey, doc FROM updates, json_each(doc)
                  )
              ) WHEN json_type(metadata) = 'array' THEN jsonb_insert(metadata, '$[#]', doc)
              ELSE jsonb_array(json(metadata), doc) END FROM updates)
        SQL
        "CASE WHEN json_valid(metadata, 10) THEN #{merged} ELSE metadata END"
      end
    end

    private def runtime_metadata_equals(key, value)
      encoded = runtime_quote(JSON.generate(value))
      if runtime_postgres?
        "metadata -> #{runtime_quote(key.to_s)} = #{encoded}::jsonb"
      else
        # Compare JSON trees so object key order is immaterial, while strings,
        # booleans, null, and missing keys remain distinct. json_each also treats
        # the requested metadata key literally instead of as a JSON path.
        columns = "fullkey, CASE WHEN type IN ('integer', 'real') THEN 'number' ELSE type END, atom"
        actual = <<~SQL
          SELECT #{columns} FROM json_tree(CASE entry.type
            WHEN 'text' THEN json_quote(entry.value)
            WHEN 'null' THEN 'null'
            WHEN 'true' THEN 'true'
            WHEN 'false' THEN 'false'
            ELSE entry.value END)
        SQL
        expected = "SELECT #{columns} FROM json_tree(#{encoded})"
        <<~SQL
          EXISTS (SELECT 1 FROM json_each(metadata) AS entry
            WHERE entry.key = #{runtime_quote(key.to_s)}
              AND NOT EXISTS (#{actual} EXCEPT #{expected})
              AND NOT EXISTS (#{expected} EXCEPT #{actual}))
        SQL
      end
    end

    private def runtime_nullable_time(value)
      value ? runtime_time(value) : "NULL"
    end

    private def runtime_parse_json(value)
      value.is_a?(String) ? JSON.parse(value) : value.to_h
    end

    private def runtime_parse_time(value)
      return nil unless value

      if value.respond_to?(:getutc)
        value.getutc
      else
        Time.parse(value.to_s + (value.to_s.match?(/(?:Z|[+-]\d{2}:?\d{2})\z/) ? "" : " UTC")).utc
      end
    end

    private def runtime_queue_columns
      runtime_postgres? ? "name, created_at, metadata, paused_at, updated_at" : "name, CAST(created_at AS text) AS created_at, json(metadata) AS metadata, CAST(paused_at AS text) AS paused_at, CAST(updated_at AS text) AS updated_at"
    end

    private def runtime_queue_from_row(row)
      return nil unless row

      River::Queue.new(
        runtime_value(row, :name),
        runtime_parse_time(runtime_value(row, :created_at)),
        runtime_parse_json(runtime_value(row, :metadata)),
        runtime_parse_time(runtime_value(row, :paused_at)),
        runtime_parse_time(runtime_value(row, :updated_at))
      )
    end

    private def runtime_rescue_retry(job, error, retry_policy, now, logger)
      retry_at = retry_policy.next_retry(job, error, now: now)
      raise ArgumentError, "next_retry must return a Time" unless retry_at.is_a?(Time)
      raise ArgumentError, "next_retry must not return a past Time" if retry_at < now

      retry_at
    rescue => retry_error
      logger&.error("River rescue retry scheduling failed; using default backoff: #{retry_error.full_message}")
      River::DefaultClientRetryPolicy.new.next_retry(job, error, now: now)
    end

    private def runtime_returning_ids(sql)
      runtime_query_rows(sql).map { |row| runtime_value(row, :id).to_i }
    end

    # Targets one job only while it's running and, when uncancelled, only while
    # no cancellation was requested. Postgres locks the row by primary key
    # first and tests the locked row, the way Go's JobRetry and JobUpdate lock
    # their target. With the state test in the statement's own WHERE, Postgres
    # can plan it on the fetching index (state, queue, ...) when few jobs are
    # running, and that scan reads every running entry, including the dead
    # ones claims and completions leave behind. Returns the CTE, the extra
    # source to join, and the condition.
    private def runtime_running_job(id, uncancelled, expected_job: nil)
      predicate = "id = #{Integer(id)}"
      predicate += " AND #{runtime_job_attempt_predicate(expected_job)}" if expected_job
      unless runtime_postgres?
        condition = "#{predicate} AND state = 'running'"
        condition += " AND NOT (#{runtime_cancel_attempted})" if uncancelled
        return ["", nil, condition]
      end

      lock = <<~SQL
        WITH running_job AS (
          SELECT id, state AS locked_state, #{runtime_cancel_attempted} AS cancel_attempted
          FROM river_job WHERE #{predicate} FOR UPDATE
        )
      SQL
      condition = "river_job.id = running_job.id AND running_job.locked_state = 'running'"
      condition += " AND NOT running_job.cancel_attempted" if uncancelled
      [lock, "running_job", condition]
    end

    private def runtime_delete_running_job(id, uncancelled: false)
      lock, source, condition = runtime_running_job(id, uncancelled)
      "#{lock}DELETE FROM river_job#{" USING #{source}" if source} WHERE #{condition}"
    end

    private def runtime_update_running_job(id, assignments, expected_job: nil, uncancelled: false)
      lock, source, condition = runtime_running_job(id, uncancelled, expected_job: expected_job)
      "#{lock}UPDATE river_job SET #{assignments.join(", ")}#{" FROM #{source}" if source} WHERE #{condition}"
    end

    # Finds conflicts and changes the whole pass in one statement.
    private def runtime_schedule_postgres(now, max)
      now = runtime_time(now)
      runtime_query_rows(<<~SQL).map { |row| runtime_value(row, :queue) }
        WITH jobs_to_schedule AS (
            SELECT id, unique_key, unique_states, priority, scheduled_at
            FROM river_job
            WHERE state IN ('retryable', 'scheduled')
                AND priority >= 0
                AND queue IS NOT NULL
                AND scheduled_at <= #{now}
            ORDER BY priority, scheduled_at, id
            LIMIT #{max}
            FOR UPDATE
        ),
        jobs_with_rownum AS (
            SELECT
                *,
                CASE
                    WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL THEN
                        ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY priority, scheduled_at, id)
                END AS row_num
            FROM jobs_to_schedule
        ),
        unique_conflicts AS (
            SELECT river_job.unique_key
            FROM river_job
            JOIN jobs_with_rownum
                ON river_job.unique_key = jobs_with_rownum.unique_key
                AND river_job.id != jobs_with_rownum.id
            WHERE river_job.unique_key IS NOT NULL
                AND river_job.unique_states IS NOT NULL
                AND river_job_state_in_bitmask(river_job.unique_states, river_job.state)
        ),
        job_updates AS (
            SELECT
                job.id,
                job.row_num IS NOT NULL AND (uc.unique_key IS NOT NULL OR job.row_num > 1) AS discard
            FROM jobs_with_rownum job
            LEFT JOIN unique_conflicts uc ON job.unique_key = uc.unique_key
        )
        UPDATE river_job
        SET state = CASE WHEN job_updates.discard THEN 'discarded'::river_job_state ELSE 'available'::river_job_state END,
            finalized_at = CASE WHEN job_updates.discard THEN #{now} ELSE river_job.finalized_at END,
            metadata = CASE WHEN job_updates.discard THEN river_job.metadata || '{"unique_key_conflict": "scheduler_discarded"}'::jsonb
                            ELSE river_job.metadata END
        FROM job_updates
        WHERE river_job.id = job_updates.id
        RETURNING river_job.queue
      SQL
    end

    # Follows Go's SQLite driver: check each unique job in order, and make it
    # available before checking the next one, so a later job with the same
    # key sees it as a conflict. SQLite allows one writer at a time. Once this
    # pass writes, other connections cannot write until it ends. If another
    # connection writes first, this pass's next write fails instead of using
    # stale rows.
    private def runtime_schedule_sqlite(now, max)
      rows = runtime_query_rows(<<~SQL)
        SELECT id, queue, unique_key IS NOT NULL AS has_unique_key
        FROM river_job
        WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= #{runtime_time(now)}
        ORDER BY priority, scheduled_at, id
        LIMIT #{max}
      SQL
      available = [] #: Array[Integer]
      discarded = [] #: Array[Integer]
      rows.each do |row|
        id = runtime_value(row, :id).to_i
        next available << id if runtime_value(row, :has_unique_key).to_i.zero?

        conflict = runtime_query_rows(<<~SQL).any?
          SELECT 1 FROM river_job
          WHERE id <> #{id}
            AND unique_key = (SELECT job.unique_key FROM river_job AS job WHERE job.id = #{id})
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
          LIMIT 1
        SQL
        next discarded << id if conflict

        runtime_execute("UPDATE river_job SET state = 'available' WHERE id = #{id}")
      end

      unless discarded.empty?
        runtime_execute(<<~SQL)
          UPDATE river_job
          SET state = 'discarded', finalized_at = #{runtime_time(now)},
              metadata = #{runtime_merge_metadata("unique_key_conflict" => "scheduler_discarded")}
          WHERE id IN (#{discarded.join(",")})
        SQL
      end
      runtime_execute("UPDATE river_job SET state = 'available' WHERE id IN (#{available.join(",")})") unless available.empty?

      rows.map { |row| runtime_value(row, :queue) }
    end

    private def runtime_state(value)
      runtime_postgres? ? "#{runtime_quote(value)}::river_job_state" : runtime_quote(value)
    end

    private def runtime_tag_contains(tag)
      if runtime_postgres?
        "tags @> ARRAY[#{runtime_quote(tag)}]::varchar[]"
      else
        "EXISTS (SELECT 1 FROM json_each(json(tags)) WHERE value = #{runtime_quote(tag)})"
      end
    end

    private def runtime_time(value)
      raise ArgumentError, "time cannot be nil" unless value

      cast = runtime_postgres? ? "::timestamptz" : ""
      encoded = if runtime_postgres?
        normalize_time(value).iso8601(6)
      else
        normalize_time(value).strftime("%Y-%m-%d %H:%M:%S.%3N")
      end
      "#{runtime_quote(encoded)}#{cast}"
    end

    private def runtime_update_value(field, value)
      case field
      when :attempt
        attempt = Integer(value)
        raise ArgumentError, "attempt must be between 0 and #{River::MAX_ATTEMPTS_LIMIT}" unless (0..River::MAX_ATTEMPTS_LIMIT).cover?(attempt)

        attempt.to_s
      when :attempted_at, :finalized_at
        value ? runtime_time(value) : "NULL"
      when :attempted_by
        values = Array(value) #: Array[untyped]
        raise ArgumentError, "attempted_by must contain only Strings" unless values.all? { |item| item.is_a?(String) }

        runtime_postgres? ? "ARRAY[#{values.map { |item| runtime_quote(item) }.join(",")}]::text[]" : runtime_json(values)
      when :errors
        input_values = Array(value) #: Array[untyped]
        values = input_values.map { |error| error.respond_to?(:to_h) ? error.to_h : error }
        runtime_postgres? ? "ARRAY[#{values.map { |item| runtime_json(item) }.join(",")}]::jsonb[]" : runtime_json(values)
      when :max_attempts
        max_attempts = Integer(value)
        raise ArgumentError, "max_attempts must be greater than zero" unless max_attempts > 0
        raise ArgumentError, "max_attempts must not exceed #{River::MAX_ATTEMPTS_LIMIT}" if max_attempts > River::MAX_ATTEMPTS_LIMIT

        max_attempts.to_s
      when :metadata
        raise ArgumentError, "metadata must be a Hash" unless value.is_a?(Hash)

        runtime_json(value)
      when :state
        runtime_state(value)
      end
    end
  end
end
