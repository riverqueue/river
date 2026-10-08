# frozen_string_literal: true

require "time"

module River::Driver
  # Database operations used by River's worker runtime. Driver gems implement a
  # small set of raw SQL primitives and include this module so the state machine
  # stays identical across ActiveRecord and Sequel.
  module Runtime
    def init_driver
      postgres_capabilities if runtime_postgres?
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
    # this inside their transaction; PostgreSQL defers delivery until commit.
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
    def job_complete(id:, finalized_at:, metadata: nil, now: Time.now.utc)
      id = Integer(id)
      assignments = ["state = 'completed'", "finalized_at = #{runtime_time(finalized_at)}"]
      assignments << "metadata = #{runtime_merge_metadata(metadata)}" unless metadata.nil? || metadata.empty?

      updated_id = runtime_returning_ids(<<~SQL).first
        UPDATE river_job SET #{assignments.join(", ")}
        WHERE id = #{id} AND state = 'running' AND NOT (#{runtime_cancel_attempted})
        RETURNING id
      SQL
      return runtime_read_job(updated_id) if updated_id

      :cancelled if job_get_cancelled_ids([id]).include?(id)
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
      deleted = runtime_returning_ids(<<~SQL).any?
        DELETE FROM river_job
        WHERE id = #{id} AND state = 'running' AND NOT (#{runtime_cancel_attempted})
        RETURNING id
      SQL
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

    def job_metadata_merge(id, metadata)
      updated_id = runtime_returning_ids(<<~SQL).first
        UPDATE river_job
        SET metadata = #{runtime_merge_metadata(metadata)}
        WHERE id = #{Integer(id)}
        RETURNING id
      SQL
      updated_id ? job_get_by_id(updated_id) : nil
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
        [updated_id, job_get_by_id(updated_id || id)]
      end
      if !updated_id && job && job.state != "running" && (job.state != "available" || job.scheduled_at > now) && job.attempt >= River::MAX_ATTEMPTS_LIMIT
        raise ArgumentError, "cannot retry a job with #{River::MAX_ATTEMPTS_LIMIT} or more attempts"
      end
      job
    end

    def job_schedule(now: Time.now.utc, max: 1_000)
      transaction do
        # Hold each selected row until its transition (including uniqueness
        # conflict handling) finishes. A concurrent retry may change its due time.
        lock = runtime_postgres? ? "FOR UPDATE SKIP LOCKED" : ""
        ids = runtime_query_rows(<<~SQL).map { |row| runtime_value(row, :id).to_i }
          SELECT id FROM river_job
          WHERE state IN ('retryable', 'scheduled') AND scheduled_at <= #{runtime_time(now)}
          ORDER BY priority, scheduled_at, id
          LIMIT #{Integer(max)} #{lock}
        SQL
        ids.each do |id|
          transaction do
            runtime_execute("UPDATE river_job SET state = 'available' WHERE id = #{id} AND state IN ('retryable', 'scheduled')")
          end
        rescue runtime_unique_violation_class
          runtime_execute(<<~SQL)
            UPDATE river_job
            SET state = 'discarded', finalized_at = #{runtime_time(now)},
                metadata = #{runtime_merge_metadata("unique_key_conflict" => "scheduler_discarded")}
            WHERE id = #{id}
          SQL
        end

        ids.length
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

      id = runtime_returning_ids(<<~SQL).first
        UPDATE river_job
        SET #{assignments.join(",\n    ")}
        WHERE id = #{Integer(id)} AND state = 'running'
        RETURNING id
      SQL
      id ? runtime_read_job(id) : nil
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

    private def runtime_claim_jobs(predicate, max, attempted_by, now)
      transaction do
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
        ids = runtime_returning_ids(<<~SQL)
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
          RETURNING id
        SQL
        ids.map { |id| runtime_read_job(id) }
      end
    end

    private def runtime_read_job(id)
      job_get_by_id(id)
    rescue River::JobRowDecodeError => error
      error.job
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
          # PostgreSQL rejects batches that upsert the same indexed row twice.
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
      # so PostgreSQL can use the partial (state, finalized_at) index.
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

    private def runtime_merge_metadata(metadata)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      if runtime_postgres?
        "metadata || #{runtime_json(metadata)}"
      else
        # PostgreSQL's || replaces top-level values, including JSON null.
        # JSON Merge Patch would recursively merge objects and delete nulls.
        merged = metadata.reduce("metadata") do |expression, (key, value)|
          path = runtime_quote("$.#{JSON.generate(key.to_s)}")
          "jsonb_set(#{expression}, #{path}, jsonb(#{runtime_quote(JSON.generate(value))}))"
        end
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
        value.getutc.iso8601(6)
      else
        value.getutc.round(3).strftime("%Y-%m-%d %H:%M:%S.%3N")
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
