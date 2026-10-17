# frozen_string_literal: true

module River::Driver
  # MySQL statements shared by the Active Record and Sequel adapters. Writes
  # lock and read back their rows in the same transaction because MySQL has no
  # RETURNING clause. Only MySQL driver instances extend this module.
  module MySQLRuntime
    def job_cancel(id, now: Time.now.utc)
      transaction do
        rows = mysql_update("river_job", "id", "id = #{Integer(id)} AND state NOT IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NULL", [
          "finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE #{runtime_time(now)} END",
          "metadata = #{runtime_merge_metadata("cancel_attempted_at" => now.iso8601(6))}",
          "state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END"
        ])
        job = job_get_by_id(id)
        runtime_notify("river_control", action: "cancel", job_id: job.id, queue: job.queue) unless rows.empty?
        job
      end
    end

    def job_complete(id:, finalized_at:, metadata: nil, now: Time.now.utc)
      assignments = ["finalized_at = #{runtime_time(finalized_at)}", "state = 'completed'"]
      assignments << "metadata = #{runtime_merge_metadata(metadata)}" if metadata && !metadata.empty?
      transaction do
        rows = mysql_update("river_job", "id", "id = #{Integer(id)} AND state = 'running' AND NOT (#{runtime_cancel_attempted})", assignments)
        next runtime_decode_sqlite_job(rows.first) unless rows.empty?

        :cancelled if job_get_cancelled_ids([Integer(id)]).include?(Integer(id))
      end
    end

    def job_delete(id)
      transaction do
        rows = mysql_delete("river_job", "id", "id = #{Integer(id)} AND state != 'running'")
        rows.empty? ? job_get_by_id(id) : runtime_decode_sqlite_job(rows.first)
      end
    end

    def job_delete_finalized(retention:, now: Time.now.utc, max: 1_000)
      clauses = retention.filter_map do |state, seconds|
        "(state = #{runtime_quote(state.to_s)} AND finalized_at < #{runtime_time(now - seconds)})" if seconds
      end
      return 0 if clauses.empty?

      predicate = "(#{clauses.join(" OR ")})"
      ids = runtime_query_rows("SELECT id FROM river_job WHERE #{predicate} ORDER BY id LIMIT #{Integer(max)}")
        .map { |row| runtime_value(row, :id) }
      return 0 if ids.empty?

      mysql_delete("river_job", "id", "#{runtime_in_clause("id", ids)} AND #{predicate}").length
    end

    def job_delete_if_running(id)
      transaction do
        deleted = mysql_delete("river_job", "id", "id = #{Integer(id)} AND state = 'running' AND NOT (#{runtime_cancel_attempted})")
        next true unless deleted.empty?

        job_get_cancelled_ids([Integer(id)]).include?(Integer(id)) ? :cancelled : false
      end
    end

    def job_delete_many(params)
      transaction do
        jobs = runtime_job_list(params, for_delete: true)
        next [] if jobs.empty?

        mysql_delete("river_job", "id", runtime_in_clause("id", jobs.map(&:id)) + " AND state != 'running'")
        jobs
      end
    end

    def job_get_by_id(id)
      row = runtime_query_rows("SELECT #{mysql_columns("river_job")} FROM river_job WHERE id = #{Integer(id)}").first
      row ? runtime_decode_sqlite_job(row) : nil
    end

    def job_insert_many(params)
      return [] if params.empty?

      nonces = sqlite_insert_nonces(params)
      transaction do
        results = params.zip(nonces).map do |param, nonce|
          metadata = (param.metadata || {}).merge("river:unique_nonce" => nonce)
          values = [runtime_json(param.encoded_args), runtime_quote(param.kind), Integer(param.max_attempts),
            runtime_json(metadata), Integer(param.priority), runtime_quote(param.queue),
            param.scheduled_at ? runtime_time(param.scheduled_at) : "UTC_TIMESTAMP(6)",
            runtime_quote(param.state), runtime_json(param.tags || []),
            param.unique_key ? "UNHEX(#{runtime_quote(param.unique_key.unpack1("H*"))})" : "NULL",
            param.unique_states ? param.unique_states.to_i(2) : "NULL"]
          runtime_execute(<<~SQL)
            INSERT INTO river_job (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
            VALUES (#{values.join(",")})
            ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
          SQL
          id = runtime_value(runtime_query_rows("SELECT LAST_INSERT_ID() AS id").first, :id)
          job = job_get_by_id(id)
          duplicate = job.metadata["river:unique_nonce"] != nonce
          if duplicate && (param.unique_key.nil? || job.unique_key != param.unique_key ||
              !job.unique_states&.include?(job.state) ||
              !River::UniqueBitmask.to_states((param.unique_states || "0").to_i(2)).include?(param.state))
            raise River::Error, "insert conflicts with a constraint other than River's unique-job index"
          end
          [job, duplicate]
        end
        params.select { |param| param.state == River::JOB_STATE_AVAILABLE }.map(&:queue).uniq.each do |queue|
          runtime_notify("river_insert", queue: queue)
        end
        results
      end
    end

    def job_metadata_merge(id, metadata)
      rows = mysql_update("river_job", "id", "id = #{Integer(id)}", ["metadata = #{runtime_merge_metadata(metadata)}"])
      rows.empty? ? nil : runtime_decode_sqlite_job(rows.first)
    end

    def job_retry(id, now: Time.now.utc)
      transaction do
        job = mysql_locked_job(id)
        next job if !job || job.state == "running" || (job.state == "available" && job.scheduled_at <= now)
        raise ArgumentError, "cannot retry a job with #{River::MAX_ATTEMPTS_LIMIT} or more attempts" if job.attempt >= River::MAX_ATTEMPTS_LIMIT

        mysql_update("river_job", "id", "id = #{Integer(id)}", [
          "max_attempts = CASE WHEN attempt = max_attempts THEN max_attempts + 1 ELSE max_attempts END",
          "metadata = JSON_REMOVE(metadata, '$.cancel_attempted_at')", "finalized_at = NULL",
          "scheduled_at = #{runtime_time(now)}", "state = 'available'"
        ])
        job_get_by_id(id)
      end
    end

    def job_set_state_if_running(id:, state:, now: Time.now.utc, attempt: nil,
      error: nil, finalized_at: nil, metadata: nil, scheduled_at: nil)
      cancel = runtime_cancel_attempted
      assignments = [] #: Array[String]
      assignments << "attempt = CASE WHEN NOT (#{cancel}) THEN #{Integer(attempt)} ELSE attempt END" unless attempt.nil?
      assignments << "errors = #{runtime_append_error(error)}" if error
      assignments << "finalized_at = CASE WHEN #{cancel} THEN #{runtime_time(now)} ELSE #{runtime_nullable_time(finalized_at)} END"
      assignments << "scheduled_at = CASE WHEN NOT (#{cancel}) THEN #{runtime_time(scheduled_at)} ELSE scheduled_at END" if scheduled_at
      # MySQL evaluates assignments left-to-right. Check cancellation before
      # replacing metadata or state.
      assignments << "state = CASE WHEN #{cancel} THEN 'cancelled' ELSE #{runtime_state(state.to_s)} END"
      assignments << "metadata = #{runtime_merge_metadata(metadata)}" if metadata && !metadata.empty?
      rows = mysql_update("river_job", "id", "id = #{Integer(id)} AND state = 'running'", assignments)
      rows.empty? ? nil : runtime_decode_sqlite_job(rows.first)
    end

    def job_update(id, params)
      assignments = params.each.map do |field, value|
        raise ArgumentError, "unknown update field: #{field}" unless River::JobUpdateParams.method_defined?(field)

        "#{field} = #{runtime_update_value(field, value)}"
      end
      return job_get_by_id(id) if assignments.empty?

      rows = mysql_update("river_job", "id", "id = #{Integer(id)}", assignments)
      rows.empty? ? nil : runtime_decode_sqlite_job(rows.first)
    end

    def leader_acquire(id, ttl: 30, now: Time.now.utc)
      transaction do
        runtime_execute("DELETE FROM river_leader WHERE expires_at < #{runtime_time(now)}")
        runtime_execute(<<~SQL)
          INSERT INTO river_leader (leader_id, elected_at, expires_at)
          VALUES (#{runtime_quote(id)}, #{runtime_time(now)}, #{runtime_time(now + ttl)})
          ON DUPLICATE KEY UPDATE name = name
        SQL
        runtime_query_rows("SELECT leader_id FROM river_leader WHERE leader_id = CAST(#{runtime_quote(id)} AS BINARY) FOR UPDATE").any?
      end
    end

    def leader_release(id)
      transaction do
        deleted = mysql_delete("river_leader", "name", "leader_id = CAST(#{runtime_quote(id)} AS BINARY)")
        runtime_notify("river_leadership", action: "resigned", leader_id: id) unless deleted.empty?
      end
    end

    def leader_renew(id, ttl: 30, now: Time.now.utc)
      mysql_update("river_leader", "name", "leader_id = CAST(#{runtime_quote(id)} AS BINARY) AND expires_at >= #{runtime_time(now)}",
        ["expires_at = #{runtime_time(now + ttl)}"]).any?
    end

    def notification_delete_before(horizon:, max: 10_000)
      mysql_delete("river_notification", "id", "created_at < #{runtime_time(horizon)}", limit: max).length
    end

    def queue_pause(name, now: Time.now.utc)
      mysql_queue_paused(name, true, now)
    end

    def queue_resume(name, now: Time.now.utc)
      mysql_queue_paused(name, false, now)
    end

    def queue_update(name, metadata:, now: Time.now.utc)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      transaction do
        rows = mysql_update("river_queue", "name", "name = CAST(#{runtime_quote(name)} AS BINARY)",
          ["metadata = #{runtime_json(metadata)}", "updated_at = #{runtime_time(now)}"])
        next nil if rows.empty?

        runtime_notify("river_control", action: "metadata_changed", queue: name, metadata: metadata)
        runtime_queue_from_row(rows.first)
      end
    end

    def queue_upsert(name, metadata: {}, now: Time.now.utc)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      transaction do
        runtime_execute(<<~SQL)
          INSERT INTO river_queue (name, created_at, metadata, updated_at)
          VALUES (#{runtime_quote(name)}, #{runtime_time(now)}, #{runtime_json(metadata)}, #{runtime_time(now)})
          ON DUPLICATE KEY UPDATE updated_at = #{runtime_time(now)}
        SQL
        queue_get(name)
      end
    end

    private def mysql_columns(table)
      columns = case table
      when "river_job"
        %w[id args attempt attempted_at attempted_by created_at errors finalized_at kind max_attempts metadata priority queue scheduled_at state tags unique_key unique_states]
      when "river_queue"
        %w[name created_at metadata paused_at updated_at]
      else
        return "*"
      end
      columns.map { |column| column.end_with?("_at") ? "CAST(#{column} AS CHAR) AS #{column}" : column }.join(", ")
    end

    private def mysql_delete(table, key, predicate, limit: nil)
      transaction do
        rows = runtime_query_rows("SELECT #{mysql_columns(table)} FROM #{table} WHERE #{predicate} ORDER BY #{key} #{"LIMIT #{Integer(limit)}" if limit} FOR UPDATE")
        runtime_execute("DELETE FROM #{table} WHERE #{runtime_in_clause(key, rows.map { |row| runtime_value(row, key.to_sym) })}") unless rows.empty?
        rows
      end
    end

    private def mysql_locked_job(id)
      row = runtime_query_rows("SELECT #{mysql_columns("river_job")} FROM river_job WHERE id = #{Integer(id)} FOR UPDATE").first
      row ? runtime_decode_sqlite_job(row) : nil
    end

    private def mysql_queue_paused(name, paused, now)
      transaction do
        predicate = (name == "*") ? "true" : "name = CAST(#{runtime_quote(name)} AS BINARY)"
        assignments = ["updated_at = CASE WHEN paused_at IS #{paused ? "NULL" : "NOT NULL"} THEN #{runtime_time(now)} ELSE updated_at END",
          "paused_at = #{paused ? "COALESCE(paused_at, #{runtime_time(now)})" : "NULL"}"]
        rows = mysql_update("river_queue", "name", predicate, assignments)
        runtime_notify("river_control", action: paused ? "pause" : "resume", queue: name) unless rows.empty?
        rows.map { |row| runtime_queue_from_row(row) }
      end
    end

    private def mysql_update(table, key, predicate, assignments)
      transaction do
        rows = runtime_query_rows("SELECT #{key} FROM #{table} WHERE #{predicate} ORDER BY #{key} FOR UPDATE")
        next [] if rows.empty?

        filter = runtime_in_clause(key, rows.map { |row| runtime_value(row, key.to_sym) })
        runtime_execute("UPDATE #{table} SET #{assignments.join(", ")} WHERE #{filter}")
        runtime_query_rows("SELECT #{mysql_columns(table)} FROM #{table} WHERE #{filter} ORDER BY #{key}")
      end
    end

    private def runtime_append_error(error)
      value = error.respond_to?(:to_h) ? error.to_h : error
      "JSON_ARRAY_APPEND(CASE WHEN JSON_TYPE(errors) = 'ARRAY' THEN errors WHEN errors IS NULL THEN JSON_ARRAY() ELSE JSON_ARRAY(errors) END, '$', #{runtime_json(value)})"
    end

    private def runtime_cancel_attempted = "JSON_CONTAINS_PATH(metadata, 'one', '$.cancel_attempted_at')"

    private def runtime_claim_jobs(predicate, max, attempted_by, now)
      transaction do
        ids = runtime_query_rows("SELECT id FROM river_job WHERE #{predicate} ORDER BY priority, scheduled_at, id LIMIT #{Integer(max)} FOR UPDATE SKIP LOCKED")
          .map { |row| runtime_value(row, :id) }
        next [] if ids.empty?

        runtime_execute(<<~SQL)
          UPDATE river_job SET attempt = LEAST(attempt + 1, 32767),
            attempted_at = #{runtime_time(now)},
            attempted_by = JSON_ARRAY_APPEND(CASE WHEN JSON_LENGTH(attempted_by) >= 100 THEN JSON_EXTRACT(attempted_by, '$[last-98 to last]') ELSE COALESCE(attempted_by, JSON_ARRAY()) END, '$', #{runtime_quote(attempted_by)}),
            state = 'running'
          WHERE #{runtime_in_clause("id", ids)}
        SQL
        ids.map { |id| runtime_read_job(id) }
      end
    end

    private def runtime_in_clause(column, values)
      predicate = super
      return predicate unless %w[kind name queue].include?(column)

      # Keep the indexed comparison so claims do not scan and lock other queues.
      "(#{predicate} AND #{super("CAST(#{column} AS BINARY)", values)})"
    end

    private def runtime_job_rows(suffix)
      runtime_query_rows("SELECT #{mysql_columns("river_job")} FROM river_job #{suffix}").map { |row| runtime_decode_sqlite_job(row) }
    end

    private def runtime_json(value)
      encoded = value.is_a?(String) ? value : JSON.generate(value)
      "CAST(#{runtime_quote(encoded)} AS JSON)"
    end

    private def runtime_locks_rows? = true

    private def runtime_merge_metadata(metadata)
      raise ArgumentError, "metadata must be a Hash" unless metadata.is_a?(Hash)

      metadata.reduce("metadata") do |expression, (key, value)|
        "JSON_SET(#{expression}, #{runtime_quote("$.#{JSON.generate(key.to_s)}")}, CAST(#{runtime_quote(JSON.generate(value))} AS JSON))"
      end
    end

    private def runtime_metadata_equals(key, value)
      "JSON_EXTRACT(metadata, #{runtime_quote("$.#{JSON.generate(key.to_s)}")}) = CAST(#{runtime_quote(JSON.generate(value))} AS JSON)"
    end

    private def runtime_order_by(params, null_order)
      "#{"#{params.sort_by} IS NULL, " unless null_order.empty?}#{params.sort_by} #{params.sort_order.to_s.upcase}, id #{params.sort_order.to_s.upcase}"
    end

    private def runtime_queue_columns = mysql_columns("river_queue")

    private def runtime_tag_contains(tag)
      "JSON_CONTAINS(tags, JSON_QUOTE(#{runtime_quote(tag)}))"
    end

    private def runtime_time(value)
      raise ArgumentError, "time cannot be nil" unless value

      runtime_quote(value.getutc.strftime("%Y-%m-%d %H:%M:%S.%6N"))
    end
  end
end
