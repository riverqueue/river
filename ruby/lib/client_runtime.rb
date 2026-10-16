# frozen_string_literal: true

require "timeout"

module River
  class ClientRuntime
    class Interrupted < StandardError; end

    # Keep wall time for queue latency and error timestamps, and monotonic time
    # for execution and completion durations. Externally executed jobs start
    # with no locally observed execution time. Snapshot queue latency before
    # retries, snoozes, or application code change the job's schedule.
    class JobTiming
      attr_accessor :finished_monotonic
      attr_reader :queue_wait_duration, :started_at, :started_monotonic

      def initialize(started_at, started_monotonic, scheduled_at)
        @queue_wait_duration = started_at - scheduled_at
        @started_at = started_at
        @finished_monotonic = @started_monotonic = started_monotonic
      end
    end

    attr_reader :periodic_jobs

    def initialize(client, driver, config)
      @client = client
      @condition = ConditionVariable.new
      @config = config
      @driver = driver
      @mutex = Mutex.new
      @producer_threads = {}
      @queue_configs = config.queues.dup
      @removed_queues = {}
      @running = {}
      @started = false
      @stopped = true
      @subscriptions = []
      @threads = []
      @periodic_jobs = PeriodicJobBundle.new(config.periodic_jobs,
        enabled: !config.leader_election_disabled, wake: method(:periodic_jobs_changed))
    end

    def finish_claimed(row, error = nil)
      timing = JobTiming.new(row.attempted_at || Time.now.utc, monotonic_now, row.scheduled_at)
      job = Job.new(@client, row)
      case error
      when nil
        now = Time.now.utc
        completed = @driver.job_complete(id: row.id, finalized_at: now, now: now)
        case completed
        when :cancelled
          finish_failed(row, job, JobCancelError.new, timing, cancelled: true)
        else
          publish(EVENT_JOB_COMPLETED, completed, timing) if completed
        end
      when JobCancelError
        finish_failed(row, job, error, timing, cancelled: true)
      when JobSnoozeError
        finish_snoozed(row, job, error, timing)
      when Interrupted
        finish_interrupted(row, job, timing)
      else
        worker = begin
          resolve_worker(row.kind) if !row.__decode_error && @config.workers.include?(row.kind)
        rescue => worker_error
          @config.logger.error("River worker initialization failed during finalization: #{worker_error.full_message}")
          nil
        end
        finish_failed(row, job, error, timing, worker: worker)
      end
    end

    def perform_job(id, allow_scheduled: false)
      @mutex.synchronize do
        raise ClientAlreadyStartedError, "synchronous execution requires an idle, stopped client" if @started || @performing

        @performing = true
        @stop_requested = false
      end

      begin
        row = @driver.job_claim(id: id, allow_scheduled: allow_scheduled, attempted_by: @config.id)
        raise ArgumentError, "job #{id} is missing, not runnable, or scheduled in the future" unless row

        @mutex.synchronize { @running[row.id] = {queue: row.queue, thread: Thread.current, working: false} }
        outcome, error = execute(row)
        final_row = begin
          @driver.job_get_by_id(id)
        rescue JobRowDecodeError => decode_error
          decode_error.job
        end
        [final_row, error, outcome]
      ensure
        @mutex.synchronize { @performing = false }
      end
    end

    def publish_queue(kind, queue)
      event = Event.new(kind, nil, queue, nil)
      @mutex.synchronize { @subscriptions.dup }.each { |subscription| subscription.publish(event) }
    end

    def healthy?
      @mutex.synchronize do
        @stop_requested || (@producer_threads.all? { |name, thread| @removed_queues[name] || thread.alive? } && (!@maintenance_thread || @maintenance_thread.alive?))
      end
    end

    def interrupt_workers
      @mutex.synchronize do
        @running.values.each { |entry| entry[:thread].raise(Interrupted) if entry[:working] }
      end
    end

    def queue_add(name, queue_config)
      name = name.to_s
      queue_config = QueueConfig.new(max_workers: queue_config) unless queue_config.is_a?(QueueConfig)
      raise ArgumentError, "invalid queue name: #{name.inspect}" unless name.match?(QUEUE_NAME_REGEX) && name.length < 128

      queue_config.resolved_fetch_poll_interval(@config)
      should_start = @mutex.synchronize do
        raise ArgumentError, "queue is already configured: #{name}" if @queue_configs.key?(name)

        @queue_configs[name] = queue_config
        @removed_queues.delete(name)
        @started && !@stop_requested
      end

      if should_start
        begin
          start_producer(name, queue_config)
        rescue
          @mutex.synchronize do
            if @queue_configs[name].equal?(queue_config) && !@producer_threads.key?(name)
              @queue_configs.delete(name)
            end
          end
          raise
        end
        start_maintenance
      end

      wake
      queue_config
    end

    def queue_remove(name)
      name = name.to_s
      producer = @mutex.synchronize do
        raise NotFoundError, "queue is not configured: #{name}" unless @queue_configs.key?(name)
        if @producer_threads[name] == Thread.current || @running.any? { |_id, entry| entry[:queue] == name && entry[:thread] == Thread.current }
          raise ThreadError, "cannot remove a queue from its own worker or producer"
        end

        @removed_queues[name] = true
        @condition.broadcast
        @producer_threads[name]
      end

      producer&.join
      running = @mutex.synchronize do
        @running.values.filter_map { |entry| entry[:thread] if entry[:queue] == name }
      end
      running.each(&:join)

      @mutex.synchronize do
        @queue_configs.delete(name)
        @producer_threads.delete(name)
      end

      true
    end

    def start
      @driver.init_driver if @driver.respond_to?(:init_driver)
      @mutex.synchronize do
        raise ClientAlreadyStartedError, "client is already started" if @started || @performing

        @started = true
        @stop_requested = false
        @stopped = false
      end

      begin
        queues = @mutex.synchronize { @queue_configs.dup }
        queues.each { |name, queue_config| start_producer(name, queue_config) }
        start_maintenance unless @queue_configs.empty? && @periodic_jobs.empty? && @config.maintenance_services.empty?

        self
      rescue
        stop(cancel: true)
        raise
      end
    end

    def started?
      @mutex.synchronize { @started }
    end

    def stop(cancel: false, wait: true)
      threads = @mutex.synchronize do
        return self if @stopped
        if wait && (@threads.include?(Thread.current) || @running.any? { |_id, entry| entry[:thread] == Thread.current })
          raise ThreadError, "cannot wait for client stop from its own runtime thread; use wait: false"
        end

        @stop_requested = true
        @condition.broadcast
        @running.values.each { |entry| entry[:thread].raise(Interrupted) if cancel && entry[:working] }
        @threads.dup
      end

      return self unless wait

      threads.each(&:join)

      running_threads = @mutex.synchronize { @running.values.map { |entry| entry[:thread] } }
      running_threads.each(&:join)

      @driver.leader_release(@config.id) unless @config.leader_election_disabled
      @mutex.synchronize do
        @started = false
        @stopped = true
        @threads.clear
        @producer_threads.clear
        @maintenance_thread = nil
      end

      self
    end

    def stopped?
      @mutex.synchronize { @stopped }
    end

    def subscribe(kinds, buffer_size: 100)
      subscription = Subscription.new(
        kinds,
        buffer_size: buffer_size,
        on_close: method(:remove_subscription)
      )
      @mutex.synchronize { @subscriptions << subscription }
      subscription
    end

    def wake
      @mutex.synchronize { @condition.broadcast }
    end

    private def begin_work(id)
      should_interrupt = @mutex.synchronize do
        entry = @running.fetch(id)
        if @stop_requested
          true
        else
          entry[:working] = true
          false
        end
      end

      raise Interrupted if should_interrupt
    end

    private def check_remote_cancellations(queue)
      entries = @mutex.synchronize { @running.select { |_id, entry| entry[:queue] == queue && entry[:working] } }
      return if entries.empty?

      @driver.job_get_cancelled_ids(entries.keys).each do |id|
        entries.fetch(id)[:thread].raise(JobCancelError)
      end
    end

    private def error_handler_cancel?(error, job)
      return false unless @config.error_handler

      result = if @config.error_handler.respond_to?(:handle_error)
        @config.error_handler.handle_error(error, job)
      else
        @config.error_handler.call(error, job)
      end
      result == :cancel || result == true
    rescue => handler_error
      @config.logger.error("River error handler failed: #{handler_error.full_message}")
      false
    end

    private def execute(row)
      timing = JobTiming.new(Time.now.utc, monotonic_now, row.scheduled_at)
      job = Job.new(@client, row)
      worker = nil #: untyped
      begin
        begin
          raise row.__decode_error if row.__decode_error

          worker = resolve_worker(row.kind)
          begin_work(row.id)
          Thread.handle_interrupt(Interrupted => :immediate, JobCancelError => :immediate) do
            invoke_worker(worker, job)
          end
        ensure
          timing.finished_monotonic = monotonic_now
          finish_work(row.id)
        end

        return [:completed, nil] if finish_transactional_completion(job, timing)

        finalize_hooks = @config.plugins.any? { |plugin| plugin.respond_to?(:job_finalize) }
        if finalize_hooks
          raise JobCancelError if @driver.job_get_cancelled_ids([row.id]).include?(row.id)

          if invoke_plugins(:job_finalize, job, JOB_STATE_COMPLETED).include?(:delete)
            raise JobCancelError if @driver.job_delete_if_running(row.id) == :cancelled

            return [:deleted, nil]
          end
        end

        completed_at = Time.now.utc
        completed = @driver.job_complete(id: row.id, finalized_at: completed_at, metadata: job.metadata_updates, now: completed_at)
        case completed
        when :cancelled then raise JobCancelError
        else publish(EVENT_JOB_COMPLETED, completed, timing) if completed
        end

        [:completed, nil]
      rescue => error
        return [:completed, nil] if finish_transactional_completion(job, timing)

        job.__capture_resumable_metadata!
        outcome = case error
        when JobSnoozeError
          finish_snoozed(row, job, error, timing)
        when JobCancelError
          finish_failed(row, job, error, timing, cancelled: true)
          :cancelled
        when Interrupted
          finish_interrupted(row, job, timing)
        else
          finish_failed(row, job, error, timing, worker: worker)
        end
        [outcome, error]
      ensure
        @mutex.synchronize do
          @running.delete(row.id)
          @condition.broadcast
        end
      end
    end

    private def finish_failed(row, job, error, timing, cancelled: false, worker: nil)
      cancelled ||= error_handler_cancel?(error, job)
      now = Time.now.utc
      attempt_error = AttemptError.new(
        at: timing.started_at,
        attempt: row.attempt,
        error: error.message,
        trace: Array(error.backtrace).join("\n")
      )
      final = cancelled || row.attempt >= row.max_attempts || !retry_allowed?(worker, job, error)
      state = if cancelled
        JOB_STATE_CANCELLED
      elsif final
        JOB_STATE_DISCARDED
      else
        JOB_STATE_RETRYABLE
      end
      scheduled_at = final ? nil : next_retry(row, error, now, worker: worker)
      state = JOB_STATE_AVAILABLE if scheduled_at && scheduled_at <= now + 5

      updated = @driver.job_set_state_if_running(
        id: row.id,
        error: attempt_error,
        finalized_at: final ? now : nil,
        metadata: job.metadata_updates,
        now: now,
        scheduled_at: scheduled_at,
        state: state
      )
      event = cancelled ? EVENT_JOB_CANCELLED : EVENT_JOB_FAILED
      publish(event, updated, timing) if updated

      case updated&.state || state
      when JOB_STATE_CANCELLED then :cancelled
      when JOB_STATE_DISCARDED then :discarded
      else :retried
      end
    end

    private def finish_interrupted(row, job, timing)
      interrupted = @driver.job_set_state_if_running(
        id: row.id,
        attempt: [row.attempt - 1, 0].max,
        metadata: job.metadata_updates,
        scheduled_at: Time.now.utc,
        state: JOB_STATE_AVAILABLE
      )
      publish(EVENT_JOB_INTERRUPTED, interrupted, timing) if interrupted

      (interrupted&.state == JOB_STATE_CANCELLED) ? :cancelled : :interrupted
    end

    private def finish_snoozed(row, job, error, timing)
      scheduled_at = Time.now.utc + error.duration
      state = (error.duration <= 5) ? JOB_STATE_AVAILABLE : JOB_STATE_SCHEDULED
      metadata = job.metadata_updates.merge("snoozes" => next_snooze_count(row.metadata["snoozes"]))
      updated = @driver.job_set_state_if_running(
        id: row.id,
        attempt: [row.attempt - 1, 0].max,
        metadata: metadata,
        scheduled_at: scheduled_at,
        state: state
      )
      publish(EVENT_JOB_SNOOZED, updated, timing) if updated
      (updated&.state == JOB_STATE_CANCELLED) ? :cancelled : :snoozed
    end

    private def finish_transactional_completion(job, timing)
      return false unless job.__completion_attempted

      completed = @driver.job_get_by_id(job.row.id)
      return false unless completed && completed.state == JOB_STATE_COMPLETED

      # Work middleware (including persisted logging) can add metadata after the
      # worker commits. Save it without changing the committed state or timestamp.
      metadata = job.metadata_updates
      completed = @driver.job_metadata_merge(job.row.id, metadata) || completed unless metadata.empty?

      publish(EVENT_JOB_COMPLETED, completed, timing)
      true
    end

    private def finish_work(id)
      @mutex.synchronize do
        entry = @running[id]
        entry[:working] = false if entry
      end
    end

    private def invoke_plugins(name, ...)
      @config.plugins.filter_map { |plugin| plugin.public_send(name, ...) if plugin.respond_to?(name) }
    end

    private def invoke_worker(worker, job)
      return perform_work(worker, job) if @config.plugins.empty?

      operation = -> do
        error = nil
        begin
          invoke_plugins(:work_begin, job)
          perform_work(worker, job)
        rescue => error
          raise
        ensure
          invoke_plugins(:work_end, job, error)
        end
      end
      @config.plugins.reverse_each do |plugin|
        next unless plugin.respond_to?(:work)

        next_operation = operation
        operation = -> { plugin.work(job, next_operation) }
      end

      operation.call
    end

    private def launch(row)
      gate = ::Queue.new
      thread = Thread.new do
        gate.pop
        begin
          Thread.handle_interrupt(Interrupted => :never, JobCancelError => :never) { execute(row) }
        rescue Interrupted, JobCancelError
          # A late asynchronous interrupt may become pending after work has
          # already finalized. The database transition won that race.
        end
      end

      @mutex.synchronize { @running[row.id] = {queue: row.queue, thread: thread, working: false} }
      gate.push(true)
    end

    private def maintenance_loop
      leader = false
      next_schedule = next_rescue = next_cleanup = Time.at(0)
      until stopping?
        now = Time.now.utc
        leader = leader ? @driver.leader_renew(@config.id, now: now) : @driver.leader_acquire(@config.id, now: now)
        if leader
          if now >= next_schedule
            @driver.job_schedule(now: now)
            run_periodic(now)
            @config.maintenance_services.each do |service|
              service.run(@client, @driver, now)
            rescue => error
              @config.logger.error("River maintenance service failed (#{service.class}): #{error.full_message}")
            end
            next_schedule = now + 5
          end

          if now >= next_rescue
            @driver.job_rescue_stuck(horizon: now - 3_600, logger: @config.logger, now: now,
              rescue_if: method(:rescue_job?), retry_policy: @config.retry_policy)
            next_rescue = now + 30
          end

          if now >= next_cleanup
            @driver.job_delete_finalized(now: now, retention: {
              JOB_STATE_CANCELLED => @config.cancelled_job_retention_period,
              JOB_STATE_COMPLETED => @config.completed_job_retention_period,
              JOB_STATE_DISCARDED => @config.discarded_job_retention_period
            })
            @driver.notification_delete_before(horizon: now - 300) if @driver.respond_to?(:notification_delete_before)
            next_cleanup = now + 30
          end
        end

        wait(5)
      end
    rescue => error
      @config.logger.error("River maintenance stopped: #{error.full_message}")
      wait(5)
      retry unless stopping?
    end

    private def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    private def next_retry(row, error, now, worker: nil)
      worker ||= @config.workers[row.kind] unless row.__decode_error
      custom = worker.next_retry(row, error) if worker.respond_to?(:next_retry)

      retry_at = custom || @config.retry_policy.next_retry(row, error, now: now)
      raise ArgumentError, "next_retry must return a Time" unless retry_at.is_a?(Time)

      (retry_at < now) ? DefaultClientRetryPolicy.new.next_retry(row, error, now: now) : retry_at
    rescue => retry_error
      @config.logger.error("River retry scheduling failed; using default backoff: #{retry_error.full_message}")
      DefaultClientRetryPolicy.new.next_retry(row, error, now: now)
    end

    # Preserve Ruby's lenient counter conversion without calling to_i on
    # booleans/collections, or accepting only a prefix of a numeric string.
    # Only canonical non-negative integers are part of the shared protocol.
    private def next_snooze_count(value)
      count = case value
      when Integer then value
      when Float then value.finite? ? value.to_i : 0
      when String then /\A-?[0-9]+\z/.match?(value) ? value.to_i : 0
      when true then 1
      else 0
      end
      # Go stores and increments a signed 64-bit integer.
      ((count + 1 + (1 << 63)) % (1 << 64)) - (1 << 63)
    end

    private def perform_work(worker, job)
      timeout = worker_timeout(worker, job)
      result = timeout ? Timeout.timeout(timeout) { worker.work(job) } : worker.work(job)
      job.__finish_resumable_work!
      result
    end

    private def periodic_jobs_changed
      start_maintenance
      wake
    end

    private def producer_loop(queue, queue_config)
      cooldown = queue_config.resolved_fetch_cooldown(@config)
      # Capture registrations (including aliases) at startup, not construction.
      fetch_options = {} #: Hash[Symbol, Array[String]]
      fetch_options[:kinds] = @config.workers.kinds if @config.fetch_only_known_kinds
      last_fetch = 0.0
      poll_interval = queue_config.resolved_fetch_poll_interval(@config)
      loop do
        draining = queue_stopping?(queue)
        break if draining && running_count(queue).zero?

        check_remote_cancellations(queue)
        if draining
          wait_for_running_jobs(queue, poll_interval)
          next
        end

        capacity = queue_config.max_workers - running_count(queue)
        if capacity.positive?
          # Inserts, completions, and spurious condition wakes must not bypass
          # the minimum interval between database fetches.
          while (sleep_for = cooldown - (monotonic_now - last_fetch)).positive? && !queue_stopping?(queue)
            wait(sleep_for)
          end
          next if queue_stopping?(queue)

          # A pause may arrive during cooldown; only read the queue immediately
          # before fetching, and avoid the read entirely when all slots are busy.
          unless @driver.queue_get(queue)&.paused_at
            jobs = @driver.job_get_available(attempted_by: @config.id, max: capacity, queue: queue, **fetch_options)
            last_fetch = monotonic_now
            jobs.each { |job| launch(job) }
            next unless jobs.empty?
          end
        end

        wait(poll_interval)
      end
    rescue => error
      @config.logger.error("River producer for #{queue.inspect} stopped: #{error.full_message}")
      if queue_stopping?(queue)
        wait_for_running_jobs(queue, poll_interval || @config.fetch_poll_interval)
      else
        wait(poll_interval || @config.fetch_poll_interval)
      end
      retry unless queue_stopping?(queue) && running_count(queue).zero?
    end

    private def publish(kind, job, timing)
      # Another actor may have changed the row after our update. Pending,
      # running, and unknown states do not represent a settled attempt.
      return unless %w[available cancelled completed discarded retryable scheduled].include?(job.state)

      kind = EVENT_JOB_CANCELLED if job.state == JOB_STATE_CANCELLED
      stats = JobStatistics.new(
        monotonic_now - timing.finished_monotonic,
        timing.queue_wait_duration,
        timing.finished_monotonic - timing.started_monotonic
      )
      event = Event.new(kind, job, nil, stats)
      @mutex.synchronize { @subscriptions.dup }.each { |subscription| subscription.publish(event) }
    end

    private def queue_stopping?(queue)
      @mutex.synchronize { @stop_requested || @removed_queues[queue] }
    end

    private def remove_subscription(subscription)
      @mutex.synchronize { @subscriptions.delete(subscription) }
    end

    private def rescue_job?(row, now)
      return true if row.__decode_error

      worker = @config.workers[row.kind]
      worker = worker.new if worker.is_a?(Class)
      timeout = worker_timeout(worker, Job.new(@client, row))
      attempted_at = row.attempted_at #: Time
      !timeout.nil? && now - attempted_at >= timeout
    rescue => error
      @config.logger.error("River rescue timeout check failed; rescuing attempt: #{error.full_message}")
      true
    end

    private def resolve_worker(kind)
      worker = @config.workers[kind]
      raise UnknownJobKindError, kind unless worker
      worker.is_a?(Class) ? worker.new : worker
    end

    private def retry_allowed?(worker, job, error)
      !worker.respond_to?(:retry?) || worker.retry?(job, error)
    rescue => retry_error
      @config.logger.error("River retry? hook failed; allowing retry: #{retry_error.full_message}")
      true
    end

    private def run_periodic(now)
      due = @periodic_jobs.due(now) do |job, error|
        @config.logger.error("River periodic job schedule failed (id=#{job.id.inspect}): #{error.full_message}")
      end
      due.each do |periodic_job|
        value = periodic_job.constructor.call
        next unless value

        args, opts = value.is_a?(Array) ? value : [value, nil]
        opts = (opts || InsertOpts.new).dup
        opts.metadata = (opts.metadata || {}).merge("periodic" => true)
        opts.metadata["river:periodic_job_id"] = periodic_job.id if periodic_job.id && !periodic_job.id.empty?
        @client.insert(args, insert_opts: opts)
      rescue => error
        @config.logger.error("River periodic job failed to insert: #{error.full_message}")
      end
    end

    private def running_count(queue)
      @mutex.synchronize { @running.count { |_id, entry| entry[:queue] == queue } }
    end

    private def start_maintenance
      return if @config.leader_election_disabled

      @mutex.synchronize do
        return unless @started && !@stop_requested
        return if @maintenance_thread&.alive?

        thread = Thread.new { maintenance_loop }
        @maintenance_thread = thread
        @threads << thread
      end
    end

    private def start_producer(name, queue_config)
      @driver.queue_upsert(name)
      @mutex.synchronize do
        return if @stop_requested || @removed_queues[name] || @producer_threads.key?(name)

        # Shutdown must see every launched thread, including a producer whose
        # database setup overlapped a stop or queue removal.
        thread = Thread.new { producer_loop(name, queue_config) }
        @producer_threads[name] = thread
        @threads << thread
      end
    end

    private def stopping?
      @mutex.synchronize { @stop_requested }
    end

    private def wait(duration)
      @mutex.synchronize { @condition.wait(@mutex, duration) unless @stop_requested }
    end

    private def wait_for_running_jobs(queue, duration)
      @mutex.synchronize do
        # Draining still needs a poll interval after stop is requested. Check
        # under the same mutex as completion so the last job's wake isn't lost.
        @condition.wait(@mutex, duration) if @running.any? { |_id, entry| entry[:queue] == queue }
      end
    end

    private def worker_timeout(worker, job)
      timeout = worker.respond_to?(:timeout) ? worker.timeout(job) : @config.job_timeout
      timeout = Float(timeout) unless timeout.nil?
      timeout = @config.job_timeout if timeout == 0
      raise ArgumentError, "worker timeout must be finite and nonnegative, or nil" if timeout && (!timeout.finite? || timeout.negative?)

      timeout
    end
  end
end
