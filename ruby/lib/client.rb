# frozen_string_literal: true

require "digest/sha2"
require "time"

module River
  # Default number of maximum attempts for a job.
  MAX_ATTEMPTS_DEFAULT = 25

  # Largest attempt limit supported by all River databases.
  MAX_ATTEMPTS_LIMIT = 32_767

  # Default priority for a job.
  PRIORITY_DEFAULT = 1

  # Default queue for a job.
  QUEUE_DEFAULT = "default"

  # Provides a River client for inserting and working jobs.
  #
  # Used in conjunction with a River driver like:
  #
  #   DB = Sequel.connect(...)
  #   client = River::Client.new(River::Driver::Sequel.new(DB))
  #
  # River drivers are found in separate gems like `riverqueue-sequel` to help
  # minimize transient dependencies.
  class Client
    # Configuration used by this client.
    attr_reader :config

    # Database driver used by this client.
    attr_reader :driver

    # Creates a client backed by the given database driver. Pass a Config to
    # customize workers, queues, plugins, and runtime behavior.
    def initialize(driver, config: nil)
      @config = config || Config.new
      @driver = driver
      @runtime = ClientRuntime.new(self, driver, @config)
      @time_now_utc = -> { Time.now.utc } # for test time stubbing
    end

    # Internal extension point used by separately packaged batch workers after
    # they atomically claim additional jobs alongside the batch leader.
    # Errors honor the registered worker's retry hooks and River's cancellation,
    # snooze, and interruption signals. Timings use the row's claim timestamp.
    def __finish_claimed_job(row, error = nil)
      @runtime.finish_claimed(row, error)
    end

    def __perform_job(id, allow_scheduled: false)
      @runtime.perform_job(id, allow_scheduled: allow_scheduled)
    end

    def __interrupt_workers = @runtime.interrupt_workers

    def __runtime_healthy? = @runtime.healthy?

    # Returns the client ID recorded on jobs while this client works them.
    def id = config.id

    # Inserts a new job for work given a job args implementation and insertion
    # options (which may be omitted).
    #
    # With job args only:
    #
    #   insert_res = client.insert(SimpleArgs.new(job_num: 1))
    #   insert_res.job # inserted job row
    #
    # With keyword options:
    #
    #   insert_res = client.insert(SimpleArgs.new(job_num: 1), queue: :high_priority)
    #   insert_res.job # inserted job row
    #
    # Job arg implementations are expected to respond to:
    #
    #   * `#kind`: A symbol or string identifying the job kind in the database.
    #   * `#to_json`: Encodes the args to JSON for persistence in the database.
    #     Must match encoding an args struct on the Go side to be workable.
    #
    # They may also respond to `#insert_opts` which is expected to return an
    # `InsertOpts` that contains options that will apply to all jobs of this
    # kind. Insertion options provided as an argument to `#insert` override
    # those returned by job args.
    #
    # For example:
    #
    #   class SimpleArgs
    #     attr_accessor :job_num
    #
    #     def initialize(job_num:)
    #       self.job_num = job_num
    #     end
    #
    #     def kind = "simple"
    #
    #     def to_json = JSON.generate({job_num: job_num})
    #   end
    #
    # See also JobArgsHash for an easy way to insert a job from a hash.
    #
    # Returns an instance of JobInsertResult. A reusable InsertOpts may instead
    # be supplied as +insert_opts:+; do not combine it with keyword options.
    # @type method insert: (jobArgs, ?insert_opts: InsertOpts?, **untyped) -> JobInsertResult
    def insert(args, insert_opts: nil, **options)
      raise ArgumentError, "use insert_opts or keyword options, not both" if insert_opts && !options.empty?
      insert_opts ||= options.empty? ? EMPTY_INSERT_OPTS : InsertOpts.new(**options)
      insert_params = make_insert_params(args, insert_opts)
      run_insert_plugins([insert_params]) { [insert_and_check_unique_job(insert_params)] }.first
    end

    # Inserts many new jobs as part of a single batch operation for improved
    # efficiency.
    #
    # Takes an array of job args or InsertManyParams which encapsulate job args
    # and a paired InsertOpts.
    #
    # With job args:
    #
    #   insert_results = client.insert_many([
    #     SimpleArgs.new(job_num: 1),
    #     SimpleArgs.new(job_num: 2)
    #   ])
    #
    # With InsertManyParams:
    #
    #   insert_results = client.insert_many([
    #     River::InsertManyParams.new(SimpleArgs.new(job_num: 1), max_attempts: 5),
    #     River::InsertManyParams.new(SimpleArgs.new(job_num: 2), queue: :high_priority)
    #   ])
    #
    # Job arg implementations are expected to respond to:
    #
    #   * `#kind`: A symbol or string identifying the job kind in the database.
    #   * `#to_json`: Encodes the args to JSON for persistence in the database.
    #     Must match encoding an args struct on the Go side to be workable.
    #
    # For example:
    #
    #   class SimpleArgs
    #     attr_accessor :job_num
    #
    #     def initialize(job_num:)
    #       self.job_num = job_num
    #     end
    #
    #     def kind = "simple"
    #
    #     def to_json = JSON.generate({job_num: job_num})
    #   end
    #
    # See also JobArgsHash for an easy way to insert a job from a hash.
    #
    # Returns one JobInsertResult for each argument, in input order.
    # Raises ArgumentError if the batch is empty.
    # A batch cannot contain repeated unique keys in their enforced states;
    # the whole batch fails instead of partially inserting it.
    def insert_many(args)
      raise ArgumentError, "no jobs to insert" if args.empty?

      all_params = args.map do |arg|
        if arg.is_a?(InsertManyParams)
          make_insert_params(arg.args, arg.insert_opts || EMPTY_INSERT_OPTS)
        else # jobArgs
          make_insert_params(arg, EMPTY_INSERT_OPTS)
        end
      end

      run_insert_plugins(all_params) do
        @driver.job_insert_many(all_params)
          .map do |job, unique_skipped_as_duplicate|
            JobInsertResult.new(job, unique_skipped_as_duplicated: unique_skipped_as_duplicate)
          end
      end
    end

    # Cancels a job by ID and returns its updated JobRow.
    #
    # Raises NotFoundError if the job does not exist.
    def job_cancel(id)
      @driver.job_cancel(id) || raise(NotFoundError, "job not found: #{id}")
    end

    # Completes a running Job and returns its updated JobRow, including pending
    # metadata and output. The original JobRow is not modified.
    #
    # Requires and joins the caller's transaction on the same Active Record
    # connection or Sequel database. Commit before returning from work;
    # propagate rollback errors so the runtime can retry the job. A committed
    # completion takes precedence over any subsequent worker error.
    #
    # Raises NotFoundError for a missing job, JobCancelError for a pending
    # cancellation, and Error if there is no active transaction or the job is
    # no longer running or completed.
    def job_complete_tx(job)
      raise ArgumentError, "job must be a River::Job" unless job.is_a?(Job)
      raise ArgumentError, "job must belong to this client" unless job.client.equal?(self)
      raise Error, "job must be running" unless job.row.state == JOB_STATE_RUNNING
      raise Error, "job_complete_tx requires an active transaction on the driver's connection" unless @driver.in_transaction?

      # Mark the attempt before the write: an interrupt can arrive after the
      # database commits but before the driver returns. The runtime checks the
      # persisted state because the caller may still roll back this transaction.
      job.__completion_attempted = true
      now = @time_now_utc.call
      completed = @driver.job_complete(id: job.row.id, finalized_at: now, metadata: job.metadata_updates, now: now)
      raise JobCancelError if completed == :cancelled

      completed ||= job_get(job.row.id)
      raise Error, "job must be running" unless completed.state == JOB_STATE_COMPLETED

      completed
    end

    # Deletes a job by ID and returns its former JobRow.
    #
    # Raises NotFoundError if the job does not exist and JobRunningError if it
    # is currently running.
    def job_delete(id)
      job = @driver.job_delete(id) || raise(NotFoundError, "job not found: #{id}")
      raise JobRunningError, "running jobs cannot be deleted" if job.state == JOB_STATE_RUNNING

      job
    end

    # Deletes jobs matching the supplied filters and returns a
    # JobDeleteManyResult. At least one filter is required. Accepts keyword
    # filters or a reusable JobListParams, but not both.
    # @type method job_delete_many: (?JobListParams?, **untyped) -> untyped
    def job_delete_many(params = nil, **filters)
      raise ArgumentError, "use params or keyword filters, not both" if params && !filters.empty?
      params ||= JobListParams.new(**filters)
      raise ArgumentError, "delete with no filters is not allowed" unless params.filters?

      JobDeleteManyResult.new(@driver.job_delete_many(params))
    end

    # Fetches a job by ID.
    #
    # Raises NotFoundError if the job does not exist.
    def job_get(id)
      @driver.job_get_by_id(id) || raise(NotFoundError, "job not found: #{id}")
    end

    # Lists jobs matching the supplied filters and ordering.
    #
    # The returned JobListResult includes a cursor suitable for the next page.
    # Accepts keyword filters or a reusable JobListParams, but not both.
    # @type method job_list: (?JobListParams?, **untyped) -> untyped
    def job_list(params = nil, **filters)
      raise ArgumentError, "use params or keyword filters, not both" if params && !filters.empty?
      params ||= JobListParams.new(**filters)
      jobs = @driver.job_list(params)
      last = jobs.last
      cursor = last && JobListCursor.new(id: last.id, sort_by: params.sort_by, sort_order: params.sort_order, value: last.public_send(params.sort_by))
      JobListResult.new(jobs, cursor)
    end

    # Makes a non-running job immediately available for another attempt and
    # returns its updated JobRow. Raises NotFoundError if it does not exist.
    # Clears any cancellation request from its previous attempt. Raises
    # ArgumentError if another attempt would exceed MAX_ATTEMPTS_LIMIT.
    def job_retry(id)
      @driver.job_retry(id) || raise(NotFoundError, "job not found: #{id}")
    end

    # Applies keyword attributes or a JobUpdateParams to a job and returns its
    # updated JobRow. Explicit nil clears nullable fields; omitted fields are
    # unchanged. Metadata must be a Hash; use an empty Hash to clear it.
    # attempt must be between 0 and MAX_ATTEMPTS_LIMIT (32,767);
    # max_attempts must be between 1 and MAX_ATTEMPTS_LIMIT.
    # Raises NotFoundError if the job does not exist.
    # @type method job_update: (Integer, ?JobUpdateParams?, **untyped) -> JobRow
    def job_update(id, params = nil, **attributes)
      raise ArgumentError, "use params or keyword attributes, not both" if params && !attributes.empty?
      params ||= JobUpdateParams.new(**attributes)
      @driver.job_update(id, params) || raise(NotFoundError, "job not found: #{id}")
    end

    # Returns the live PeriodicJobBundle used to add and remove periodic jobs.
    def periodic_jobs
      raise ArgumentError, "cannot modify periodic jobs when leader_election_disabled is true" if @config.leader_election_disabled

      @runtime.periodic_jobs
    end

    # Adds a queue to the client and returns self. If the client is running, it
    # begins working the queue immediately.
    def queue_add(name, queue_config)
      @runtime.queue_add(name.to_s, queue_config)
      self
    end

    # Fetches a queue by name.
    #
    # Raises NotFoundError if the queue does not exist.
    def queue_get(name)
      @driver.queue_get(name.to_s) || raise(NotFoundError, "queue not found: #{name}")
    end

    # Lists up to +max+ known queues.
    def queue_list(max: 100)
      QueueListResult.new(@driver.queue_list(max: max))
    end

    # Pauses a named queue, or all queues when +name+ is +"*"+.
    def queue_pause(name)
      name = name.to_s
      queues = @driver.queue_pause(name)
      @runtime.wake
      queues.each do |queue|
        @runtime.publish_queue(EVENT_QUEUE_PAUSED, queue)
      end

      true
    end

    # Removes a configured queue, waits for active jobs in it to finish, and
    # returns self.
    # Raises ThreadError if called from that queue's own worker or producer.
    def queue_remove(name)
      @runtime.queue_remove(name.to_s)
      self
    end

    # Resumes a named queue, or all queues when +name+ is +"*"+.
    def queue_resume(name)
      name = name.to_s
      queues = @driver.queue_resume(name)
      @runtime.wake
      queues.each do |queue|
        @runtime.publish_queue(EVENT_QUEUE_RESUMED, queue)
      end

      true
    end

    # Replaces a queue's metadata and returns the updated Queue.
    def queue_update(name, metadata:)
      @driver.queue_update(name.to_s, metadata: metadata) || raise(NotFoundError, "queue not found: #{name}")
    end

    # Starts polling configured queues and working jobs in background threads.
    # Returns self.
    def start
      @runtime.start
      self
    end

    # Returns true while the client runtime is started.
    def started? = @runtime.started?

    # Stops fetching and producing periodic jobs, waits for active jobs to
    # finish, and returns self. Pass wait: false to request stop without
    # waiting; call stop again to finish draining and release resources.
    # In-flight fetches or maintenance operations may finish. Until a waiting
    # stop completes, started? remains true and stopped? remains false.
    # Raises ThreadError for a waiting call from this client's runtime threads.
    def stop(wait: true)
      @runtime.stop(wait: wait)
      self
    end

    # Stops fetching new jobs, interrupts active work, and returns self.
    # Raises ThreadError if called from this client's own runtime threads.
    def stop_and_cancel
      @runtime.stop(cancel: true)
      self
    end

    # Returns true when the client runtime is fully stopped.
    def stopped? = @runtime.stopped?

    # Subscribes to event kinds and returns a Subscription.
    #
    # Call Subscription#close when the subscription is no longer needed.
    def subscribe(*kinds, buffer_size: 100)
      @runtime.subscribe(kinds, buffer_size: buffer_size)
    end

    # Default states that are used during a unique insert. Can be overridden by
    # setting UniqueOpts#by_state.
    DEFAULT_UNIQUE_STATES = [
      JOB_STATE_AVAILABLE,
      JOB_STATE_COMPLETED,
      JOB_STATE_PENDING,
      JOB_STATE_RETRYABLE,
      JOB_STATE_RUNNING,
      JOB_STATE_SCHEDULED
    ].freeze

    EMPTY_INSERT_OPTS = InsertOpts.new.freeze

    INITIAL_STATES = [JOB_STATE_AVAILABLE, JOB_STATE_PENDING, JOB_STATE_SCHEDULED].freeze

    REQUIRED_UNIQUE_STATES = [
      JOB_STATE_AVAILABLE,
      JOB_STATE_PENDING,
      JOB_STATE_RUNNING,
      JOB_STATE_SCHEDULED
    ].freeze

    TAG_RE = /\A\w[\w-]+\w\z/

    private_constant :DEFAULT_UNIQUE_STATES, :EMPTY_INSERT_OPTS, :INITIAL_STATES, :REQUIRED_UNIQUE_STATES, :TAG_RE

    private def insert_and_check_unique_job(insert_params)
      job, unique_skipped_as_duplicate = @driver.job_insert(insert_params)
      JobInsertResult.new(job, unique_skipped_as_duplicated: unique_skipped_as_duplicate)
    end

    private def make_insert_params(args, insert_opts)
      raise ArgumentError, "args should respond to `#kind`" if !args.respond_to?(:kind)

      # ~all objects in Ruby respond to `#to_json`, so check non-nil instead.
      args_json = args.to_json
      raise ArgumentError, "args should return non-nil from `#to_json`" if !args_json

      args_insert_opts = if args.respond_to?(:insert_opts)
        args_with_insert_opts = args #: _JobArgsWithInsertOpts # rubocop:disable Layout/LeadingCommentSpace
        args_with_insert_opts.insert_opts || EMPTY_INSERT_OPTS
      else
        EMPTY_INSERT_OPTS
      end

      max_attempts = insert_opts.max_attempts || args_insert_opts.max_attempts || MAX_ATTEMPTS_DEFAULT
      raise ArgumentError, "max_attempts must be greater than zero" unless max_attempts > 0
      raise ArgumentError, "max_attempts must not exceed #{MAX_ATTEMPTS_LIMIT}" if max_attempts > MAX_ATTEMPTS_LIMIT

      queue = (insert_opts.queue || args_insert_opts.queue || QUEUE_DEFAULT).to_s
      raise ArgumentError, "invalid queue name: #{queue.inspect}" unless queue.match?(QUEUE_NAME_REGEX) && queue.length < 128

      scheduled_at = insert_opts.scheduled_at || args_insert_opts.scheduled_at
      now = @time_now_utc.call
      state = validate_insert_state(insert_opts.state || args_insert_opts.state || ((scheduled_at && scheduled_at > now) ? JOB_STATE_SCHEDULED : JOB_STATE_AVAILABLE))

      insert_params = Driver::JobInsertParams.new(
        args: args,
        encoded_args: args_json,
        kind: args.kind.to_s,
        max_attempts: max_attempts,
        metadata: (args_insert_opts.metadata || {}).merge(insert_opts.metadata || {}),
        priority: insert_opts.priority || args_insert_opts.priority || PRIORITY_DEFAULT,
        queue: queue,
        scheduled_at: scheduled_at&.getutc || now,
        state: state,
        tags: validate_tags(insert_opts.tags || args_insert_opts.tags || [])
      )

      unique_opts = insert_opts.unique_opts || args_insert_opts.unique_opts
      if unique_opts && !unique_opts.empty?
        unique_key, unique_states = make_unique_key_and_bitmask(insert_params, unique_opts)
        insert_params.unique_key = unique_key
        insert_params.unique_states = unique_states
      end

      insert_params
    end

    private def make_unique_key_and_bitmask(insert_params, unique_opts)
      unique_key = ""

      # It's extremely important here that this unique key format and algorithm
      # match the one in the main River library _exactly_. Don't change them
      # unless they're updated everywhere.
      unless unique_opts.exclude_kind
        unique_key += "&kind=#{insert_params.kind}"
      end

      if unique_opts.by_args
        encoded_args = UniqueArgs.encode(insert_params.encoded_args, unique_opts.by_args)
        unique_key += "&args=#{encoded_args}"
      end

      if unique_opts.by_period && unique_opts.by_period != 0
        raise ArgumentError, "by_period should not be less than 1 second" if unique_opts.by_period < 1

        lower_period_bound = truncate_time(insert_params.scheduled_at || @time_now_utc.call, unique_opts.by_period).utc

        unique_key += "&period=#{lower_period_bound.strftime("%FT%TZ")}"
      end

      if unique_opts.by_queue
        unique_key += "&queue=#{insert_params.queue}"
      end

      # Without kind or another dimension, every job would share the same key.
      raise ArgumentError, "exclude_kind requires by_args, by_queue, or by_period" if unique_key.empty?

      unique_key_hash = Digest::SHA256.digest(unique_key)
      states = unique_opts.by_state
      unique_states = validate_unique_states((states && !states.empty?) ? states : DEFAULT_UNIQUE_STATES)

      [unique_key_hash, UniqueBitmask.from_states(unique_states)]
    end

    private def run_insert_plugins(all_params, &insert_operation)
      unless config.plugins.any? { |plugin| plugin.respond_to?(:insert_begin) || plugin.respond_to?(:insert_end) || plugin.respond_to?(:insert_many) }
        results = insert_operation.call
        @runtime.wake
        return results
      end

      operation = -> do
        all_params.each do |insert_params|
          config.plugins.each do |plugin|
            plugin.insert_begin(insert_params) if plugin.respond_to?(:insert_begin)
          end
        end

        # Hooks may change any job in the batch, including an earlier entry.
        # Validate the final states only after all insertion hooks have run.
        all_params.each { |params| params.state = validate_insert_state(params.state) }

        results = insert_operation.call
        results.each do |result|
          config.plugins.reverse_each do |plugin|
            plugin.insert_end(result) if plugin.respond_to?(:insert_end)
          end
        end

        results
      end

      config.plugins.reverse_each do |plugin|
        next unless plugin.respond_to?(:insert_many)

        next_operation = operation
        operation = -> { plugin.insert_many(all_params, next_operation) }
      end

      results = @driver.transaction { operation.call }
      @runtime.wake
      results
    end

    # Truncates the given time down to the interval. For example:
    #
    #   Thu Jan 15 21:26:36 UTC 2024 @ 15 minutes ->
    #   Thu Jan 15 21:15:00 UTC 2024
    private def truncate_time(time, interval_seconds)
      # Go truncates from year 1, not the Unix epoch. Rational arithmetic also
      # avoids floating-point rounding across a period boundary.
      epoch_offset = 62_135_596_800
      interval = interval_seconds.to_r
      Time.at(((time.to_r + epoch_offset) / interval).floor * interval - epoch_offset)
    end

    # Moves an integer that may occupy the entire uint64 space to one that's
    # bounded within int64. Allows overflow.
    private def uint64_to_int64(int)
      [int].pack("Q").unpack1("q") #: Integer # rubocop:disable Layout/LeadingCommentSpace
    end

    private def validate_insert_state(state)
      state = state.to_s #: jobStateInitial # rubocop:disable Layout/LeadingCommentSpace
      raise ArgumentError, "invalid insertion state: #{state.inspect}; must be available, pending, or scheduled" unless INITIAL_STATES.include?(state)

      state
    end

    private def validate_tags(tags)
      tags.each do |tag|
        raise ArgumentError, "tags should be 255 characters or less" if tag.length > 255
        raise ArgumentError, "tag should match regex #{TAG_RE.inspect}" unless TAG_RE.match(tag)
      end
    end

    private def validate_unique_states(states)
      states = states.map(&:to_s) #: Array[jobStateAll] # rubocop:disable Layout/LeadingCommentSpace
      REQUIRED_UNIQUE_STATES.each do |required_state|
        raise ArgumentError, "by_state should include required state #{required_state}" unless states.include?(required_state)
      end

      states
    end
  end

  # A single job to insert that's part of an #insert_many batch insert. Unlike
  # sending raw job args, supports an InsertOpts to pair with the job.
  class InsertManyParams
    # Job args to insert.
    attr_reader :args

    # Insertion options to use with the insert.
    attr_reader :insert_opts

    # Pairs job arguments with per-job keyword options for Client#insert_many.
    # A reusable InsertOpts may be supplied instead as +insert_opts:+.
    # @type method initialize: (jobArgs, ?insert_opts: InsertOpts?, **untyped) -> void
    def initialize(args, insert_opts: nil, **options)
      raise ArgumentError, "use insert_opts or keyword options, not both" if insert_opts && !options.empty?

      @args = args
      @insert_opts = insert_opts || (InsertOpts.new(**options) unless options.empty?)
    end
  end

  # Result of a single insertion.
  class JobInsertResult
    # Inserted job row, or an existing job row if insert was skipped due to a
    # previously existing unique job.
    attr_reader :job

    # True if for a unique job, the insertion was skipped due to an equivalent
    # job matching unique property already being present.
    def unique_skipped_as_duplicate? = @unique_skipped_as_duplicate

    # Compatibility with the spelling used in earlier releases.
    alias_method :unique_skipped_as_duplicated, :unique_skipped_as_duplicate?

    # Creates an insertion result. Applications normally receive instances from
    # Client#insert or Client#insert_many. The keyword retains its original
    # spelling for compatibility with earlier releases.
    def initialize(job, unique_skipped_as_duplicated:)
      @job = job
      @unique_skipped_as_duplicate = unique_skipped_as_duplicated
    end
  end

  # Compatibility with the insertion result class name used in earlier releases.
  InsertResult = JobInsertResult
end
