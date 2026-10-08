# frozen_string_literal: true

module River
  # Options for job insertion, and which can be provided by implementing
  # #insert_opts on job args, or specified as a parameter on #insert or
  # #insert_many.
  class InsertOpts
    # The maximum number of total attempts (including both the original run and
    # all retries) before a job is abandoned and set as discarded.
    # Must be between 1 and MAX_ATTEMPTS_LIMIT (32,767).
    # Defaults to MAX_ATTEMPTS_DEFAULT.
    attr_accessor :max_attempts

    # Arbitrary metadata to merge into the persisted job. River and River Pro
    # reserve keys prefixed with `river:` and workflow metadata keys.
    attr_accessor :metadata

    # The priority of the job, with 1 being the highest priority and 4 being the
    # lowest. When fetching available jobs to work, the highest priority jobs
    # will always be fetched before any lower priority jobs are fetched. Note
    # that if your workers are swamped with more high-priority jobs then they
    # can handle, lower priority jobs may not be fetched.
    #
    # Defaults to PRIORITY_DEFAULT.
    attr_accessor :priority

    # The name of the job queue in which to insert the job, as a symbol or string.
    # Must contain 1 to 127 ASCII letters, digits, underscores, hyphens, colons, or periods.
    #
    # Defaults to QUEUE_DEFAULT.
    attr_accessor :queue

    # A time in future at which to schedule the job (i.e. in cases where it
    # shouldn't be run immediately). The job is guaranteed not to run before
    # this time, but may run slightly after depending on the number of other
    # scheduled jobs and how busy the queue is.
    #
    # Use of this option generally only makes sense when passing options into
    # Insert rather than when a job args is returning `#insert_opts`, however,
    # it will work in both cases.
    attr_accessor :scheduled_at

    # Initial state. Primarily intended for extensions such as workflows and
    # sequences, which insert blocked jobs as `:pending`. Accepts only
    # `available`, `pending`, or `scheduled`, as symbols or strings.
    attr_accessor :state

    # An arbitrary list of keywords to add to the job. They have no functional
    # behavior and are meant entirely as a user-specified construct to help
    # group and categorize jobs.
    #
    # If tags are specified from both a job args override and from options on
    # Insert, the latter takes precedence. Tags are not merged.
    attr_accessor :tags

    # Options relating to job uniqueness. No unique options means that the job
    # is never treated as unique.
    attr_accessor :unique_opts

    # Creates options that override the defaults for a single job insertion.
    def initialize(
      max_attempts: nil,
      metadata: nil,
      priority: nil,
      queue: nil,
      scheduled_at: nil,
      state: nil,
      tags: nil,
      unique_opts: nil
    )
      self.max_attempts = max_attempts
      self.metadata = metadata
      self.priority = priority
      self.queue = queue
      self.scheduled_at = scheduled_at
      self.state = state
      self.tags = tags
      self.unique_opts = unique_opts
    end
  end

  # Parameters for uniqueness for a job.
  #
  # If all properties are nil, no uniqueness is enforced. As each property is
  # initialized, it's added as a dimension on the uniqueness matrix, and with
  # any property on, the job's kind counts toward uniqueness unless #exclude_kind
  # is enabled.
  #
  # So for example, if only #by_queue is on, then for the given job kind, only a
  # single instance is allowed in any given queue, regardless of other
  # properties on the job. If both #by_args and #by_queue are on, then for the
  # given job kind, a single instance is allowed for each combination of args
  # and queues. If either args or queue is changed on a new job, it's allowed to
  # be inserted as a new job.
  class UniqueOpts
    # Empty options don't enable uniqueness. Zero periods and empty state lists
    # have the same meaning as their unset equivalents in the Go client.
    def empty?
      !by_args && !by_queue && !exclude_kind && (!by_period || by_period == 0) && (!by_state || by_state.empty?)
    end

    # Indicates that uniqueness should be enforced for any specific instance of
    # encoded args for a job. An array of symbol or string keys selects only
    # those top-level arguments. Nested arrays select paths, for example
    # +by_args: [[:account, :id], :label]+. Dots in a string key are literal.
    #
    # Default is false, meaning that as long as any other unique property is
    # enabled, uniqueness will be enforced for a kind regardless of input args.
    attr_accessor :by_args

    # Defines uniqueness within a given period. The effective scheduled time
    # (or current time for unscheduled jobs) is rounded down to a period boundary
    # in UTC. Equivalent times in different zones produce the same unique key.
    #
    # The period should be specified in seconds. So a job that's unique every 15
    # minute period would have a value of 900. Nonzero periods must be at least
    # one second; zero disables period-based uniqueness.
    #
    # Default is no unique period, meaning that as long as any other unique
    # property is enabled, uniqueness will be enforced across all jobs of the
    # kind in the database, regardless of when they were scheduled.
    attr_accessor :by_period

    # Indicates that uniqueness should be enforced within each queue.
    #
    # Default is false, meaning that as long as any other unique property is
    # enabled, uniqueness will be enforced for a kind across all queues.
    attr_accessor :by_queue

    # Indicates that uniqueness should be enforced across any of the states in
    # the given set. For example, if the given states were `(scheduled,
    # running)` then a new job could be inserted even if one of the same kind
    # was already being worked by the queue (new jobs are inserted as
    # `available`).
    #
    # Unlike other unique options, ByState gets a default when it's not set for
    # user convenience. The default is equivalent to:
    #
    #   by_state: %i[available completed pending running retryable scheduled]
    #
    # With this setting, any jobs of the same kind that have been cancelled or
    # discarded, but not yet cleaned out by the system, won't count towards the
    # uniqueness of a new insert.
    #
    # The pending, scheduled, available, and running states are required when
    # customizing this list. State names accept symbols or strings.
    attr_accessor :by_state

    # Indicates that the job kind should not be considered for uniqueness. This
    # is useful when you want to enforce uniqueness based on other properties
    # across multiple worker types.
    #
    # Requires #by_args, #by_queue, or a nonzero #by_period. Otherwise every job
    # would share the same key, so insertion raises ArgumentError. #by_state
    # alone does not supply a key dimension.
    attr_accessor :exclude_kind

    # Creates a set of dimensions used to determine whether an inserted job is
    # unique.
    def initialize(
      by_args: nil,
      by_period: nil,
      by_queue: nil,
      by_state: nil,
      exclude_kind: nil
    )
      self.by_args = by_args
      self.by_period = by_period
      self.by_queue = by_queue
      self.by_state = by_state
      self.exclude_kind = exclude_kind
    end
  end
end
