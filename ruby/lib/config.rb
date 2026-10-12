# frozen_string_literal: true

require "logger"
require "securerandom"
require "socket"

module River
  FETCH_COOLDOWN_DEFAULT = 0.1
  FETCH_POLL_INTERVAL_DEFAULT = 1.0
  JOB_TIMEOUT_DEFAULT = 60.0
  QUEUE_NAME_REGEX = /\A[a-zA-Z0-9_\-:.]+\z/
  QUEUE_NUM_WORKERS_MAX = 10_000

  # Concurrency and polling behavior for one configured queue.
  class QueueConfig
    # Normalized concurrency and polling settings for the queue.
    attr_reader :fetch_cooldown, :fetch_poll_interval, :max_workers

    # Configures worker concurrency and optional polling behavior for one queue.
    def initialize(max_workers:, fetch_cooldown: nil, fetch_poll_interval: nil)
      @fetch_cooldown = fetch_cooldown.nil? ? nil : Float(fetch_cooldown)
      @fetch_poll_interval = fetch_poll_interval.nil? ? nil : Float(fetch_poll_interval)
      @max_workers = Integer(max_workers)

      raise ArgumentError, "fetch intervals must be finite" unless [@fetch_cooldown, @fetch_poll_interval].compact.all?(&:finite?)
      raise ArgumentError, "max_workers must be between 1 and #{QUEUE_NUM_WORKERS_MAX}" unless (1..QUEUE_NUM_WORKERS_MAX).cover?(@max_workers)
      raise ArgumentError, "fetch_cooldown must be zero or greater" if @fetch_cooldown&.negative?
      raise ArgumentError, "fetch_poll_interval must be zero or greater" if @fetch_poll_interval&.negative?
    end

    def resolved_fetch_cooldown(config)
      fetch_cooldown || config.fetch_cooldown
    end

    def resolved_fetch_poll_interval(config)
      value = fetch_poll_interval || config.fetch_poll_interval
      raise ArgumentError, "fetch_poll_interval cannot be less than fetch_cooldown" if value < resolved_fetch_cooldown(config)

      value
    end
  end

  # Lazily generates a client ID once per process. The process ID check
  # regenerates it in forked children without needing a fork hook.
  class DefaultClientID # :nodoc:
    HOSTNAME_MAX_LENGTH = 80

    def initialize
      @mutex = Mutex.new
      @state = nil
    end

    def value
      pid = Process.pid
      @state&.[](pid) || @mutex.synchronize { @state&.[](pid) || generate(pid) }
    end

    # The PID and ID are swapped in as one frozen hash so a lock-free reader
    # never pairs one process's PID with another's ID.
    private def generate(pid)
      id = "#{Socket.gethostname[0, HOSTNAME_MAX_LENGTH]}-#{pid}-#{SecureRandom.hex(6)}".freeze
      @state = {pid => id}.freeze
      id
    end
  end
  private_constant :DefaultClientID

  # Complete configuration for a River client and its worker runtime.
  class Config
    # Configured values used by Client and its runtime.
    attr_reader :cancelled_job_retention_period, :completed_job_retention_period,
      :discarded_job_retention_period, :error_handler, :fetch_cooldown,
      :fetch_only_known_kinds, :fetch_poll_interval, :job_timeout,
      :leader_election_disabled, :logger, :maintenance_services,
      :periodic_jobs, :plugins, :queues, :retry_policy, :workers

    # Creates a client configuration.
    #
    # Configure queues and workers here to enable job processing; a client with
    # neither may still be used for insertion and administrative operations.
    # An explicit +id+ is used exactly as given and must be unique among all
    # running clients. Without one, each process using this config generates
    # its own ID; see #id. +fetch_only_known_kinds+ leaves unregistered kinds
    # untouched in shared queues. +leader_election_disabled+ prevents this
    # client's maintenance; another eligible client must run scheduling,
    # rescue, and cleanup.
    def initialize(
      queues: {},
      workers: Workers.new,
      id: nil,
      fetch_cooldown: FETCH_COOLDOWN_DEFAULT,
      fetch_only_known_kinds: false,
      fetch_poll_interval: FETCH_POLL_INTERVAL_DEFAULT,
      job_timeout: JOB_TIMEOUT_DEFAULT,
      leader_election_disabled: false,
      retry_policy: DefaultClientRetryPolicy.new,
      error_handler: nil,
      plugins: [],
      maintenance_services: [],
      periodic_jobs: [],
      cancelled_job_retention_period: 86_400,
      completed_job_retention_period: 86_400,
      discarded_job_retention_period: 604_800,
      logger: nil
    )
      @cancelled_job_retention_period = retention(cancelled_job_retention_period)
      @completed_job_retention_period = retention(completed_job_retention_period)
      @discarded_job_retention_period = retention(discarded_job_retention_period)
      @error_handler = error_handler
      @fetch_cooldown = Float(fetch_cooldown)
      @fetch_only_known_kinds = fetch_only_known_kinds
      @fetch_poll_interval = Float(fetch_poll_interval)
      @default_id = DefaultClientID.new
      @id = id
      @job_timeout = job_timeout.nil? ? nil : Float(job_timeout)
      @leader_election_disabled = leader_election_disabled
      @logger = logger || Logger.new($stdout, level: Logger::WARN)
      @maintenance_services = maintenance_services.dup.freeze
      @periodic_jobs = periodic_jobs.dup.freeze
      @plugins = plugins.dup.freeze
      @queues = normalize_queues(queues).freeze
      @retry_policy = retry_policy
      @workers = workers

      validate
    end

    # Returns the client ID recorded in +attempted_by+ and used for leadership.
    #
    # An explicitly configured ID is returned unchanged. Otherwise, the ID is
    # generated on first use in each process and stays stable for the life of
    # that process, so clients built from a config before a fork (for example,
    # by a preloading Puma, Unicorn, or Pitchfork server) get distinct IDs in
    # each forked child instead of sharing the parent's.
    def id
      @id || @default_id.value
    end

    # Returns a new Config with the supplied values replacing this config's
    # corresponding settings. Without an +id+ override, the copy shares this
    # config's ID, including its per-process generated default.
    def with(**overrides)
      copy = Config.new(id: @id,
        cancelled_job_retention_period: cancelled_job_retention_period,
        completed_job_retention_period: completed_job_retention_period,
        discarded_job_retention_period: discarded_job_retention_period,
        error_handler: error_handler,
        fetch_cooldown: fetch_cooldown,
        fetch_only_known_kinds: fetch_only_known_kinds,
        fetch_poll_interval: fetch_poll_interval,
        job_timeout: job_timeout,
        leader_election_disabled: leader_election_disabled,
        logger: logger,
        maintenance_services: maintenance_services,
        periodic_jobs: periodic_jobs,
        plugins: plugins,
        queues: queues,
        retry_policy: retry_policy,
        workers: workers, **overrides)
      copy.adopt_default_id(@default_id) unless overrides.key?(:id)
      copy
    end

    protected def adopt_default_id(default_id)
      @default_id = default_id
    end

    private def normalize_queues(queues)
      queues.to_h do |name, queue_config|
        config = queue_config.is_a?(QueueConfig) ? queue_config : QueueConfig.new(max_workers: queue_config)
        [name.to_s, config]
      end
    end

    private def retention(value)
      return nil if value.nil? || value == -1

      seconds = Float(value)
      raise ArgumentError, "retention must be finite and nonnegative, or nil/-1 to disable" unless seconds.finite? && seconds >= 0

      seconds
    end

    private def validate
      raise ArgumentError, "fetch intervals and job_timeout must be finite" unless [fetch_cooldown, fetch_poll_interval, job_timeout].compact.all?(&:finite?)
      raise ArgumentError, "id must be between 1 and 127 characters" if (explicit_id = @id) && !(1...128).cover?(explicit_id.length)
      raise ArgumentError, "fetch_cooldown must be at least 0.001 seconds" if fetch_cooldown < 0.001
      raise ArgumentError, "fetch_poll_interval cannot be less than fetch_cooldown" if fetch_poll_interval < fetch_cooldown
      raise ArgumentError, "job_timeout must be greater than zero or nil" if job_timeout && job_timeout <= 0
      raise ArgumentError, "retry_policy must respond to next_retry" unless retry_policy.respond_to?(:next_retry)
      raise ArgumentError, "workers must be a River::Workers" unless workers.is_a?(Workers)
      raise ArgumentError, "periodic_jobs must be empty when leader_election_disabled is true" if leader_election_disabled && !periodic_jobs.empty?

      queues.each do |name, queue_config|
        raise ArgumentError, "invalid queue name: #{name.inspect}" unless name.match?(QUEUE_NAME_REGEX) && name.length < 128

        queue_config.resolved_fetch_poll_interval(self)
      end
    end
  end
end
