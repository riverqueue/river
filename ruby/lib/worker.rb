# frozen_string_literal: true

module River
  # Registry that maps job kinds to worker objects.
  class Workers
    # Creates an empty worker registry.
    def initialize
      @workers = {}
    end

    # Registers a worker for a job kind and optional aliases, returning self.
    #
    # With one argument, the kind is read from +worker.kind+ or
    # +worker.class.kind+. Pass a kind and worker separately to override it.
    # Kinds and aliases accept symbols or strings.
    def add(kind_or_worker, worker = nil, aliases: [])
      if worker
        kind = kind_or_worker.to_s
      else
        worker = kind_or_worker
        kind = worker.respond_to?(:kind) ? worker.kind.to_s : worker.class.kind.to_s
      end

      candidates = [kind] + aliases.map(&:to_s)
      candidates.each do |candidate|
        raise ArgumentError, "worker for kind #{candidate.inspect} is already registered" if @workers.key?(candidate)
      end

      candidates.each do |candidate|
        @workers[candidate] = worker
      end

      self
    end

    # Returns the worker registered for +kind+, or nil when none is registered.
    def [](kind)
      @workers[kind.to_s]
    end

    # Returns a registered worker. Like Hash#fetch, raises KeyError when missing
    # unless a default argument or fallback block is supplied.
    def fetch(kind, ...)
      workers = @workers #: untyped
      workers.fetch(kind.to_s, ...)
    end

    # Returns true if a worker is registered for +kind+.
    def include?(kind)
      @workers.key?(kind.to_s)
    end

    # Returns the registered job kinds, including aliases.
    def kinds
      @workers.keys.freeze
    end
  end

  # A job being worked, with access to its persisted row and attempt-local
  # metadata changes.
  class Job
    # Client working this job.
    attr_reader :client

    # Persisted JobRow claimed for this attempt.
    attr_reader :row

    def initialize(client, row)
      @client = client
      @logger = nil
      @metadata_updates = {}
      @row = row
      initialize_resumable_state
    end

    # Returns the job arguments decoded from their persisted JSON.
    def args = row.args

    # Returns the attempt-local logger supplied by JobPersistedLogging::Plugin.
    # Output is saved to job metadata when the attempt finishes, not streamed to the DB.
    def logger
      @logger || raise(Error, "job.logger requires River::JobPersistedLogging::Plugin in Config#plugins")
    end

    # Internal middleware boundary; never replaces the client's process logger.
    def __with_logger(logger)
      previous = @logger
      @logger = logger
      begin
        yield
      ensure
        @logger = previous
      end
    end

    # Returns a snapshot of persisted metadata merged with this attempt's changes.
    def metadata = JSON.parse(JSON.generate(row.metadata.merge(@metadata_updates)))

    # Returns a copy of metadata changes waiting to be persisted when work
    # finishes.
    def metadata_updates
      JSON.parse(JSON.generate(@metadata_updates))
    end

    def method_missing(name, ...)
      return row.public_send(name, ...) if row.respond_to?(name)

      super
    end

    # Stores a worker result under the conventional +"output"+ metadata key.
    def output=(value)
      update_metadata("output" => value)
    end

    def respond_to_missing?(name, include_private = false)
      row.respond_to?(name, include_private) || super
    end

    # Merges JSON-compatible values into the job metadata to be persisted when
    # work finishes. Values are validated and copied immediately, so invalid JSON
    # fails the work without also preventing its failure from being recorded.
    # Keys are converted to strings. Returns self.
    def update_metadata(values)
      @metadata_updates.merge!(JSON.parse(JSON.generate(values.transform_keys(&:to_s))))
      self
    end
  end

  # Default retry schedule used when a worker does not provide its own policy.
  class DefaultClientRetryPolicy
    # Match Go's maximum signed 64-bit nanosecond duration exactly.
    MAX_DELAY = Rational((1 << 63) - 1, 1_000_000_000)
    private_constant :MAX_DELAY

    # Creates River's default quartic-backoff retry policy. A Random source may
    # be injected to make jitter deterministic.
    def initialize(random: Random)
      @random = random
    end

    # Returns the Time at which +job+ should next be attempted.
    def next_retry(job, _error = nil, now: Time.now.utc)
      error_count = Array(job.errors).length + 1
      seconds = Integer(error_count**4)
      return now + MAX_DELAY if seconds >= MAX_DELAY

      delay = seconds + (seconds * (@random.rand * 0.2 - 0.1))
      now + [delay, MAX_DELAY].min
    end
  end
end
