# frozen_string_literal: true

module River
  # Definition of a recurring job and the schedule used to construct it.
  class PeriodicJob
    # Callable that produces job arguments, or an arguments/options pair.
    attr_reader :constructor

    # Stable string identifier used for removal and durable Pro scheduling.
    attr_reader :id

    # Whether the job should run immediately when its scheduler starts.
    attr_reader :run_on_start

    # Object or callable that computes the next run time.
    attr_reader :schedule

    # Creates a periodic job. +schedule+ may respond to +next(time)+ or be a
    # callable, and must return a Time strictly after its argument. Supply a
    # factory block or a +constructor+ callable (not both),
    # invoked whenever the job is due. +id+ accepts a symbol or string; nil
    # leaves the registration anonymous.
    def initialize(schedule:, constructor: nil, id: nil, run_on_start: false, &block)
      raise ArgumentError, "use constructor or a block, not both" if constructor && block

      @constructor = constructor || block
      raise ArgumentError, "a factory block or callable constructor is required" unless @constructor.respond_to?(:call)
      raise ArgumentError, "schedule must respond to next or call" unless schedule.respond_to?(:next) || schedule.respond_to?(:call)
      @id = id&.to_s
      @run_on_start = run_on_start
      @schedule = schedule
    end

    # Returns the next scheduled Time after +now+.
    def next_at(now)
      next_time = schedule.respond_to?(:next) ? schedule.next(now) : schedule.call(now)
      raise ArgumentError, "schedule must return a future Time" unless next_time.is_a?(Time) && next_time > now
      next_time
    end
  end

  # A fixed-duration schedule suitable for PeriodicJob.
  class PeriodicInterval
    # Creates an interval measured in seconds.
    def initialize(seconds)
      @seconds = Float(seconds, exception: true) #: Float
      raise ArgumentError, "period must be finite" unless @seconds.finite?
      raise ArgumentError, "period must be greater than zero" unless @seconds.positive?
    end

    # Returns the next Time one interval after +time+.
    def next(time)
      time + @seconds
    end
  end

  # Thread-safe collection used to change a client's periodic jobs at runtime.
  class PeriodicJobBundle
    def initialize(jobs, wake:, enabled: true)
      @enabled = enabled
      @jobs = {}
      @mutex = Mutex.new
      @next_handle = 0
      @wake = wake
      add_many(jobs)
    end

    # Adds a periodic job and returns a handle that can be passed to #remove.
    def add(job)
      add_many([job]).fetch(0)
    end

    # Adds periodic jobs atomically and returns their handles in input order.
    # Invalid schedules or duplicate IDs leave the registry unchanged.
    def add_many(jobs)
      raise ArgumentError, "periodic jobs require leader election" if !@enabled && !jobs.empty?

      now = Time.now.utc
      entries = jobs.map { |job| {job: job, next_at: job.run_on_start ? now : job.next_at(now)} }
      handles = @mutex.synchronize do
        ids = @jobs.values.to_h { |entry| [entry[:job].id, true] }
        jobs.each do |job|
          next unless job.id
          raise ArgumentError, "periodic job ID is already registered: #{job.id}" if ids[job.id]
          ids[job.id] = true
        end
        entries.map do |entry|
          @next_handle += 1
          @jobs[@next_handle] = entry
          @next_handle
        end
      end
      @wake.call unless handles.empty?
      handles
    end

    # Removes all periodic jobs from the bundle and returns self.
    def clear
      @mutex.synchronize { @jobs.clear }
      self
    end

    def due(now)
      # Application schedules may reenter the registry, so evaluate outside its
      # lock and only advance registrations that haven't been removed or claimed.
      entries = @mutex.synchronize { @jobs.map { |handle, entry| [handle, entry, entry[:next_at]] } }
      entries.filter_map do |handle, entry, next_at|
        next if next_at > now

        following = entry[:job].next_at(now)
        @mutex.synchronize do
          next unless @jobs[handle].equal?(entry) && entry[:next_at] == next_at
          entry[:next_at] = following
          entry[:job]
        end
      rescue => error
        raise unless block_given?
        yield(entry[:job], error)
        nil
      end
    end

    # Returns true when the bundle has no registered periodic jobs.
    def empty?
      @mutex.synchronize { @jobs.empty? }
    end

    # Removes the job associated with +handle+, returning the PeriodicJob or
    # nil when no such handle exists.
    def remove(handle)
      @mutex.synchronize { @jobs.delete(handle)&.fetch(:job) }
    end

    # Removes the periodic job with +id+. Returns whether a job was removed.
    def remove_by_id(id)
      id = id&.to_s
      @mutex.synchronize do
        pair = @jobs.find { |_handle, entry| entry[:job].id == id }
        !!(pair && @jobs.delete(pair.first))
      end
    end
  end
end
