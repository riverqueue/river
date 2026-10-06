# frozen_string_literal: true

module River
  EVENT_JOB_CANCELLED = :job_cancelled
  EVENT_JOB_COMPLETED = :job_completed
  EVENT_JOB_FAILED = :job_failed
  EVENT_JOB_INTERRUPTED = :job_interrupted
  EVENT_JOB_SNOOZED = :job_snoozed
  EVENT_QUEUE_PAUSED = :queue_paused
  EVENT_QUEUE_RESUMED = :queue_resumed

  # Event emitted by a Client and delivered through a Subscription.
  Event = Data.define(:kind, :job, :queue, :stats)

  # Timing statistics attached to job lifecycle events, in seconds.
  # run_duration measures worker execution including work hooks;
  # complete_duration measures finalization after execution finishes.
  # queue_wait_duration uses the schedule at the start of execution, before
  # retries or snoozes change it.
  # Execution and completion durations use a monotonic clock. Externally
  # executed jobs have zero run_duration because this runtime does not observe
  # their execution; their queue wait ends at the row's attempted_at timestamp.
  JobStatistics = Data.define(:complete_duration, :queue_wait_duration, :run_duration)

  # Bounded stream of selected Client lifecycle events.
  class Subscription
    # Creates an event subscription. Applications normally receive instances
    # from Client#subscribe.
    def initialize(kinds, buffer_size: 100, on_close: nil)
      @closed = false
      @kinds = kinds.map(&:to_sym).freeze
      @mutex = Mutex.new
      @on_close = on_close
      @queue = SizedQueue.new(buffer_size)
    end

    # Closes the subscription and releases it from its client. Closing more than
    # once is safe. Buffered events remain readable; blocked readers wake once
    # the buffer is drained.
    def close
      on_close = @mutex.synchronize do
        return if @closed

        @closed = true
        @queue.close
        @on_close.tap { @on_close = nil }
      end

      on_close&.call(self)
      nil
    end

    # Yields events as they arrive until the subscription is closed. Returns an
    # Enumerator when no block is given.
    def each
      return enum_for(:each) unless block_given?

      loop do
        event = @queue.pop
        break if event.nil?

        yield event
      end
    end

    # Removes and returns the next queued value, blocking unless +non_block+ is
    # true. A blocking pop returns nil once the subscription is closed and its
    # buffered events have been drained.
    #
    # A non-blocking pop raises ThreadError when no event is available.
    def pop(non_block = false)
      @queue.pop(non_block)
    end

    def publish(event)
      @mutex.synchronize do
        return if @closed || !@kinds.include?(event.kind)

        @queue.push(event, true)
      end
    rescue ThreadError
      nil
    end
  end
end
