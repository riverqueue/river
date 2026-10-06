# frozen_string_literal: true

module River
  class Error < StandardError; end

  class NotFoundError < Error; end

  class JobRunningError < Error; end

  # A stored job contains fields that cannot be decoded. Administrative reads
  # raise this error; consumers fail this job's attempt without blocking peers.
  class JobRowDecodeError < Error
    attr_reader :job

    def initialize(job, detail)
      @job = job
      super("job row #{job.id} couldn't be decoded: #{detail}")
      job.__decode_error = self
    end
  end

  class JobCancelError < Error
    # Original exception that caused cancellation, when supplied.
    attr_reader :cause

    def initialize(message = "job cancelled", cause: nil)
      @cause = cause
      super(message)
    end
  end

  class JobSnoozeError < Error
    # Number of seconds for which the job should be snoozed.
    attr_reader :duration

    def initialize(duration)
      @duration = Float(duration, exception: true) #: Float
      raise ArgumentError, "duration must be finite" unless @duration.finite?
      raise ArgumentError, "duration must be zero or greater" if @duration.negative?

      super("job snoozed for #{@duration} seconds")
    end
  end

  class UnknownJobKindError < Error
    # Unregistered job kind encountered by the runtime.
    attr_reader :kind

    def initialize(kind)
      @kind = kind
      super("unknown job kind: #{kind}")
    end
  end

  class ClientNotStartedError < Error; end

  class ClientAlreadyStartedError < Error; end

  # Returns an exception that tells River to cancel the current job.
  #
  # An Exception argument is retained as the cancellation's cause; any other
  # non-nil argument is used as its message. Raise the result from a worker:
  #
  #   raise River.job_cancel("account closed")
  def self.job_cancel(error = nil)
    case error
    when Exception
      JobCancelError.new(error.message, cause: error)
    when nil
      JobCancelError.new
    else
      JobCancelError.new(error.to_s)
    end
  end

  # Returns an exception that tells River to reschedule the current job after
  # +duration+ seconds without treating the attempt as an error.
  #
  #   raise River.job_snooze(30)
  def self.job_snooze(duration)
    JobSnoozeError.new(duration)
  end
end
