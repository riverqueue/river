# frozen_string_literal: true

require "io/wait"

module River
  # Owns a dedicated worker process. Call run on its main thread, after boot.
  # A supervisor should restart failed processes. An unresponsive stop ends
  # the process with exit!(1), bypassing application at_exit handlers.
  class WorkerRunner
    # Grace and finalization deadlines are finite, nonnegative seconds.
    def initialize(client, finalization_timeout: 5, out: $stdout, stop_timeout: 30)
      @client = client
      @finalization_timeout = Float(finalization_timeout, exception: true) #: Float
      @out = out
      @stop_timeout = Float(stop_timeout, exception: true) #: Float
      [@finalization_timeout, @stop_timeout].each do |value|
        raise ArgumentError, "stop deadlines must be finite and nonnegative" unless value.finite? && value >= 0
      end
    end

    # Starts the client and blocks until stopped. Returns 0 for a clean drain,
    # 1 for a runtime failure or interrupted stop. Restores signal handlers.
    def run
      raise ArgumentError, "worker runner must run on the main thread of the main Ractor" unless Thread.current == Thread.main && Ractor.current == Ractor.main
      raise ArgumentError, "worker runner requires a stopped client" if @client.started?
      raise ArgumentError, "configure at least one worker queue" if @client.config.queues.empty?

      reader, writer = IO.pipe
      handlers = {} #: Hash[String, untyped]
      %w[INT TERM TSTP].each do |signal|
        handlers[signal] = Signal.trap(signal) { writer.write_nonblock((signal == "TSTP") ? "Q" : "S", exception: false) }
      end

      begin
        log("starting")
        @client.job_list(JobListParams.new(limit: 1))
        @client.start
        log("ready pid=#{Process.pid}")

        supervise(reader)
      ensure
        handlers.each { |signal, handler| Signal.trap(signal, handler) }
        reader.close
        writer.close
      end
    end

    private def log(message)
      @out.puts("River worker: #{message}")
      @out.flush
    end

    private def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    private def supervise(reader)
      deadline = 0.0
      failed = false
      interrupted = false
      stopper = nil #: Thread?
      loop do
        signals = reader.wait_readable(0.1) ? reader.read_nonblock(4_096) : ""
        if signals.include?("Q")
          @client.stop(wait: false)
          log("stop requested; no new work will be fetched")
        end

        unless @client.__runtime_healthy?
          log("runtime thread exited unexpectedly")
          failed = true
        end

        if !stopper && (signals.include?("S") || failed)
          @client.stop(wait: false)
          log("draining active attempts")
          deadline = monotonic_now + @stop_timeout
          stopper = Thread.new { @client.stop }
          stopper.report_on_exception = false
          # Consume only the first stop request; a second escalates immediately.
          signals = signals.sub("S", "")
        end

        next unless stopper

        if stopper.join(0)
          stopper.value
          log("stopped")
          return (failed || interrupted) ? 1 : 0
        end

        next unless signals.include?("S") || monotonic_now >= deadline

        if interrupted
          log("finalization deadline exceeded; forcing process exit")
          Process.exit!(1)
        end

        interrupted = true
        log("interrupting active attempts")
        @client.__interrupt_workers
        deadline = monotonic_now + @finalization_timeout
      end
    end
  end
end
