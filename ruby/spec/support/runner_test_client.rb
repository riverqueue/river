# frozen_string_literal: true

# Lifecycle double for deterministic deadline/error tests. Database-backed
# subprocess tests exercise real claims and signals in the driver suites.
class RunnerTestClient < River::Client
  attr_reader :interruptions, :stop_thread

  def initialize(healthy: true, signals: ["TERM"], stall: false, unresponsive: false)
    @config = River::Config.new(queues: {"test" => 1})
    @healthy = healthy
    @interruptions = 0
    @release = Queue.new
    @signals = signals
    @stall = stall
    @unresponsive = unresponsive
  end

  def __interrupt_workers
    @interruptions += 1
    release unless @unresponsive
  end

  def __runtime_healthy? = @healthy

  def job_list(_params) = []

  def release = @release.push(true)

  def start
    @signals.each { |signal| Process.kill(signal, Process.pid) }
    self
  end

  def started? = false

  def stop(wait: true)
    return self unless wait

    @stop_thread = Thread.current
    @release.pop if @stall
    self
  end
end
