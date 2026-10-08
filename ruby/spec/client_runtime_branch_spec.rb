# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe River::ClientRuntime do
  it "leaves startup retryable when capability detection fails before any workers start" do
    attempts, releases = 0, 0
    driver = Object.new
    driver.define_singleton_method(:init_driver) do
      attempts += 1
      raise "detection failed" if attempts == 1
    end
    driver.define_singleton_method(:leader_release) { |_id| releases += 1 }
    value = runtime(driver: driver)

    expect { value.start }.to raise_error("detection failed")
    expect(value).not_to be_started
    expect(value).to be_stopped
    expect(releases).to eq(0)
    expect(value.start).to equal(value)
    value.stop
    expect(releases).to eq(1)
  end

  def row(id: 1, metadata: {}, max_attempts: 1)
    River::JobRow.new(
      id: id,
      args: {},
      attempt: 1,
      created_at: Time.now.utc,
      kind: "branch_worker",
      max_attempts: max_attempts,
      metadata: metadata,
      priority: 1,
      queue: "branch",
      scheduled_at: Time.now.utc,
      state: River::JOB_STATE_RUNNING
    )
  end

  def config(worker: Object.new, queues: {})
    River::Config.new(
      id: "branch-runtime",
      logger: Logger.new(StringIO.new),
      queues: queues,
      workers: River::Workers.new.add("branch_worker", worker)
    )
  end

  def runtime(driver: Object.new, worker: Object.new, queues: {})
    described_class.new(Object.new, driver, config(queues: queues, worker: worker))
  end

  def execute(runtime, value)
    runtime.instance_variable_set(
      :@running,
      value.id => {queue: value.queue, thread: Thread.current, working: false}
    )
    runtime.send(:execute, value)
  end

  describe "event timings" do
    it "counts finalization hooks as completion time" do
      clock = 100.0
      worker = Object.new
      worker.define_singleton_method(:work) { |_job| clock += 2 }
      plugin = Object.new
      plugin.define_singleton_method(:job_finalize) { |_job, _state| clock += 3 }
      updated = row
      driver = Object.new
      driver.define_singleton_method(:job_get_cancelled_ids) { |_ids| [] }
      driver.define_singleton_method(:job_complete) do |**|
        clock += 4
        updated.state = "completed"
        updated
      end
      value = described_class.new(Object.new, driver, config(worker: worker).with(plugins: [plugin]))
      value.define_singleton_method(:monotonic_now) { clock }
      subscription = value.subscribe([:job_completed])

      execute(value, updated)

      expect(subscription.pop(true).stats).to have_attributes(complete_duration: 7.0, run_duration: 2.0)
    end

    [
      [nil, :job_completed],
      [River::JobCancelError.new, :job_cancelled],
      [River::ClientRuntime::Interrupted.new, :job_interrupted],
      [River::JobSnoozeError.new(10), :job_snoozed],
      [RuntimeError.new("failed"), :job_failed]
    ].each do |error, event_kind|
      it "separates worker and completion durations for #{event_kind}" do
        clock = 100.0
        worker = Object.new
        worker.define_singleton_method(:work) do |_job|
          clock += 2
          raise error if error
        end
        updated = row(max_attempts: 25)
        updated.scheduled_at = Time.now.utc - 60
        driver = Object.new
        driver.define_singleton_method(:job_complete) do |**|
          clock += 3
          updated.state = "completed"
          updated
        end
        driver.define_singleton_method(:job_set_state_if_running) do |**params|
          clock += 3
          updated.scheduled_at = params[:scheduled_at] if params[:scheduled_at]
          updated.state = params.fetch(:state)
          updated
        end
        value = runtime(driver: driver, worker: worker)
        value.define_singleton_method(:monotonic_now) { clock }
        subscription = value.subscribe([event_kind])

        execute(value, updated)

        expect(subscription.pop(true).stats).to have_attributes(
          complete_duration: 3.0, queue_wait_duration: be_within(1).of(60), run_duration: 2.0
        )
      end
    end

    [nil, RuntimeError.new("failed")].each do |error|
      it "measures finalization of externally claimed jobs with error #{error.inspect}" do
        clock = 100.0
        updated = row
        updated.scheduled_at = Time.now.utc - 60
        driver = Object.new
        driver.define_singleton_method(:job_complete) do |**|
          clock += 3
          updated.state = "completed"
          updated
        end
        driver.define_singleton_method(:job_set_state_if_running) do |**params|
          clock += 3
          updated.state = params.fetch(:state)
          updated
        end
        value = runtime(driver: driver)
        value.define_singleton_method(:monotonic_now) { clock }
        subscription = value.subscribe([error ? :job_failed : :job_completed])

        value.finish_claimed(updated, error)

        expect(subscription.pop(true).stats).to have_attributes(
          complete_duration: 3.0, queue_wait_duration: be_within(1).of(60), run_duration: 0.0
        )
      end
    end
  end

  it "detects dead runtime threads, excluding intentionally removed or stopped producers" do
    value = runtime
    thread = Object.new
    alive = true
    thread.define_singleton_method(:alive?) { alive }
    value.instance_variable_set(:@producer_threads, {"branch" => thread})
    expect(value.healthy?).to be true
    alive = false
    expect(value.healthy?).to be false
    value.instance_variable_set(:@removed_queues, {"branch" => true})
    expect(value.healthy?).to be true
    value.instance_variable_set(:@maintenance_thread, thread)
    expect(value.healthy?).to be false
    alive = true
    expect(value.healthy?).to be true
    alive = false
    value.instance_variable_set(:@stop_requested, true)
    expect(value.healthy?).to be true
  end

  it "interrupts only working attempts through the client runner extension" do
    value = runtime
    errors = []
    thread = Object.new
    thread.define_singleton_method(:raise) { |error| errors << error }
    value.instance_variable_set(:@running, {1 => {thread: thread, working: true}, 2 => {thread: thread, working: false}})
    client = River::Client.new(Object.new)
    client.instance_variable_set(:@runtime, value)
    client.__interrupt_workers
    expect(errors).to eq([River::ClientRuntime::Interrupted])
    expect(client.__runtime_healthy?).to be true
  end

  [true, false].each do |retry_error|
    it "honors worker retry? returning #{retry_error}" do
      worker = Object.new
      worker.define_singleton_method(:retry?) { |_job, _error| retry_error }
      worker.define_singleton_method(:next_retry) { |_job, _error| Time.now.utc + 60 }
      updates = []
      driver = Object.new
      driver.define_singleton_method(:job_set_state_if_running) { |**params|
        updates << params
        nil
      }

      value = row(max_attempts: 25)
      timing = described_class::JobTiming.new(Time.now.utc, 0.0, value.scheduled_at)
      runtime(driver: driver, worker: worker).send(:finish_failed, value,
        River::Job.new(Object.new, value), RuntimeError.new("failed"), timing, worker: worker)

      expect(updates.last[:state]).to eq(retry_error ? River::JOB_STATE_RETRYABLE : River::JOB_STATE_DISCARDED)
    end
  end

  it "does not publish completion when an externally claimed job lost its running state" do
    driver = Object.new
    driver.define_singleton_method(:job_complete) { |**| nil }

    expect(runtime(driver: driver).finish_claimed(row)).to be_nil
  end

  it "rejects dynamic periodic registration when leader election is disabled" do
    value = described_class.new(Object.new, Object.new, config.with(leader_election_disabled: true))
    periodic = River::PeriodicJob.new(schedule: River::PeriodicInterval.new(60)) {}
    value.start

    expect { value.periodic_jobs.add(periodic) }.to raise_error(ArgumentError, "periodic jobs require leader election")
    expect(value.periodic_jobs).to be_empty
    expect(value.periodic_jobs.add_many([])).to eq([])
  ensure
    value&.stop
  end

  it "keeps one maintenance thread when periodic jobs are added to a running client" do
    driver = Object.new
    driver.define_singleton_method(:leader_release) { |_| }
    value = runtime(driver: driver)
    entered, release = Queue.new, Queue.new
    value.define_singleton_method(:maintenance_loop) do
      entered << true
      release.pop
    end
    value.start
    periodic = River::PeriodicJob.new(schedule: River::PeriodicInterval.new(60)) {}
    value.periodic_jobs.add(periodic)
    Timeout.timeout(3) { entered.pop }
    thread = value.instance_variable_get(:@maintenance_thread)

    value.periodic_jobs.add(periodic)

    expect(value.instance_variable_get(:@threads)).to eq([thread])
    expect(entered).to be_empty
  ensure
    release << true if release
    value&.stop
  end

  it "starts a producer and maintenance when adding a queue to a running client" do
    value = runtime
    calls = []
    value.instance_variable_set(:@started, true)
    value.instance_variable_set(:@stop_requested, false)
    value.define_singleton_method(:start_producer) { |name, queue_config| calls << [:producer, name, queue_config] }
    value.define_singleton_method(:start_maintenance) { calls << [:maintenance] }

    queue_config = value.queue_add("dynamic", 2)

    expect(calls).to eq([[:producer, "dynamic", queue_config], [:maintenance]])
  end

  it "allows retrying a queue addition after its database setup fails" do
    driver = Object.new
    attempts = 0
    driver.define_singleton_method(:queue_upsert) do |_name|
      attempts += 1
      raise "queue setup failed" if attempts == 1
    end
    value = runtime(driver: driver)
    value.instance_variable_set(:@started, true)
    value.define_singleton_method(:producer_loop) { |*_args| }
    value.define_singleton_method(:start_maintenance) {}

    expect { value.queue_add("dynamic", 1) }.to raise_error("queue setup failed")
    expect { value.queue_add("dynamic", 1) }.not_to raise_error
    expect(value.queue_remove("dynamic")).to be true
  end

  it "does not launch services after a stop during database setup" do
    driver = Object.new
    value = runtime(driver: driver, queues: {branch: 1})
    driver.define_singleton_method(:queue_upsert) { |_| value.stop }
    driver.define_singleton_method(:leader_release) { |_| }
    value.define_singleton_method(:producer_loop) { |*_args| }
    value.define_singleton_method(:maintenance_loop) {}

    value.start

    expect(value).to be_stopped
    expect(value.instance_variable_get(:@threads)).to be_empty
  end

  [:started, :replaced].each do |change|
    it "preserves a queue #{change} concurrently with a failed addition" do
      driver = Object.new
      value = runtime(driver: driver)
      value.instance_variable_set(:@started, true)
      value.define_singleton_method(:producer_loop) { |*_args| }
      value.define_singleton_method(:start_maintenance) {}
      queue_config = River::QueueConfig.new(max_workers: 1)
      attempts = 0
      driver.define_singleton_method(:queue_upsert) do |name|
        attempts += 1
        if attempts == 1
          if change == :started
            value.send(:start_producer, name, queue_config)
          else
            value.queue_remove(name)
            value.queue_add(name, 2)
          end
          raise "original setup failed"
        end
      end

      expect { value.queue_add("dynamic", queue_config) }.to raise_error("original setup failed")
      expect(value.queue_remove("dynamic")).to be true
    end
  end

  it "does not launch a producer for a queue removed during database setup" do
    driver = Object.new
    value = runtime(driver: driver, queues: {branch: 1})
    driver.define_singleton_method(:queue_upsert) { |_| value.queue_remove("branch") }
    value.define_singleton_method(:producer_loop) { |*_args| }

    value.start

    expect(value.instance_variable_get(:@threads)).to be_empty
  end

  it "allows queue changes during startup without launching a producer twice" do
    driver = Object.new
    value = runtime(driver: driver, queues: {branch: 1})
    driver.define_singleton_method(:queue_upsert) { |name| value.queue_add("dynamic", 1) if name == "branch" }
    value.define_singleton_method(:producer_loop) { |*_args| }
    value.define_singleton_method(:start_maintenance) {}

    value.start
    value.send(:start_producer, "dynamic", River::QueueConfig.new(max_workers: 1))
    expect(value.instance_variable_get(:@threads).length).to eq(2)
    value.queue_remove("branch")
    value.queue_remove("dynamic")
  end

  it "joins only work belonging to a queue as it is removed" do
    value = runtime(queues: {keep: 1, remove: 1})
    joins = []
    producer = Object.new
    producer.define_singleton_method(:join) { joins << :producer }
    removed_worker = Object.new
    removed_worker.define_singleton_method(:join) { joins << :removed_worker }
    kept_worker = Object.new
    kept_worker.define_singleton_method(:join) { joins << :kept_worker }
    value.instance_variable_set(:@producer_threads, {"remove" => producer})
    value.instance_variable_set(
      :@running,
      {
        1 => {queue: "remove", thread: removed_worker, working: true},
        2 => {queue: "keep", thread: kept_worker, working: true}
      }
    )

    expect(value.queue_remove("remove")).to be true
    expect(joins).to eq([:producer, :removed_worker])
  end

  it "rejects blocking lifecycle calls from a producer before requesting shutdown" do
    value = runtime(queues: {branch: 1})
    value.instance_variable_set(:@stopped, false)
    value.instance_variable_set(:@producer_threads, {"branch" => Thread.current})
    value.instance_variable_set(:@threads, [Thread.current])

    expect { value.queue_remove("branch") }.to raise_error(ThreadError, /own worker or producer/)
    expect { value.stop }.to raise_error(ThreadError, /wait: false/)
    expect(value.send(:queue_stopping?, "branch")).to be_falsey
  end

  it "handles a temporarily failing producer and retries until stopped" do
    driver = Object.new
    attempts = 0
    driver.define_singleton_method(:queue_get) do |_queue|
      attempts += 1
      raise "temporary producer failure"
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:wait) { |_duration| @stop_requested = true if attempts == 2 }

    expect { value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1)) }.not_to raise_error
    expect(attempts).to eq(2)
  end

  it "does not retry a failed producer after its queue stops" do
    driver = Object.new
    attempts = 0
    driver.define_singleton_method(:queue_get) do |_queue|
      attempts += 1
      raise "terminal producer failure"
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:wait) { |_duration| @stop_requested = true }

    expect { value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1)) }.not_to raise_error
    expect(attempts).to eq(1)
  end

  it "can poll a queue before its persisted queue row is visible" do
    driver = Object.new
    driver.define_singleton_method(:queue_get) { |_queue| nil }
    fetched = false
    driver.define_singleton_method(:job_get_available) do |**|
      fetched = true
      []
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:wait) { |_duration| @stop_requested = true }

    value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1))

    expect(fetched).to be true
  end

  it "honors fetch cooldown even when inserts or completions wake the producer early" do
    now = 100.0
    fetched_at = []
    waits = []
    job = row
    driver = Object.new
    driver.define_singleton_method(:queue_get) { |_| nil }
    driver.define_singleton_method(:job_get_available) do |**|
      fetched_at << now
      [job]
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:monotonic_now) { now }
    value.define_singleton_method(:launch) { |_| @stop_requested = true if fetched_at.length == 2 }
    value.define_singleton_method(:wait) do |duration|
      waits << duration
      now += (waits.length == 1) ? 0.25 : duration
    end

    value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1, fetch_cooldown: 1, fetch_poll_interval: 1))

    expect(fetched_at).to eq([100.0, 101.0])
    expect(waits).to eq([1.0, 0.75])
  end

  it "stops a producer woken during its fetch cooldown without another fetch" do
    now = 100.0
    fetched_at = []
    job = row
    driver = Object.new
    driver.define_singleton_method(:queue_get) { |_| nil }
    driver.define_singleton_method(:job_get_available) do |**|
      fetched_at << now
      [job]
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:monotonic_now) { now }
    value.define_singleton_method(:launch) { |_| }
    value.define_singleton_method(:wait) { |_| @stop_requested = true }

    value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1, fetch_cooldown: 1, fetch_poll_interval: 1))

    expect(fetched_at).to eq([100.0])
  end

  it "keeps observing active jobs when shutdown interrupts the fetch cooldown" do
    checked = []
    fetched = 0
    job = row
    driver = Object.new
    driver.define_singleton_method(:job_get_cancelled_ids) { |ids|
      checked << ids
      []
    }
    driver.define_singleton_method(:queue_get) { |_| nil }
    driver.define_singleton_method(:job_get_available) { |**|
      fetched += 1
      [job]
    }
    value = runtime(driver: driver)
    value.define_singleton_method(:monotonic_now) { 100.0 }
    value.define_singleton_method(:launch) do |job|
      @running[job.id] = {queue: "branch", thread: Thread.current, working: true}
    end
    value.define_singleton_method(:wait) { |_| @stop_requested = true }
    value.define_singleton_method(:wait_for_running_jobs) { |_, _| @running.clear }

    value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 2, fetch_cooldown: 1, fetch_poll_interval: 1))

    expect(fetched).to eq(1)
    expect(checked).to eq([[1], [1]])
  end

  it "rechecks queue pause state after waiting for its fetch cooldown" do
    now = 100.0
    paused_at = nil
    fetched_at = []
    job = row
    driver = Object.new
    driver.define_singleton_method(:queue_get) { |_| Struct.new(:paused_at).new(paused_at) }
    driver.define_singleton_method(:job_get_available) do |**|
      fetched_at << now
      [job]
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:monotonic_now) { now }
    value.define_singleton_method(:launch) { |_| @stop_requested = true if fetched_at.length == 2 }
    value.define_singleton_method(:wait) do |duration|
      @stop_requested = true if paused_at
      paused_at = Time.now.utc
      now += duration
    end

    value.send(:producer_loop, "branch", River::QueueConfig.new(max_workers: 1, fetch_cooldown: 1, fetch_poll_interval: 1))

    expect(fetched_at).to eq([100.0])
  end

  it "skips maintenance work while another client holds leadership" do
    driver = Object.new
    driver.define_singleton_method(:leader_acquire) { |_id, **| false }
    value = runtime(driver: driver)
    checks = 0
    value.define_singleton_method(:stopping?) do
      checks += 1
      checks >= 2
    end

    value.define_singleton_method(:wait) { |_duration| }

    value.send(:maintenance_loop)

    expect(checks).to eq(2)
  end

  it "supports drivers without a notification outbox cleaner" do
    calls = []
    driver = Object.new
    driver.define_singleton_method(:leader_acquire) { |*_args, **_options| true }
    [:job_schedule, :job_rescue_stuck, :job_delete_finalized].each do |method|
      driver.define_singleton_method(method) { |**_options| calls << method }
    end
    value = runtime(driver: driver)
    value.define_singleton_method(:wait) { |_duration| @stop_requested = true }

    value.send(:maintenance_loop)
    expect(calls).to eq([:job_schedule, :job_rescue_stuck, :job_delete_finalized])
  end

  it "retries a temporarily failing maintenance loop" do
    driver = Object.new
    driver.define_singleton_method(:leader_acquire) { |_id, **| raise "temporary maintenance failure" }
    value = runtime(driver: driver)
    checks = 0
    value.define_singleton_method(:stopping?) do
      checks += 1
      checks >= 3
    end

    value.define_singleton_method(:wait) { |_duration| }

    expect { value.send(:maintenance_loop) }.not_to raise_error
    expect(checks).to eq(3)
  end

  it "does not retry failed maintenance after stop begins" do
    driver = Object.new
    driver.define_singleton_method(:leader_acquire) { |_id, **| raise "terminal maintenance failure" }
    value = runtime(driver: driver)
    checks = 0
    value.define_singleton_method(:stopping?) do
      checks += 1
      checks >= 2
    end

    value.define_singleton_method(:wait) { |_duration| }

    expect { value.send(:maintenance_loop) }.not_to raise_error
    expect(checks).to eq(2)
  end

  it "isolates a failing service while continuing maintenance and renewing leadership" do
    calls = []
    output = StringIO.new
    driver = Object.new
    [:leader_acquire, :leader_renew].each do |method|
      driver.define_singleton_method(method) do |*_args, **_options|
        calls << method
        true
      end
    end
    [:job_schedule, :job_rescue_stuck, :job_delete_finalized, :notification_delete_before].each do |method|
      driver.define_singleton_method(method) { |**_options| calls << method }
    end
    service = Object.new
    service.define_singleton_method(:run) do |*_args|
      calls << :broken_service
      raise "persistent service failure"
    end
    healthy_service = Object.new
    healthy_service.define_singleton_method(:run) { |*_args| calls << :healthy_service }
    value = described_class.new(Object.new, driver, config.with(logger: Logger.new(output), maintenance_services: [service, healthy_service]))
    waits = 0
    value.define_singleton_method(:wait) do |_duration|
      waits += 1
      @stop_requested = true if waits == 2
    end

    value.send(:maintenance_loop)

    expect(calls).to eq([
      :leader_acquire, :job_schedule, :broken_service, :healthy_service,
      :job_rescue_stuck, :job_delete_finalized, :notification_delete_before, :leader_renew
    ])
    expect(output.string).to include("River maintenance service failed", "persistent service failure")
  end

  it "logs and isolates a periodic constructor failure" do
    output = StringIO.new
    periodic = River::PeriodicJob.new(
      constructor: -> { raise "periodic failed" },
      run_on_start: true,
      schedule: River::PeriodicInterval.new(60)
    )
    runtime_config = config.with(logger: Logger.new(output), periodic_jobs: [periodic])
    value = described_class.new(Object.new, Object.new, runtime_config)

    expect { value.send(:run_periodic, Time.now.utc + 1) }.not_to raise_error
    expect(output.string).to include("River periodic job failed to insert", "periodic failed")
  end

  it "logs a broken periodic schedule and still inserts healthy jobs" do
    output = StringIO.new
    broken = River::PeriodicJob.new(schedule: ->(_) { raise "bad schedule" }, run_on_start: true) {}
    good = River::PeriodicJob.new(schedule: River::PeriodicInterval.new(60), run_on_start: true) { :args }
    inserted = []
    client = Object.new
    client.define_singleton_method(:insert) { |args, **| inserted << args }
    value = described_class.new(client, Object.new, config.with(logger: Logger.new(output), periodic_jobs: [broken, good]))

    value.send(:run_periodic, Time.now.utc + 1)

    expect(inserted).to eq([:args])
    expect(output.string).to include("River periodic job schedule failed", "bad schedule")
  end

  it "handles a job deleted after successful work" do
    worker = Class.new {
      def work(_job)
      end
    }

    driver = Object.new
    driver.define_singleton_method(:job_complete) { |**| nil }
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect { execute(value, row) }.not_to raise_error
  end

  it "uses the completion operation without fetching a full job" do
    worker = Class.new {
      def work(_job)
      end
    }

    driver = Object.new
    checked = []
    driver.define_singleton_method(:job_complete) { |**params|
      checked << params[:id]
      nil
    }
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect(execute(value, row)).to eq([:completed, nil])
    expect(checked).to eq([1])
  end

  it "turns a post-work cancellation marker into a cancelled attempt" do
    worker = Class.new {
      def work(_job)
      end
    }

    driver = Object.new
    driver.define_singleton_method(:job_complete) { |**| :cancelled }
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect(execute(value, row).first).to eq(:cancelled)
  end

  it "handles an interrupt after another actor has already transitioned the job" do
    worker = Class.new { def work(_job) = raise(River::ClientRuntime::Interrupted) }
    driver = Object.new
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect { execute(value, row) }.not_to raise_error
  end

  it "handles a snooze after another actor has already transitioned the job" do
    worker = Class.new { def work(_job) = raise(River.job_snooze(10)) }
    driver = Object.new
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect { execute(value, row) }.not_to raise_error
  end

  it "handles a failure after another actor has already transitioned the job" do
    worker = Class.new { def work(_job) = raise("failed") }
    driver = Object.new
    driver.define_singleton_method(:job_set_state_if_running) { |**| nil }
    value = runtime(driver: driver, worker: worker)

    expect { execute(value, row) }.not_to raise_error
  end

  it "checks cancellation in one batch, excluding inactive work and other queues" do
    raised = []
    fake_thread = Object.new
    fake_thread.define_singleton_method(:raise) { |error| raised << error }
    checked = []
    driver = Object.new
    driver.define_singleton_method(:job_get_cancelled_ids) { |ids|
      checked << ids
      [3]
    }
    value = runtime(driver: driver)
    value.instance_variable_set(
      :@running,
      {
        1 => {queue: "other", thread: fake_thread, working: true},
        2 => {queue: "branch", thread: fake_thread, working: false},
        3 => {queue: "branch", thread: fake_thread, working: true}
      }
    )

    value.send(:check_remote_cancellations, "branch")

    expect(raised).to eq([River::JobCancelError])
    expect(checked).to eq([[3]])
  end

  it "interrupts work that begins after stop was requested" do
    value = runtime
    value.instance_variable_set(:@stop_requested, true)
    value.instance_variable_set(
      :@running,
      1 => {queue: "branch", thread: Thread.current, working: false}
    )

    expect { value.send(:begin_work, 1) }.to raise_error(River::ClientRuntime::Interrupted)
  end

  it "tolerates work disappearing before its working flag is cleared" do
    value = runtime

    expect(value.send(:finish_work, 999)).to be_nil
  end

  it "does not start a second live maintenance thread" do
    value = runtime
    release = Queue.new
    thread = Thread.new { release.pop }
    value.instance_variable_set(:@maintenance_thread, thread)
    value.instance_variable_set(:@threads, [thread])

    expect(value.send(:start_maintenance)).to be_nil
    expect(value.instance_variable_get(:@threads)).to eq([thread])
  ensure
    release << true
    thread&.join
  end

  it "does not enter a condition wait once stop is requested" do
    value = runtime
    value.instance_variable_set(:@stop_requested, true)

    expect(value.send(:wait, 0)).to be_nil
  end

  it "waits during shutdown only while the draining queue still has active attempts" do
    waits = []
    condition = Object.new
    condition.define_singleton_method(:wait) { |mutex, duration| waits << [mutex.owned?, duration] }
    value = runtime
    value.instance_variable_set(:@condition, condition)
    value.instance_variable_set(:@stop_requested, true)
    running = {1 => {queue: "branch"}, 2 => {queue: "other"}}
    value.instance_variable_set(:@running, running)

    value.send(:wait_for_running_jobs, "branch", 2)
    running.delete(1)
    value.send(:wait_for_running_jobs, "branch", 2)

    expect(waits).to eq([[true, 2]])
  end
end
