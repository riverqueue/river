# frozen_string_literal: true

require "spec_helper"
require "riverqueue-sequel"
require "stringio"
require_relative "support/river_sqlite_schema_fixture"

RUNTIME_FEATURE_DB = if RiverTestDatabase.enabled?(:sqlite)
  Sequel.sqlite.tap do |database|
    database.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) }
  end
end

class RuntimeFeatureArgs < River::JobArgsHash
  def initialize(value = 1, kind: "runtime_feature")
    super(kind, {"value" => value})
  end
end

RSpec.describe "River worker execution features", database: :sqlite do
  before do
    @clients = []
    RUNTIME_FEATURE_DB[:river_notification].delete
    RUNTIME_FEATURE_DB[:river_queue].delete
    RUNTIME_FEATURE_DB[:river_leader].delete
    RUNTIME_FEATURE_DB[:river_job].delete
  end

  after do
    @clients.reverse_each(&:stop_and_cancel)
  end

  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      raise "timed out waiting for condition" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep(0.005)
    end
  end

  def build_client(worker = nil, workers: nil, queues: {"runtime" => 2}, **overrides)
    workers ||= River::Workers.new.tap { |registry| registry.add("runtime_feature", worker) if worker }
    config = River::Config.new(
      id: "runtime-feature-client",
      fetch_cooldown: 0.001,
      fetch_poll_interval: 0.005,
      queues: queues,
      workers: workers,
      **overrides
    )
    River::Client.new(River::Driver::Sequel.new(RUNTIME_FEATURE_DB), config: config).tap { |client| @clients << client }
  end

  def insert(client, value = 1, **options)
    client.insert(
      RuntimeFeatureArgs.new(value),
      insert_opts: River::InsertOpts.new(queue: "runtime", **options)
    ).job
  end

  def event_from(subscription)
    wait_until do
      subscription.pop(true)
    rescue ThreadError
      nil
    end
  end

  it "starts and stops cleanly without configured queues" do
    client = build_client(queues: {})

    expect(client.start).to equal(client)
    expect(client).to be_started
    expect(client.stop).to equal(client)
    expect(client).to be_stopped
  end

  it "does not stop an already running client when start is called twice" do
    client = build_client(Class.new { def work(_job) = nil })
    client.start

    expect { client.start }.to raise_error(River::ClientAlreadyStartedError)
    expect(client).to be_started
    job = insert(client)
    wait_until { client.job_get(job.id).state == River::JOB_STATE_COMPLETED }
  end

  it "makes stop idempotent before and after a run" do
    client = build_client(queues: {})

    expect(client.stop).to equal(client)
    expect(client.start.stop.stop).to equal(client)
  end

  it "restores stopped state when startup fails" do
    driver = Object.new
    driver.define_singleton_method(:queue_upsert) { |_name| raise "cannot create queue" }
    driver.define_singleton_method(:leader_release) { |_id| }
    config = River::Config.new(queues: {runtime: 1})
    client = River::Client.new(driver, config: config)

    expect { client.start }.to raise_error(RuntimeError, "cannot create queue")
    expect(client).to be_stopped
    expect(client).not_to be_started
  end

  it "waits for active work during a graceful stop" do
    entered = Queue.new
    release = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) do |_job|
      entered << true
      release.pop
    end

    client = build_client(worker)
    job = insert(client)
    client.start
    entered.pop

    stopper = Thread.new { client.stop }
    sleep(0.02)

    expect(stopper).to be_alive
    release << true
    stopper.join

    expect(client.job_get(job.id)).to have_attributes(state: River::JOB_STATE_COMPLETED)
  ensure
    release << true if release && release.empty?

    stopper&.join
  end

  it "instantiates a worker class separately for each job" do
    instances = Queue.new
    worker_class = Class.new do
      define_method(:initialize) { instances << object_id }
      def work(_job)
      end
    end

    client = build_client(worker_class)
    jobs = [insert(client, 1), insert(client, 2)]
    client.start
    wait_until { jobs.all? { |job| client.job_get(job.id).state == River::JOB_STATE_COMPLETED } }

    expect([instances.pop, instances.pop].uniq.length).to eq(2)
  end

  it "never exceeds a queue's configured worker concurrency" do
    lock = Mutex.new
    current = 0
    maximum = 0
    worker = Object.new
    worker.define_singleton_method(:work) do |_job|
      lock.synchronize do
        current += 1
        maximum = [maximum, current].max
      end

      sleep(0.03)
    ensure
      lock.synchronize { current -= 1 }
    end

    client = build_client(worker, queues: {"runtime" => 2})
    jobs = 5.times.map { |index| insert(client, index) }
    client.start
    wait_until { jobs.all? { |job| client.job_get(job.id).state == River::JOB_STATE_COMPLETED } }

    expect(maximum).to eq(2)
  end

  it "discards an unknown job kind with an explanatory attempt error" do
    client = build_client(workers: River::Workers.new)
    job = client.insert(
      RuntimeFeatureArgs.new(kind: "missing"),
      insert_opts: River::InsertOpts.new(max_attempts: 1, queue: "runtime")
    ).job
    client.start

    discarded = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_DISCARDED && row }

    expect(discarded.errors.last.error).to eq("unknown job kind: missing")
  end

  it "treats JobCancelError raised by a worker as final cancellation" do
    worker = Class.new { def work(_job) = raise(River.job_cancel("worker cancelled")) }
    client = build_client(worker)
    subscription = client.subscribe(River::EVENT_JOB_CANCELLED)
    job = insert(client)
    client.start

    cancelled = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_CANCELLED && row }

    expect(cancelled).to have_attributes(errors: contain_exactly(have_attributes(error: "worker cancelled")), finalized_at: be_a(Time))
    expect(event_from(subscription)).to have_attributes(job: have_attributes(id: job.id), kind: River::EVENT_JOB_CANCELLED)
  end

  it "schedules long snoozes and publishes a snoozed event" do
    worker = Class.new { def work(_job) = raise(River.job_snooze(10)) }
    client = build_client(worker)
    subscription = client.subscribe(River::EVENT_JOB_SNOOZED)
    job = insert(client)
    client.start

    snoozed = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_SCHEDULED && row }

    expect(snoozed).to have_attributes(attempt: 0, metadata: include("snoozes" => 1))
    expect(snoozed.scheduled_at).to be > Time.now.utc + 8
    expect(event_from(subscription).kind).to eq(River::EVENT_JOB_SNOOZED)
  end

  it "allows a worker-specific nil timeout to disable the client timeout" do
    worker = Class.new do
      def timeout(_job) = nil
      def work(_job) = sleep(0.03)
    end

    client = build_client(worker, job_timeout: 0.005)
    job = insert(client)
    client.start

    expect(wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_COMPLETED && row }).to be_a(River::JobRow)
  end

  it "uses the client timeout when a worker returns zero" do
    worker = Class.new do
      def timeout(_job) = 0
      def work(_job) = sleep(1)
    end

    client = build_client(worker, job_timeout: 0.005)
    job = insert(client, max_attempts: 1)
    client.start

    discarded = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_DISCARDED && row }

    expect(discarded.errors.last.error).to match(/execution expired/)
  end

  [:worker_retry, :worker_next_retry, :policy_next_retry, :invalid_retry_time].each do |failure|
    it "persists the original work error when #{failure} fails" do
      log = StringIO.new
      worker = Object.new
      worker.define_singleton_method(:work) { |_job| raise "work failed" }
      policy = River::DefaultClientRetryPolicy.new
      case failure
      when :worker_retry
        worker.define_singleton_method(:retry?) { |_job, _error| raise "retry hook failed" }
      when :worker_next_retry
        worker.define_singleton_method(:next_retry) { |_job, _error| raise "retry hook failed" }
      when :policy_next_retry
        policy.define_singleton_method(:next_retry) { |_job, _error, now:| raise "retry hook failed" }
      when :invalid_retry_time
        worker.define_singleton_method(:next_retry) { |_job, _error| "tomorrow" }
      end
      client = build_client(worker, logger: Logger.new(log), retry_policy: policy)
      job = insert(client)
      subscription = client.subscribe(River::EVENT_JOB_FAILED)
      started_at = Time.now.utc

      result = client.__perform_job(job.id)

      expect(result[0]).to have_attributes(state: "available", errors: contain_exactly(have_attributes(error: "work failed")))
      expect(result[0].scheduled_at).to be_between(started_at + 0.8, Time.now.utc + 1.2)
      expect(result[2]).to eq(:retried)
      expect(subscription.pop(true)).to have_attributes(kind: River::EVENT_JOB_FAILED)
      expect(log.string).to include("River", "retry")
    end
  end

  [:retry, :snooze, :interrupt].each do |transition|
    it "reports cancellation when it wins a race with #{transition}" do
      worker = Object.new
      worker.define_singleton_method(:work) do |job|
        job.client.job_cancel(job.id)
        case transition
        when :retry then raise "retry"
        when :snooze then raise River.job_snooze(60)
        when :interrupt then raise River::ClientRuntime::Interrupted
        end
      end
      client = build_client(worker)
      job = insert(client)
      subscription = client.subscribe(River::EVENT_JOB_CANCELLED, River::EVENT_JOB_FAILED, River::EVENT_JOB_SNOOZED, River::EVENT_JOB_INTERRUPTED)

      result = client.__perform_job(job.id)

      expect(result[0].state).to eq("cancelled")
      expect(result[2]).to eq(:cancelled)
      expect(subscription.pop(true)).to have_attributes(kind: River::EVENT_JOB_CANCELLED, job: have_attributes(state: "cancelled"))
    end
  end

  it "uses a worker-specific future retry time" do
    retry_at = Time.now.utc + 60
    worker = Object.new
    worker.define_singleton_method(:work) { |_job| raise "retry later" }
    worker.define_singleton_method(:next_retry) { |_job, _error| retry_at }
    client = build_client(worker)
    job = insert(client)
    client.start

    retryable = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_RETRYABLE && row }

    expect(retryable.scheduled_at).to be_within(0.001).of(retry_at)
    expect(retryable.errors.last.error).to eq("retry later")
  end

  it "uses the executing worker instance to calculate its retry time" do
    worker = Class.new do
      attr_reader :retry_at

      def work(_job)
        @retry_at = Time.now.utc + 60
        raise "retry later"
      end

      def next_retry(_job, _error)
        raise "wrong instance" unless @retry_at

        @retry_at
      end
    end

    client = build_client(worker)
    job = insert(client)
    client.start

    retryable = wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_RETRYABLE && row }

    expect(retryable.scheduled_at).to be > Time.now.utc + 50
    expect(retryable.errors.last.error).to eq("retry later")
  end

  it "falls back to default backoff when custom retry time is in the past" do
    worker = Object.new
    worker.define_singleton_method(:next_retry) { |_job, _error| Time.at(0) }
    client = build_client(worker, queues: {})
    inserted = client.insert(RuntimeFeatureArgs.new, insert_opts: River::InsertOpts.new(max_attempts: 2)).job
    running = client.driver.job_get_available(attempted_by: client.id, max: 1, queue: "default").first
    before = Time.now.utc

    client.__finish_claimed_job(running, RuntimeError.new("retry"))
    updated = client.job_get(inserted.id)

    expect(updated).to have_attributes(errors: contain_exactly(have_attributes(error: "retry")), state: River::JOB_STATE_AVAILABLE)
    expect(updated.scheduled_at).to be_between(before + 0.8, Time.now.utc + 1.2)
  end

  it "supports an error-handler object that cancels a job" do
    handled = nil
    handler = Object.new
    handler.define_singleton_method(:handle_error) do |error, job|
      handled = [error, job]
      true
    end

    client = build_client(Object.new, error_handler: handler, queues: {})
    inserted = client.insert(RuntimeFeatureArgs.new).job
    running = client.driver.job_get_available(attempted_by: client.id, max: 1, queue: "default").first
    error = RuntimeError.new("cancel")

    client.__finish_claimed_job(running, error)

    expect(client.job_get(inserted.id)).to have_attributes(state: River::JOB_STATE_CANCELLED)
    expect(handled).to have_attributes(
      first: equal(error),
      last: be_a(River::Job)
    )
  end

  it "logs an error-handler failure and continues normal retry handling" do
    output = StringIO.new
    logger = Logger.new(output)
    handler = ->(_error, _job) { raise "handler failed" }
    client = build_client(Object.new, error_handler: handler, logger: logger, queues: {})
    inserted = client.insert(RuntimeFeatureArgs.new, insert_opts: River::InsertOpts.new(max_attempts: 2)).job
    running = client.driver.job_get_available(attempted_by: client.id, max: 1, queue: "default").first

    client.__finish_claimed_job(running, RuntimeError.new("work failed"))

    expect(client.job_get(inserted.id)).to have_attributes(state: River::JOB_STATE_AVAILABLE)
    expect(output.string).to include("River error handler failed", "handler failed")
  end

  it "runs plugin work middleware outside-in around worker execution" do
    calls = []
    worker = Object.new
    worker.define_singleton_method(:work) { |_job| calls << :work }
    first = Object.new
    first.define_singleton_method(:work) do |_job, operation|
      calls << :first_before
      operation.call
      calls << :first_after
    end

    second = Object.new
    second.define_singleton_method(:work) do |_job, operation|
      calls << :second_before
      operation.call
      calls << :second_after
    end

    client = build_client(worker, plugins: [first, second])
    job = insert(client)
    client.start
    wait_until { client.job_get(job.id).state == River::JOB_STATE_COMPLETED }

    expect(calls).to eq([:first_before, :second_before, :work, :second_after, :first_after])
  end

  it "runs plugin work callbacks in registration order" do
    calls = []
    plugin_one = Object.new
    plugin_one.define_singleton_method(:work_begin) { |_job| calls << :one_begin }
    plugin_one.define_singleton_method(:work_end) { |_job, _error| calls << :one_end }
    plugin_two = Object.new
    plugin_two.define_singleton_method(:work_begin) { |_job| calls << :two_begin }
    plugin_two.define_singleton_method(:work_end) { |_job, _error| calls << :two_end }
    client = build_client(Class.new {
      def work(_job)
      end
    }, plugins: [plugin_one, plugin_two])
    job = insert(client)
    client.start
    wait_until { client.job_get(job.id).state == River::JOB_STATE_COMPLETED }

    expect(calls).to eq([:one_begin, :two_begin, :one_end, :two_end])
  end

  it "allows a finalize plugin to delete ephemeral jobs" do
    plugin = Object.new
    plugin.define_singleton_method(:job_finalize) { |_job, _state| :delete }
    client = build_client(Class.new {
      def work(_job)
      end
    }, plugins: [plugin])
    job = insert(client)
    client.start

    wait_until { client.driver.job_get_by_id(job.id).nil? }

    expect { client.job_get(job.id) }.to raise_error(River::NotFoundError)
  end

  it "completes jobs after non-deleting finalization hooks" do
    calls = []
    plugin = Object.new
    plugin.define_singleton_method(:job_finalize) { |_job, state| calls << state }
    client = build_client(Class.new { def work(_job) = nil }, plugins: [plugin])
    row = insert(client)
    client.start
    wait_until { client.job_get(row.id).state == "completed" }
    expect(calls).to eq(["completed"])
  end

  it "checks cancellation before invoking finalization hooks" do
    calls = []
    plugin = Object.new
    plugin.define_singleton_method(:job_finalize) { |_job, state| calls << state }
    worker = Class.new
    client = build_client(worker, plugins: [plugin])
    row = insert(client)
    worker.define_method(:work) { |_job| client.job_cancel(row.id) }
    client.start
    wait_until { client.job_get(row.id).state == "cancelled" }
    expect(calls).to be_empty
  end

  it "completes an externally claimed job through the extension boundary" do
    client = build_client(queues: {})
    inserted = client.insert(RuntimeFeatureArgs.new).job
    running = client.driver.job_get_available(attempted_by: client.id, max: 1, queue: "default").first
    subscription = client.subscribe(River::EVENT_JOB_COMPLETED)

    client.__finish_claimed_job(running)

    expect(client.job_get(inserted.id)).to have_attributes(state: River::JOB_STATE_COMPLETED)
    expect(subscription.pop(true).kind).to eq(River::EVENT_JOB_COMPLETED)
  end
end

RSpec.describe "River dynamic queues and maintenance features", database: :sqlite do
  before do
    @clients = []
    RUNTIME_FEATURE_DB[:river_notification].delete
    RUNTIME_FEATURE_DB[:river_queue].delete
    RUNTIME_FEATURE_DB[:river_leader].delete
    RUNTIME_FEATURE_DB[:river_job].delete
  end

  after do
    @clients.reverse_each(&:stop_and_cancel)
  end

  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      raise "timed out waiting for condition" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep(0.005)
    end
  end

  def build_client(worker, queues: {}, **overrides)
    config = River::Config.new(
      id: "runtime-maintenance-client",
      fetch_cooldown: 0.001,
      fetch_poll_interval: 0.005,
      queues: queues,
      workers: River::Workers.new.add("runtime_feature", worker),
      **overrides
    )
    River::Client.new(River::Driver::Sequel.new(RUNTIME_FEATURE_DB), config: config).tap { |client| @clients << client }
  end

  it "processes work from a queue added before startup" do
    client = build_client(Class.new {
      def work(_job)
      end
    })

    expect(client.queue_add("dynamic", 1)).to equal(client)
    job = client.insert(RuntimeFeatureArgs.new, insert_opts: River::InsertOpts.new(queue: "dynamic")).job

    client.start

    expect(wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_COMPLETED && row }).to be_a(River::JobRow)
  end

  it "validates dynamic queue addition and removal" do
    client = build_client(Object.new)
    client.queue_add("dynamic", River::QueueConfig.new(max_workers: 1))

    expect { client.queue_add("dynamic", 1) }.to raise_error(ArgumentError, "queue is already configured: dynamic")
    expect { client.queue_add("not valid", 1) }.to raise_error(ArgumentError, /invalid queue name/)
    expect(client.queue_remove("dynamic")).to equal(client)
    expect { client.queue_remove("dynamic") }.to raise_error(River::NotFoundError, "queue is not configured: dynamic")
  end

  it "does not work jobs while their queue is paused" do
    client = build_client(Class.new {
      def work(_job)
      end
    }, queues: {"runtime" => 1})
    client.driver.queue_upsert("runtime")
    client.queue_pause("runtime")
    job = client.insert(RuntimeFeatureArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    sleep(0.03)

    expect(client.job_get(job.id)).to have_attributes(state: River::JOB_STATE_AVAILABLE)
    client.queue_resume("runtime")

    expect(wait_until { (row = client.job_get(job.id)).state == River::JOB_STATE_COMPLETED && row }).to be_a(River::JobRow)
  end

  it "runs custom maintenance services while holding leadership" do
    calls = Queue.new
    service = Object.new
    service.define_singleton_method(:run) { |client, driver, now| calls << [client, driver, now] }
    client = build_client(Object.new, maintenance_services: [service])

    client.start
    invocation = wait_until {
      begin
        calls.pop(true)
      rescue
        nil
      end
    }

    expect(invocation[0]).to equal(client)
    expect(invocation[1]).to equal(client.driver)
    expect(invocation[2]).to be_a(Time)
  end

  [:configured, :added_before_start, :added_after_start].each do |registration|
    it "produces #{registration} periodic jobs without consuming any queues" do
      periodic = River::PeriodicJob.new(
        id: "producer-only", run_on_start: true, schedule: River::PeriodicInterval.new(60)
      ) { RuntimeFeatureArgs.new }
      client = build_client(Object.new, periodic_jobs: (registration == :configured) ? [periodic] : [])
      client.periodic_jobs.add(periodic) if registration == :added_before_start
      client.start
      client.periodic_jobs.add(periodic) if registration == :added_after_start

      inserted = wait_until { client.job_list.jobs.first }

      expect(inserted).to have_attributes(state: "available", attempt: 0, metadata: include("river:periodic_job_id" => "producer-only"))
      expect(client.queue_list.queues).to be_empty
      client.stop
      expect(client).to be_stopped
    end
  end

  it "respects client and worker timeouts during stuck-job rescue" do
    worker = Class.new do
      def timeout(job)
        job.args.fetch("timeout")
      end
    end
    output = StringIO.new
    client = build_client(worker, job_timeout: 10_800, logger: Logger.new(output), queues: {"unused" => 1})
    now = Time.now.utc
    jobs = [0, nil, 10_800, 60, "invalid"].map do |timeout|
      row = client.insert(River::JobArgsHash.new("runtime_feature", {"timeout" => timeout}), scheduled_at: now - 7_200).job
      client.driver.job_claim(id: row.id, attempted_by: "worker", now: now - 7_200)
    end
    cancelled = client.insert(River::JobArgsHash.new("runtime_feature", {"timeout" => nil}), scheduled_at: now - 7_200).job
    client.driver.job_claim(id: cancelled.id, attempted_by: "worker", now: now - 7_200)
    client.job_cancel(cancelled.id)
    corrupt = client.insert(RuntimeFeatureArgs.new, scheduled_at: now - 7_200).job
    client.driver.send(:runtime_execute, "UPDATE river_job SET args = '{' WHERE id = #{corrupt.id}")
    client.driver.job_claim(id: corrupt.id, attempted_by: "worker", now: now - 7_200)
    cleaned = Queue.new
    client.driver.define_singleton_method(:job_delete_finalized) do |**options|
      super(**options).tap { cleaned << true }
    end

    client.start
    Timeout.timeout(3) { cleaned.pop }

    expect(jobs.map { |job| client.job_get(job.id).state }).to eq(%w[running running running retryable retryable])
    expect(client.job_get(cancelled.id).state).to eq("cancelled")
    expect(client.driver.send(:runtime_read_job, corrupt.id).state).to eq("retryable")
    expect(output.string).to include("River rescue timeout check failed")
  end

  it "continues rescue and cleanup after a retry policy fails" do
    output = StringIO.new
    policy = Object.new
    policy.define_singleton_method(:next_retry) { |*args, **options| raise "broken rescue policy" }
    client = build_client(Object.new, logger: Logger.new(output), queues: {"runtime" => 1}, retry_policy: policy)
    now = Time.now.utc
    stuck = client.insert(RuntimeFeatureArgs.new, scheduled_at: now - 7_200).job
    client.driver.job_claim(id: stuck.id, attempted_by: "old-worker", now: now - 7_200)
    expired = client.insert(RuntimeFeatureArgs.new).job
    client.job_update(expired.id, state: "completed", finalized_at: now - 172_800)

    client.start
    wait_until { client.driver.job_get_by_id(expired.id).nil? }

    expect(client.job_get(stuck.id)).to have_attributes(state: "retryable", scheduled_at: be > now)
    expect(output.string).to include("River rescue retry scheduling failed", "broken rescue policy")
  end

  it "inserts a periodic job whose constructor returns bare arguments" do
    periodic = River::PeriodicJob.new(
      id: "bare",
      run_on_start: true,
      schedule: River::PeriodicInterval.new(60)
    ) { RuntimeFeatureArgs.new }
    client = build_client(
      Class.new {
        def work(_job)
        end
      },
      periodic_jobs: [periodic],
      queues: {"default" => 1}
    )
    client.start

    completed = wait_until do
      client.job_list(River::JobListParams.new(kinds: ["runtime_feature"], states: [River::JOB_STATE_COMPLETED])).jobs.first
    end

    expect(completed).to be_a(River::JobRow)
  end

  it "skips a periodic job whose constructor returns nil" do
    maintenance_ran = Queue.new
    service = Object.new
    service.define_singleton_method(:run) { |_client, _driver, _now| maintenance_ran << true }
    periodic = River::PeriodicJob.new(
      constructor: -> {},
      run_on_start: true,
      schedule: River::PeriodicInterval.new(60)
    )
    client = build_client(
      Object.new,
      maintenance_services: [service],
      periodic_jobs: [periodic],
      queues: {"default" => 1}
    )

    client.start
    wait_until {
      begin
        maintenance_ran.pop(true)
      rescue
        nil
      end
    }

    expect(client.job_list.jobs).to be_empty
  end
end
