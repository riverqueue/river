# frozen_string_literal: true

require "spec_helper"
require "riverqueue-sequel"
require_relative "support/river_sqlite_schema_fixture"

RUNTIME_DB = if !RiverTestDatabase.enabled?(:sqlite)
  nil
elsif ENV["RUNTIME_DATABASE_URL"]
  Sequel.connect(ENV.fetch("RUNTIME_DATABASE_URL"))
else
  Sequel.sqlite.tap do |database|
    database.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) }
  end
end

class RuntimeArgs < River::JobArgsHash
  def initialize(value = 1)
    super("runtime", {"value" => value})
  end
end

RSpec.describe "River worker runtime", database: :sqlite do
  def wait_until(timeout: 3)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      raise "timed out waiting for condition" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep(0.01)
    end
  end

  def build_client(worker, queue: "runtime", **config_overrides)
    workers = River::Workers.new.add("runtime", worker)
    config = River::Config.new(
      fetch_cooldown: 0.001,
      fetch_poll_interval: 0.01,
      queues: {queue => River::QueueConfig.new(max_workers: 2)},
      workers: workers,
      **config_overrides
    )
    River::Client.new(River::Driver::Sequel.new(RUNTIME_DB), config: config)
  end

  before do
    RUNTIME_DB[:river_notification].delete
    RUNTIME_DB[:river_queue].delete
    RUNTIME_DB[:river_leader].delete
    RUNTIME_DB[:river_job].delete
  end

  it "claims, works, completes, emits events, and persists worker output" do
    worker = Class.new do
      def work(job)
        job.update_metadata("worked" => true)
        job.output = {"doubled" => job.args.fetch("value") * 2}
      end
    end

    client = build_client(worker)
    subscription = client.subscribe(River::EVENT_JOB_COMPLETED)
    inserted = client.insert(RuntimeArgs.new(3), insert_opts: River::InsertOpts.new(queue: "runtime")).job

    expect(client.start).to equal(client)
    completed = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_COMPLETED && row }
    event = wait_until do
      subscription.pop(true)
    rescue ThreadError
      nil
    end

    expect(completed).to have_attributes(
      attempt: 1,
      attempted_by: [client.id],
      metadata: have_attributes(to_h: include("output" => {"doubled" => 6}, "worked" => true))
    )
    expect(event).to have_attributes(
      job: have_attributes(id: completed.id),
      kind: River::EVENT_JOB_COMPLETED,
      stats: have_attributes(run_duration: be >= 0)
    )
    expect(client.started?).to be true
    expect(client.stop).to equal(client)
    expect(client.stopped?).to be true
    expect(subscription.close).to be_nil
    expect(client.instance_variable_get(:@runtime).instance_variable_get(:@subscriptions)).to be_empty
  ensure
    client&.stop_and_cancel
  end

  it "records failures and discards jobs at max attempts" do
    worker = Class.new do
      def work(_job) = raise("nope")
    end

    client = build_client(worker)
    subscription = client.subscribe(River::EVENT_JOB_FAILED)
    inserted = client.insert(
      RuntimeArgs.new,
      insert_opts: River::InsertOpts.new(max_attempts: 1, queue: "runtime")
    ).job
    client.start

    discarded = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_DISCARDED && row }

    expect(discarded.finalized_at).to be_a(Time)
    expect(discarded.errors.last).to have_attributes(attempt: 1, error: "nope")
    expect(subscription.pop(true).kind).to eq(River::EVENT_JOB_FAILED)
  ensure
    client&.stop_and_cancel
  end

  it "snoozes without consuming an attempt and then works the job" do
    worker = Class.new do
      def initialize = @first = true

      def work(_job)
        if @first
          @first = false
          raise River.job_snooze(0.01)
        end
      end
    end.new
    client = build_client(worker)
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start

    completed = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_COMPLETED && row }

    expect(completed).to have_attributes(attempt: 1)
    expect(completed.metadata.fetch("snoozes")).to eq(1)
  ensure
    client&.stop_and_cancel
  end

  it "cancels running work remotely" do
    worker = Class.new do
      def work(_job) = sleep(10)
    end

    client = build_client(worker)
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    wait_until { client.job_get(inserted.id).state == River::JOB_STATE_RUNNING }

    expect(client.job_cancel(inserted.id).metadata).to include("cancel_attempted_at")
    cancelled = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_CANCELLED && row }

    expect(cancelled.finalized_at).to be_a(Time)
  ensure
    client&.stop_and_cancel
  end

  it "interrupts running work on hard stop and makes the job available again" do
    worker = Class.new do
      def work(_job) = sleep(10)
    end

    client = build_client(worker)
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    wait_until { client.job_get(inserted.id).state == River::JOB_STATE_RUNNING }

    client.stop_and_cancel

    expect(client.job_get(inserted.id)).to have_attributes(attempt: 0, state: River::JOB_STATE_AVAILABLE)
  ensure
    client&.stop_and_cancel
  end

  it "supports job and queue administration" do
    client = build_client(Class.new {
      def work(_job)
      end
    })
    first = client.insert(RuntimeArgs.new(1), insert_opts: River::InsertOpts.new(queue: "runtime")).job
    second = client.insert(RuntimeArgs.new(2), insert_opts: River::InsertOpts.new(queue: "runtime")).job

    expect(client.job_get(first.id).id).to eq(first.id)
    expect(client.job_list(River::JobListParams.new(ids: [second.id])).jobs.map(&:id)).to eq([second.id])
    expect(client.job_update(first.id, River::JobUpdateParams.new(max_attempts: 30))).to have_attributes(max_attempts: 30)
    expect(client.job_cancel(first.id)).to have_attributes(state: River::JOB_STATE_CANCELLED)
    expect(client.job_retry(first.id)).to have_attributes(state: River::JOB_STATE_AVAILABLE)
    expect(client.job_delete_many(River::JobListParams.new(ids: [second.id])).jobs.map(&:id)).to eq([second.id])
    expect { client.job_get(second.id) }.to raise_error(River::NotFoundError)
    expect { client.job_delete_many(River::JobListParams.new) }.to raise_error(ArgumentError)

    client.driver.queue_upsert("runtime")
    subscription = client.subscribe(River::EVENT_QUEUE_PAUSED, River::EVENT_QUEUE_RESUMED)

    expect(client.queue_pause("runtime")).to be true
    expect(client.queue_get("runtime").paused_at).to be_a(Time)
    expect(subscription.pop(true).kind).to eq(River::EVENT_QUEUE_PAUSED)
    expect(client.queue_update("runtime", metadata: {"team" => "ruby"}).metadata).to eq("team" => "ruby")
    expect(client.queue_list.queues.map(&:name)).to include("runtime")
    expect(client.queue_resume("runtime")).to be true
    expect(client.queue_get("runtime").paused_at).to be_nil
    expect(subscription.pop(true).kind).to eq(River::EVENT_QUEUE_RESUMED)
  end

  it "adds and removes queue producers dynamically" do
    client = build_client(Class.new {
      def work(_job)
      end
    })
    client.queue_add("dynamic", River::QueueConfig.new(max_workers: 1))
    client.start
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "dynamic")).job
    wait_until { client.job_get(inserted.id).state == River::JOB_STATE_COMPLETED }

    expect(client.queue_remove("dynamic")).to equal(client)
    waiting = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "dynamic")).job
    sleep(0.03)

    expect(client.job_get(waiting.id).state).to eq(River::JOB_STATE_AVAILABLE)
  ensure
    client&.stop_and_cancel
  end

  it "requests stop without waiting, then drains through a later stop call" do
    entered = Queue.new
    release = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) do |_job|
      entered.push(true)
      release.pop
    end

    client = build_client(worker, queues: {"runtime" => 1})
    running = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    Timeout.timeout(3) { entered.pop }

    expect(client.stop(wait: false)).to equal(client)
    expect(client.stop(wait: false)).to equal(client)
    expect(client).to have_attributes(started?: true, stopped?: false)
    expect(client.job_get(running.id)).to have_attributes(attempt: 1, state: River::JOB_STATE_RUNNING)
    expect { client.start }.to raise_error(River::ClientAlreadyStartedError)

    waiting = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    release.push(true)
    Timeout.timeout(3) { client.stop }

    expect(client).to have_attributes(started?: false, stopped?: true)
    expect(client.job_get(running.id)).to have_attributes(attempt: 1, state: River::JOB_STATE_COMPLETED)
    expect(client.job_get(waiting.id).state).to eq(River::JOB_STATE_AVAILABLE)

    client.start
    Timeout.timeout(3) { entered.pop }
    release.push(true)
    wait_until { client.job_get(waiting.id).state == River::JOB_STATE_COMPLETED }
    client.stop

    expect(client.stop(wait: false)).to equal(client)
  ensure
    release&.push(true)
    client&.stop_and_cancel
  end

  it "can escalate a nonblocking stop to cancellation" do
    entered = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) do |_job|
      entered.push(true)
      sleep(30)
    end

    client = build_client(worker)
    row = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    Timeout.timeout(3) { entered.pop }
    client.stop(wait: false)
    Timeout.timeout(3) { client.stop_and_cancel }

    expect(client.job_get(row.id)).to have_attributes(attempt: 0, state: River::JOB_STATE_AVAILABLE)
    expect(client).to be_stopped
  ensure
    client&.stop_and_cancel
  end

  it "validates runtime configuration and worker registration" do
    expect { River::QueueConfig.new(max_workers: 0) }.to raise_error(ArgumentError)
    expect { River::Config.new(fetch_cooldown: 0) }.to raise_error(ArgumentError)
    expect { River::Config.new(job_timeout: 0) }.to raise_error(ArgumentError)
    expect { River::JobListParams.new(limit: 0) }.to raise_error(ArgumentError)
    expect { River::PeriodicInterval.new(0) }.to raise_error(ArgumentError)

    workers = River::Workers.new.add("runtime", Object.new)

    expect { workers.add("runtime", Object.new) }.to raise_error(ArgumentError)
    expect(workers).to include("runtime")

    client = build_client(Object.new)

    expect { client.queue_add("invalid queue", 1) }.to raise_error(ArgumentError)
  end

  it "schedules, rescues, cleans, and elects a maintenance leader" do
    client = build_client(Class.new {
      def work(_job)
      end
    })
    scheduled = client.insert(
      RuntimeArgs.new,
      insert_opts: River::InsertOpts.new(queue: "runtime", scheduled_at: Time.now.utc - 10, state: :scheduled)
    ).job

    expect(client.driver.job_schedule).to eq(1)
    expect(client.job_get(scheduled.id).state).to eq(River::JOB_STATE_AVAILABLE)

    stuck = client.driver.job_get_available(attempted_by: "stuck", max: 1, queue: "runtime").first
    client.job_update(stuck.id, River::JobUpdateParams.new(attempted_at: Time.now.utc - 7_200))

    expect(client.driver.job_rescue_stuck(
      horizon: Time.now.utc - 3_600,
      retry_policy: River::DefaultClientRetryPolicy.new(random: Random.new(1))
    )).to eq(1)
    expect(client.job_get(stuck.id).state).to satisfy { |state| [River::JOB_STATE_AVAILABLE, River::JOB_STATE_RETRYABLE].include?(state) }

    old = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.driver.job_cancel(old.id, now: Time.now.utc - 10)

    expect(client.driver.job_delete_finalized(
      now: Time.now.utc,
      retention: {River::JOB_STATE_CANCELLED => 0}
    )).to eq(1)
    expect(client.driver.job_get_by_id(old.id)).to be_nil

    now = Time.now.utc

    expect(client.driver.leader_acquire("leader-a", now: now)).to be true
    expect(client.driver.leader_acquire("leader-b", now: now)).to be false
    expect(client.driver.leader_renew("leader-a", now: now)).to be true
    client.driver.leader_release("leader-a")

    expect(client.driver.leader_acquire("leader-b", now: now)).to be true
  end

  it "filters jobs by metadata, tags, priorities, queues, and cursor" do
    client = build_client(Class.new {
      def work(_job)
      end
    })
    first = client.insert(
      RuntimeArgs.new(1),
      insert_opts: River::InsertOpts.new(
        metadata: {"tenant" => "one"}, priority: 2, queue: "runtime", tags: %w[alpha shared]
      )
    ).job
    second = client.insert(
      RuntimeArgs.new(2),
      insert_opts: River::InsertOpts.new(
        metadata: {"tenant" => "two"}, priority: 3, queue: "other", tags: %w[beta shared]
      )
    ).job

    expect(client.job_list(River::JobListParams.new(metadata: {tenant: "one"})).jobs.map(&:id)).to eq([first.id])
    expect(client.job_list(River::JobListParams.new(tags_all: %w[alpha shared])).jobs.map(&:id)).to eq([first.id])
    expect(client.job_list(River::JobListParams.new(tags_any: %w[missing beta])).jobs.map(&:id)).to eq([second.id])
    expect(client.job_list(River::JobListParams.new(priorities: [3], queues: ["other"])).jobs.map(&:id)).to eq([second.id])
    expect(client.job_list(River::JobListParams.new(after_id: second.id, sort_order: :desc)).jobs.map(&:id)).to eq([first.id])
  end

  it "runs plugin middleware and callbacks around retries" do
    calls = []
    worker = Object.new
    worker.define_singleton_method(:work) do |job|
      calls << :work
      unless job.metadata["retried"]
        job.update_metadata("retried" => true)
        raise "retry"
      end
    end

    worker.define_singleton_method(:next_retry) { |_job, _error| Time.now.utc }
    plugin = Object.new
    plugin.define_singleton_method(:insert_begin) { |_params| calls << :insert_begin }
    plugin.define_singleton_method(:insert_end) { |_result| calls << :insert_end }
    plugin.define_singleton_method(:work_begin) { |_job| calls << :work_begin }
    plugin.define_singleton_method(:work_end) { |_job, error| calls << (error ? :work_error : :work_end) }
    plugin.define_singleton_method(:work) do |_job, operation|
      calls << :middleware
      operation.call
    end

    client = build_client(worker, plugins: [plugin])
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start

    completed = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_COMPLETED && row }

    expect(completed.attempt).to eq(2)
    expect(calls).to eq([
      :insert_begin, :insert_end,
      :middleware, :work_begin, :work, :work_error,
      :middleware, :work_begin, :work, :work_end
    ])
  ensure
    client&.stop_and_cancel
  end

  it "resumes checkpointed steps and cursors after a failed attempt" do
    calls = []
    worker = Object.new
    failed_once = false
    worker.define_singleton_method(:work) do |job|
      job.resumable_step("first") { calls << "first" }
      job.resumable_step_cursor("items", default: 0) do |cursor|
        calls << "items:#{cursor}"
        ((cursor + 1)..2).each do |item|
          calls << "item:#{item}"
          job.resumable_set_cursor(item)
          unless failed_once
            failed_once = true
            raise "retry resumable work"
          end
        end
      end

      job.resumable_step("last") { calls << "last" }
    end

    worker.define_singleton_method(:next_retry) { |_job, _error| Time.now.utc }
    client = build_client(worker)
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start

    completed = wait_until { (row = client.job_get(inserted.id)).state == River::JOB_STATE_COMPLETED && row }

    expect(completed.attempt).to eq(2)
    expect(completed.metadata.to_h).to include(
      River::RESUMABLE_STEP_METADATA_KEY => "first",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 1}
    )
    expect(calls).to eq(["first", "items:0", "item:1", "items:1", "item:2", "last"])
  ensure
    client&.stop_and_cancel
  end

  it "persists an explicit resumable checkpoint immediately" do
    observed = nil
    worker = Class.new do
      define_method(:work) do |job|
        job.resumable_step_cursor("page", default: {}) do
          job.resumable_checkpoint(cursor: {"last_id" => 42})
          observed = job.client.job_get(job.id).metadata
        end
      end
    end.new
    client = build_client(worker)
    inserted = client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    client.start
    wait_until { client.job_get(inserted.id).state == River::JOB_STATE_COMPLETED }

    expect(observed.to_h).to include(
      River::RESUMABLE_STEP_METADATA_KEY => "page",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"page" => {"last_id" => 42}}
    )
  ensure
    client&.stop_and_cancel
  end

  it "honors error-handler cancellation and worker timeouts" do
    cancel_client = build_client(
      Class.new { def work(_job) = raise("cancel me") },
      error_handler: ->(_error, _job) { :cancel }
    )
    cancelled = cancel_client.insert(RuntimeArgs.new, insert_opts: River::InsertOpts.new(queue: "runtime")).job
    cancel_client.start
    wait_until { cancel_client.job_get(cancelled.id).state == River::JOB_STATE_CANCELLED }
    cancel_client.stop

    timeout_client = build_client(Class.new { def work(_job) = sleep(1) }, job_timeout: 0.01)
    timed_out = timeout_client.insert(
      RuntimeArgs.new,
      insert_opts: River::InsertOpts.new(max_attempts: 1, queue: "runtime")
    ).job
    timeout_client.start
    discarded = wait_until { (row = timeout_client.job_get(timed_out.id)).state == River::JOB_STATE_DISCARDED && row }

    expect(discarded.errors.last.error).to match(/execution expired/)
  ensure
    cancel_client&.stop_and_cancel
    timeout_client&.stop_and_cancel
  end

  it "runs periodic jobs and supports restart" do
    periodic = River::PeriodicJob.new(
      id: "runtime-periodic",
      constructor: -> { [RuntimeArgs.new, River::InsertOpts.new(queue: "runtime")] },
      run_on_start: true,
      schedule: River::PeriodicInterval.new(60)
    )
    client = build_client(Class.new {
      def work(_job)
      end
    }, periodic_jobs: [periodic])
    client.start
    wait_until do
      client.job_list(River::JobListParams.new(kinds: ["runtime"], states: [River::JOB_STATE_COMPLETED])).jobs.any?
    end

    expect { client.start }.to raise_error(River::ClientAlreadyStartedError)
    client.stop

    expect(client.start.stop).to equal(client)
  ensure
    client&.stop_and_cancel
  end

  it "manages periodic registrations and subscriptions" do
    wake_count = 0
    jobs = River::PeriodicJobBundle.new([], wake: -> { wake_count += 1 })
    future = River::PeriodicJob.new(
      id: "future", constructor: -> {}, schedule: ->(time) { time + 60 }
    )
    handle = jobs.add(future)

    expect(jobs.add_many([])).to eq([])
    expect { jobs.add(future) }.to raise_error(ArgumentError)
    expect(jobs.due(Time.now.utc - 60)).to eq([])
    expect(jobs.remove(handle)).not_to be_nil
    jobs.add(future)

    expect(jobs.remove_by_id("future")).to be true
    expect(jobs.remove_by_id("missing")).to be false
    jobs.clear

    expect(wake_count).to eq(2)

    event = River::Event.new(River::EVENT_JOB_COMPLETED, nil, nil, nil)
    subscription = River::Subscription.new([River::EVENT_JOB_COMPLETED], buffer_size: 1)
    subscription.publish(event)
    subscription.publish(event) # full buffers drop rather than blocking workers

    expect(subscription.each.first).to equal(event)
    subscription.close

    expect(subscription.each.to_a).to eq([])
  end
end
