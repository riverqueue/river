# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe "River notification dispatch and recovery" do
  let(:driver) { Object.new }
  let(:output) { StringIO.new }
  let(:runtime) do
    River::ClientRuntime.new(Object.new, driver, River::Config.new(
      id: "notification-client", leader_election_disabled: true, logger: Logger.new(output)
    ))
  end

  [false, true].each do |fail_first|
    it "waits for cancellation hints and retries failures=#{fail_first}" do
      driver.define_singleton_method(:notification_listener) { Object.new }
      value = River::ClientRuntime.new(Object.new, driver, River::Config.new(
        fetch_cooldown: 0.001, fetch_poll_interval: 0.01, leader_election_disabled: true, logger: Logger.new(output)
      ))
      queries = Queue.new
      failures = 0
      driver.define_singleton_method(:job_get_cancelled_ids) do |_ids|
        queries << true
        if fail_first && failures.zero?
          failures += 1
          raise "temporary read failure"
        end
        []
      end
      value.instance_variable_set(:@running, 42 => {queue: "one", thread: Thread.current, working: true})
      checker = Thread.new { value.send(:cancellation_loop) }
      sleep 0.05
      expect(queries).to be_empty
      value.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))
      Timeout.timeout(2) { queries.pop }
      Timeout.timeout(2) { queries.pop } if fail_first
      sleep 0.05
      expect(queries).to be_empty
    ensure
      value&.instance_variable_get(:@mutex)&.synchronize do
        value.instance_variable_set(:@stop_requested, true)
        value.instance_variable_get(:@running).clear
        value.instance_variable_get(:@condition).broadcast
      end
      checker&.join
    end
  end

  it "delivers an identified cancellation while a read is blocked and ignores older attempts" do
    value = runtime
    queried = Queue.new
    release = Queue.new
    raised = Queue.new
    driver.define_singleton_method(:job_get_cancelled_ids) do |_ids|
      queried << true
      release.pop
      []
    end
    thread = Object.new
    thread.define_singleton_method(:raise) { |error| raised << error }
    attempted_at = Time.iso8601("2026-01-02T03:04:05.6789Z")
    entry = {attempt: 2, attempted_at: attempted_at, queue: "one", thread: thread, working: true}
    value.instance_variable_set(:@running, 42 => entry)
    checker = Thread.new { value.send(:check_remote_cancellations) }
    Timeout.timeout(2) { queried.pop }
    [{attempt: 1, attempted_at: attempted_at}, {attempt: 2, attempted_at: attempted_at - 1}].each do |identity|
      value.send(:receive_notification, "river_control", JSON.generate(action: "cancel", job_id: 42, queue: "one", attempt: identity[:attempt], attempted_at: identity[:attempted_at].iso8601(6)))
      expect(raised).to be_empty
    end
    value.send(:receive_notification, "river_control", JSON.generate(action: "cancel", job_id: 42, queue: "one", attempt: 2, attempted_at: attempted_at.iso8601(6)))
    expect(Timeout.timeout(2) { raised.pop }).to eq(River::JobCancelError)
  ensure
    release&.push(true)
    checker&.join
  end

  it "backs off repeated connection failures without indefinitely delaying shutdown" do
    listener = Object.new
    listener.define_singleton_method(:poll) { |_| raise "connection lost" }
    listener.define_singleton_method(:close) {}
    attempts = 0
    driver.define_singleton_method(:notification_listener) do
      attempts += 1
      listener if attempts <= 7
    end
    delays = []
    runtime.define_singleton_method(:sleep) { |duration| delays << duration }

    runtime.send(:notification_loop, Queue.new)

    expect(delays).to eq([0.1, 0.2, 0.4, 0.8, 1.0, 1.0, 1.0])
  end

  it "cancels an attempt before its worker starts but ignores another queue's job" do
    driver.define_singleton_method(:job_get_cancelled_ids) { |ids| ids }
    entry = {queue: "one", thread: Thread.current, working: false}
    runtime.instance_variable_set(:@running, 42 => entry)
    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "two", job_id: 42))
    expect(entry[:cancelled]).to be_nil

    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))
    expect { runtime.send(:begin_work, 42) }.to raise_error(River::JobCancelError)
    expect(entry[:working]).to be false
  end

  [false, true].each do |working|
    it "ignores a delayed cancellation notice after retry#{" while working" if working}" do
      raised = []
      thread = Object.new
      thread.define_singleton_method(:raise) { |error| raised << error }
      entry = {queue: "one", thread: thread, working: working}
      runtime.instance_variable_set(:@running, 42 => entry)
      driver.define_singleton_method(:job_get_cancelled_ids) { |_ids| [] }

      runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))
      runtime.send(:check_remote_cancellations)
      runtime.send(:begin_work, 42) unless working

      expect(raised).to be_empty
      expect(entry[:cancelled]).to be_nil
      expect(entry[:working]).to be true
    end
  end

  it "does not interrupt a replacement registered during a cancellation check" do
    raised = []
    thread = Object.new
    thread.define_singleton_method(:raise) { |error| raised << error }
    original = {queue: "one", thread: thread, working: true}
    replacement = original.dup
    value = runtime
    value.instance_variable_set(:@running, 42 => original)
    driver.define_singleton_method(:job_get_cancelled_ids) do |ids|
      value.instance_variable_set(:@running, 42 => replacement)
      ids
    end

    value.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))
    value.send(:check_remote_cancellations)

    expect(raised).to be_empty
    expect(replacement[:cancelled]).to be_nil
  end

  it "retries a failed cancellation check made before the worker starts" do
    raised = Queue.new
    thread = Object.new
    thread.define_singleton_method(:raise) { |error| raised << error }
    entry = {check_before_work: true, queue: "one", thread: thread, working: false}
    runtime.instance_variable_set(:@running, 42 => entry)
    queries = 0
    driver.define_singleton_method(:job_get_cancelled_ids) do |ids|
      queries += 1
      raise "temporary read failure" if queries == 1

      ids
    end

    runtime.send(:begin_work, 42)
    checker = Thread.new { runtime.send(:cancellation_loop) }

    expect(Timeout.timeout(2) { raised.pop }).to eq(River::JobCancelError)
    expect(entry[:cancelled]).to be true
    expect(output.string).to include("temporary read failure")
  ensure
    runtime.instance_variable_get(:@mutex).synchronize do
      runtime.instance_variable_set(:@stop_requested, true)
      runtime.instance_variable_get(:@running).clear
      runtime.instance_variable_get(:@condition).broadcast
    end
    checker&.join
  end

  it "ignores invalid payloads without disrupting the receiver" do
    [nil, "{", "null", "[]", "{}"].each { |payload| runtime.send(:receive_notification, "river_insert", payload) }
    runtime.instance_variable_set(:@running, 42 => {attempt: 1, queue: "one"})
    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", attempt: 1, attempted_at: "invalid", job_id: 42, queue: "one"))
    runtime.instance_variable_set(:@running, {})
    runtime.send(:receive_notification, "unknown", "{}")
    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: "42"))
    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))

    expect(runtime.instance_variable_get(:@running)).to be_empty
    expect(output.string).to include("ignored invalid notification")
  end

  it "keeps a client healthy when its backend cannot listen" do
    driver.define_singleton_method(:notification_listener) { nil }
    runtime.start
    expect(runtime).to be_healthy
    runtime.stop
    expect(runtime).to be_stopped
  ensure
    runtime.stop(cancel: true)
  end

  it "preserves maintenance wakeups received before the elector starts waiting" do
    condition = runtime.instance_variable_get(:@maintenance_condition)
    condition.define_singleton_method(:wait) { |*_args| raise "unexpected maintenance wait" }

    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "resigned", leader_id: "another-client"))

    expect { runtime.send(:wait_for_maintenance, 5, 0) }.not_to raise_error
  end

  it "retries connection failures after losing an established subscription" do
    closes = 0
    listener = Object.new
    listener.define_singleton_method(:poll) { |_| raise "connection lost" }
    listener.define_singleton_method(:close) { closes += 1 }
    attempts = 0
    driver.define_singleton_method(:notification_listener) do
      attempts += 1
      raise "reconnect failed" if attempts == 2
      listener if attempts == 1
    end
    ready = Queue.new

    runtime.send(:notification_loop, ready)

    expect(ready.pop(true)).to be true
    expect(ready).to be_empty
    expect(attempts).to eq(3)
    expect(closes).to eq(1)
    expect(output.string).to include("connection lost", "reconnect failed")
  end

  it "routes leadership messages only to the appropriate elector" do
    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "request_resign"))
    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "resigned", leader_id: "notification-client"))
    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "resigned", leader_id: 42))
    expect(runtime.instance_variable_get(:@maintenance_generation)).to eq(0)
    expect(runtime.instance_variable_get(:@resign_requested)).to be false

    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "resigned", leader_id: "another-client"))
    expect(runtime.instance_variable_get(:@maintenance_generation)).to eq(1)
    runtime.instance_variable_set(:@leader, true)
    runtime.send(:receive_notification, "river_leadership", JSON.generate(action: "request_resign"))
    expect(runtime.instance_variable_get(:@resign_requested)).to be true
    expect(runtime.instance_variable_get(:@maintenance_generation)).to eq(2)
  end

  it "skips reconnect backoff after stopping and closes the failed listener" do
    value = runtime
    closed = false
    listener = Object.new
    listener.define_singleton_method(:poll) do |_|
      value.instance_variable_set(:@stop_requested, true)
      raise "connection lost during stop"
    end
    listener.define_singleton_method(:close) { closed = true }
    driver.define_singleton_method(:notification_listener) { listener }

    runtime.send(:notification_loop, Queue.new)

    expect(closed).to be true
    expect(output.string).to include("connection lost during stop")
  end

  it "unblocks startup if shutdown wins the race to start receiving" do
    driver.define_singleton_method(:notification_listener) { raise "unexpected subscription" }
    runtime.instance_variable_set(:@stop_requested, true)
    runtime.send(:start_notifications)
    expect(runtime.instance_variable_get(:@threads)).to be_empty

    ready = Queue.new
    runtime.send(:notification_loop, ready)
    expect(ready.pop(true)).to be true
  end

  it "wakes only the queue named by an insertion" do
    runtime.queue_add("one", 1)
    runtime.queue_add("two", 1)
    generations = runtime.instance_variable_get(:@queue_generations).dup
    runtime.send(:receive_notification, "river_insert", JSON.generate(queue: "one"))
    expect(runtime.instance_variable_get(:@queue_generations)).to eq(generations.merge("one" => generations["one"] + 1))
  end
end
