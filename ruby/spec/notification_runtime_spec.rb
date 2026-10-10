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
    entry = {queue: "one", thread: Thread.current, working: false}
    runtime.instance_variable_set(:@running, 42 => entry)
    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "two", job_id: 42))
    expect(entry[:cancelled]).to be_nil

    runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: "one", job_id: 42))
    expect { runtime.send(:begin_work, 42) }.to raise_error(River::JobCancelError)
    expect(entry[:working]).to be false
  end

  it "ignores invalid payloads without disrupting the receiver" do
    [nil, "{", "null", "[]", "{}"].each { |payload| runtime.send(:receive_notification, "river_insert", payload) }
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
