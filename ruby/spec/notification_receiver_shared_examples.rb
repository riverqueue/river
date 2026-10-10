# frozen_string_literal: true

require "timeout"
require "stringio"

RSpec.shared_examples "notification receiving" do |backend|
  def observe_fetches
    fetches = Queue.new
    original = @driver.method(:job_get_available)
    @driver.define_singleton_method(:job_get_available) do |**options|
      result = original.call(**options)
      fetches << result
      result
    end
    fetches
  end

  def read_notifications(listener, count: 1)
    result = []
    Timeout.timeout(3) do
      result.concat(Thread.new { listener.poll(0.01) }.value) until result.length >= count
    end
    result.map { |topic, payload| [topic, JSON.parse(payload)] }
  end

  def receive_signal(queue)
    Timeout.timeout(3) { queue.pop }
  end

  def receiver_client(**options, &work)
    River::Client.new(@driver, config: River::Config.new(
      fetch_cooldown: 0.001, fetch_poll_interval: 30,
      leader_election_disabled: true, queues: {notifications: 1},
      workers: River::Workers.new.add(:notification, &work || ->(_job) {}), **options
    ))
  end

  if backend == :postgres
    it "bounds the default connection timeout and preserves an explicit timeout" do
      @driver.transaction do
        pool = @driver.send(:runtime_connection_pool)
        raw = if pool.respond_to?(:with_connection)
          pool.with_connection(&:raw_connection)
        else
          pool.synchronize { |connection| connection }
        end
        original = raw.method(:conninfo_hash)
        connection = nil
        {"0" => "5", "3" => "3"}.each do |configured, expected|
          raw.define_singleton_method(:conninfo_hash) { original.call.merge(connect_timeout: configured) }
          connection, = @driver.send(:runtime_notification_connection)
          expect(connection.conninfo_hash[:connect_timeout]).to eq(expected)
          connection.close
        end
      ensure
        raw.singleton_class.remove_method(:conninfo_hash)
        connection.close if connection && !connection.finished?
      end
    end

    it "drains buffered Postgres messages and closes failed subscriptions" do
      listener = @driver.notification_listener
      @driver.transaction do
        3.times { |id| @driver.send(:runtime_notify, "river_insert", queue: "queue-#{id}") }
      end
      expect(read_notifications(listener, count: 3).map(&:last)).to eq(3.times.map { |id| {"queue" => "queue-#{id}"} })
      listener.close
      expect { listener.close }.not_to raise_error

      connection, schema = @driver.send(:runtime_notification_connection)
      connection.exec("BEGIN")
      expect { connection.exec("SELECT 1 / 0") }.to raise_error(PG::DivisionByZero)
      expect { River::Driver::NotificationListener::Postgres.new(connection, schema) }.to raise_error(PG::InFailedSqlTransaction)
      expect(connection.finished?).to be true
    ensure
      listener&.close
      connection.close if connection && !connection.finished?
    end

    it "reconnects after Postgres terminates the listener connection" do
      fetches = observe_fetches
      listeners = Queue.new
      original = @driver.method(:notification_listener)
      @driver.define_singleton_method(:notification_listener) do
        original.call.tap { |listener| listeners << listener }
      end
      worked = Queue.new
      client = receiver_client(logger: Logger.new(StringIO.new)) { |job| worked << job.id }
      client.start
      listener = receive_signal(listeners)
      receive_signal(fetches)
      pid = listener.instance_variable_get(:@connection).backend_pid
      @driver.send(:runtime_execute, "SELECT pg_terminate_backend(#{pid})")
      expect(receive_signal(listeners)).not_to equal(listener)

      row = River::Client.new(@driver).insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
      expect(receive_signal(worked)).to eq(row.id)
      expect(client.__runtime_healthy?).to be true
    ensure
      client&.stop_and_cancel
    end
  end

  it "broadcasts committed requests to independent listeners without replaying history" do
    client = River::Client.new(@driver)
    client.request_resign
    first = @driver.notification_listener
    second = @driver.notification_listener
    expect(first.poll(0.01)).to be_empty

    @driver.transaction do
      client.request_resign
      expect(Thread.new { first.poll(0.01) }.value).to be_empty
    end

    expected = [["river_leadership", {"action" => "request_resign", "leader_id" => ""}]]
    expect(read_notifications(first)).to eq(expected)
    expect(read_notifications(second)).to eq(expected)
    @driver.transaction do
      client.request_resign
      raise @driver.rollback_exception
    end
    expect(first.poll(0.01)).to be_empty
  ensure
    first&.close
    second&.close
  end

  it "fails startup if subscribing fails and allows a subsequent start" do
    original = @driver.method(:notification_listener)
    attempts = 0
    @driver.define_singleton_method(:notification_listener) do
      attempts += 1
      raise "subscription failed" if attempts == 1

      original.call
    end
    client = receiver_client
    expect { client.start }.to raise_error("subscription failed")
    expect(client.stopped?).to be true
    expect(client.start).to equal(client)
    expect(client.__runtime_healthy?).to be true
  ensure
    client&.stop_and_cancel
  end

  it "hands leadership to a waiting follower after request_resign" do
    elected = Queue.new
    service = Object.new
    service.define_singleton_method(:run) { |client, _driver, _now| elected << client.id }
    first = receiver_client(id: "notification-leader", leader_election_disabled: false, maintenance_services: [service], queues: {})
    second = receiver_client(id: "notification-follower", leader_election_disabled: false, maintenance_services: [service], queues: {})
    first.start
    expect(receive_signal(elected)).to eq(first.id)
    second.start

    River::Client.new(@driver).request_resign

    expect(receive_signal(elected)).to eq(second.id)
    expect(@driver.leader_renew(first.id)).to be false
    expect(@driver.leader_renew(second.id)).to be true
  ensure
    first&.stop_and_cancel
    second&.stop_and_cancel
  end

  it "honors a cancellation received between claiming a job and registering its worker" do
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    events = client.subscribe(:job_cancelled)
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
    runtime = client.instance_variable_get(:@runtime)
    original = @driver.method(:job_get_available)
    @driver.define_singleton_method(:job_get_available) do |**options|
      jobs = original.call(**options)
      jobs.each do |job|
        # Deliver at the precise point where another connection can observe
        # the committed claim, before the producer registers the attempt.
        job_cancel(job.id)
        runtime.send(:receive_notification, "river_control", JSON.generate(action: "cancel", queue: job.queue, job_id: job.id))
      end
      jobs
    end
    client.start

    expect(Timeout.timeout(3) { events.pop }.job.id).to eq(row.id)
    expect(worked).to be_empty
    client.stop
    expect(runtime.instance_variable_get(:@fetching_queues)).to be_empty
  ensure
    client&.stop_and_cancel
    events&.close
  end

  it "ignores malformed and unknown messages and continues receiving" do
    fetches = observe_fetches
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    client.start
    receive_signal(fetches)
    [nil, [], {}, {queue: 42}, {queue: "notifications", action: "unknown"}].each do |payload|
      @driver.send(:runtime_notify, "river_control", payload)
    end
    @driver.send(:runtime_notify, "river_leadership", action: "unknown")
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job

    expect(receive_signal(worked)).to eq(row.id)
    expect(client.__runtime_healthy?).to be true
  ensure
    client&.stop_and_cancel
  end

  it "keeps polling available when notification receiving is disabled" do
    @driver.define_singleton_method(:notification_listener) { raise "unexpected notification listener" }
    worked = Queue.new
    client = receiver_client(poll_only: true, fetch_poll_interval: 0.01) { |job| worked << job.id }
    client.start
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
    expect(receive_signal(worked)).to eq(row.id)
  ensure
    client&.stop_and_cancel
  end

  it "notifies workers when another client retries a job" do
    fetches = observe_fetches
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications, state: :pending).job
    client.start
    receive_signal(fetches)

    River::Client.new(@driver).job_retry(row.id)

    expect(receive_signal(worked)).to eq(row.id)
  ensure
    client&.stop_and_cancel
  end

  it "notifies workers when scheduled jobs become available" do
    fetches = observe_fetches
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications, scheduled_at: Time.now.utc - 1, state: :scheduled).job
    client.start
    receive_signal(fetches)

    @driver.job_schedule

    expect(receive_signal(worked)).to eq(row.id)
  ensure
    client&.stop_and_cancel
  end

  it "observes remote queue pause and wildcard resume without waiting for a poll" do
    fetches = observe_fetches
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    events = client.subscribe(:queue_paused, :queue_resumed)
    client.start
    receive_signal(fetches)
    other = River::Client.new(@driver)
    other.queue_pause("*")
    expect(Timeout.timeout(3) { events.pop }).to have_attributes(kind: :queue_paused, queue: have_attributes(name: "notifications"))
    row = other.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
    other.queue_resume("*")

    expect(receive_signal(worked)).to eq(row.id)
    expect(Timeout.timeout(3) { events.pop }.kind).to eq(:queue_resumed)
  ensure
    client&.stop_and_cancel
    events&.close
  end

  it "publishes queue controls while every worker slot is occupied" do
    entered = Queue.new
    client = receiver_client {
      entered << true
      sleep
    }
    events = client.subscribe(:queue_paused, :queue_resumed)
    client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications)
    client.start
    receive_signal(entered)

    other = River::Client.new(@driver)
    other.queue_pause(:notifications)
    expect(Timeout.timeout(3) { events.pop }.kind).to eq(:queue_paused)
    other.queue_resume(:notifications)
    expect(Timeout.timeout(3) { events.pop }.kind).to eq(:queue_resumed)
  ensure
    client&.stop_and_cancel
    events&.close
  end

  it "receives cancellations while gracefully draining a long-running worker" do
    entered = Queue.new
    client = receiver_client(job_timeout: nil) do |job|
      entered << job.id
      sleep
    end
    events = client.subscribe(:job_cancelled)
    row = client.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
    client.start
    expect(receive_signal(entered)).to eq(row.id)
    client.stop(wait: false)

    River::Client.new(@driver).job_cancel(row.id)

    expect(Timeout.timeout(3) { events.pop }.job.id).to eq(row.id)
    Timeout.timeout(3) { client.stop }
    expect(client.stopped?).to be true
  ensure
    client&.stop_and_cancel
    events&.close
  end

  it "recovers from a listener failure and receives again after a restart" do
    fetches = observe_fetches
    failures = Queue.new
    failed = Queue.new
    opened = Queue.new
    worked = Queue.new
    output = StringIO.new
    original = @driver.method(:notification_listener)
    @driver.define_singleton_method(:notification_listener) do
      listener = original.call
      poll = listener.method(:poll)
      listener.define_singleton_method(:poll) do |timeout|
        unless failures.empty?
          failures.pop
          failed << true
          raise "simulated listener failure"
        end
        poll.call(timeout)
      end
      opened << listener
      listener
    end
    client = receiver_client(logger: Logger.new(output)) { |job| worked << job.id }
    client.start
    receive_signal(opened)
    receive_signal(fetches)
    failures << true
    receive_signal(failed)
    other = River::Client.new(@driver)
    row = other.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job

    expect(receive_signal(worked)).to eq(row.id)
    expect(client.__runtime_healthy?).to be true
    expect(output.string).to include("simulated listener failure")
    client.stop
    client.start
    row = other.insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job
    expect(receive_signal(worked)).to eq(row.id)
  ensure
    client&.stop_and_cancel
  end

  it "wakes idle workers after an insertion from another client" do
    fetches = observe_fetches
    worked = Queue.new
    client = receiver_client { |job| worked << job.id }
    client.start
    expect(receive_signal(fetches)).to be_empty

    row = River::Client.new(@driver).insert(River::JobArgsHash.new(:notification, {}), queue: :notifications).job

    expect(receive_signal(worked)).to eq(row.id)
  ensure
    client&.stop_and_cancel
  end
end
