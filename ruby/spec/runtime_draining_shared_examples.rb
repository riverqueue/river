# frozen_string_literal: true

require "stringio"
require "timeout"

RSpec.shared_examples "cancellation while draining" do
  it "allows a worker to request nonblocking stop and finish normally" do
    worker = Object.new
    worker.define_singleton_method(:work) { |job| job.client.stop(wait: false) }
    client = River::Client.new(@driver, config: River::Config.new(
      fetch_cooldown: 0.001, fetch_poll_interval: 0.01, job_timeout: nil,
      leader_election_disabled: true, logger: Logger.new(StringIO.new),
      queues: {drain: 1}, workers: River::Workers.new.add("drain", worker)
    ))
    subscription = client.subscribe(:job_completed)
    job = client.insert(River::JobArgsHash.new("drain", {}), queue: :drain).job
    client.start

    expect(Timeout.timeout(3) { subscription.pop }).to have_attributes(job: have_attributes(id: job.id, state: "completed"))
    expect(Timeout.timeout(3) { client.stop }).to equal(client)
    expect(client).to be_stopped
  ensure
    client&.stop_and_cancel
    subscription&.close
  end

  %i[queue_remove stop stop_and_cancel].each do |operation|
    it "rejects a blocking #{operation} from its own worker without stopping the queue" do
      errors = Queue.new
      worker = Object.new
      worker.define_singleton_method(:work) do |job|
        if operation == :queue_remove
          job.client.queue_remove(:drain)
        else
          job.client.public_send(operation)
        end
      rescue ThreadError => error
        errors << error
      end
      client = River::Client.new(@driver, config: River::Config.new(
        fetch_cooldown: 0.001, fetch_poll_interval: 0.01, job_timeout: nil,
        leader_election_disabled: true, logger: Logger.new(StringIO.new),
        queues: {drain: 1}, workers: River::Workers.new.add("drain", worker)
      ))
      subscription = client.subscribe(:job_completed)
      client.start

      2.times do
        job = client.insert(River::JobArgsHash.new("drain", {}), queue: :drain).job
        error = Timeout.timeout(3) { errors.pop }

        expect(error.message).to include((operation == :queue_remove) ? "own worker" : "wait: false")
        expect(Timeout.timeout(3) { subscription.pop }).to have_attributes(job: have_attributes(id: job.id, state: "completed"))
        expect(client).to be_started
      end
    ensure
      client&.stop_and_cancel
      subscription&.close
    end
  end

  %i[queue_remove stop stop_without_wait].product([false, true]).each do |operation, poll_failure|
    it "observes remote cancellation after #{operation} begins draining with polling failure=#{poll_failure}" do
      draining = Queue.new
      entered = Queue.new
      release = Queue.new
      worker = Object.new
      worker.define_singleton_method(:work) do |job|
        entered << job.id
        release.pop
      end
      client = River::Client.new(@driver, config: River::Config.new(
        fetch_cooldown: 0.001, fetch_poll_interval: 0.01, job_timeout: nil,
        leader_election_disabled: true, logger: Logger.new(StringIO.new),
        queues: {drain: 1}, workers: River::Workers.new.add("drain", worker)
      ))
      runtime = client.instance_variable_get(:@runtime)
      signalled = false
      failures = 0
      @driver.define_singleton_method(:job_get_cancelled_ids) do |ids|
        if poll_failure && signalled && failures.zero?
          failures += 1
          raise "temporary cancellation query failure"
        end
        super(ids)
      end
      runtime.define_singleton_method(:queue_stopping?) do |queue|
        stopping = super(queue)
        if stopping && !signalled
          signalled = true
          draining << true
        end
        stopping
      end
      subscription = client.subscribe(:job_cancelled, :job_completed)
      job = client.insert(River::JobArgsHash.new("drain", {}), queue: :drain).job
      client.start
      expect(Timeout.timeout(5) { entered.pop }).to eq(job.id)
      waiting = client.insert(River::JobArgsHash.new("drain", {}), queue: :drain).job

      stopper = case operation
      when :queue_remove then Thread.new { client.queue_remove(:drain) }
      when :stop then Thread.new { client.stop }
      else
        expect(client.stop(wait: false)).to equal(client)
        nil
      end
      Timeout.timeout(5) { draining.pop }

      # Persist through another client so only database polling can deliver it.
      River::Client.new(@driver).job_cancel(job.id)
      event = Timeout.timeout(3) { subscription.pop }

      expect(event).to have_attributes(kind: :job_cancelled, job: have_attributes(id: job.id, state: "cancelled"))
      expect(Timeout.timeout(3) { stopper.value }).to equal(client) if stopper
      expect(client.job_get(waiting.id)).to have_attributes(attempt: 0, state: "available")
      expect(entered).to be_empty
      expect(failures).to eq(poll_failure ? 1 : 0)
      expect(client).to be_started if operation == :queue_remove
    ensure
      release&.close
      client&.stop_and_cancel
      stopper&.join
      subscription&.close
    end
  end
end
