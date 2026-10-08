# frozen_string_literal: true

require "stringio"

RSpec.shared_examples "externally claimed job finalization" do
  [
    [River::ClientRuntime::Interrupted.new, :job_interrupted, "available", 0],
    [River::JobCancelError.new, :job_cancelled, "cancelled", 1],
    [River::JobSnoozeError.new(60), :job_snoozed, "scheduled", 0]
  ].each do |error, event_kind, state, attempt|
    it "honors #{error.class} reported for an externally claimed job" do
      worker = Class.new { def initialize = raise("must not initialize for control signals") }
      client = River::Client.new(@driver, config: River::Config.new(workers: River::Workers.new.add(:external, worker)))
      inserted = client.insert(River::JobArgsHash.new(:external, {})).job
      claimed = @driver.job_claim(id: inserted.id, attempted_by: "external")
      events = client.subscribe(event_kind)
      before = Time.now.utc

      client.__finish_claimed_job(claimed, error)

      updated = client.job_get(inserted.id)
      expect(updated).to have_attributes(attempt: attempt, state: state)
      if event_kind == :job_cancelled
        expect(updated.errors).to contain_exactly(have_attributes(error: error.message))
        expect(updated.finalized_at).to be_a(Time)
      else
        expect(Array(updated.errors)).to be_empty
        expect(updated.finalized_at).to be_nil
        delay = (event_kind == :job_snoozed) ? 60 : 0
        expect(updated.scheduled_at).to be_between(before + delay - 0.001, Time.now.utc + delay + 0.001)
      end
      expect(updated.metadata).to include("snoozes" => 1) if event_kind == :job_snoozed
      expect(events.pop(true).job).to have_attributes(id: inserted.id, state: state)
    ensure
      events&.close
    end
  end

  [false, true].product([false, true]).each do |worker_class, retry_allowed|
    it "honors retry hooks for an external job with worker_class=#{worker_class} and retry_allowed=#{retry_allowed}" do
      calls = []
      retry_at = Time.now.utc + 60
      worker = Class.new do
        define_method(:next_retry) do |_job, _error|
          raise "retry hooks must use the same instance" unless @checked_retry

          retry_at
        end

        define_method(:retry?) do |job, error|
          @checked_retry = true
          calls << [job.id, error]
          retry_allowed
        end
      end
      workers = River::Workers.new.add(:external, worker_class ? worker : worker.new, aliases: [:external_alias])
      client = River::Client.new(@driver, config: River::Config.new(workers: workers))
      inserted = client.insert(River::JobArgsHash.new(:external_alias, {})).job
      claimed = @driver.job_claim(id: inserted.id, attempted_by: "external")
      error = RuntimeError.new("external work failed")
      events = client.subscribe(:job_failed)

      client.__finish_claimed_job(claimed, error)

      expect(calls).to eq([[inserted.id, error]])
      expect(client.job_get(inserted.id)).to have_attributes(
        attempt: 1, errors: contain_exactly(have_attributes(error: "external work failed")),
        finalized_at: retry_allowed ? nil : be_a(Time),
        state: retry_allowed ? "retryable" : "discarded"
      )
      event = events.pop(true)
      expect(event.job.id).to eq(inserted.id)
      expect(event.job.scheduled_at).to be_within(0.001).of(retry_at) if retry_allowed
    ensure
      events&.close
    end
  end

  [:cancelled, :completed, :failed].each do |outcome|
    it "measures external #{outcome} timing from the claim timestamp" do
      client = River::Client.new(@driver)
      scheduled_at = Time.now.utc - 600
      attempted_at = scheduled_at + 60
      inserted = client.insert(River::JobArgsHash.new(:external, {}), scheduled_at: scheduled_at).job
      claimed = @driver.job_claim(id: inserted.id, attempted_by: "external", now: attempted_at)
      events = client.subscribe(:job_completed, :job_failed, :job_cancelled)
      client.job_cancel(inserted.id) if outcome == :cancelled

      client.__finish_claimed_job(claimed, (outcome == :failed) ? RuntimeError.new("external work failed") : nil)

      event = events.pop(true)
      expect(event.kind).to eq(:"job_#{outcome}")
      expect(event.stats).to have_attributes(queue_wait_duration: be_within(0.001).of(60), run_duration: 0.0)
      unless outcome == :completed
        expect(client.job_get(inserted.id).errors.last.at).to be_within(0.001).of(attempted_at)
      end
    ensure
      events&.close
    end
  end

  [:initialize, :next_retry, :retry?].each do |hook|
    it "records the external work error even if #{hook} raises" do
      output = StringIO.new
      worker = Class.new
      worker.define_method(hook) { |*| raise "worker callback failed" }
      client = River::Client.new(@driver, config: River::Config.new(
        logger: Logger.new(output), workers: River::Workers.new.add(:external, worker)
      ))
      inserted = client.insert(River::JobArgsHash.new(:external, {})).job
      claimed = @driver.job_claim(id: inserted.id, attempted_by: "external")

      client.__finish_claimed_job(claimed, RuntimeError.new("external work failed"))

      expect(client.job_get(inserted.id)).to have_attributes(
        state: "available", errors: contain_exactly(have_attributes(error: "external work failed"))
      )
      expect(output.string).to include("worker callback failed")
    end
  end

  it "skips worker hooks when reporting an externally claimed job's decode error" do
    worker = Class.new { def initialize = raise("must not initialize for decode failures") }
    policy = Object.new
    retry_at = Time.now.utc + 60
    policy.define_singleton_method(:next_retry) { |*, now:| retry_at }
    client = River::Client.new(@driver, config: River::Config.new(
      retry_policy: policy, workers: River::Workers.new.add(:external, worker)
    ))
    inserted = client.insert(River::JobArgsHash.new(:external, {})).job
    claimed = @driver.job_claim(id: inserted.id, attempted_by: "external")
    error = River::JobRowDecodeError.new(claimed, "invalid arguments")

    client.__finish_claimed_job(claimed, error)

    expect(client.job_get(inserted.id)).to have_attributes(
      errors: contain_exactly(have_attributes(error: error.message)),
      scheduled_at: be_within(0.001).of(retry_at), state: "retryable"
    )
  end
end
