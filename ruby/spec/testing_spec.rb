# frozen_string_literal: true

require "spec_helper"
require "open3"
require "riverqueue-sequel"
require "riverqueue/testing"
require "riverqueue/testing/minitest"
require "riverqueue/testing/rspec"
require_relative "support/river_sqlite_schema_fixture"

RSpec.describe River::Testing, database: :sqlite do
  include River::Testing::Assertions
  include River::Testing::RSpec

  let(:database) do
    Sequel.sqlite.tap { |db| db.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) } }
  end

  let(:worker) { Object.new.tap { |object| object.define_singleton_method(:work) { |_job| } } }
  let(:plugins) { [] }
  let(:client) do
    River::Client.new(River::Driver::Sequel.new(database), config: River::Config.new(
      plugins: plugins,
      workers: River::Workers.new.add("testing", worker)
    ))
  end

  after { database.disconnect }

  def insert(value = 1, **options)
    client.insert(River::JobArgsHash.new("testing", {"value" => value}), insert_opts: River::InsertOpts.new(scheduled_at: Time.now.utc - 1, state: "available", **options)).job
  end

  it "propagates resumable step failures before later worker code or success hooks run" do
    calls = []
    failure = RuntimeError.new("step failed")
    plugin = Object.new
    plugin.define_singleton_method(:work_end) { |_job, error| calls << error }
    plugins << plugin
    worker.define_singleton_method(:work) do |job|
      job.resumable_step(:download) { raise failure }
      calls << :must_not_run
    end

    result = described_class.perform_job(client, insert.id)

    expect(result.error).to equal(failure)
    expect(calls).to eq([failure])
  end

  [Float::NAN, Float::INFINITY, -Float::INFINITY, -1].each do |timeout|
    it "reports an invalid worker timeout of #{timeout} as a normal attempt error" do
      worker.define_singleton_method(:timeout) { |_job| timeout }
      worker.define_singleton_method(:work) { |_job| raise "must not execute" }

      result = described_class.perform_job(client, insert.id)

      expect(result.outcome).to eq(:retried)
      expect(result.error).to be_a(ArgumentError).and have_attributes(message: /timeout must be finite/)
    end
  end

  {
    cancelled: River.job_cancel("stop"),
    snoozed: River.job_snooze(60),
    interrupted: River::ClientRuntime::Interrupted.new
  }.each do |outcome, exception|
    it "does not swallow #{outcome} control flow inside a resumable step" do
      calls = []
      worker.define_singleton_method(:work) do |job|
        job.resumable_step_cursor :items, default: 0 do
          job.resumable_set_cursor 1
          raise exception
        end
        calls << :must_not_run
      end

      result = described_class.perform_job(client, insert.id)

      expect(result.outcome).to eq(outcome)
      expect(result.error).to equal(exception)
      expect(result.job.metadata[River::RESUMABLE_CURSOR_METADATA_KEY]).to eq("items" => 1)
      expect(calls).to be_empty
    end
  end

  [false, true].each do |with_cursor|
    it "does not re-persist a rolled-back #{with_cursor ? "cursor" : "step"} checkpoint" do
      received = []
      worker.define_singleton_method(:work) do |job|
        operation = ->(cursor = nil) do
          received << cursor
          job.client.driver.transaction do
            job.client.insert(River::JobArgsHash.new("child", {}))
            with_cursor ? job.resumable_checkpoint(cursor: 42) : job.resumable_checkpoint
            raise "rollback checkpoint" if job.attempt == 1
          end
        end
        if with_cursor
          job.resumable_step_cursor(:import, default: 0, &operation)
        else
          job.resumable_step(:import, &operation)
        end
      end
      row = insert

      first = described_class.perform_job(client, row.id)

      expect(first.error.message).to eq("rollback checkpoint")
      expect(first.job.metadata).not_to have_key(River::RESUMABLE_STEP_METADATA_KEY)
      expect(first.job.metadata).not_to have_key(River::RESUMABLE_CURSOR_METADATA_KEY)
      expect(client.job_list(River::JobListParams.new(kinds: ["child"])).jobs).to be_empty
      client.job_retry row.id
      expect(described_class.perform_job(client, row.id).outcome).to eq(:completed)
      expect(received).to eq(with_cursor ? [0, 0] : [nil, nil])
      expect(client.job_list(River::JobListParams.new(kinds: ["child"])).jobs.length).to eq(1)
    end
  end

  it "loads neither framework nor testing helpers by default" do
    script = <<~RUBY
      require "riverqueue"
      abort "testing loaded by default" if defined?(River::Testing)
      require "riverqueue/testing"
      abort "RSpec loaded" if defined?(RSpec)
      abort "Minitest loaded" if defined?(Minitest)
    RUBY
    output, status = Open3.capture2e(RbConfig.ruby, "-Ilib", "-e", script)

    expect(status.success?).to be(true), output
  end

  it "normalizes only literal identifier filters without mutating caller values" do
    attributes = {kind: :testing, queue: "default", state: :available, args: :unchanged}.freeze

    expect(described_class.normalize_attributes(attributes))
      .to eq(kind: "testing", queue: "default", state: "available", args: :unchanged)
    expect(attributes[:kind]).to eq(:testing)
  end

  it "provides execution outcome assertions with the original error in failures" do
    completed = described_class.perform_job(client, insert.id)

    expect(assert_job_completed(completed)).to equal(completed)
    expect { assert_job_cancelled(completed) }.to raise_error(River::Testing::AssertionError)
    expect { assert_job_discarded(completed) }.to raise_error(River::Testing::AssertionError)
    worker.define_singleton_method(:work) { |_job| raise River.job_cancel("not needed") }
    cancelled = described_class.perform_job(client, insert.id)

    expect(assert_job_cancelled(cancelled)).to equal(cancelled)
    expect { assert_job_completed(cancelled) }.to raise_error(River::Testing::AssertionError, /not needed/)
    worker.define_singleton_method(:work) { |_job| raise "broken" }
    discarded = described_class.perform_job(client, insert(1, max_attempts: 1).id)

    expect(assert_job_discarded(discarded)).to equal(discarded)
  end

  it "returns the new matching row and ignores existing jobs and other attributes" do
    insert
    row = assert_job_inserted(client, args: {"value" => 2}, kind: :testing, queue: :orders, state: :available) do
      insert(1)
      insert(2, queue: "orders")
    end

    expect(row).to have_attributes(args: {"value" => 2}, kind: "testing", queue: "orders")
    assert_no_jobs_inserted(client) {}
  end

  it "compares nested args exactly, not as a subset" do
    assert_no_jobs_inserted(client, args: {}) { insert }
  end

  it "does not count a uniqueness conflict as insertion" do
    options = River::UniqueOpts.new(by_args: true)
    insert(1, unique_opts: options)

    assert_no_jobs_inserted(client) { insert(1, unique_opts: options) }
  end

  it "paginates snapshots beyond 100 rows" do
    101.times { insert }
    expect(assert_jobs_inserted(client, count: 2) { 2.times { insert } }.length).to eq(2)
  end

  it "reports assertion mismatches and validates inputs before invoking the block" do
    expect { assert_job_inserted(client) {} }.to raise_error(River::Testing::AssertionError, /got 0/)
    expect { assert_jobs_inserted(client, count: -1) {} }.to raise_error(ArgumentError)
    expect { assert_jobs_inserted(client, count: "1") {} }.to raise_error(ArgumentError)
    expect { assert_job_inserted(client, typo: 1) { raise "should not run" } }.to raise_error(ArgumentError, /typo/)
    expect { assert_job_inserted(client) }.to raise_error(ArgumentError, /block/)
    expect { assert_no_jobs_inserted(client) { raise "application error" } }.to raise_error("application error")
  end

  it "supports RSpec block matches, counts, zero counts, and negation" do
    expect { insert }.to insert_job(client, kind: "testing")
    expect { 2.times { insert } }.to insert_jobs(client, count: 2)
    expect {}.to insert_jobs(client, count: 0)
    expect {}.not_to insert_job(client)
    expect {}.not_to insert_jobs(client, count: 2)
    matcher = insert_job(client)

    expect(matcher.supports_value_expectations?).to be false
    expect(matcher.description).to include("River jobs")
    expect { expect {}.to insert_job(client) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /got 0/)
    expect { expect { insert }.not_to insert_jobs(client, count: 2) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /got 1/)
    expect { insert_jobs(client, count: -1) }.to raise_error(ArgumentError)
    expect { insert_jobs(client, count: nil) }.to raise_error(ArgumentError)
  end

  it "matches nested RSpec expectations and scheduled times without weakening literal equality" do
    scheduled_at = Time.now.utc + 60
    expect { insert(42, metadata: {"nested" => {"items" => [1, 2]}}, scheduled_at: scheduled_at) }.to insert_job(
      client,
      args: a_hash_including("value" => be > 40),
      metadata: a_hash_including("nested" => {"items" => [be_a(Integer), 2]}),
      scheduled_at: be_within(0.01).of(scheduled_at)
    )
    expect { insert(42) }.not_to insert_job(client, args: {})
    expect { insert(42) }.not_to insert_job(client, args: a_hash_including("value" => be < 10))
  end

  it "counts only matching new rows and clones attribute matchers between candidates" do
    insert(42)
    expect { [1, 42, 43].each { |value| insert(value) } }.to insert_jobs(
      client, count: 2, args: a_hash_including("value" => be > 40)
    )
    options = River::UniqueOpts.new(by_args: true)
    insert(99, unique_opts: options)
    expect { insert(99, unique_opts: options) }.not_to insert_job(client, args: a_hash_including("value" => 99))
  end

  it "supports flexible count chains and preserves zero-match negation" do
    expect { 3.times { insert } }.to insert_jobs(client).at_least(2)
    expect { insert }.to insert_jobs(client).at_most(2)
    expect {}.to insert_jobs(client).at_most(0)
    expect { 2.times { insert } }.to insert_job(client).exactly(2)
    expect {}.not_to insert_jobs(client).at_most(3)
    expect { expect { insert }.to insert_jobs(client).at_least(2) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /at least 2.*got 1/)
    expect { expect { 2.times { insert } }.to insert_jobs(client).at_most(1) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /at most 1.*got 2/)
    expect { expect { insert }.not_to insert_jobs(client).at_least(2) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /got 1/)
  end

  it "composes insertion matchers while invoking the application block once" do
    calls = 0
    expect {
      calls += 1
      insert(1, queue: "one")
      insert(2, queue: "two")
    }.to insert_job(client, queue: "one").and insert_job(client, queue: "two")
    expect(calls).to eq(1)
  end

  it "matches existing persisted jobs in any state without counting only new inserts" do
    expect(client).not_to have_job(kind: "testing")
    first = insert(1)
    insert(2)
    client.job_cancel(first.id)
    expect(client).to have_job(kind: :testing, queue: :default).exactly(2)
    expect(client).to have_job(id: first.id, args: a_hash_including("value" => 1), state: "cancelled")
    expect(client).to have_job(state: :available).and have_job(state: :cancelled)
    expect(client).not_to have_job(queue: "absent")
    expect(have_job.supports_value_expectations?).to be true
    expect(have_job.supports_block_expectations?).to be false
    expect { expect(client).to have_job(kind: "absent") }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /at least 1.*got 0/)
    expect { expect(client).not_to have_job.exactly(3) }.to raise_error(RSpec::Expectations::ExpectationNotMetError, /got 2/)
  end

  it "paginates existing-job matches" do
    100.times { insert }
    last = insert(101)
    expect(client).to have_job(id: last.id, args: {"value" => 101})
    expect(client).to have_job.exactly(101)
  end

  it "validates count chains and attributes before any application work" do
    [-1, nil, "1", 1.5].each do |count|
      %i[at_least at_most exactly].each do |method|
        expect { have_job.public_send(method, count) }.to raise_error(ArgumentError, /nonnegative integer/)
      end
    end
    expect { have_job(typo: 1) }.to raise_error(ArgumentError, /typo/)
    expect { expect { raise "should not run" }.to insert_job(client, typo: 1) }.to raise_error(ArgumentError, /typo/)
    expect { expect { raise "application error" }.to insert_job(client) }.to raise_error("application error")
  end

  it "describes nested expectations and resets matching results when reused" do
    matcher = have_job(args: a_hash_including("value" => be > 10))
    expect(matcher.matches?(client)).to be false
    expect(matcher.failure_message).to include("a hash including", "got 0")
    insert(42)
    expect(matcher.matches?(client)).to be true
    expect(matcher.does_not_match?(client)).to be false
    expect(matcher.failure_message_when_negated).to include("a hash including", "got 1")
  end

  it "uses native Minitest assertions without autorun" do
    test = Class.new do
      include Minitest::Assertions
      include River::Testing::Minitest

      attr_accessor :assertions
    end.new
    test.assertions = 0
    row = test.assert_job_inserted(client) { insert }

    expect(row.kind).to eq("testing")
    expect { test.assert_no_jobs_inserted(client) { insert } }.to raise_error(Minitest::Assertion)
    expect(test.assertions).to eq(2)
  end

  it "executes one job on the caller's thread with output, metadata, plugins, and events" do
    threads = []
    callbacks = []
    plugins << Object.new.tap do |plugin|
      plugin.define_singleton_method(:work_begin) { |_job| callbacks << :begin }
      plugin.define_singleton_method(:work_end) { |_job, error| callbacks << error }
      plugin.define_singleton_method(:work) do |_job, operation|
        callbacks << :before
        operation.call
        callbacks << :after
      end
    end
    worker.define_singleton_method(:work) do |job|
      threads << Thread.current
      job.output = {"ok" => true}
      job.update_metadata("worked" => true)
    end

    subscription = client.subscribe(River::EVENT_JOB_COMPLETED)
    row = insert
    other = insert
    result = described_class.perform_job(client, row.id)

    expect(result).to have_attributes(id: row.id, error: nil, outcome: :completed)
    expect(result.job).to have_attributes(attempt: 1, attempted_by: [client.id], state: "completed")
    expect(result.job.metadata).to include("worked" => true, "output" => {"ok" => true})
    expect(threads).to eq([Thread.current])
    expect(callbacks).to eq([:before, :begin, nil, :after])
    expect(client.job_get(other.id).state).to eq("available")
    expect(subscription.pop(true).kind).to eq(River::EVENT_JOB_COMPLETED)
    expect(client.started?).to be false
  end

  it "enforces the worker timeout through the real pipeline" do
    worker.define_singleton_method(:timeout) { |_job| 0.001 }
    worker.define_singleton_method(:work) { |_job| sleep(1) }
    result = described_class.perform_job(client, insert.id)

    expect(result).to have_attributes(error: be_a(Timeout::Error), outcome: :retried)
    expect(result.job.attempt).to eq(1)
  end

  it "returns the original exception for retries and discards" do
    error = RuntimeError.new("boom")
    worker.define_singleton_method(:work) { |_job| raise error }
    result = described_class.perform_job(client, insert.id)

    expect(result).to have_attributes(error: equal(error), outcome: :retried)
    expect(result.job.errors.last.error).to eq("boom")
    expect(described_class.perform_job(client, insert(1, max_attempts: 1).id)).to have_attributes(error: equal(error), outcome: :discarded)
  end

  it "handles cancellation, snoozing, interruption, and deletion" do
    worker.define_singleton_method(:work) { |_job| raise River.job_cancel("cancel") }

    expect(described_class.perform_job(client, insert.id)).to have_attributes(error: be_a(River::JobCancelError), outcome: :cancelled)
    worker.define_singleton_method(:work) { |_job| raise River.job_snooze(60) }
    result = described_class.perform_job(client, insert.id)

    expect(result).to have_attributes(error: be_a(River::JobSnoozeError), outcome: :snoozed)
    expect(result.job).to have_attributes(attempt: 0, state: "scheduled")
    worker.define_singleton_method(:work) { |_job| raise River::ClientRuntime::Interrupted }

    expect(described_class.perform_job(client, insert.id).outcome).to eq(:interrupted)
    worker.define_singleton_method(:work) { |_job| }
    deleting = River::Client.new(client.driver, config: River::Config.new(
      plugins: [Object.new.tap { |p| p.define_singleton_method(:job_finalize) { |_job, _state| :delete } }],
      workers: client.config.workers
    ))

    expect(described_class.perform_job(deleting, insert.id)).to have_attributes(error: nil, job: nil, outcome: :deleted)
  end

  it "requires explicit early execution and preserves scheduled_at" do
    row = insert(1, scheduled_at: Time.now.utc + 3_600)

    expect { described_class.perform_job(client, row.id) }.to raise_error(ArgumentError, /future/)
    result = described_class.perform_job(client, row.id, allow_scheduled: true)

    expect(result.job).to have_attributes(scheduled_at: row.scheduled_at, state: "completed")
    expect { described_class.perform_job(client, row.id, allow_scheduled: true) }.to raise_error(ArgumentError)
    expect { described_class.perform_job(client, -1) }.to raise_error(ArgumentError)
  end

  it "refuses active clients and nested execution, and can run after stop" do
    row = insert
    client.start

    expect { described_class.perform_job(client, row.id) }.to raise_error(River::ClientAlreadyStartedError)
    client.stop
    operation = ->(_job) do
      expect { client.start }.to raise_error(River::ClientAlreadyStartedError)
      expect { described_class.perform_job(client, row.id) }.to raise_error(River::ClientAlreadyStartedError)
    end
    worker.define_singleton_method(:work) { |job| operation.call(job) }

    expect(described_class.perform_job(client, row.id).outcome).to eq(:completed)
  end

  it "drains due jobs and their children without running other queues or future jobs" do
    seen = []
    operation = ->(job) do
      seen << job.args.fetch("value")
      insert(3) if job.args.fetch("value") == 2
    end
    worker.define_singleton_method(:work) { |job| operation.call(job) }
    insert(2, priority: 2)
    insert(1, priority: 1)
    insert(4, queue: "other")
    insert(5, scheduled_at: Time.now.utc + 3_600)

    expect(described_class.drain(client, max_jobs: 3, queue: :default).map(&:outcome)).to eq([:completed] * 3)
    expect(seen).to eq([1, 2, 3])
    expect(described_class.drain(client, queue: "empty")).to eq([])
  end

  it "bounds attempts, including self-enqueuing workers, and validates the limit" do
    operation = -> { insert }
    worker.define_singleton_method(:work) { |_job| operation.call }
    insert

    expect { described_class.drain(client, max_jobs: 2, queue: "default") }.to raise_error(River::Testing::DrainLimitError)
    expect { described_class.drain(client, max_jobs: 0, queue: "default") }.to raise_error(ArgumentError)
    expect { described_class.drain(client, max_jobs: nil, queue: "default") }.to raise_error(ArgumentError)
  end
end
