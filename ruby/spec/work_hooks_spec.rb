# frozen_string_literal: true

require "spec_helper"
require "riverqueue-sequel"
require "riverqueue/testing"
require_relative "support/river_sqlite_schema_fixture"

RSpec.describe "River work hooks", database: :sqlite do
  let(:calls) { [] }
  let(:database) do
    Sequel.sqlite.tap { |db| db.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) } }
  end
  let(:handled_errors) { [] }
  let(:plugins) { [] }
  let(:worker) { Object.new }
  let(:client) do
    River::Client.new(River::Driver::Sequel.new(database), config: River::Config.new(
      error_handler: ->(error, _job) {
        handled_errors << error
        false
      },
      plugins: plugins,
      workers: River::Workers.new.add("work_hooks", worker)
    ))
  end

  after { database.disconnect }

  def add_end_hook(&block)
    Object.new.tap do |plugin|
      plugin.define_singleton_method(:work_end, &block)
      plugins << plugin
    end
  end

  def perform
    row = client.insert(River::JobArgsHash.new("work_hooks", {})).job
    River::Testing.perform_job(client, row.id)
  end

  it "allows an end hook to fail a successful attempt" do
    failure = RuntimeError.new("end hook failed")
    worker.define_singleton_method(:work) { |_job| :success }
    received = []
    add_end_hook do |_job, error|
      received << error
      failure
    end

    result = perform

    expect(received).to eq([nil])
    expect(result).to have_attributes(error: failure, outcome: :retried)
    expect(result.job.errors.last.error).to eq("end hook failed")
    expect(handled_errors).to eq([failure])
  end

  it "chains replacement errors and nil through end hooks in registration order" do
    original = RuntimeError.new("worker failed")
    replacement = RuntimeError.new("replacement")
    final = RuntimeError.new("final")
    worker.define_singleton_method(:work) { |_job| raise original }
    received = []
    [replacement, nil, final].each do |next_error|
      add_end_hook do |job, error|
        received << [job.id, error]
        next_error
      end
    end
    # Plugins without this hook must not change the accumulated error.
    plugins.insert(1, Object.new)
    subscription = client.subscribe(River::EVENT_JOB_FAILED)

    result = perform

    expect(received).to eq([[result.id, original], [result.id, replacement], [result.id, nil]])
    expect(result).to have_attributes(error: final, outcome: :retried)
    expect(result.job.errors.map(&:error)).to eq(["final"])
    expect(handled_errors).to eq([final])
    expect(subscription.pop(true).job.errors.last.error).to eq("final")
  end

  it "clears a worker failure before returning successfully through middleware" do
    original = RuntimeError.new("worker failed")
    received = []
    worker.define_singleton_method(:work) { |_job| raise original }
    plugin = add_end_hook do |_job, error|
      received << error
      nil
    end
    plugin.define_singleton_method(:work) do |_job, operation|
      received << :before
      received << operation.call
      received << :after
    end
    subscription = client.subscribe(River::EVENT_JOB_COMPLETED)

    result = perform

    expect(received).to eq([:before, original, nil, :after])
    expect(result).to have_attributes(error: nil, outcome: :completed)
    expect(result.job).to have_attributes(errors: [], state: "completed")
    expect(handled_errors).to be_empty
    expect(subscription.pop(true).kind).to eq(River::EVENT_JOB_COMPLETED)
  end

  it "preserves a successful worker return value through hooks and middleware" do
    received = calls
    worker.define_singleton_method(:work) do |_job|
      received << :work
      :worker_result
    end
    first = add_end_hook do |_job, error|
      received << [:first_end, error]
      error
    end
    first.define_singleton_method(:work_begin) { |_job| received << :begin }
    first.define_singleton_method(:work) do |_job, operation|
      received << :before
      received << operation.call
      received << :after
    end
    add_end_hook do |_job, error|
      received << [:second_end, error]
      error
    end

    result = perform

    expect(result).to have_attributes(error: nil, outcome: :completed)
    expect(calls).to eq([:before, :begin, :work, [:first_end, nil], [:second_end, nil], :worker_result, :after])
  end

  it "preserves the original error and backtrace when an end hook returns it" do
    failure = RuntimeError.new("worker failed")
    failure.set_backtrace(["worker.rb:12:in 'work'"])
    worker.define_singleton_method(:work) { |_job| raise failure }
    received = []
    plugin = add_end_hook do |_job, error|
      received << [:end, error]
      error
    end
    plugin.define_singleton_method(:work) do |_job, operation|
      operation.call
    rescue => error
      received << [:middleware, error]
      raise
    end

    result = perform

    expect(received).to eq([[:end, failure], [:middleware, failure]])
    expect(result.error).to equal(failure)
    expect(result.outcome).to eq(:retried)
    expect(result.job.errors.last).to have_attributes(error: "worker failed", trace: "worker.rb:12:in 'work'")
    expect(handled_errors).to eq([failure])
  end

  {
    cancelled: [River.job_cancel("cancelled by end hook"), "cancelled"],
    interrupted: [River::ClientRuntime::Interrupted.new, "available"],
    snoozed: [River.job_snooze(60), "scheduled"]
  }.each do |outcome, (replacement, state)|
    it "replaces a worker failure with a #{outcome} outcome" do
      worker.define_singleton_method(:work) { |_job| raise "worker failed" }
      add_end_hook { |_job, _error| replacement }

      result = perform

      expect(result.error).to equal(replacement)
      expect(result.outcome).to eq(outcome)
      expect(result.job.state).to eq(state)
      expect(result.job.errors.map(&:error)).to eq((outcome == :cancelled) ? [replacement.message] : [])
    end
  end

  it "skips end hooks when the worker raises outside StandardError" do
    fatal = Exception.new("fatal worker error")
    worker.define_singleton_method(:work) { |_job| raise fatal }
    received = []
    plugin = add_end_hook do |_job, error|
      received << :end
      error
    end
    plugin.define_singleton_method(:work) do |_job, operation|
      operation.call
    ensure
      received << :cleanup
    end

    expect { perform }.to raise_error { |error| expect(error).to equal(fatal) }
    expect(received).to eq([:cleanup])
  end

  it "skips the worker and all end hooks when a begin hook fails" do
    failure = RuntimeError.new("begin failed")
    received = []
    worker.define_singleton_method(:work) { |_job| received << :work }
    first = add_end_hook do |_job, error|
      received << :first_end
      error
    end
    first.define_singleton_method(:work_begin) do |_job|
      received << :first_begin
      raise failure
    end
    first.define_singleton_method(:work) do |_job, operation|
      operation.call
    rescue => error
      received << [:middleware, error]
      raise
    end
    second = add_end_hook do |_job, error|
      received << :second_end
      error
    end
    second.define_singleton_method(:work_begin) { |_job| received << :second_begin }

    result = perform

    expect(result.error).to equal(failure)
    expect(result.outcome).to eq(:retried)
    expect(received).to eq([:first_begin, [:middleware, failure]])
  end

  [nil, RuntimeError.new("worker failed")].each do |worker_error|
    it "stops the end-hook chain when a hook raises after #{worker_error ? "failure" : "success"}" do
      failure = RuntimeError.new("end hook raised")
      worker.define_singleton_method(:work) { |_job| raise worker_error if worker_error }
      received = []
      plugin = add_end_hook do |_job, error|
        received << error
        raise failure
      end
      plugin.define_singleton_method(:work) do |_job, operation|
        operation.call
      rescue => error
        received << [:middleware, error]
        raise
      end
      add_end_hook do |_job, error|
        received << :must_not_run
        error
      end

      result = perform

      expect(result.error).to equal(failure)
      expect(result.outcome).to eq(:retried)
      expect(received).to eq([worker_error, [:middleware, failure]])
      expect(result.job.errors.last.error).to eq("end hook raised")
    end
  end
end
