# frozen_string_literal: true

require "spec_helper"
require "riverqueue-sequel"
require "riverqueue/testing"
require_relative "support/river_sqlite_schema_fixture"
require_relative "row_decoding_shared_examples"
require_relative "runtime_draining_shared_examples"
require_relative "runtime_finishing_shared_examples"
require_relative "transactional_completion_shared_examples"

RSpec.describe "upstream runtime parity", database: :sqlite do
  it_behaves_like "SQLite corrupt job runtime"
  it_behaves_like "cancellation while draining"
  it_behaves_like "externally claimed job finalization"
  it_behaves_like "transactional job completion"

  before do
    @database = Sequel.sqlite
    @database.synchronize { |connection| RiverSQLiteSchemaFixture.load(connection) }
    @driver = River::Driver::Sequel.new(@database)
  end

  after do
    @client&.stop_and_cancel
    @database.disconnect
  end

  def build_client(**options)
    @client = River::Client.new(@driver, config: River::Config.new(
      queues: {default: 2}, fetch_cooldown: 0.001, fetch_poll_interval: 0.005, **options
    ))
  end

  def receive(queue)
    Timeout.timeout(5) { queue.pop }
  end

  it "finalizes attempts whose workers produce invalid JSON metadata" do
    worker = Object.new
    worker.define_singleton_method(:work) do |job|
      job.update_metadata("kept" => true)
      job.output = Float::NAN
    end
    client = build_client(workers: River::Workers.new.add(:known, worker))
    row = client.insert(River::JobArgsHash.new(:known, {}), max_attempts: 1).job

    result = River::Testing.perform_job(client, row.id)
    expect(result).to have_attributes(outcome: :discarded, error: be_a(JSON::GeneratorError))
    expect(result.job).to have_attributes(state: "discarded", errors: contain_exactly(have_attributes(error: include("NaN"))))
    expect(result.job.metadata).to include("kept" => true)
    expect(result.job.metadata).not_to have_key("output")
  end

  it "reports a decode failure through synchronous execution without invoking the worker" do
    worker = Object.new
    worker.define_singleton_method(:work) { |_| raise "must not work a partial row" }
    client = build_client(workers: River::Workers.new.add(:known, worker))
    row = client.insert(River::JobArgsHash.new(:known, {}), max_attempts: 1).job
    @database[:river_job].where(id: row.id).update(tags: "{")

    result = River::Testing.perform_job(client, row.id)
    expect(result).to have_attributes(outcome: :discarded, error: be_a(River::JobRowDecodeError))
    expect(result.job).to have_attributes(state: "discarded", errors: contain_exactly(have_attributes(error: include("tags:"))))
  end

  it "does not emit completion events for rows still running, pending, or in an unknown state" do
    client = build_client
    events = client.subscribe(:job_completed)
    row = client.insert(River::JobArgsHash.new(:known, {})).job
    @driver.define_singleton_method(:job_complete) { |**| row }

    %w[running pending future_state].each do |state|
      row.state = state
      client.__finish_claimed_job(row)
    end
    expect { events.pop(true) }.to raise_error(ThreadError)
  ensure
    events&.close
  end

  it "allows eligible clients to manage periodic jobs" do
    expect(build_client.periodic_jobs).to be_a(River::PeriodicJobBundle)
  end

  it "fetches only registered kinds and aliases captured at startup" do
    workers = River::Workers.new
    client = build_client(workers: workers, fetch_only_known_kinds: true)
    worker = Class.new { def work(_job) = nil }
    workers.add(:known, worker, aliases: [:old_kind])
    unknown, *known = [:unknown, :known, :old_kind].map { |kind| client.insert(River::JobArgsHash.new(kind, {})).job }
    events = client.subscribe(:job_completed)

    client.start
    expect(2.times.map { receive(events).job.id }).to match_array(known.map(&:id))
    client.stop
    expect(client.job_get(unknown.id)).to have_attributes(attempt: 0, state: "available")
  end

  it "does not turn an empty kind registry into an unfiltered fetch" do
    client = build_client(fetch_only_known_kinds: true)
    row = client.insert(River::JobArgsHash.new(:unknown, {})).job
    fetches = Queue.new
    @driver.define_singleton_method(:job_get_available) do |**options|
      result = super(**options)
      fetches << options
      result
    end

    client.start
    expect(receive(fetches).fetch(:kinds)).to eq([])
    client.stop
    expect(client.job_get(row.id)).to have_attributes(attempt: 0, state: "available")
  end

  it "runs configured and dynamically added queues without leadership or maintenance" do
    worker = Class.new { def work(_job) = nil }
    client = build_client(workers: River::Workers.new.add(:known, worker), leader_election_disabled: true)
    maintenance_calls = Queue.new
    [:leader_acquire, :leader_renew, :leader_release, :job_schedule, :job_rescue_stuck, :job_delete_finalized, :notification_delete_before].each do |method|
      @driver.define_singleton_method(method) { |*args, **options| maintenance_calls << method }
    end
    expect { client.periodic_jobs }.to raise_error(ArgumentError, /leader_election_disabled/)
    events = client.subscribe(:job_completed)
    client.insert(River::JobArgsHash.new(:known, {}))

    client.start
    client.queue_add(:dynamic, 1)
    client.insert(River::JobArgsHash.new(:known, {}), insert_opts: River::InsertOpts.new(queue: :dynamic))
    expect(2.times.map { receive(events).job.queue }).to match_array(%w[default dynamic])
    client.stop
    expect(maintenance_calls).to be_empty
    expect(@database[:river_leader].count).to eq(0)
  end

  it "does not lose a cancellation committed while a fetch is returning" do
    claimed = Queue.new
    release = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) { |_job| Queue.new.pop }
    client = build_client(workers: River::Workers.new.add(:known, worker), leader_election_disabled: true)
    row = client.insert(River::JobArgsHash.new(:known, {})).job
    events = client.subscribe(:job_cancelled)
    @driver.define_singleton_method(:job_get_available) do |**options|
      jobs = super(**options)
      unless jobs.empty?
        claimed << true
        release.pop
      end
      jobs
    end

    client.start
    receive(claimed)
    client.job_cancel(row.id)
    release << true
    expect(receive(events).job).to have_attributes(id: row.id, state: "cancelled")
  ensure
    release << true if release
  end

  it "gives the error handler the row for each separately reported job" do
    handled = []
    client = build_client(error_handler: ->(_error, job) { handled << [job.id, job.args] })
    rows = [1, 2].map do |value|
      inserted = client.insert(River::JobArgsHash.new(:known, {value: value}), insert_opts: River::InsertOpts.new(max_attempts: 1)).job
      @driver.job_claim(id: inserted.id, attempted_by: "test")
    end
    rows.each { |row| client.__finish_claimed_job(row, RuntimeError.new("failed")) }
    expect(handled).to eq(rows.map { |row| [row.id, row.args] })
  end

  it "keeps other running jobs' outcomes independent of a remote cancellation" do
    started = Queue.new
    release = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) do |job|
      started << job.id
      release.pop
      raise "peer failed" if job.args.fetch("role") == "failure"
    end
    worker.define_singleton_method(:next_retry) { |_job, _error| Time.now.utc + 60 }
    client = build_client(queues: {default: 3}, workers: River::Workers.new.add(:known, worker), leader_election_disabled: true)
    cancelled, failed, completed = %w[cancel failure success].map do |role|
      client.insert(River::JobArgsHash.new(:known, {role: role})).job
    end
    events = client.subscribe(:job_cancelled, :job_failed, :job_completed)

    client.start
    expect(3.times.map { receive(started) }).to match_array([cancelled.id, failed.id, completed.id])
    canceller = River::Client.new(River::Driver::Sequel.new(@database))
    canceller.job_cancel(cancelled.id)
    expect(receive(events)).to have_attributes(kind: :job_cancelled, job: have_attributes(id: cancelled.id, state: "cancelled"))

    2.times { release << true }
    expect(2.times.map { receive(events).kind }).to match_array([:job_failed, :job_completed])
    client.stop
    expect(client.job_get(failed.id)).to have_attributes(state: "retryable", errors: contain_exactly(have_attributes(error: "peer failed")))
    expect(client.job_get(completed.id)).to have_attributes(state: "completed", errors: [])
  ensure
    events&.close
  end

  it "rejects a SQLite batch that would update the same unique row twice" do
    client = build_client
    options = River::InsertOpts.new(unique_opts: River::UniqueOpts.new(by_args: true))
    batch = [1, 2, 2].map do |value|
      River::InsertManyParams.new(River::JobArgsHash.new(:known, {value: value}), insert_opts: options)
    end
    expect { client.insert_many(batch) }.to raise_error(ArgumentError, "unique key appears more than once in batch")
    expect(client.job_list.jobs).to be_empty
    expect(@database[:river_notification].count).to eq(0)
  end

  it "allows unindexed SQLite unique keys and gives every inserted row a separate nonce" do
    client = build_client
    params = [[nil, nil], ["", nil], ["key", nil], ["key", "00000000"], ["key", "00010000"]].map do |key, states|
      param = client.send(:make_insert_params, River::JobArgsHash.new(:known, {}), River::InsertOpts.new)
      param.unique_key = key
      param.unique_states = states
      param
    end
    results = @driver.job_insert_many(params)
    expect(results.map(&:last)).to eq([false] * params.length)
    nonces = results.map { |row, _duplicate| row.metadata.fetch("river:unique_nonce") }
    expect(nonces).to all(match(/\A[0-9a-f]{16}\z/))
    expect(nonces.uniq.length).to eq(params.length)
  end
end
