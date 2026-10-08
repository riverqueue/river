# frozen_string_literal: true

require "timeout"

RSpec.shared_examples "Yugabyte driver compatibility" do |notifications|
  let(:client) { River::Client.new(@driver) }
  let(:unique_options) { River::InsertOpts.new(unique_opts: River::UniqueOpts.new(by_args: true)) }

  def args(value)
    River::JobArgsHash.new(:yugabyte_test, {value: value})
  end

  it "detects capabilities lazily and caches only successful detection" do
    queries = 0
    @driver.define_singleton_method(:runtime_query_rows) do |sql|
      if sql.include?("current_setting('server_version_num')")
        queries += 1
        raise "temporary detection failure" if queries == 1
      end
      super(sql)
    end
    expect { @driver.init_driver }.to raise_error("temporary detection failure")
    @driver.init_driver
    capabilities = @driver.postgres_capabilities
    expect(capabilities).to have_attributes(unique_insert_mode: :metadata_nonce, unique_insert_sql: "false", supports_listen_notify: !!notifications)
    expect(@driver.postgres_capabilities).to equal(capabilities)
    row = client.insert(args(0)).job
    client.job_cancel(row.id)
    expect(queries).to eq(2)
  end

  it "detects conflicts in a mixed batch without changing application metadata" do
    original = client.insert(args(1), insert_opts: unique_options).job
    metadata = {"keep" => {"nested" => [1, nil]}, "river:unique_nonce" => "application value"}.freeze
    options = River::InsertOpts.new(metadata: metadata, unique_opts: River::UniqueOpts.new(by_args: true))
    results = client.insert_many([
      River::InsertManyParams.new(args(1), insert_opts: options),
      River::InsertManyParams.new(args(2), insert_opts: options),
      args(3)
    ])

    expect(results.map(&:unique_skipped_as_duplicate?)).to eq([true, false, false])
    expect(results.first.job).to have_attributes(id: original.id, metadata: original.metadata)
    expect(results[1].job.metadata).to include("keep" => metadata.fetch("keep"), "river:unique_nonce" => match(/\A[0-9a-f]{16}\z/))
    expect(results[2].job.metadata["river:unique_nonce"]).to eq(results[1].job.metadata["river:unique_nonce"])
    expect(metadata["river:unique_nonce"]).to eq("application value")
  end

  it "recognizes duplicates written without a valid nonce, including by Go" do
    [{}, {"river:unique_nonce" => nil}, {"river:unique_nonce" => 123}, {"river:unique_nonce" => {"nested" => true}}].each_with_index do |metadata, index|
      original = client.insert(args(index), insert_opts: unique_options).job
      client.job_update(original.id, River::JobUpdateParams.new(metadata: metadata))
      duplicate = client.insert(args(index), insert_opts: unique_options)
      expect(duplicate).to have_attributes(unique_skipped_as_duplicate?: true, job: have_attributes(id: original.id, metadata: metadata))
    end
  end

  it "rolls back a batch that upserts the same indexed row twice" do
    repeated = River::InsertManyParams.new(args(1), insert_opts: unique_options)
    expect { client.insert_many([args(0), repeated, repeated]) }.to raise_error(StandardError)
    expect(client.job_list.jobs).to be_empty
  end

  it "keeps inserts and cancellation in the application's transaction" do
    kept = client.insert(args(1)).job
    @driver.transaction do
      client.insert(args(2))
      client.job_cancel(kept.id)
      raise @driver.rollback_exception
    end
    expect(client.job_list.jobs).to contain_exactly(have_attributes(id: kept.id, state: "available", metadata: kept.metadata))
  end

  it "elects, renews, and replaces leaders without requiring notifications" do
    now = Time.utc(2026, 1, 2)
    expect(@driver.leader_acquire("first", now: now)).to be true
    expect(@driver.leader_acquire("second", now: now)).to be false
    expect(@driver.leader_renew("first", now: now + 1)).to be true
    @driver.leader_release("first")
    expect(@driver.leader_acquire("second", now: now + 2)).to be true
    @driver.leader_release("second")
  end

  it "polls committed remote cancellations while notifications are unavailable" do
    entered = Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) do |_job|
      entered << true
      Queue.new.pop
    end
    consumer = River::Client.new(@driver, config: River::Config.new(
      workers: River::Workers.new.add(:yugabyte_test, worker), queues: {default: 1},
      fetch_cooldown: 0.001, fetch_poll_interval: 0.01
    ))
    events = consumer.subscribe(:job_cancelled)
    publisher = River::Client.new(new_driver.call)
    row = publisher.insert(args(1)).job
    consumer.start
    Timeout.timeout(10) { entered.pop }
    publisher.driver.transaction do
      publisher.job_cancel(row.id)
      observer = Thread.new { consumer.job_get(row.id) }
      expect(Timeout.timeout(10) { observer.value }.metadata).not_to have_key("cancel_attempted_at")
      raise publisher.driver.rollback_exception
    end
    publisher.driver.transaction { publisher.job_cancel(row.id) }
    expect(Timeout.timeout(10) { events.pop }.job).to have_attributes(id: row.id, state: "cancelled")
  ensure
    consumer&.stop_and_cancel
  end
end
