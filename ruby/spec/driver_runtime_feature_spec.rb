# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"
require_relative "driver_runtime_shared_examples"

RSpec.describe "River driver runtime contracts" do
  around(:each) { |example| available_test_transaction(&example) }

  let(:driver) { River::Driver::Sequel.new(available_test_database) }

  it_behaves_like "driver job state machine"
  it_behaves_like "driver queue and leadership state"
end

RSpec.describe "SQLite numeric metadata counters", database: :sqlite do
  around(:each) { |example| sqlite_test_transaction(&example) }

  let(:driver) { River::Driver::Sequel.new(SQLITE_DB) }

  it_behaves_like "overflowing metadata counters"
end

RSpec.describe "River shared driver runtime edge cases", database: :sqlite do
  let(:postgres_driver) do
    River::Driver::Sequel.new(SQLITE_DB).tap do |driver|
      driver.define_singleton_method(:runtime_postgres?) { true }
    end
  end

  let(:sqlite_driver) { River::Driver::Sequel.new(SQLITE_DB) }

  it "does not issue Postgres notifications when Yugabyte disables them" do
    capabilities = River::Driver::PostgresCapabilities.new(product: "YugabyteDB", version_num: 150_012, yb_listen_notify_enabled: false)
    postgres_driver.define_singleton_method(:postgres_capabilities) { capabilities }
    postgres_driver.define_singleton_method(:runtime_execute) { |_sql| raise "unexpected notification" }
    params = Struct.new(:state, :queue).new("available", "default")

    expect(postgres_driver.notification_listener).to be_nil
    expect { postgres_driver.init_driver }.not_to raise_error
    expect { postgres_driver.send(:postgres_notify_insert, [params]) }.not_to raise_error
    expect { postgres_driver.send(:runtime_notify, "river_control", {action: "cancel"}) }.not_to raise_error
  end

  it "does not use the SQLite notification cleaner on Postgres" do
    expect(postgres_driver.notification_delete_before(horizon: Time.now.utc)).to eq(0)
  end

  it "returns a freshly read job if deletion loses a race" do
    existing = Struct.new(:state).new(River::JOB_STATE_AVAILABLE)
    replacement = Struct.new(:state).new(River::JOB_STATE_RUNNING)
    reads = [existing, replacement]
    driver = Object.new.extend(River::Driver::Runtime)
    driver.define_singleton_method(:job_get_by_id) { |_id| reads.shift }
    driver.define_singleton_method(:runtime_returning_ids) { |_sql| [] }

    expect(driver.job_delete(1)).to equal(replacement)
  end

  it "uses the shared unfiltered-list implementation" do
    expected = [Object.new]
    driver = Object.new.extend(River::Driver::Runtime)
    driver.define_singleton_method(:runtime_job_list_without_params) { expected }

    expect(driver.job_list(:all)).to equal(expected)
  end

  it "builds Postgres claim SQL with row locking and array history" do
    sql = nil
    driver = Object.new.extend(River::Driver::Runtime)
    driver.define_singleton_method(:runtime_postgres?) { true }
    driver.define_singleton_method(:runtime_quote) { |value| "'#{value}'" }
    driver.define_singleton_method(:transaction) { |&block| block.call }
    driver.define_singleton_method(:runtime_returning_ids) do |statement|
      sql = statement
      []
    end

    expect(driver.job_get_available(attempted_by: "client", max: 2, queue: "work")).to eq([])
    expect(sql).to include("array_append", "FOR UPDATE SKIP LOCKED")
  end

  it "rejects nil runtime timestamps" do
    expect { postgres_driver.send(:runtime_time, nil) }.to raise_error(ArgumentError, "time cannot be nil")
  end

  it "serializes timestamps without mutating the caller's timezone" do
    time = Time.new(2026, 1, 2, 3, 4, 5.123, "+05:30")
    [postgres_driver, sqlite_driver].each do |driver|
      driver.send(:runtime_time, time)
      expect(time.utc_offset).to eq(19_800)
      expect(driver.send(:runtime_time, time.freeze)).to include("2026-01-01")
    end
  end

  it "accepts pre-encoded runtime JSON and raw errors" do
    expect(postgres_driver.send(:runtime_json, '{"ready":true}')).to include('{"ready":true}')
    expect(postgres_driver.send(:runtime_append_error, 7)).to include("7")
  end

  it "returns nil for an unsupported internal update field" do
    expect(postgres_driver.send(:runtime_update_value, :unknown, true)).to be_nil
  end

  it "serializes nil timestamps as SQL NULL" do
    expect(postgres_driver.send(:runtime_update_value, :attempted_at, nil)).to eq("NULL")
    expect(postgres_driver.send(:runtime_update_value, :finalized_at, nil)).to eq("NULL")
  end

  it "serializes attempted-by and raw errors for SQLite" do
    attempted_by = sqlite_driver.send(:runtime_update_value, :attempted_by, ["one"])
    errors = sqlite_driver.send(:runtime_update_value, :errors, [7])

    expect(attempted_by).to include("one")
    expect(errors).to include("7")
  end

  it "renders every shared Postgres-specific SQL value" do
    expect(postgres_driver.send(:runtime_tag_contains, "tag")).to include("tags @>")
    expect(postgres_driver.send(:runtime_metadata_equals, :tenant, 7)).to include("metadata ->", "::jsonb")
    expect(postgres_driver.send(:runtime_state, River::JOB_STATE_AVAILABLE)).to end_with("::river_job_state")
    expect(postgres_driver.send(:runtime_time, Time.utc(2026, 1, 2))).to end_with("::timestamptz")
    expect(postgres_driver.send(:runtime_merge_metadata, {"ready" => true})).to include("metadata ||")
    expect(postgres_driver.send(:runtime_cancel_attempted)).to eq("metadata ? 'cancel_attempted_at'")
    expect(postgres_driver.send(:runtime_update_value, :attempted_by, ["one"])).to include("ARRAY[", "::text[]")
    expect(postgres_driver.send(:runtime_update_value, :errors, [7])).to include("ARRAY[", "::jsonb[]")
    expect(postgres_driver.send(:runtime_queue_columns)).to eq("name, created_at, metadata, paused_at, updated_at")
  end

  it "accepts already-decoded JSON and Time values from Postgres adapters" do
    metadata = {"ready" => true}
    time = Time.utc(2026, 1, 2, 3, 4, 5)

    expect(postgres_driver.send(:runtime_parse_json, metadata)).to eq(metadata)
    expect(postgres_driver.send(:runtime_parse_time, time)).to eq(time)
  end

  it "parses timestamps that already carry a UTC suffix" do
    parsed = postgres_driver.send(:runtime_parse_time, "2026-01-02T03:04:05Z")

    expect(parsed).to eq(Time.utc(2026, 1, 2, 3, 4, 5))
  end
end
