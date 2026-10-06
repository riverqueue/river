# frozen_string_literal: true

require "pg"
require_relative "support/conformance_fixtures"

shared_context "PostgreSQL notification listener" do |topic|
  around do |example|
    @insert_listener = PG.connect(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test")
    row = @driver.send(:runtime_query_rows, "SELECT current_schema() AS name").first
    @insert_channel = "#{row[:name] || row["name"]}.#{topic}"
    @insert_listener.exec("LISTEN #{PG::Connection.quote_ident(@insert_channel)}")
    example.run
  ensure
    @insert_listener&.close
  end

  # The marker is committed after the operation under test. Receiving it proves
  # we've drained all earlier notifications, without a sleep/absence timeout.
  def insert_notifications
    marker = SecureRandom.hex(12)
    @insert_listener.exec_params("SELECT pg_notify($1, $2)", [@insert_channel, marker])
    payloads = []
    loop do
      payload = nil
      notification = @insert_listener.wait_for_notify(5) { |_channel, _pid, value| payload = value }
      raise "notification marker did not arrive" unless notification
      break if payload == marker

      payloads << JSON.parse(payload)
    end
    payloads
  end
end

shared_examples "PostgreSQL insert notifications" do
  include_context "PostgreSQL notification listener", "river_insert"

  it "notifies each available queue once and delivers only after the caller commits" do
    client = River::Client.new(@driver)
    @driver.transaction do
      client.insert_many(%w[one one two pending scheduled].map do |queue|
        state = %w[pending scheduled].include?(queue) ? queue : :available
        River::InsertManyParams.new(River::JobArgsHash.new(:notify, {}), insert_opts: River::InsertOpts.new(queue: queue, state: state))
      end)
      expect(insert_notifications).to be_empty
    end
    expect(insert_notifications).to contain_exactly({"queue" => "one"}, {"queue" => "two"})
  end

  it "does not publish rolled-back insertions or notifications from failed middleware" do
    client = River::Client.new(@driver)
    @driver.transaction do
      client.insert(River::JobArgsHash.new(:notify, {}))
      raise @driver.rollback_exception
    end
    expect(insert_notifications).to be_empty

    plugin = Object.new
    plugin.define_singleton_method(:insert_many) do |_params, operation|
      operation.call
      raise "middleware failed"
    end
    client = River::Client.new(@driver, config: River::Config.new(plugins: [plugin]))
    expect { client.insert(River::JobArgsHash.new(:notify, {})) }.to raise_error("middleware failed")
    expect(insert_notifications).to be_empty
    expect(client.job_list.jobs).to be_empty
  end
end

shared_examples "PostgreSQL cancellation notifications" do
  include_context "PostgreSQL notification listener", "river_control"

  it "publishes Go-compatible cancellation only when the transaction commits" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new(:notify, {})).job
    @driver.job_claim(id: row.id, attempted_by: "test")
    @driver.transaction do
      client.job_cancel(row.id)
      expect(insert_notifications).to be_empty
    end
    expect(insert_notifications).to eq([{"action" => "cancel", "job_id" => row.id, "queue" => "default"}])
  end

  it "does not notify rolled-back, missing, or already-finalized cancellations" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new(:notify, {})).job
    @driver.transaction do
      client.job_cancel(row.id)
      raise @driver.rollback_exception
    end
    expect(client.job_get(row.id).state).to eq("available")
    expect(insert_notifications).to be_empty
    client.job_cancel(row.id)
    expect(insert_notifications.length).to eq(1)
    client.job_cancel(row.id)
    expect { client.job_cancel(-1) }.to raise_error(River::NotFoundError)
    expect(insert_notifications).to be_empty
  end
end

shared_examples "SQLite cancellation notifications" do
  def cancellation_notifications
    @driver.send(:runtime_query_rows, "SELECT payload FROM river_notification WHERE topic = 'river_control' ORDER BY id")
      .map { |row| JSON.parse(@driver.send(:runtime_value, row, :payload)) }
  end

  it "makes cancellation and its outbox notification visible together on commit" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new(:notify, {})).job
    @driver.job_claim(id: row.id, attempted_by: "test")
    @driver.transaction do
      client.job_cancel(row.id)
      expect(cancellation_notifications.length).to eq(1)
      observer = Thread.new { cancellation_notifications }
      expect(Timeout.timeout(5) { observer.value }).to be_empty
    end
    expect(cancellation_notifications).to eq([{"action" => "cancel", "job_id" => row.id, "queue" => "default"}])
  end

  it "rolls back both cancellation and the outbox row" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new(:notify, {})).job
    @driver.transaction do
      client.job_cancel(row.id)
      raise @driver.rollback_exception
    end
    expect(client.job_get(row.id)).to have_attributes(state: "available", metadata: satisfy { |metadata| !metadata.key?("cancel_attempted_at") })
    expect(cancellation_notifications).to be_empty
  end

  it "cleans expired outbox rows in bounded batches without reusing notification IDs" do
    now = Time.utc(2026, 1, 2)
    times = [now - 2, now - 1, now, now + 1]
    times.each do |time|
      @driver.send(:runtime_execute, "INSERT INTO river_notification (topic, payload, created_at) VALUES ('river_control', '{}', #{@driver.send(:runtime_time, time)})")
    end
    expect(@driver.notification_delete_before(horizon: now, max: 1)).to eq(1)
    expect(@driver.notification_delete_before(horizon: now, max: 1)).to eq(1)
    expect(@driver.notification_delete_before(horizon: now, max: 1)).to eq(0)
    last_id = @driver.send(:runtime_query_rows, "SELECT max(id) AS id FROM river_notification").first
    expect(@driver.notification_delete_before(horizon: now + 2)).to eq(2)
    @driver.send(:runtime_execute, "INSERT INTO river_notification (topic, payload) VALUES ('river_control', '{}')")
    next_id = @driver.send(:runtime_query_rows, "SELECT max(id) AS id FROM river_notification").first
    expect(@driver.send(:runtime_value, next_id, :id)).to be > @driver.send(:runtime_value, last_id, :id)
  end
end

shared_examples "PostgreSQL queue control notifications" do
  include_context "PostgreSQL notification listener", "river_control"

  it "publishes Go's pause, resume, and metadata payloads only after commit" do
    @driver.queue_upsert("priority")
    @driver.transaction do
      @driver.queue_pause("priority")
      @driver.queue_resume("priority")
      @driver.queue_update("priority", metadata: {"owner" => "candidate"})
      expect(insert_notifications).to be_empty
    end
    fixtures = RiverConformanceFixtures.load("protocol_values").fetch("notifications")
    expected = %w[pause resume metadata_changed].map do |name|
      fixtures.find { |fixture| fixture.fetch("name") == name }.fetch("payload")
    end
    expect(insert_notifications).to eq(expected)
  end

  it "rolls back queue updates and notifications together" do
    @driver.queue_upsert("priority")
    @driver.transaction do
      @driver.queue_pause("priority")
      @driver.queue_update("priority", metadata: {"owner" => "rolled back"})
      raise @driver.rollback_exception
    end
    expect(insert_notifications).to be_empty
    expect(@driver.queue_get("priority")).to have_attributes(paused_at: nil, metadata: {})
  end

  it "notifies wildcard controls and ignores missing queues" do
    @driver.queue_upsert("one")
    @driver.queue_upsert("two")
    @driver.queue_pause("*")
    @driver.queue_resume("*")
    @driver.queue_pause("missing")
    @driver.queue_resume("missing")
    @driver.queue_update("missing", metadata: {})
    expect(insert_notifications).to eq([
      {"action" => "pause", "queue" => "*"}, {"action" => "resume", "queue" => "*"}
    ])
  end
end

shared_examples "PostgreSQL leadership notifications" do
  include_context "PostgreSQL notification listener", "river_leadership"

  it "broadcasts Go's resignation payload only when the leader's deletion commits" do
    @driver.leader_acquire("client-1")
    @driver.leader_release("other-client")
    expect(insert_notifications).to be_empty
    @driver.transaction do
      @driver.leader_release("client-1")
      expect(insert_notifications).to be_empty
      raise @driver.rollback_exception
    end
    expect(@driver.leader_renew("client-1")).to be true
    expect(insert_notifications).to be_empty
    @driver.transaction do
      @driver.leader_release("client-1")
      expect(insert_notifications).to be_empty
    end
    fixture = RiverConformanceFixtures.load("protocol_values").fetch("notifications").find { |entry| entry.fetch("name") == "resigned" }
    expect(insert_notifications).to eq([fixture.fetch("payload")])
    @driver.leader_release("client-1")
    expect(insert_notifications).to be_empty
  end
end
