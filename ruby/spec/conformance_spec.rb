# frozen_string_literal: true

require "spec_helper"

# The fixture-backed checks run once, alongside SQLite, without a Go setup in
# every PostgreSQL matrix entry.
return unless RiverTestDatabase.enabled?(:sqlite)
require_relative "support/conformance_fixtures"
require_relative "support/river_test_schema"
require "riverqueue-activerecord"
require "riverqueue-sequel"
require "riverqueue/testing"

class ConformanceRecord < ActiveRecord::Base
  self.abstract_class = true
end

RSpec.describe "Go-generated conformance fixtures" do
  fixture_source = RiverConformanceFixtures.read("unique_keys")
  # Preserve raw argument tokens, including duplicate keys and negative zero.
  # The Go generator uses four spaces for each case's root object.
  cases = fixture_source.scan(/^    \{.*?^    \}/m)
  expected_count = JSON.parse(fixture_source).values_at("cases", "typed_only_cases").sum(&:length)
  raise "fixture case extraction is incomplete" unless expected_count.positive? && cases.length == expected_count

  cases.each do |raw|
    raw = raw.gsub(/"(?:[^"\\]|\\.)*"|\s+/m) { |token| token.start_with?('"') ? token : "" }
    fixture = JSON.parse(raw)
    it "matches Go's #{fixture.fetch("name")} golden through insertion preparation" do
      args = Struct.new(:kind, :encoded) { def to_json = encoded }.new(fixture.fetch("kind"), River::UniqueArgs.members(raw).fetch("args"))
      dimensions = fixture.fetch("options")
      options = River::InsertOpts.new(queue: fixture.fetch("queue"),
        scheduled_at: fixture["scheduled_at"] && Time.iso8601(fixture.fetch("scheduled_at")),
        unique_opts: River::UniqueOpts.new(
          by_args: dimensions.fetch("by_args") && (fixture["selected_unique_components"] || true),
          by_period: dimensions.fetch("by_period_nanos").positive? ? Rational(dimensions.fetch("by_period_nanos"), 1_000_000_000) : nil,
          by_queue: dimensions.fetch("by_queue"), by_state: dimensions["by_state"], exclude_kind: dimensions.fetch("exclude_kind")
        ))
      client = River::Client.new(Object.new)
      client.instance_variable_set(:@time_now_utc, -> { Time.iso8601(fixture.fetch("now")) })
      if fixture["expected_error"]
        expect { client.send(:make_insert_params, args, options) }.to raise_error(ArgumentError, "unique args must encode a JSON object")
      else
        prepared = client.send(:make_insert_params, args, options)
        expect(prepared.unique_key.unpack1("H*")).to eq(fixture.fetch("expected_sha256"))
        expect(prepared.unique_states.to_i(2)).to eq(fixture.fetch("expected_state_mask"))
      end
    end
  end

  cron = RiverConformanceFixtures.load("cron_schedules")
  cron.values_at("cron_cases", "cron_named_zone_cases").flatten.each do |fixture|
    it "matches Go's cron schedule #{fixture.fetch("name")}" do
      from = Time.iso8601(fixture.fetch("from"))
      # Ruby supplies the default calendar zone explicitly; Go uses the
      # location on its reference time. Prefixes override this in both APIs.
      if fixture.fetch("next").empty?
        # Fugit rejects impossible dates at construction instead of returning
        # Go's zero time. Assert that difference rather than skipping the case.
        expect { River::PeriodicCron.new(fixture.fetch("expression")) }.to raise_error(ArgumentError)
      else
        schedule = River::PeriodicCron.new(fixture.fetch("expression"), timezone: from.strftime("%:z"))
        fixture.fetch("next").each do |expected|
          from = schedule.next(from)
          expect(from).to eq(Time.iso8601(expected))
        end
      end
    end
  end

  # Retain documented Ruby/Fugit extensions. Account for them explicitly so
  # every new Go rejection must be considered, rather than silently skipped.
  ruby_cron_extensions = ["* * * * * *", "0 9 * * 7", "* 24 * * *", "5-1 * * * *"]
  cron.fetch("cron_invalid").each do |expression|
    it "checks Go's invalid cron expression #{expression.inspect}" do
      if ruby_cron_extensions.include?(expression)
        expect(River::PeriodicCron.new(expression).next(Time.utc(2026))).to be_a(Time)
      else
        expect { River::PeriodicCron.new(expression) }.to raise_error(ArgumentError)
      end
    end
  end

  protocol = RiverConformanceFixtures.load("protocol_values")

  it "accounts for every protocol key and notification in the Go fixtures" do
    expect(protocol.fetch("metadata_keys").keys).to match_array(%w[output periodic_job_id rescue_count resumable_cursor resumable_step unique_nonce])
    expect(protocol.fetch("notifications").map { |fixture| fixture.fetch("name") }).to match_array(%w[cancel insert metadata_changed pause request_resign resigned resume])
    expect(protocol.fetch("topics").keys).to match_array(%w[control insert leadership])
    expect(protocol.fetch("job_states")).not_to be_empty
    expect(protocol.fetch("retry_cases")).not_to be_empty
    expect(cron.fetch("cron_invalid")).to include(*ruby_cron_extensions)
    expect(cron.fetch("cron_cases")).not_to be_empty
    expect(cron.fetch("cron_named_zone_cases")).not_to be_empty
    expect(RiverConformanceFixtures.load("snooze_counters").fetch("snooze_counters")).not_to be_empty
  end

  protocol.fetch("job_states").each do |fixture|
    it "matches the unique state bit for #{fixture.fetch("state")}" do
      state = fixture.fetch("state")
      bit = fixture.fetch("unique_bit")
      expect(River::UniqueBitmask.from_states([state]).to_i(2)).to eq(bit)
      expect(River::UniqueBitmask.to_states(bit)).to eq([state])
      expect(River.const_get("JOB_STATE_#{state.upcase}")).to eq(state)
    end
  end

  it "round trips Go's attempt error through the production decoder" do
    fixture = protocol.fetch("attempt_error")
    error = River::Driver::JobRowDecoder.attempt_error(fixture)
    expect(error).to have_attributes(at: Time.iso8601(fixture.fetch("at")),
      attempt: fixture.fetch("attempt"), error: fixture.fetch("error"), trace: fixture.fetch("trace"))
    encoded = JSON.parse(JSON.generate(error.to_h))
    expect(Time.iso8601(encoded.delete("at"))).to eq(Time.iso8601(fixture.fetch("at")))
    expect(encoded).to eq(fixture.except("at"))
  end

  protocol.fetch("retry_cases").each do |fixture|
    it "matches retry bounds for #{fixture.fetch("error_count")} errors" do
      now = Time.iso8601(fixture.fetch("now"))
      row = Struct.new(:errors).new(Array.new(fixture.fetch("error_count") - 1))
      policy = River::DefaultClientRetryPolicy.new(random: Random.new(fixture.fetch("seed")))
      delay = (policy.next_retry(row, now: now).to_r - now.to_r) * 1_000_000_000
      expect(delay).to be_between(fixture.fetch("min_delay_ns"), fixture.fetch("max_delay_ns")).inclusive
    end
  end

  it "uses Go's resumable metadata keys" do
    keys = protocol.fetch("metadata_keys")
    expect(River::RESUMABLE_CURSOR_METADATA_KEY).to eq(keys.fetch("resumable_cursor"))
    expect(River::RESUMABLE_STEP_METADATA_KEY).to eq(keys.fetch("resumable_step"))
  end

  # These exercise actual insertion/cancellation and the SQL outbox in both
  # drivers without requiring a server. PostgreSQL delivery/commit ordering is
  # separately exercised by the shared driver specs.
  %w[activerecord sequel].each do |adapter|
    context "#{adapter} persisted protocol" do
      around do |example|
        if adapter == "activerecord"
          ConformanceRecord.establish_connection(adapter: "sqlite3", database: ":memory:")
          @driver = River::Driver::ActiveRecord.new(connection_class: ConformanceRecord)
        else
          database = Sequel.sqlite
          @driver = River::Driver::Sequel.new(database)
        end
        RiverTestSchema.load(@driver)
        @client = River::Client.new(@driver)
        example.run
      ensure
        (adapter == "activerecord") ? ConformanceRecord.remove_connection : database&.disconnect
      end

      # Ruby emits these messages; its polling runtime does not consume
      # request_resign or the other notification fixtures yet.
      %w[cancel insert metadata_changed pause resigned resume].each do |name|
        fixture = protocol.fetch("notifications").find { |notification| notification.fetch("name") == name }
        raise "Missing #{name} notification fixture" unless fixture

        it "emits Go's #{name} topic and payload" do
          # Operation arguments are independent of the fixture's wire keys and
          # action names so format changes fail instead of changing our input.
          case name
          when "cancel", "insert"
            row = @client.insert(River::JobArgsHash.new(:notify, {}), queue: "priority").job
            if name == "cancel"
              @driver.send(:runtime_execute, "UPDATE river_job SET id = 42 WHERE id = #{row.id}")
              @driver.job_claim(id: 42, attempted_by: "conformance")
              @client.job_cancel(42)
            end
          when "metadata_changed", "pause", "resume"
            @driver.queue_upsert("priority")
            case name
            when "metadata_changed" then @driver.queue_update("priority", metadata: {"owner" => "candidate"})
            when "pause" then @driver.queue_pause("priority")
            when "resume"
              @driver.queue_pause("priority")
              @driver.queue_resume("priority")
            end
          when "resigned"
            @driver.leader_acquire("client-1")
            @driver.leader_release("client-1")
          end
          notification = @driver.send(:runtime_query_rows, "SELECT topic, payload FROM river_notification ORDER BY id DESC LIMIT 1").first
          expect(@driver.send(:runtime_value, notification, :topic)).to eq(fixture.fetch("topic"))
          topic = if name == "resigned"
            "leadership"
          else
            ((name == "insert") ? "insert" : "control")
          end
          expect(@driver.send(:runtime_value, notification, :topic)).to eq(protocol.fetch("topics").fetch(topic))
          expect(JSON.parse(@driver.send(:runtime_value, notification, :payload))).to eq(fixture.fetch("payload"))
        end
      end

      it "rolls back queue controls, resignation, and their outbox messages together" do
        @driver.queue_upsert("priority")
        @driver.leader_acquire("client-1")
        @driver.transaction do
          @driver.queue_pause("priority")
          @driver.queue_resume("priority")
          @driver.queue_update("priority", metadata: {"owner" => "rolled back"})
          @driver.leader_release("client-1")
          expect(@driver.send(:runtime_query_rows, "SELECT id FROM river_notification").size).to eq(4)
          raise @driver.rollback_exception
        end
        expect(@driver.queue_get("priority")).to have_attributes(paused_at: nil, metadata: {})
        expect(@driver.leader_renew("client-1")).to be true
        expect(@driver.send(:runtime_query_rows, "SELECT id FROM river_notification")).to be_empty
      end

      RiverConformanceFixtures.load("snooze_counters").fetch("snooze_counters").each do |fixture|
        it "persists Go's snooze counter for #{fixture.fetch("name")}" do
          worker = Class.new { def work(_job) = raise(River.job_snooze(60)) }
          client = River::Client.new(@driver, config: River::Config.new(workers: River::Workers.new.add(:snooze, worker)))
          row = client.insert(River::JobArgsHash.new(:snooze, {}), metadata: fixture.fetch("metadata")).job
          result = River::Testing.perform_job(client, row.id)
          expect(result).to have_attributes(outcome: :snoozed, error: be_a(River::JobSnoozeError))
          expect(result.job).to have_attributes(attempt: 0, state: "scheduled")
          expect(result.job.metadata.fetch("snoozes")).to eq(fixture.fetch("expected_snoozes"))
        end
      end

      it "persists output and Go's rescue counter" do
        keys = protocol.fetch("metadata_keys")
        now = Time.now.utc
        worker = Class.new { def work(job) = job.output = {"ok" => true} }
        client = River::Client.new(@driver, config: River::Config.new(workers: River::Workers.new.add(:output, worker)))
        row = client.insert(River::JobArgsHash.new(:output, {}), scheduled_at: now - 120,
          metadata: {keys.fetch("rescue_count") => 2}).job
        @driver.job_claim(id: row.id, attempted_by: "crashed", now: now - 60)
        expect(@driver.job_rescue_stuck(horizon: now - 30, now: now, retry_policy: River::DefaultClientRetryPolicy.new)).to eq(1)
        result = River::Testing.perform_job(client, row.id, allow_scheduled: true)
        expect(result).to have_attributes(outcome: :completed, error: nil)
        expect(result.job.metadata).to include(keys.fetch("output") => {"ok" => true}, keys.fetch("rescue_count") => 3)
      end

      it "stores Go's unique insert nonce and preserves it on a duplicate" do
        key = protocol.fetch("metadata_keys").fetch("unique_nonce")
        args = River::JobArgsHash.new(:unique, {})
        first = @client.insert(args, unique_opts: River::UniqueOpts.new(by_args: true))
        duplicate = @client.insert(args, unique_opts: River::UniqueOpts.new(by_args: true))
        expect(first.job.metadata.fetch(key)).to be_a(String)
        expect(first.job.metadata.fetch(key)).not_to be_empty
        expect(duplicate.job.metadata.fetch(key)).to eq(first.job.metadata.fetch(key))
      end

      [nil, "", "conformance_periodic"].each do |id|
        it "marks periodic jobs with ID #{id.inspect} without mutating constructor options" do
          metadata = {"owner" => "ruby", "periodic" => false}.freeze
          opts = River::InsertOpts.new(metadata: metadata, queue: "priority").freeze
          args = River::JobArgsHash.new(:periodic, {})
          args.define_singleton_method(:insert_opts) { River::InsertOpts.new(metadata: {"from_args" => true}) }
          periodic = River::PeriodicJob.new(id: id, schedule: River::PeriodicInterval.new(60), run_on_start: true) { [args, opts] }
          client = River::Client.new(@driver, config: River::Config.new(periodic_jobs: [periodic]))
          client.instance_variable_get(:@runtime).send(:run_periodic, Time.now.utc + 1)
          row = client.job_list.jobs.fetch(0)
          expect(row.queue).to eq("priority")
          expect(row.metadata).to include("owner" => "ruby", "periodic" => true, "from_args" => true)
          key = protocol.fetch("metadata_keys").fetch("periodic_job_id")
          if id && !id.empty?
            expect(row.metadata.fetch(key)).to eq(id)
          else
            expect(row.metadata).not_to have_key(key)
          end
          expect(opts.metadata).to equal(metadata)
          expect(metadata).to eq("owner" => "ruby", "periodic" => false)
        end
      end

      it "resumes from and persists Go's step and cursor metadata" do
        keys = protocol.fetch("metadata_keys")
        calls = []
        worker = Object.new
        worker.define_singleton_method(:work) do |job|
          job.resumable_step("before") { calls << :before }
          job.resumable_step_cursor("process") do |cursor|
            calls << cursor
            job.resumable_set_cursor("offset" => 3)
            raise "retry"
          end
        end
        client = River::Client.new(@driver, config: River::Config.new(workers: River::Workers.new.add(:resumable, worker)))
        row = client.insert(River::JobArgsHash.new(:resumable, {}), metadata: {
          keys.fetch("resumable_step") => "process", keys.fetch("resumable_cursor") => {"process" => {"offset" => 2}}
        }).job
        result = River::Testing.perform_job(client, row.id)
        expect(result).to have_attributes(outcome: :retried, error: have_attributes(message: "retry"))
        expect(calls).to eq([{"offset" => 2}])
        expect(result.job.metadata).to include(keys.fetch("resumable_step") => "process",
          keys.fetch("resumable_cursor") => {"process" => {"offset" => 3}})
      end
    end
  end
end
