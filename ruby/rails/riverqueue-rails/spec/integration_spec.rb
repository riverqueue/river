# frozen_string_literal: true

require "spec_helper"

class IntegrationRecord < ActiveRecord::Base
  include GlobalID::Identification
end

class IntegrationCurrent < ActiveSupport::CurrentAttributes
  attribute :customer
end

class IntegrationCallbackJob < ActiveJob::Base
  before_perform { IntegrationJob.seen << :before }
  after_perform { IntegrationJob.seen << :after }

  def perform
    IntegrationJob.seen << [:work, IntegrationCurrent.customer]
    IntegrationCurrent.customer = "must be cleared"
  end
end

class IntegrationJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = (ActiveJob.gem_version >= Gem::Version.new("8.0")) ? false : :never
  class_attribute :seen, default: Queue.new

  def perform(value, flag: false)
    self.class.seen << [value, flag, job_id, provider_job_id, I18n.locale, Time.zone&.name]
  end
end

class IntegrationRetryJob < IntegrationJob
  retry_on ArgumentError, attempts: 2, wait: 0

  def perform(*)
    self.class.seen << executions
    raise ArgumentError, "retry me"
  end
end

class IntegrationDiscardJob < IntegrationJob
  discard_on ArgumentError

  def perform(*)
    raise ArgumentError, "discard me"
  end
end

class IntegrationBrokenJob < IntegrationJob
  def perform(*)
    raise "unhandled"
  end
end

class IntegrationInterruptibleJob < IntegrationJob
  retry_on StandardError, attempts: 3, wait: 0

  def perform(action)
    case action
    when "cancel"
      raise River.job_cancel("cancelled")
    when "snooze"
      raise River.job_snooze(60)
    else
      self.class.seen << :started
      sleep 60
    end
  end
end

class IntegrationMailer < ActionMailer::Base
  default from: "river@example.test"

  def receipt
    mail(body: "Thanks", subject: "Receipt", to: "customer@example.test")
  end
end

adapters = RiverTestDatabase.enabled?(:sqlite) ? [:sqlite] : []
if RiverTestDatabase.enabled?(:postgres)
  begin
    require "pg"
    PG.connect(ENV["TEST_DATABASE_URL"] || "postgres://localhost/river_test").close
    adapters << :postgres
  rescue PG::Error => error
    raise if ENV["CI"] == "true" || ENV["RIVER_REQUIRE_DATABASES"] == "1"

    warn "Skipping Rails Postgres tests: #{error.message}"
  end
end

adapters.each do |backend|
  RSpec.describe "Rails integration with #{backend}", database: backend do
    around do |example|
      ClientTestDatabase.with_active_record(backend) do |driver|
        @driver = driver
        previous = Rails.application.config.river
        settings = River::Rails::Configuration.new
        settings.configure do
          River::Config.new(fetch_cooldown: 0.001, fetch_poll_interval: 0.01,
            logger: Rails.logger, queues: {"default" => 3, "mailers" => 1})
        end

        Rails.application.config.river = settings
        ActiveJob::Base.queue_adapter = :river
        IntegrationJob.seen = Queue.new
        ActionMailer::Base.deliveries.clear
        example.run
      ensure
        @consumer&.stop_and_cancel
        Rails.application.config.river = previous
      end
    end

    it "boots the adapter without starting consumers" do
      expect(ActiveJob::Base.queue_adapter).to be_a(ActiveJob::QueueAdapters::RiverAdapter)
      expect(Rails.application.config.river.client.started?).to be false
    end

    it "resolves Active Job classes again after Rails reloads" do
      old_class = ReloadableJob
      ReloadableJob.perform_later
      Rails.application.reloader.reload!

      expect(ReloadableJob).not_to equal(old_class)
      work_until { rows.first.state == "completed" }

      expect(IntegrationJob.seen.pop).to eq(ReloadableJob.object_id)
    end

    it "exposes canonical migration and status Rails tasks" do
      require "rake"
      Rails.application.load_tasks unless Rake::Task.task_defined?("river:migrate")

      expect { Rake::Task["river:migrate"].execute }.not_to raise_error
      expect { Rake::Task["river:status"].execute }.to output(/applied 7/).to_stdout
    end

    it "inserts and executes serialized keyword arguments, IDs, and context" do
      job = Time.use_zone("Asia/Tokyo") { IntegrationJob.perform_later("hello", flag: true) }

      expect(rows.first).to have_attributes(kind: "active_job", max_attempts: 25, priority: 1, queue: "default")
      work_until { rows.first.state == "completed" }

      expect(IntegrationJob.seen.pop).to eq(["hello", true, job.job_id, job.provider_job_id, :en, "Asia/Tokyo"])
    end

    it "inserts in the application's transaction and rolls back together" do
      ActiveRecord::Base.transaction do
        IntegrationJob.perform_later("rollback")

        expect(rows.length).to eq(1)
        raise ActiveRecord::Rollback
      end

      expect(rows).to be_empty
    end

    it "preserves delayed execution timestamps" do
      time = Time.now.utc + 3600
      IntegrationJob.set(wait_until: time).perform_later("later")

      expect(rows.first).to have_attributes(scheduled_at: be_within(0.001).of(time), state: "scheduled")
    end

    it "respects an explicit after-commit policy" do
      original = IntegrationJob.enqueue_after_transaction_commit
      IntegrationJob.enqueue_after_transaction_commit = (ActiveJob.gem_version >= Gem::Version.new("8.0")) ? true : :always
      ActiveRecord::Base.transaction do
        IntegrationJob.perform_later("after commit")
        expect(rows).to be_empty
      end

      expect(rows.length).to eq(1)
    ensure
      IntegrationJob.enqueue_after_transaction_commit = original
    end

    it "does not enqueue deferred work on rollback" do
      original = IntegrationJob.enqueue_after_transaction_commit
      IntegrationJob.enqueue_after_transaction_commit = (ActiveJob.gem_version >= Gem::Version.new("8.0")) ? true : :always
      ActiveRecord::Base.transaction do
        IntegrationJob.perform_later("rollback")
        raise ActiveRecord::Rollback
      end

      expect(rows).to be_empty
    ensure
      IntegrationJob.enqueue_after_transaction_commit = original
    end

    it "preserves callbacks and resets CurrentAttributes after execution" do
      IntegrationCallbackJob.perform_later
      work_until { rows.first.state == "completed" }

      expect(3.times.map { IntegrationJob.seen.pop }).to eq([:before, [:work, nil], :after])
      expect(IntegrationCurrent.customer).to be_nil
    end

    it "supports bulk enqueueing and provider IDs" do
      jobs = [IntegrationJob.new("one"), IntegrationJob.new("two")]
      ActiveJob.perform_all_later(jobs)

      expect(rows.length).to eq(2)
      expect(jobs.map(&:provider_job_id)).to eq(rows.map { |row| row.id.to_s })
      expect(jobs.all?(&:successfully_enqueued?)).to be true
    end

    it "preserves delayed timestamps in a bulk enqueue" do
      job = IntegrationJob.new("later")
      job.scheduled_at = Time.now.utc + 3600
      ActiveJob.perform_all_later([job])

      expect(rows.first).to have_attributes(scheduled_at: be_within(0.001).of(job.scheduled_at), state: "scheduled")
    end

    it "does not partially insert a batch with an invalid job" do
      valid = IntegrationJob.new("valid")
      invalid = IntegrationJob.new("invalid")
      invalid.priority = 9

      expect { ActiveJob.perform_all_later([valid, invalid]) }.to raise_error(ArgumentError)
      expect(rows).to be_empty
      expect(valid.provider_job_id).to be_nil
    end

    it "rejects invalid priorities without silently clamping" do
      expect { IntegrationJob.set(priority: 0).perform_later("bad") }.to raise_error(ArgumentError, /priority/)
      expect(rows).to be_empty
    end

    it "preserves supported priorities and explicit queues" do
      IntegrationJob.set(priority: 4, queue: "custom").perform_later("low")
      expect(rows.first).to have_attributes(priority: 4, queue: "custom")
    end

    it "lets Active Job own retries without multiplying the retry budget" do
      first = IntegrationRetryJob.perform_later
      work_until do
        # Promote the due Active Job retry without waiting for the maintenance
        # loop's five-second tick. Both attempts still run in real workers.
        @driver.job_schedule
        rows.length == 2 && rows.any? { |row| row.state == "discarded" }
      end

      expect(rows.map(&:state)).to eq(%w[completed discarded])
      expect(rows.map { |row| row.args.fetch("job").fetch("job_id") }.uniq).to eq([first.job_id])
      expect(rows.first.metadata.fetch("active_job_outcome")).to eq("retried")
      expect(IntegrationJob.seen.size).to eq(2)
    end

    it "records handled discard outcomes" do
      IntegrationDiscardJob.perform_later
      work_until { rows.first.state == "completed" }

      expect(rows.first.metadata.fetch("active_job_outcome")).to eq("discarded")
    end

    it "discards unhandled errors immediately" do
      IntegrationBrokenJob.perform_later
      work_until { rows.first.state == "discarded" }

      expect(rows.first).to have_attributes(attempt: 1, state: "discarded")
    end

    it "discards errors reported for externally claimed Active Jobs" do
      IntegrationBrokenJob.perform_later
      client = Rails.application.config.river.build_client
      claimed = @driver.job_claim(id: rows.first.id, attempted_by: "external")

      client.__finish_claimed_job(claimed, RuntimeError.new("external work failed"))

      expect(rows).to contain_exactly(have_attributes(
        attempt: 1, errors: contain_exactly(have_attributes(error: "external work failed")), state: "discarded"
      ))
    end

    it "recovers a claimed job after a simulated crash" do
      IntegrationJob.perform_later("rescue")
      @driver.job_get_available(attempted_by: "crashed", max: 1, queue: "default")
      @driver.job_rescue_stuck(horizon: Time.now.utc + 1, retry_policy: River::DefaultClientRetryPolicy.new)

      expect(rows.first).to have_attributes(attempt: 1, state: "retryable")
    end

    it "interrupts work without triggering a broad Active Job retry handler" do
      IntegrationInterruptibleJob.perform_later("wait")
      @consumer = Rails.application.config.river.build_client.start
      wait_until { !IntegrationJob.seen.empty? }
      @consumer.stop_and_cancel

      expect(rows.length).to eq(1)
      expect(rows.first).to have_attributes(attempt: 0, state: "available")
    end

    it "does not turn River cancellation into an Active Job retry" do
      IntegrationInterruptibleJob.perform_later("cancel")
      work_until { rows.first.state == "cancelled" }

      expect(rows.length).to eq(1)
    end

    it "does not turn River snoozing into an Active Job retry" do
      IntegrationInterruptibleJob.perform_later("snooze")
      work_until { rows.first.state == "scheduled" }

      expect(rows.length).to eq(1)
      expect(rows.first.attempt).to eq(0)
    end

    it "deserializes GlobalIDs through Active Job" do
      ActiveRecord::Base.connection.create_table(:integration_records) { |table| table.string :name }
      IntegrationRecord.reset_column_information
      record = IntegrationRecord.create!(name: "customer")
      IntegrationJob.perform_later(record)
      work_until { rows.first.state == "completed" }

      expect(IntegrationJob.seen.pop.first).to eq(record)
    end

    it "discards missing GlobalIDs without an extra backend retry cycle" do
      ActiveRecord::Base.connection.create_table(:integration_records) { |table| table.string :name }
      IntegrationRecord.reset_column_information
      record = IntegrationRecord.create!(name: "deleted")
      IntegrationJob.perform_later(record)
      record.destroy!
      work_until { rows.first.state == "discarded" }

      expect(rows.first).to have_attributes(attempt: 1, state: "discarded")
    end

    it "delivers Action Mailer jobs" do
      IntegrationMailer.receipt.deliver_later
      work_until { rows.first.state == "completed" }

      expect(ActionMailer::Base.deliveries.map(&:subject)).to eq(["Receipt"])
    end

    it "rejects unknown envelope versions" do
      client = Rails.application.config.river.client
      client.insert(River::JobArgsHash.new("active_job", {"job" => {}, "version" => 999}))
      work_until { rows.first.state == "discarded" }

      expect(rows.first.errors.last.error).to include("Unsupported Active Job envelope")
    end
  end
end
