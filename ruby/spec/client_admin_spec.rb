# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"

class AdminArgs
  def initialize(value)
    @value = value
  end

  def kind = "admin"

  def to_json = JSON.dump(value: @value)
end

RSpec.describe "River client job administration" do
  around(:each) { |example| available_test_transaction(&example) }

  let(:driver) { River::Driver::Sequel.new(available_test_database) }
  let(:client) { River::Client.new(driver) }

  def insert_job(value = 1, **options)
    client.insert(AdminArgs.new(value), insert_opts: River::InsertOpts.new(**options)).job
  end

  def claim(job)
    driver.job_get_available(attempted_by: "worker-test", max: 1, queue: job.queue).first
  end

  it "gets an inserted job" do
    inserted = insert_job

    expect(client.job_get(inserted.id)).to have_attributes(id: inserted.id, args: {"value" => 1})
  end

  it "raises when getting an unknown job" do
    expect { client.job_get(-1) }.to raise_error(River::NotFoundError, "job not found: -1")
  end

  it "updates mutable job fields" do
    inserted = insert_job
    attempted_at = Time.now.utc - 10
    error = River::AttemptError.new(at: attempted_at, attempt: 2, error: "failed", trace: "trace")

    updated = client.job_update(inserted.id, River::JobUpdateParams.new(
      attempt: 2,
      attempted_at: attempted_at,
      attempted_by: ["worker-one"],
      errors: [error],
      max_attempts: 5,
      metadata: {"tenant" => "one"},
      state: River::JOB_STATE_RETRYABLE
    ))

    expect(updated).to have_attributes(
      attempt: 2,
      attempted_at: be_within(0.001).of(attempted_at),
      attempted_by: ["worker-one"],
      max_attempts: 5,
      metadata: {"tenant" => "one"},
      state: River::JOB_STATE_RETRYABLE
    )
    expect(updated.errors.first).to have_attributes(attempt: 2, error: "failed")
  end

  it "returns the unchanged job for an empty update" do
    inserted = insert_job

    expect(client.job_update(inserted.id, River::JobUpdateParams.new)).to have_attributes(id: inserted.id)
  end

  it "raises when updating an unknown job" do
    expect { client.job_update(-1, River::JobUpdateParams.new(attempt: 1)) }
      .to raise_error(River::NotFoundError, "job not found: -1")
  end

  it "cancels an available job immediately" do
    cancelled = client.job_cancel(insert_job.id)

    expect(cancelled).to have_attributes(
      finalized_at: be_a(Time),
      metadata: include("cancel_attempted_at"),
      state: River::JOB_STATE_CANCELLED
    )
  end

  it "marks a running job for remote cancellation without finalizing it" do
    running = claim(insert_job)
    cancelled = client.job_cancel(running.id)

    expect(cancelled).to have_attributes(finalized_at: nil, state: River::JOB_STATE_RUNNING)
    expect(cancelled.metadata).to include("cancel_attempted_at")
  end

  it "leaves an already completed job unchanged when cancellation is requested" do
    job = insert_job
    running = claim(job)
    driver.job_set_state_if_running(id: running.id, finalized_at: Time.now.utc, state: River::JOB_STATE_COMPLETED)

    expect(client.job_cancel(job.id)).to have_attributes(state: River::JOB_STATE_COMPLETED)
  end

  it "raises when cancelling an unknown job" do
    expect { client.job_cancel(-1) }.to raise_error(River::NotFoundError, "job not found: -1")
  end

  it "deletes a non-running job" do
    inserted = insert_job

    expect(client.job_delete(inserted.id)).to have_attributes(id: inserted.id)
    expect { client.job_get(inserted.id) }.to raise_error(River::NotFoundError)
  end

  it "refuses to delete a running job" do
    running = claim(insert_job)

    expect { client.job_delete(running.id) }
      .to raise_error(River::JobRunningError, "running jobs cannot be deleted")
    expect(client.job_get(running.id)).to have_attributes(state: River::JOB_STATE_RUNNING)
  end

  it "raises when deleting an unknown job" do
    expect { client.job_delete(-1) }.to raise_error(River::NotFoundError, "job not found: -1")
  end

  it "deletes matching jobs in bulk while preserving running jobs" do
    first = insert_job(1)
    running = claim(first)
    second = insert_job(2)

    result = client.job_delete_many(River::JobListParams.new(kinds: ["admin"]))

    expect(result.jobs.map(&:id)).to eq([second.id])
    expect(client.job_get(running.id)).to have_attributes(state: River::JOB_STATE_RUNNING)
    expect { client.job_get(second.id) }.to raise_error(River::NotFoundError)
  end

  it "requires a filter for bulk deletion" do
    expect { client.job_delete_many(River::JobListParams.new) }
      .to raise_error(ArgumentError, "delete with no filters is not allowed")
    expect { client.job_delete_many(nil) }
      .to raise_error(ArgumentError, "delete with no filters is not allowed")
  end

  it "retries a finalized job and increases an exhausted max-attempt count" do
    inserted = insert_job
    client.job_update(inserted.id, River::JobUpdateParams.new(
      attempt: 3,
      finalized_at: Time.now.utc,
      max_attempts: 3,
      state: River::JOB_STATE_DISCARDED
    ))

    retried = client.job_retry(inserted.id)

    expect(retried).to have_attributes(attempt: 3, finalized_at: nil, max_attempts: 4, state: River::JOB_STATE_AVAILABLE)
  end

  it "does not retry a running job" do
    running = claim(insert_job)

    expect(client.job_retry(running.id)).to have_attributes(state: River::JOB_STATE_RUNNING)
  end

  it "raises when retrying an unknown job" do
    expect { client.job_retry(-1) }.to raise_error(River::NotFoundError, "job not found: -1")
  end

  it "returns jobs with a pagination cursor" do
    first = insert_job(1)
    second = insert_job(2)

    page = client.job_list(River::JobListParams.new(limit: 1))
    next_page = client.job_list(River::JobListParams.new(after: page.last_cursor))

    expect(page.jobs.map(&:id)).to eq([first.id])
    expect(page.last_cursor).to have_attributes(id: first.id, sort_by: :id, sort_order: :asc, value: first.id)
    expect(next_page.jobs.map(&:id)).to eq([second.id])
  end

  it "returns a nil cursor for an empty job list" do
    result = client.job_list

    expect(result).to have_attributes(
      jobs: be_empty,
      last_cursor: be_nil
    )
  end
end

RSpec.describe "River client queue administration" do
  around(:each) { |example| available_test_transaction(&example) }

  let(:driver) { River::Driver::Sequel.new(available_test_database) }
  let(:client) { River::Client.new(driver) }

  it "gets, lists, and updates queues" do
    driver.queue_upsert("beta")
    driver.queue_upsert("alpha")

    expect(client.queue_get("alpha")).to have_attributes(metadata: {}, name: "alpha")
    expect(client.queue_list(max: 1).queues.map(&:name)).to eq(["alpha"])
    expect(client.queue_update("alpha", metadata: {"team" => "ruby"}).metadata).to eq("team" => "ruby")
  end

  it "raises for missing queue lookups and updates" do
    expect { client.queue_get("missing") }.to raise_error(River::NotFoundError, "queue not found: missing")
    expect { client.queue_update("missing", metadata: {}) }
      .to raise_error(River::NotFoundError, "queue not found: missing")
  end

  it "pauses and resumes a named queue and publishes events" do
    driver.queue_upsert("one")
    subscription = client.subscribe(River::EVENT_QUEUE_PAUSED, River::EVENT_QUEUE_RESUMED)

    expect(client.queue_pause("one")).to be true
    expect(client.queue_get("one").paused_at).to be_a(Time)
    expect(subscription.pop(true)).to have_attributes(kind: River::EVENT_QUEUE_PAUSED, queue: have_attributes(name: "one"))

    expect(client.queue_resume("one")).to be true
    expect(client.queue_get("one").paused_at).to be_nil
    expect(subscription.pop(true)).to have_attributes(kind: River::EVENT_QUEUE_RESUMED, queue: have_attributes(name: "one"))
  end

  it "pauses and resumes all queues" do
    driver.queue_upsert("one")
    driver.queue_upsert("two")

    client.queue_pause("*")

    expect(client.queue_list.queues).to all(have_attributes(paused_at: be_a(Time)))
    client.queue_resume("*")

    expect(client.queue_list.queues).to all(have_attributes(paused_at: nil))
  end

  it "treats pausing an unknown queue as an idempotent operation" do
    subscription = client.subscribe(River::EVENT_QUEUE_PAUSED)

    expect(client.queue_pause("missing")).to be true
    expect { subscription.pop(true) }.to raise_error(ThreadError)
  end
end
