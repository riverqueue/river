# frozen_string_literal: true

require "riverqueue/testing"

# A worker definition loaded by the main Ractor and instantiated by each runtime.
class RactorTestWorker
  def self.kind = "ractor_test"

  def work(job) = job.output = job.args.fetch("value") * 2
end

# Deliberately small, Ractor-local stand-in for database I/O. This lets the real
# client/runtime run without ORM globals, native drivers, or RSpec mocks.
class RactorTestDriver
  def initialize
    @rows = {}
    @mutex = Mutex.new
  end

  def job_insert(params)
    @mutex.synchronize do
      row = River::JobRow.new(
        id: @rows.length + 1, args: JSON.parse(params.encoded_args), attempt: 0,
        created_at: Time.now.utc, kind: params.kind, max_attempts: params.max_attempts,
        metadata: params.metadata.dup, priority: params.priority, queue: params.queue,
        scheduled_at: params.scheduled_at, state: params.state, tags: params.tags,
        unique_key: params.unique_key, unique_states: params.unique_states
      )
      @rows[row.id] = row
      [row, false]
    end
  end

  def job_insert_many(params) = params.map { |param| job_insert(param) }

  def job_get_by_id(id) = @mutex.synchronize { @rows.fetch(id) }

  def job_claim(id:, attempted_by:, allow_scheduled: false)
    @mutex.synchronize do
      row = @rows.fetch(id)
      row.state = River::JOB_STATE_RUNNING
      row.attempt += 1
      row.attempted_by = [attempted_by]
      row
    end
  end

  def job_set_state_if_running(id:, now: nil, error: nil, metadata: nil, **attributes)
    @mutex.synchronize do
      row = @rows.fetch(id)
      attributes.each { |name, value| row.public_send(:"#{name}=", value) }
      row.errors = Array(row.errors) + [error] if error
      row.metadata.merge!(metadata) if metadata
      row
    end
  end

  def job_complete(**attributes)
    job_set_state_if_running(**attributes, state: River::JOB_STATE_COMPLETED)
  end

  def job_metadata_merge(id, metadata)
    @mutex.synchronize do
      @rows.fetch(id).tap { |row| row.metadata.merge!(metadata) }
    end
  end

  def job_get_available(queue:, max:, attempted_by:)
    ids = @mutex.synchronize do
      @rows.values.select { |row| row.queue == queue && row.state == River::JOB_STATE_AVAILABLE }.first(max).map(&:id)
    end
    ids.map { |id| job_claim(id: id, attempted_by: attempted_by) }
  end

  def job_get_cancelled_ids(_ids) = []

  def queue_get(_name) = nil

  def queue_upsert(_name) = nil

  def leader_acquire(_id, now:) = true

  def leader_renew(_id, now:) = true

  def leader_release(_id) = nil

  def job_schedule(now:) = nil

  def job_rescue_stuck(horizon:, now:, retry_policy:) = nil

  def job_delete_finalized(now:, retention:) = nil
end
