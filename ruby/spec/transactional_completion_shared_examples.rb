# frozen_string_literal: true

require "riverqueue/testing"

RSpec.shared_examples "transactional job completion" do
  let(:completion_plugins) { [] }
  let(:completion_worker) { Object.new }
  let(:completion_client) do
    River::Client.new(@driver, config: River::Config.new(
      plugins: completion_plugins, workers: River::Workers.new.add(:transactional, completion_worker)
    ))
  end

  def claim_completion_job
    row = insert_completion_job
    River::Job.new(completion_client, @driver.job_claim(id: row.id, attempted_by: "test"))
  end

  # Open an application transaction directly, rather than using River's driver
  # transaction wrapper, to exercise the public Active Record and Sequel APIs.
  def completion_transaction(&block)
    if @driver.respond_to?(:connection_class)
      @driver.connection_class.transaction(requires_new: true, &block)
    else
      @driver.instance_variable_get(:@db).transaction(savepoint: true, &block)
    end
  end

  def insert_completion_job
    completion_client.insert(River::JobArgsHash.new(:transactional, {}), metadata: {"existing" => true}).job
  end

  it "commits completion, pending metadata, and other writes in the caller's transaction" do
    events = completion_client.subscribe(:job_completed, :job_failed)
    transaction = method(:completion_transaction)
    check_events = -> { expect { events.pop(true) }.to raise_error(ThreadError) }
    snapshots = []
    completion_worker.define_singleton_method(:work) do |job|
      job.output = {"result" => 42}
      transaction.call do
        job.client.insert(River::JobArgsHash.new(:followup, {}))
        snapshots << job.client.job_complete_tx(job)
        snapshots << job.row
        check_events.call
      end
    end
    row = insert_completion_job

    result = River::Testing.perform_job(completion_client, row.id)

    expect(result).to have_attributes(error: nil, outcome: :completed)
    expect(result.job).to have_attributes(
      finalized_at: be_a(Time), metadata: include("existing" => true, "output" => {"result" => 42}), state: "completed"
    )
    expect(snapshots[0]).to have_attributes(finalized_at: result.job.finalized_at, state: "completed")
    expect(snapshots[1]).to have_attributes(finalized_at: nil, state: "running")
    expect(completion_client.job_list(kinds: [:followup]).jobs.length).to eq(1)
    expect(events.pop(true)).to have_attributes(kind: :job_completed, job: have_attributes(id: row.id, state: "completed"))
    expect { events.pop(true) }.to raise_error(ThreadError)
  ensure
    events&.close
  end

  it "rolls back completion and other writes and retries a failed attempt" do
    events = completion_client.subscribe(:job_completed, :job_failed)
    transaction = method(:completion_transaction)
    completion_worker.define_singleton_method(:work) do |job|
      transaction.call do
        job.client.insert(River::JobArgsHash.new(:followup, {}))
        job.client.job_complete_tx(job)
        raise "roll back the work"
      end
    end
    row = insert_completion_job

    result = River::Testing.perform_job(completion_client, row.id)

    expect(result).to have_attributes(error: have_attributes(message: "roll back the work"), outcome: :retried)
    expect(result.job).to have_attributes(finalized_at: nil, errors: contain_exactly(have_attributes(error: "roll back the work")))
    expect(result.job.state).not_to eq("completed")
    expect(completion_client.job_list(kinds: [:followup]).jobs).to be_empty
    expect(events.pop(true).kind).to eq(:job_failed)
    expect { events.pop(true) }.to raise_error(ThreadError)
  ensure
    events&.close
  end

  [RuntimeError.new("after commit"), River::JobCancelError.new, River::JobSnoozeError.new(60), River::ClientRuntime::Interrupted.new].each do |error|
    it "keeps a committed completion when the worker raises #{error.class}" do
      events = completion_client.subscribe(:job_completed, :job_failed, :job_cancelled, :job_snoozed, :job_interrupted)
      transaction = method(:completion_transaction)
      completion_worker.define_singleton_method(:work) do |job|
        transaction.call { job.client.job_complete_tx(job) }
        raise error
      end
      row = insert_completion_job

      result = River::Testing.perform_job(completion_client, row.id)

      expect(result).to have_attributes(error: nil, outcome: :completed)
      expect(result.job).to have_attributes(attempt: 1, state: "completed")
      expect(Array(result.job.errors)).to be_empty
      expect(events.pop(true)).to have_attributes(kind: :job_completed, job: have_attributes(id: row.id, state: "completed"))
      expect { events.pop(true) }.to raise_error(ThreadError)
    ensure
      events&.close
    end
  end

  it "allows normal runtime completion after the caller deliberately rolls back" do
    transaction = method(:completion_transaction)
    completion_worker.define_singleton_method(:work) do |job|
      transaction.call do
        job.client.job_complete_tx(job)
        raise job.client.driver.rollback_exception
      end
      job.output = "after rollback"
    end

    result = River::Testing.perform_job(completion_client, insert_completion_job.id)

    expect(result).to have_attributes(error: nil, outcome: :completed)
    expect(result.job).to have_attributes(metadata: include("output" => "after rollback"), state: "completed")
  end

  it "honors cancellation and rolls back the caller's other writes" do
    transaction = method(:completion_transaction)
    completion_worker.define_singleton_method(:work) do |job|
      job.client.job_cancel(job.id)
      transaction.call do
        job.client.insert(River::JobArgsHash.new(:followup, {}))
        job.client.job_complete_tx(job)
      end
    end

    result = River::Testing.perform_job(completion_client, insert_completion_job.id)

    expect(result).to have_attributes(error: be_a(River::JobCancelError), outcome: :cancelled)
    expect(result.job.state).to eq("cancelled")
    expect(completion_client.job_list(kinds: [:followup]).jobs).to be_empty
  end

  it "returns an already committed completion without changing its timestamp or metadata" do
    job = claim_completion_job
    first = completion_transaction { completion_client.job_complete_tx(job) }
    job.output = "too late"

    repeated = completion_transaction { completion_client.job_complete_tx(job) }

    expect(repeated).to have_attributes(finalized_at: first.finalized_at, metadata: first.metadata, state: "completed")
  end

  it "rejects jobs that are not running before writing" do
    job = River::Job.new(completion_client, insert_completion_job)

    expect { completion_client.job_complete_tx(job) }.to raise_error(River::Error, "job must be running")
    expect(completion_client.job_get(job.id).state).to eq("available")
  end

  it "rejects stale running jobs whose persisted state changed" do
    job = claim_completion_job
    completion_client.job_update(job.id, state: :scheduled)

    expect { completion_transaction { completion_client.job_complete_tx(job) } }.to raise_error(River::Error, "job must be running")
    expect(completion_client.job_get(job.id).state).to eq("scheduled")
  end

  %w[running completed cancelled].product([false, true]).each do |state, reuse_attempt|
    it "rejects a stale attempt when its replacement is #{state} with reuse_attempt=#{reuse_attempt}" do
      job = claim_completion_job
      @driver.job_set_state_if_running(id: job.id, state: "available")
      @driver.job_claim(id: job.id, attempted_by: "replacement", now: job.attempted_at + 1)
      completion_client.job_update(job.id, attempt: job.attempt) if reuse_attempt
      @driver.job_complete(id: job.id, finalized_at: Time.now.utc) if state == "completed"
      completion_client.job_cancel(job.id) if state == "cancelled"
      replacement = completion_client.job_get(job.id)
      job.output = "stale output"

      expect do
        completion_transaction do
          completion_client.insert(River::JobArgsHash.new(:followup, {}))
          completion_client.job_complete_tx(job)
        end
      end.to raise_error(River::Error, "job attempt is no longer current")

      expect(completion_client.job_get(job.id)).to have_attributes(
        attempt: replacement.attempt, attempted_at: replacement.attempted_at,
        finalized_at: replacement.finalized_at, metadata: replacement.metadata, state: replacement.state
      )
      expect(completion_client.job_list(kinds: [:followup]).jobs).to be_empty
    end
  end

  it "checks attempt ownership in the completion write itself" do
    job = claim_completion_job
    replacement = nil
    @driver.define_singleton_method(:job_complete) do |**options|
      job_set_state_if_running(id: job.id, state: "available")
      replacement = job_claim(id: job.id, attempted_by: "replacement", now: job.attempted_at + 1)
      super(**options)
    end

    completion_transaction do
      expect { completion_client.job_complete_tx(job) }.to raise_error(River::Error, "job attempt is no longer current")
    end

    expect(completion_client.job_get(job.id)).to have_attributes(
      attempt: replacement.attempt, attempted_at: replacement.attempted_at, finalized_at: nil, state: "running"
    )
  end

  it "rejects jobs belonging to another client" do
    job = claim_completion_job

    expect { River::Client.new(@driver).job_complete_tx(job) }.to raise_error(ArgumentError, "job must belong to this client")
    expect(completion_client.job_get(job.id).state).to eq("running")
  end

  it "rejects job rows and IDs instead of a running Job" do
    row = insert_completion_job

    [row, row.id].each do |argument|
      expect { completion_client.job_complete_tx(argument) }.to raise_error(ArgumentError, "job must be a River::Job")
    end
  end

  it "rejects completion outside a transaction without changing the job" do
    job = claim_completion_job
    job.output = "not saved"

    expect { completion_client.job_complete_tx(job) }.to raise_error(
      River::Error, "job_complete_tx requires an active transaction on the driver's connection"
    )

    expect(completion_client.job_get(job.id)).to have_attributes(
      finalized_at: nil, metadata: job.row.metadata, state: "running"
    )
  end

  it "reports a missing job without publishing a completion event" do
    transaction = method(:completion_transaction)
    completion_worker.define_singleton_method(:work) do |job|
      job.client.driver.job_delete_if_running(job.id)
      transaction.call { job.client.job_complete_tx(job) }
    end
    events = completion_client.subscribe(:job_completed)
    row = insert_completion_job

    result = River::Testing.perform_job(completion_client, row.id)

    expect(result.error).to be_a(River::NotFoundError)
    expect { events.pop(true) }.to raise_error(ThreadError)
  ensure
    events&.close
  end

  it "reports the committed snapshot if the job is deleted before saving later metadata" do
    transaction = method(:completion_transaction)
    completion_worker.define_singleton_method(:work) do |job|
      job.output = 42
      transaction.call { job.client.job_complete_tx(job) }
    end
    @driver.define_singleton_method(:job_metadata_merge) do |id, metadata, expected_job = nil|
      job_delete(id)
      super(id, metadata, expected_job)
    end
    events = completion_client.subscribe(:job_completed)
    row = insert_completion_job

    result = River::Testing.perform_job(completion_client, row.id)

    expect(result).to have_attributes(error: nil, job: nil, outcome: :completed)
    expect(events.pop(true).job).to have_attributes(id: row.id, metadata: include("output" => 42), state: "completed")
  ensure
    events&.close
  end

  context "when the completed job is retried" do
    let(:finalize_calls) { [] }
    let(:finalize_result) { nil }
    let(:completion_plugins) do
      calls = finalize_calls
      result = finalize_result
      finalizer = Object.new
      finalizer.define_singleton_method(:job_finalize) do |*args|
        calls << args
        result
      end
      [finalizer]
    end

    def retry_completed_job(id, state)
      completion_client.job_retry(id)
      unless state == "available"
        @driver.job_claim(id: id, attempted_by: "replacement", now: Time.now.utc + 1)
        @driver.job_complete(id: id, finalized_at: Time.now.utc) if state == "completed"
      end
      @driver.job_metadata_merge(id, {"output" => "replacement"})
    end

    [nil, :delete].each do |finalize_result|
      context "with finalizer returning #{finalize_result.inspect}" do
        let(:finalize_result) { finalize_result }

        [nil, RuntimeError.new("after retry"), River::JobCancelError.new, River::JobSnoozeError.new(60), River::ClientRuntime::Interrupted.new].product(%w[available running completed]).each do |error, state|
          it "preserves the #{state} replacement when the old worker finishes with #{error.class}" do
            transaction = method(:completion_transaction)
            retry_job = method(:retry_completed_job)
            replacement = nil
            completion_worker.define_singleton_method(:work) do |job|
              transaction.call { job.client.job_complete_tx(job) }
              replacement = retry_job.call(job.id, state)
              job.output = "old attempt"
              raise error if error
            end
            events = completion_client.subscribe(:job_completed, :job_failed, :job_cancelled, :job_snoozed, :job_interrupted)

            result = River::Testing.perform_job(completion_client, insert_completion_job.id)

            expect(result).to have_attributes(error: nil, outcome: :completed)
            expect(result.job).to have_attributes(
              attempt: replacement.attempt, attempted_at: replacement.attempted_at, errors: replacement.errors,
              finalized_at: replacement.finalized_at, metadata: replacement.metadata,
              scheduled_at: replacement.scheduled_at, state: replacement.state
            )
            expect(finalize_calls).to be_empty
            expect { events.pop(true) }.to raise_error(ThreadError)
          ensure
            events&.close
          end
        end
      end
    end

    it "recognizes a replacement even when its attempt counter matches" do
      transaction = method(:completion_transaction)
      retry_job = method(:retry_completed_job)
      replacement = nil
      completion_worker.define_singleton_method(:work) do |job|
        transaction.call { job.client.job_complete_tx(job) }
        retry_job.call(job.id, "running")
        replacement = job.client.job_update(job.id, attempt: job.attempt)
        job.output = "old attempt"
      end

      result = River::Testing.perform_job(completion_client, insert_completion_job.id)

      expect(result).to have_attributes(error: nil, outcome: :completed)
      expect(result.job).to have_attributes(
        attempt: replacement.attempt, attempted_at: replacement.attempted_at, errors: replacement.errors,
        finalized_at: replacement.finalized_at, metadata: replacement.metadata,
        scheduled_at: replacement.scheduled_at, state: replacement.state
      )
      expect(finalize_calls).to be_empty
    end

    %w[available running completed].each do |state|
      it "preserves a #{state} replacement created before the metadata write" do
        transaction = method(:completion_transaction)
        committed = nil
        completion_worker.define_singleton_method(:work) do |job|
          job.output = "committed"
          committed = transaction.call { job.client.job_complete_tx(job) }
          job.output = "too late"
        end
        retry_job = method(:retry_completed_job)
        replacement = nil
        @driver.define_singleton_method(:job_metadata_merge) do |id, metadata, expected_job = nil|
          replacement = retry_job.call(id, state) if expected_job
          super(id, metadata, expected_job)
        end
        events = completion_client.subscribe(:job_completed)

        result = River::Testing.perform_job(completion_client, insert_completion_job.id)

        expect(result).to have_attributes(error: nil, outcome: :completed)
        expect(result.job).to have_attributes(
          attempt: replacement.attempt, attempted_at: replacement.attempted_at, errors: replacement.errors,
          finalized_at: replacement.finalized_at, metadata: replacement.metadata,
          scheduled_at: replacement.scheduled_at, state: replacement.state
        )
        expect(events.pop(true).job).to have_attributes(
          attempt: committed.attempt, finalized_at: committed.finalized_at,
          metadata: committed.metadata, state: "completed"
        )
        expect { events.pop(true) }.to raise_error(ThreadError)
        expect(finalize_calls).to be_empty
      ensure
        events&.close
      end
    end
  end

  context "with work middleware and finalization hooks" do
    let(:finalize_calls) { [] }
    let(:completion_plugins) do
      calls = finalize_calls
      finalizer = Object.new
      finalizer.define_singleton_method(:job_finalize) do |*args|
        calls << args
        :delete
      end
      [River::JobPersistedLogging::Plugin.new, finalizer]
    end

    [false, true].product([:delete, :retention]).each do |raise_after_commit, deletion|
      it "reports committed completion after #{deletion} with raise_after_commit=#{raise_after_commit}" do
        transaction = method(:completion_transaction)
        committed = nil
        completion_worker.define_singleton_method(:work) do |job|
          job.output = "committed"
          committed = transaction.call { job.client.job_complete_tx(job) }
          if deletion == :delete
            job.client.job_delete(job.id)
          else
            job.client.driver.job_delete_finalized(retention: {"completed" => 0}, now: Time.now.utc + 1)
          end
          job.output = "after deletion"
          raise "after commit" if raise_after_commit
        end
        events = completion_client.subscribe(:job_completed, :job_failed)

        result = River::Testing.perform_job(completion_client, insert_completion_job.id)

        expect(result).to have_attributes(error: nil, job: nil, outcome: :completed)
        expect(events.pop(true)).to have_attributes(
          kind: :job_completed, job: have_attributes(
            attempt: committed.attempt, finalized_at: committed.finalized_at,
            metadata: committed.metadata, state: "completed"
          )
        )
        expect { events.pop(true) }.to raise_error(ThreadError)
        expect(finalize_calls).to be_empty
      ensure
        events&.close
      end
    end

    [:outer, :savepoint].each do |rollback|
      it "does not retain a snapshot after a #{rollback} rollback followed by deletion" do
        transaction = method(:completion_transaction)
        completion_worker.define_singleton_method(:work) do |job|
          transaction.call do
            transaction.call do
              job.client.job_complete_tx(job)
              raise job.client.driver.rollback_exception if rollback == :savepoint
            end
            raise job.client.driver.rollback_exception if rollback == :outer
          end
          job.client.driver.job_delete_if_running(job.id)
          raise "after rollback"
        end
        events = completion_client.subscribe(:job_completed)

        result = River::Testing.perform_job(completion_client, insert_completion_job.id)

        expect(result).to have_attributes(error: have_attributes(message: "after rollback"), job: nil, outcome: :retried)
        expect { events.pop(true) }.to raise_error(ThreadError)
      ensure
        events&.close
      end
    end

    [false, true].each do |raise_after_commit|
      it "preserves logs and skips normal finalization hooks with raise_after_commit=#{raise_after_commit}" do
        transaction = method(:completion_transaction)
        completion_worker.define_singleton_method(:work) do |job|
          job.logger << "before commit\n"
          transaction.call { job.client.job_complete_tx(job) }
          job.logger << "after commit\n"
          raise "after commit" if raise_after_commit
        end
        events = completion_client.subscribe(:job_completed)

        result = River::Testing.perform_job(completion_client, insert_completion_job.id)

        expect(result).to have_attributes(error: nil, outcome: :completed)
        expect(result.job.metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "before commit\nafter commit\n"}])
        expect(events.pop(true).job.metadata).to eq(result.job.metadata)
        expect(finalize_calls).to be_empty
      ensure
        events&.close
      end
    end
  end
end
