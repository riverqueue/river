# frozen_string_literal: true

require "timeout"
require "riverqueue/testing"
require_relative "runtime_draining_shared_examples"
require_relative "runtime_finishing_shared_examples"
require_relative "transactional_completion_shared_examples"

RSpec.shared_examples "Postgres state update races" do
  it "skips a concurrent retry when selecting jobs for bulk deletion" do
    client = River::Client.new(@driver)
    first, second = 2.times.map do
      row = client.insert(River::JobArgsHash.new(:race, {})).job
      client.job_update(row.id, finalized_at: Time.now.utc, state: "discarded")
    end
    locked, release = Queue.new, Queue.new
    retrier = Thread.new do
      @driver.transaction do
        client.job_retry(first.id)
        locked << true
        release.pop
      end
    end
    Timeout.timeout(5) { locked.pop }
    deleter = Thread.new do
      @driver.transaction do
        @driver.send(:runtime_execute, "SET LOCAL statement_timeout = '5s'")
        client.job_delete_many(states: ["discarded"], limit: 1).jobs
      end
    end

    expect(Timeout.timeout(3) { deleter.value }).to contain_exactly(have_attributes(id: second.id))
    release << true
    retrier.value
    expect(client.job_get(first.id).state).to eq("available")
  ensure
    release << true if release
    retrier&.join
    deleter&.join
  end

  [:job_complete, :job_delete_if_running, :job_discard].each do |operation|
    it "honors a cancellation committed while #{operation} waits for the row lock" do
      client = River::Client.new(@driver)
      row = client.insert(River::JobArgsHash.new(:race, {})).job
      @driver.job_claim(id: row.id, attempted_by: "test")
      locked, release, started = Queue.new, Queue.new, Queue.new
      canceller = Thread.new do
        @driver.transaction do
          client.job_cancel(row.id)
          locked << true
          release.pop
        end
      end
      Timeout.timeout(5) { locked.pop }
      completer = Thread.new do
        @driver.transaction do
          @driver.send(:runtime_execute, "SET LOCAL statement_timeout = '5s'")
          pid = @driver.send(:runtime_query_rows, "SELECT pg_backend_pid() AS pid").first.values.first
          started << pid
          case operation
          when :job_complete
            @driver.job_complete(id: row.id, finalized_at: Time.now.utc)
          when :job_delete_if_running
            @driver.job_delete_if_running(row.id)
          else
            @driver.job_set_state_if_running(id: row.id, finalized_at: Time.now.utc, state: "discarded")
          end
        end
      end
      pid = Timeout.timeout(5) { started.pop }
      Timeout.timeout(5) do
        loop do
          blockers = @driver.send(:runtime_query_rows, "SELECT cardinality(pg_blocking_pids(#{Integer(pid)})) AS count").first.values.first
          break if blockers.positive?

          Thread.pass
        end
      end
      release << true

      result = Timeout.timeout(5) { completer.value }
      if operation == :job_discard
        expect(result).to have_attributes(state: "cancelled", finalized_at: be_a(Time))
      else
        expect(result).to eq(:cancelled)
      end
      expect(client.job_get(row.id)).to have_attributes(
        state: (operation == :job_discard) ? "cancelled" : "running", metadata: include("cancel_attempted_at")
      )
    ensure
      release << true if release
      canceller&.join
      completer&.join
    end
  end

  [:job_cancel, :job_retry].each do |operation|
    it "returns the committed row when #{operation} waits behind the same operation" do
      client = River::Client.new(@driver)
      row = client.insert(River::JobArgsHash.new(:race, {})).job
      client.job_cancel(row.id) if operation == :job_retry
      locked, release, started = Queue.new, Queue.new, Queue.new
      winner = Thread.new do
        @driver.transaction do
          updated = client.public_send(operation, row.id)
          locked << updated
          release.pop
          updated
        end
      end
      committed = Timeout.timeout(5) { locked.pop }
      loser = Thread.new do
        @driver.transaction do
          @driver.send(:runtime_execute, "SET LOCAL statement_timeout = '5s'")
          pid = @driver.send(:runtime_query_rows, "SELECT pg_backend_pid() AS pid").first.values.first
          started << pid
          client.public_send(operation, row.id)
        end
      end
      pid = Timeout.timeout(5) { started.pop }
      # Observe the actual lock wait; merely starting a thread doesn't establish
      # that its statement snapshot predates the winning transaction's commit.
      Timeout.timeout(5) do
        loop do
          blockers = @driver.send(:runtime_query_rows, "SELECT cardinality(pg_blocking_pids(#{Integer(pid)})) AS count").first.values.first
          break if blockers.positive?

          Thread.pass
        end
      end
      release << true
      actual = Timeout.timeout(5) { loser.value }
      expect(actual).to have_attributes(id: row.id, state: committed.state,
        finalized_at: committed.finalized_at, scheduled_at: committed.scheduled_at)
      expect(winner.value.state).to eq(committed.state)
    ensure
      release << true if release
      winner&.join
      loser&.join
    end
  end
end

RSpec.shared_examples "Postgres finalized job list plans" do
  it "uses the finalized-time index for single-state listings in both directions" do
    @driver.send(:runtime_execute, <<~SQL)
      INSERT INTO river_job (state, kind, args, finalized_at)
      SELECT (ARRAY['cancelled', 'completed', 'discarded'])[1 + n % 3]::river_job_state,
             'list_plan', '{}', now() + n * interval '1 millisecond'
      FROM generate_series(1, 10000) n
    SQL
    @driver.send(:runtime_execute, "ANALYZE river_job")
    plans = []
    original = @driver.method(:runtime_job_rows)
    @driver.define_singleton_method(:runtime_job_rows) do |suffix|
      plans << runtime_query_rows("EXPLAIN SELECT * FROM river_job #{suffix}").map { |row| row.values.join }.join("\n")
      original.call(suffix)
    end

    %w[cancelled completed discarded].product([:asc, :desc]).each do |state, order|
      jobs = @driver.job_list(River::JobListParams.new(states: [state], sort_by: :finalized_at, sort_order: order, limit: 10))
      expect(jobs.length).to eq(10)
      expect(plans.last).to include("Index Scan", "river_job_state_and_finalized_at_index")
    end
  end
end

RSpec.shared_examples "Postgres rescue concurrency" do
  [:complete, :reclaim].each do |change|
    it "preserves a concurrent #{change} and rescues another stuck job in the batch" do
      now = Time.now.utc
      client = River::Client.new(@driver)
      first, second = 2.times.map do
        row = client.insert(River::JobArgsHash.new("rescue_test", {}),
          insert_opts: River::InsertOpts.new(scheduled_at: now - 120, state: "available")).job
        @driver.job_claim(id: row.id, attempted_by: "old-worker", now: now - 120)
      end
      locked = Queue.new
      release = Queue.new
      writer = Thread.new do
        @driver.transaction do
          if change == :complete
            @driver.job_complete(id: first.id, finalized_at: now, metadata: {"output" => "done"}, now: now)
          else
            @driver.job_set_state_if_running(id: first.id, scheduled_at: now, state: "available", now: now)
            @driver.job_claim(id: first.id, attempted_by: "new-worker", now: now)
          end
          expected = @driver.send(:runtime_query_rows, "SELECT * FROM river_job WHERE id = #{first.id}")
          locked << expected
          release.pop(timeout: 5)
        end
      end

      expected = Timeout.timeout(5) { locked.pop }
      expect(@driver.job_rescue_stuck(horizon: now - 60, max: 1, now: now, retry_policy: River::DefaultClientRetryPolicy.new)).to eq(1)
      expect(@driver.job_get_by_id(second.id).state).to eq("retryable")
      release << true
      writer.value
      expect(@driver.send(:runtime_query_rows, "SELECT * FROM river_job WHERE id = #{first.id}")).to eq(expected)
      expect(@driver.job_rescue_stuck(horizon: now - 60, now: now, retry_policy: River::DefaultClientRetryPolicy.new)).to eq(0)
    ensure
      release << true if release
      writer&.join
    end
  end
end

RSpec.shared_examples "SQL scheduling concurrency" do
  it "skips locked jobs without overwriting a concurrent reschedule" do
    client = River::Client.new(@driver)
    now = Time.now.utc
    first, second = [now - 2, now - 1].map do |scheduled_at|
      client.insert(River::JobArgsHash.new("driver_e2e", {"value" => 1}),
        insert_opts: River::InsertOpts.new(scheduled_at: scheduled_at, state: "scheduled")).job
    end
    locked = Queue.new
    release = Queue.new
    writer = Thread.new do
      @driver.transaction do
        @driver.send(:runtime_execute, "UPDATE river_job SET scheduled_at = #{@driver.send(:runtime_time, now + 60)} WHERE id = #{first.id}")
        locked << true
        release.pop(timeout: 5)
      end
    end

    Timeout.timeout(5) { locked.pop }
    expect(@driver.job_schedule(now: now, max: 1)).to eq(1)
    expect(@driver.job_get_by_id(second.id).state).to eq("available")
    release << true
    writer.value
    expect(@driver.job_get_by_id(first.id)).to have_attributes(state: "scheduled", scheduled_at: be_within(0.001).of(now + 60))
    expect(@driver.job_schedule(now: now)).to eq(0)
  ensure
    release << true if release
    writer&.join
  end
end

RSpec.shared_examples "client driver end to end" do
  it_behaves_like "cancellation while draining"
  it_behaves_like "externally claimed job finalization"
  it_behaves_like "transactional job completion"
  [false, true].product([false, true]).each do |with_cursor, rollback|
    it "resumes after a #{rollback ? "rolled-back" : "committed"} #{with_cursor ? "cursor" : "step"} checkpoint" do
      received = []
      worker.define_method(:work) do |job|
        job.resumable_step(:prepare) {}
        operation = ->(cursor = nil) do
          received << cursor
          job.client.driver.transaction do
            job.client.insert(River::JobArgsHash.new("checkpoint_child", {})) unless cursor == 42
            with_cursor ? job.resumable_checkpoint(cursor: 42) : job.resumable_checkpoint
            raise "rollback checkpoint" if rollback && job.attempt == 1
          end
          raise "retry after commit" if job.attempt == 1
        end
        if with_cursor
          job.resumable_step_cursor(:import, default: 0, &operation)
        else
          job.resumable_step(:import, &operation)
        end
      end
      row = e2e_insert
      first = River::Testing.perform_job(client, row.id)
      expect(first.outcome).to eq(:retried)
      expect(first.job.metadata[River::RESUMABLE_STEP_METADATA_KEY]).to eq(rollback ? nil : "import")
      expect(client.job_list(River::JobListParams.new(kinds: ["checkpoint_child"])).jobs.length).to eq(rollback ? 0 : 1)

      client.job_retry row.id
      expect(River::Testing.perform_job(client, row.id).outcome).to eq(:completed)
      expected_cursors = if with_cursor
        [0, rollback ? 0 : 42]
      else
        rollback ? [nil, nil] : [nil]
      end
      expect(received).to eq(expected_cursors)
      expect(client.job_list(River::JobListParams.new(kinds: ["checkpoint_child"])).jobs.length).to eq(1)
    end
  end

  it "accepts symbolic identifiers for insertion, uniqueness, filtering, and updates" do
    states = %i[available pending running scheduled].freeze
    args_keys = [:account_id].freeze
    opts = River::InsertOpts.new(queue: :imports, state: :pending,
      unique_opts: River::UniqueOpts.new(by_args: args_keys, by_state: states, by_queue: true))
    first, second = client.insert_many([1, 2].map do |account_id|
      River::InsertManyParams.new(River::JobArgsHash.new(:import, {account_id: account_id}), insert_opts: opts)
    end).map(&:job)
    duplicate = client.insert(River::JobArgsHash.new("import", {account_id: 1, ignored: true}),
      insert_opts: River::InsertOpts.new(queue: "imports", state: "pending",
        unique_opts: River::UniqueOpts.new(by_args: ["account_id"], by_state: states.map(&:to_s), by_queue: true)))

    expect(first).to have_attributes(kind: "import", queue: "imports", state: "pending")
    expect(second.id).not_to eq(first.id)
    expect(duplicate.unique_skipped_as_duplicate?).to be true
    expect(duplicate.job.id).to eq(first.id)
    filters = {kinds: [:import].freeze, queues: [:imports].freeze, states: [:pending].freeze}
    expect(client.job_list(River::JobListParams.new(**filters)).jobs.map(&:id)).to eq([first.id, second.id])
    expect(client.job_update(first.id, River::JobUpdateParams.new(state: :available)).state).to eq("available")
    expect(client.job_delete_many(River::JobListParams.new(**filters)).jobs.map(&:id)).to eq([second.id])
    expect(opts.state).to eq(:pending)
  end

  it "accepts symbols in queue administration and publishes queue events" do
    @driver.queue_upsert("imports")
    subscription = client.subscribe(:queue_paused, :queue_resumed)

    expect(client.queue_get(:imports).name).to eq("imports")
    expect(client.queue_update(:imports, metadata: {team: "data"}).metadata).to eq("team" => "data")
    client.queue_pause :imports
    expect(client.queue_get(:imports).paused_at).to be_a(Time)
    expect(subscription.pop.kind).to eq(:queue_paused)
    client.queue_resume :imports
    expect(client.queue_get(:imports).paused_at).to be_nil
    expect(subscription.pop.kind).to eq(:queue_resumed)
    client.queue_add :imports, 1
    expect(client.queue_remove(:imports)).to equal(client)
  ensure
    subscription&.close
  end

  let(:worker) do
    Class.new do
      def self.kind = "driver_e2e"

      def next_retry(_job, _error) = Time.now.utc + 0.05

      def work(job)
        raise "permanent failure" if job.args["fail"]
        raise "temporary failure" if job.args["retry"] && job.attempt == 1

        job.output = {"value" => job.args.fetch("value") * 2}
      end
    end
  end

  let(:plugins) { [] }

  let(:client) do
    River::Client.new(@driver, config: River::Config.new(
      fetch_cooldown: 0.001,
      fetch_poll_interval: 0.01,
      queues: {"driver_e2e" => 2},
      plugins: plugins,
      workers: River::Workers.new.add(worker)
    ))
  end

  after { client.stop_and_cancel if @driver }

  def e2e_insert(**args)
    client.insert(River::JobArgsHash.new("driver_e2e", args), insert_opts: River::InsertOpts.new(queue: "driver_e2e")).job
  end

  def next_event(subscription)
    Timeout.timeout(5) { subscription.pop }
  end

  context "with job-persisted logging" do
    let(:plugins) { [River::JobPersistedLogging::Plugin.new] }

    it "appends retry logs to Go-format history and includes them in completion events" do
      worker.define_method(:work) do |job|
        job.logger << "attempt #{job.attempt}\n"
        job.update_metadata application: "kept"
        raise "retry" if job.attempt == 1
      end
      previous = {"attempt" => 0, "log" => "from Go\n"}
      row = client.insert(River::JobArgsHash.new("driver_e2e", {}), insert_opts: River::InsertOpts.new(
        metadata: {"river:log" => [previous], "unrelated" => true}
      )).job
      subscription = client.subscribe(:job_failed, :job_completed)

      first = River::Testing.perform_job(client, row.id)
      expect(first.outcome).to eq(:retried)
      expect(next_event(subscription).job.metadata.fetch("river:log")).to eq([
        previous, {"attempt" => 1, "log" => "attempt 1\n"}
      ])
      client.job_retry(row.id)
      second = River::Testing.perform_job(client, row.id)
      expect(second.outcome).to eq(:completed)
      expected = [previous, {"attempt" => 1, "log" => "attempt 1\n"}, {"attempt" => 2, "log" => "attempt 2\n"}]
      expect(next_event(subscription).job.metadata.fetch("river:log")).to eq(expected)
      expect(client.job_get(row.id).metadata).to include("river:log" => expected, "application" => "kept", "unrelated" => true)
    ensure
      subscription&.close
    end

    {cancelled: River.job_cancel("cancel"), snoozed: River.job_snooze(10), discarded: RuntimeError.new("fail")}.each do |outcome, error|
      it "persists logs when work is #{outcome}" do
        worker.define_method(:work) do |job|
          job.logger << "before #{outcome}\n"
          raise error
        end
        row = client.insert(River::JobArgsHash.new("driver_e2e", {}), insert_opts: River::InsertOpts.new(max_attempts: 1)).job
        result = River::Testing.perform_job(client, row.id)

        expect(result.outcome).to eq(outcome)
        expect(result.error).to equal(error)
        expect(client.job_get(row.id).metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "before #{outcome}\n"}])
      end
    end

    it "persists logs when a worker times out" do
      worker.define_method(:timeout) { |_job| 0.01 }
      worker.define_method(:work) do |job|
        job.logger << "before timeout\n"
        sleep(60)
      end
      row = e2e_insert
      result = River::Testing.perform_job(client, row.id)

      expect(result.error).to be_a(Timeout::Error)
      expect(result.outcome).to eq(:retried)
      expect(client.job_get(row.id).metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "before timeout\n"}])
    end

    it "persists logs during an interrupted shutdown with the original attempt number" do
      entered = Queue.new
      worker.define_method(:work) do |job|
        job.logger << "before interruption\n"
        entered << true
        sleep(60)
      end
      row = e2e_insert
      client.start
      Timeout.timeout(5) { entered.pop }
      client.stop_and_cancel

      stored = client.job_get(row.id)
      expect(stored).to have_attributes(state: "available", attempt: 0)
      expect(stored.metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "before interruption\n"}])
    end
  end

  it "asserts insertions and executes synchronously inside the caller's transaction" do
    row = nil
    @driver.transaction do
      inserted = River::Testing.inserted_jobs(client, args: {"value" => 9}, kind: "driver_e2e") { e2e_insert(value: 9) }

      expect(inserted.length).to eq(1)
      row = inserted.first
      result = River::Testing.perform_job(client, row.id)

      expect(result).to have_attributes(id: row.id, error: nil, outcome: :completed)
      expect(result.job).to have_attributes(
        attempt: 1,
        attempted_by: [client.id],
        metadata: include("output" => {"value" => 18}),
        state: "completed"
      )
      raise @driver.rollback_exception
    end

    expect(@driver.job_get_by_id(row.id)).to be_nil
  end

  it "drains only the selected queue and returns real worker errors" do
    worker.define_method(:next_retry) { |_job, _error| Time.now.utc + 3_600 }
    [{"value" => 2}, {"fail" => true, "value" => 3}].each do |args|
      client.insert(River::JobArgsHash.new("driver_e2e", args), insert_opts: River::InsertOpts.new(
        queue: "driver_e2e", scheduled_at: Time.now.utc - 1, state: "available"
      ))
    end

    client.insert(River::JobArgsHash.new("driver_e2e", {"value" => 4}))
    results = River::Testing.drain(client, queue: "driver_e2e")

    expect(results.map(&:outcome)).to eq([:completed, :retried])
    expect(results.last.error).to have_attributes(message: "permanent failure")
    expect(client.job_list(River::JobListParams.new(queues: ["default"])).jobs.first.state).to eq("available")
  end

  it "rolls back enqueues and hides uncommitted jobs from another connection" do
    rolled_back = nil
    @driver.transaction do
      rolled_back = e2e_insert(value: 1)
      observed = Thread.new { @driver.job_get_by_id(rolled_back.id) }.value

      expect(observed).to be_nil
      raise @driver.rollback_exception
    end

    expect(@driver.job_get_by_id(rolled_back.id)).to be_nil
    expect(client.job_list.jobs).to be_empty
  end

  it "works committed bulk inserts in background threads and persists output" do
    subscription = client.subscribe(River::EVENT_JOB_COMPLETED)
    rows = @driver.transaction do
      client.insert_many((1..3).map do |value|
        River::InsertManyParams.new(River::JobArgsHash.new("driver_e2e", {"value" => value}),
          insert_opts: River::InsertOpts.new(queue: "driver_e2e"))
      end).map(&:job)
    end

    client.start
    events = rows.map { next_event(subscription) }

    expect(events.map { |event| event.job.id }).to match_array(rows.map(&:id))
    rows.each do |row|
      expect(client.job_get(row.id)).to have_attributes(
        id: row.id,
        attempt: 1,
        attempted_by: [client.id],
        finalized_at: be_a(Time),
        metadata: include("output" => {"value" => row.args.fetch("value") * 2}),
        state: River::JOB_STATE_COMPLETED
      )
    end

    client.stop

    expect(client).to be_stopped
  ensure
    subscription&.close
  end

  it "retries a failed attempt, records its error, and then completes" do
    subscription = client.subscribe(River::EVENT_JOB_COMPLETED, River::EVENT_JOB_FAILED)
    row = e2e_insert(retry: true, value: 7)
    client.start

    expect(next_event(subscription)).to have_attributes(kind: River::EVENT_JOB_FAILED)
    expect(next_event(subscription)).to have_attributes(kind: River::EVENT_JOB_COMPLETED)
    expect(client.job_get(row.id)).to have_attributes(
      attempt: 2,
      errors: contain_exactly(have_attributes(attempt: 1, error: "temporary failure")),
      metadata: include("output" => {"value" => 14}),
      state: River::JOB_STATE_COMPLETED
    )
  ensure
    subscription&.close
  end

  it "discards exhausted jobs and supports retry and cancellation administration" do
    subscription = client.subscribe(River::EVENT_JOB_FAILED)
    row = client.insert(River::JobArgsHash.new("driver_e2e", {"fail" => true}),
      insert_opts: River::InsertOpts.new(max_attempts: 1, queue: "driver_e2e")).job
    client.start

    expect(next_event(subscription).job).to have_attributes(
      id: row.id,
      errors: contain_exactly(have_attributes(error: "permanent failure")),
      finalized_at: be_a(Time),
      state: River::JOB_STATE_DISCARDED
    )
    client.stop

    expect(client.job_retry(row.id)).to have_attributes(finalized_at: nil, max_attempts: 2, state: River::JOB_STATE_AVAILABLE)
    expect(client.job_cancel(row.id)).to have_attributes(finalized_at: be_a(Time), state: River::JOB_STATE_CANCELLED)
    expect(client.job_delete(row.id)).to have_attributes(id: row.id)
    expect(@driver.job_get_by_id(row.id)).to be_nil
  ensure
    subscription&.close
  end
end
