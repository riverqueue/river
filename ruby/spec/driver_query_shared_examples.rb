# frozen_string_literal: true

require_relative "support/transaction_helpers"

RSpec.shared_examples "driver query efficiency" do
  let(:client) { River::Client.new(@driver) }
  let(:args) { River::JobArgsHash.new(:query_efficiency, {value: 123}) }

  it "claims a batch in one statement and returns each updated snapshot" do
    jobs = client.insert_many(Array.new(5) { River::InsertManyParams.new(args) }).map(&:job)
    claimed = nil
    statements = sql_statements(@driver) do
      claimed = @driver.job_get_available(queue: "default", max: jobs.length, attempted_by: "test")
    end

    expect(statements.length).to eq(1)
    expect(claimed.map(&:id)).to match_array(jobs.map(&:id))
    expect(claimed).to all(have_attributes(attempt: 1, attempted_by: ["test"], state: "running"))
  end

  it "claims each job once across concurrent batches" do
    jobs = client.insert_many(Array.new(20) { River::InsertManyParams.new(args) }).map(&:job)
    start = Queue.new
    threads = Array.new(4) do
      Thread.new do
        start.pop
        @driver.job_get_available(queue: "default", max: 5, attempted_by: "test")
      end
    end
    4.times { start << true }
    claimed = Timeout.timeout(5) { threads.flat_map(&:value) }

    expect(claimed.map(&:id)).to match_array(jobs.map(&:id))
    expect(claimed).to all(have_attributes(attempt: 1, state: "running"))
  ensure
    threads&.each(&:join)
  end

  it "cleans finalized jobs with one bounded statement" do
    jobs = client.insert_many(Array.new(3) { River::InsertManyParams.new(args) }).map(&:job)
    jobs.each { |job| client.job_update(job.id, state: :completed, finalized_at: Time.now.utc - 120) }
    count = nil
    statements = sql_statements(@driver) do
      count = @driver.job_delete_finalized(retention: {completed: 60}, max: 2)
    end

    expect(statements.length).to eq(1)
    expect(count).to eq(2)
    expect(client.job_list.jobs.map(&:id)).to eq([jobs.last.id])
  end

  it "elects a new Postgres leader without selecting the row after insertion" do
    @driver.init_driver
    # Warm Sequel's SQLite transaction configuration before recording SQL.
    @driver.transaction {}
    acquired = nil
    statements = sql_statements(@driver) { acquired = @driver.leader_acquire("query_efficiency") }

    expect(acquired).to be true
    postgres = @driver.migration_backend == :postgresql
    expect(statements.length).to eq(postgres ? 4 : 5)
    expect(statements.grep(/\ASELECT .*FROM river_leader/i).length).to eq(postgres ? 0 : 1)
  end

  it "inserts pending jobs in one statement and preserves caller rollback" do
    # Warm adapter schema caches before counting operation SQL.
    client.insert(args, state: :pending)
    inserted = nil
    statements = sql_statements(@driver) { inserted = client.insert(args, state: :pending).job }

    expect(statements.length).to eq(1)
    expect(inserted).to have_attributes(args: {"value" => 123}, state: "pending")
    rolled_back_id = nil
    @driver.transaction do
      rolled_back_id = client.insert(args, state: :pending).job.id
      raise @driver.rollback_exception
    end
    expect(@driver.job_get_by_id(rolled_back_id)).to be_nil
  end

  it "returns completion, metadata, update, and deletion snapshots without refetches" do
    job = client.insert(args).job
    @driver.job_claim(id: job.id, attempted_by: "test")
    completed = merged = updated = deleted = nil
    statements = sql_statements(@driver) { completed = @driver.job_complete(id: job.id, finalized_at: Time.now.utc) }
    expect(statements.length).to eq(1)
    expect(completed).to have_attributes(id: job.id, state: "completed", finalized_at: be_a(Time))

    statements = sql_statements(@driver) { merged = @driver.job_metadata_merge(job.id, {"output" => 42}) }
    expect(statements.length).to eq(1)
    expect(merged.metadata).to include("output" => 42)

    statements = sql_statements(@driver) { updated = client.job_update(job.id, max_attempts: 30) }
    expect(statements.length).to eq(1)
    expect(updated.max_attempts).to eq(30)

    statements = sql_statements(@driver) { deleted = client.job_delete(job.id) }
    expect(statements.length).to eq(1)
    expect(deleted).to have_attributes(id: job.id, max_attempts: 30, metadata: merged.metadata)
  end

  it "returns retry state from the write and sends its notification atomically" do
    job = client.insert(args, state: :pending).job
    # Sequel reads SQLite isolation settings when opening its first transaction.
    @driver.transaction {}
    retried = nil
    statements = sql_statements(@driver) { retried = client.job_retry(job.id) }

    expect(statements.length).to eq(4)
    expect(retried).to have_attributes(id: job.id, state: "available")
    expect(statements.count { |sql| sql.match?(/\ASELECT .*FROM ["`]?river_job/i) }).to eq(0)
  end

  it "upserts queues and requests resignation with one statement each" do
    @driver.init_driver
    queue = nil
    statements = sql_statements(@driver) { queue = @driver.queue_upsert("query_efficiency", metadata: {"value" => 123}) }
    expect(statements.length).to eq(1)
    expect(queue).to have_attributes(name: "query_efficiency", metadata: {"value" => 123})

    expect(sql_statements(@driver) { @driver.request_resign }.length).to eq(1)
  end
end

RSpec.shared_examples "Postgres completion plans" do
  [:job_complete, :job_delete_if_running, :job_metadata_merge, :job_set_state_if_running].each do |operation|
    it "targets the primary key for #{operation} when running jobs are absent from statistics" do
      @driver.init_driver
      @driver.send(:runtime_execute, <<~SQL)
        INSERT INTO river_job (id, args, kind)
        SELECT number, '{}'::jsonb, 'completion_plan'
        FROM generate_series(1, 10000) AS number ORDER BY (number * 7919) % 10000
      SQL
      @driver.send(:runtime_execute, "ANALYZE river_job")
      job = @driver.job_claim(id: 1, attempted_by: "test")
      statements = sql_statements(@driver) do
        case operation
        when :job_complete
          @driver.job_complete(id: job.id, finalized_at: Time.now.utc)
        when :job_delete_if_running
          @driver.job_delete_if_running(job.id)
        when :job_metadata_merge
          @driver.job_metadata_merge(job.id, {"output" => "saved"}, job)
        else
          @driver.job_set_state_if_running(id: job.id, state: "retryable", scheduled_at: Time.now.utc)
        end
      end
      expect(statements.length).to eq(1)
      plan = @driver.send(:runtime_query_rows, "EXPLAIN (FORMAT JSON) #{statements.first}").first.values.first
      plan = JSON.parse(plan) if plan.is_a?(String)
      nodes = [plan.first.fetch("Plan")]
      nodes.each { |node| nodes.concat(node.fetch("Plans", [])) }
      indexes = nodes.filter_map { |node| node["Index Name"] }

      expect(indexes).to eq(["river_job_pkey"])
    end
  end
end
