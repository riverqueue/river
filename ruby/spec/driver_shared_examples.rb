# frozen_string_literal: true

require_relative "driver_runtime_shared_examples"

class SimpleArgs
  attr_accessor :job_num

  def initialize(job_num:)
    self.job_num = job_num
  end

  def kind = "simple"

  def to_json = JSON.dump({job_num: job_num})
end

# Lets us test job-specific insertion opts by making `#insert_opts` an accessor.
# Real args that make use of this functionality will probably want to make
# `#insert_opts` a non-accessor method instead.
class SimpleArgsWithInsertOpts < SimpleArgs
  attr_accessor :insert_opts
end

shared_examples "driver shared examples" do
  it_behaves_like "driver job state machine"
  it_behaves_like "driver queue and leadership state"

  describe "cross-kind uniqueness" do
    [:insert, :insert_many].each do |operation|
      it "preserves the original kind and arguments on a duplicate #{operation}" do
        options = River::InsertOpts.new(unique_opts: River::UniqueOpts.new(by_args: true, exclude_kind: true))
        original = client.insert(River::JobArgsHash.new("original", {"id" => 1}), insert_opts: options).job
        args = River::JobArgsHash.new("different", {"id" => 1})
        result = if operation == :insert
          client.insert(args, insert_opts: options)
        else
          client.insert_many([River::InsertManyParams.new(args, insert_opts: options)]).first
        end

        expect(result.unique_skipped_as_duplicate?).to be true
        expect(result.job).to have_attributes(id: original.id, kind: "original", args: original.args)
        expect(client.job_get(original.id)).to have_attributes(
          args: original.args, kind: original.kind, metadata: original.metadata, state: original.state
        )
      end
    end
  end

  describe "insertion attempt limits" do
    it "accepts a single attempt and explicit overrides of argument-level limits" do
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(max_attempts: 0)

      expect(client.insert(args, max_attempts: 1).job.max_attempts).to eq(1)
      expect(client.insert_many([River::InsertManyParams.new(args, max_attempts: 1)]).first.job.max_attempts).to eq(1)
    end

    [0, -1].each do |max_attempts|
      it "rejects #{max_attempts} attempts in single inserts without writing any jobs" do
        args = SimpleArgsWithInsertOpts.new(job_num: 1)
        args.insert_opts = River::InsertOpts.new(max_attempts: max_attempts)

        expect { client.insert(args) }.to raise_error(ArgumentError, "max_attempts must be greater than zero")
        expect { client.insert(SimpleArgs.new(job_num: 1), insert_opts: args.insert_opts) }
          .to raise_error(ArgumentError, "max_attempts must be greater than zero")
        expect { client.insert(SimpleArgs.new(job_num: 1), max_attempts: max_attempts) }
          .to raise_error(ArgumentError, "max_attempts must be greater than zero")
        expect(client.job_list.jobs).to be_empty
      end

      it "rejects #{max_attempts} attempts in batch inserts without writing any jobs" do
        args = SimpleArgsWithInsertOpts.new(job_num: 2)
        args.insert_opts = River::InsertOpts.new(max_attempts: max_attempts)

        expect { client.insert_many([SimpleArgs.new(job_num: 1), args]) }
          .to raise_error(ArgumentError, "max_attempts must be greater than zero")
        expect do
          client.insert_many([
            SimpleArgs.new(job_num: 1),
            River::InsertManyParams.new(SimpleArgs.new(job_num: 2), max_attempts: max_attempts)
          ])
        end.to raise_error(ArgumentError, "max_attempts must be greater than zero")
        expect(client.job_list.jobs).to be_empty
      end
    end
  end

  it "filters kinds before claiming jobs without consuming unknown attempts" do
    now = Time.utc(2026, 1, 2)
    unknown, known = ["unknown", "known'kind"].map do |kind|
      client.insert(River::JobArgsHash.new(kind, {}), insert_opts: River::InsertOpts.new(scheduled_at: now - 1)).job
    end
    expect(driver.job_get_available(queue: "default", max: 10, attempted_by: "test", kinds: [], now: now)).to eq([])
    expect(driver.job_get_available(queue: "default", max: 1, attempted_by: "test", kinds: ["known'kind"], now: now))
      .to contain_exactly(have_attributes(id: known.id, attempt: 1, state: "running"))
    expect(driver.job_get_by_id(unknown.id)).to have_attributes(attempt: 0, state: "available")
    expect(driver.job_get_available(queue: "default", max: 10, attempted_by: "test", now: now))
      .to contain_exactly(have_attributes(id: unknown.id))
  end

  %i[insert_begin insert_end insert_many].each do |hook|
    it "rolls back #{hook} writes and the enqueue without aborting the caller's transaction" do
      plugin = Object.new
      plain_client = client
      failure = RuntimeError.new("hook failed")
      plugin.define_singleton_method(hook) do |*arguments|
        arguments.last.call if hook == :insert_many
        plain_client.insert(SimpleArgs.new(job_num: 99))
        raise failure
      end
      hooked_client = River::Client.new(driver, config: River::Config.new(plugins: [plugin]))

      driver.transaction do
        kept = plain_client.insert(SimpleArgs.new(job_num: 1)).job
        expect { hooked_client.insert(SimpleArgs.new(job_num: 2)) }.to raise_error { |error| expect(error).to equal(failure) }
        expect(plain_client.job_list.jobs.map(&:id)).to eq([kept.id])
        expect(plain_client.insert(SimpleArgs.new(job_num: 3)).job.args).to eq("job_num" => 3)
      end
      expect(plain_client.job_list.jobs.map { |row| row.args.fetch("job_num") }).to eq([1, 3])
    end
  end

  it "merges metadata shallowly, preserving nulls and literal keys on both databases" do
    job = client.insert(SimpleArgs.new(job_num: 1), insert_opts: River::InsertOpts.new(
      metadata: {"keep" => 1, "nested" => {"old" => true}, "nullable" => 2}
    )).job
    updates = {'a"b' => "literal", "a.b" => [1, true], "a\\b" => false, "nested" => {"new" => true}, "nullable" => nil}

    driver.job_metadata_merge(job.id, updates)

    expect(client.job_get(job.id).metadata.to_h).to eq(job.metadata.to_h.merge(updates))
  end

  it "rolls back cancellation if publishing its notification fails" do
    job = client.insert(SimpleArgs.new(job_num: 1)).job
    driver.define_singleton_method(:runtime_notify) { |*_args| raise "notification failed" }

    expect { client.job_cancel(job.id) }.to raise_error("notification failed")
    expect(client.job_get(job.id)).to have_attributes(state: "available", metadata: job.metadata)
  end

  it "does not clean up a finalized job retried after cleanup selected it" do
    job = client.insert(SimpleArgs.new(job_num: 1)).job
    client.job_update(job.id, River::JobUpdateParams.new(finalized_at: Time.now.utc - 120, state: River::JOB_STATE_COMPLETED))
    driver.define_singleton_method(:runtime_query_rows) do |sql|
      rows = super(sql)
      job_retry(job.id) if sql.start_with?("SELECT id FROM river_job WHERE")

      rows
    end

    expect(driver.job_delete_finalized(retention: {River::JOB_STATE_COMPLETED => 60})).to eq(0)
    expect(client.job_get(job.id).state).to eq(River::JOB_STATE_AVAILABLE)
  end

  it "keeps static driver constants shareable across Ractor boundaries" do
    values = %i[SQLITE_CONFLICT_WHERE SQLITE_JOB_COLUMNS UNIQUE_INSERT_METADATA_KEY]
      .map { |name| driver.class.const_get(name, false) }

    expect(values).to all(satisfy { |value| Ractor.shareable?(value) })
  end

  it "implements the worker runtime SQL primitives" do
    queue = driver.queue_upsert("runtime-primitives", metadata: {"source" => "spec"})

    expect(queue).to have_attributes(metadata: {"source" => "spec"}, name: "runtime-primitives")
    expect(driver.queue_list(max: 10).map(&:name)).to include("runtime-primitives")
    expect(driver.job_list(River::JobListParams.new(limit: 1))).to be_an(Array)
    expect(driver.send(:runtime_job_list_without_params)).to be_an(Array)
    expect(driver.send(:runtime_postgres?)).to satisfy { |value| value == true || value == false }
    expect(driver.send(:runtime_quote, "value")).to be_a(String)
    expect(driver.send(:runtime_unique_violation_class)).to be <= StandardError
    expect(driver.send(:runtime_value, {"id" => 1}, :id)).to eq(1)
  end

  describe "unique insertion" do
    it "rejects repeated active unique keys atomically in a returning batch" do
      kept = client.insert(SimpleArgs.new(job_num: 99)).job
      options = River::InsertOpts.new(unique_opts: River::UniqueOpts.new(by_args: true))
      batch = [2, 1, 1].map { |number| River::InsertManyParams.new(SimpleArgs.new(job_num: number), insert_opts: options) }

      expect { client.insert_many(batch) }.to raise_error(StandardError)
      expect(client.job_list.jobs.map(&:id)).to eq([kept.id])
    end

    it "allows internal driver inserts outside their indexed states and preserves duplicate flags" do
      unique = River::UniqueOpts.new(by_args: true, by_state: [:available, :pending, :running, :scheduled])
      batch = %w[retryable available retryable].map do |state|
        params = client.send(:make_insert_params, SimpleArgs.new(job_num: 1), River::InsertOpts.new(unique_opts: unique))
        params.state = state
        params
      end
      results = driver.job_insert_many(batch)
      jobs = results.map(&:first)
      expect(jobs.map(&:id).uniq.length).to eq(3)
      expect(results.map(&:last)).to eq([false, false, false])

      duplicate = client.insert(SimpleArgs.new(job_num: 1), insert_opts: River::InsertOpts.new(unique_opts: unique))
      expect(duplicate).to have_attributes(unique_skipped_as_duplicate?: true, job: have_attributes(id: jobs[1].id))
      if driver.migration_backend == :sqlite
        nonces = jobs.map { |job| job.metadata.fetch("river:unique_nonce") }
        expect(nonces).to all(match(/\A[0-9a-f]{16}\z/))
        expect(nonces.uniq.length).to eq(3)
      end
    end

    it "inserts a unique job once" do
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        unique_opts: River::UniqueOpts.new(
          by_queue: true
        )
      )

      insert_res = client.insert(args)

      expect(insert_res).to have_attributes(
        job: be_a(River::JobRow),
        unique_skipped_as_duplicate?: be(false)
      )
      original_job = insert_res.job

      insert_res = client.insert(args)

      expect(insert_res).to have_attributes(
        job: have_attributes(id: original_job.id),
        unique_skipped_as_duplicate?: be(true)
      )
    end

    it "decodes uniqueness states consistently when inserting, fetching, and listing" do
      [nil, River::UniqueOpts.new(by_args: true)].each_with_index do |unique, number|
        inserted = client.insert(SimpleArgs.new(job_num: number), insert_opts: River::InsertOpts.new(unique_opts: unique)).job
        expected = unique ? %w[available completed pending retryable running scheduled] : nil

        expect(inserted.unique_states).to eq(expected)
        expect(client.job_get(inserted.id).unique_states).to eq(expected)
        expect(client.job_list.jobs.find { |job| job.id == inserted.id }.unique_states).to eq(expected)
      end
    end

    it "inserts a unique job with custom states" do
      client = River::Client.new(driver)

      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        unique_opts: River::UniqueOpts.new(
          by_queue: true,
          by_state: [River::JOB_STATE_AVAILABLE, River::JOB_STATE_PENDING, River::JOB_STATE_RUNNING, River::JOB_STATE_SCHEDULED]
        )
      )

      insert_res = client.insert(args)

      expect(insert_res).to have_attributes(
        job: be_a(River::JobRow),
        unique_skipped_as_duplicate?: be(false)
      )
      original_job = insert_res.job

      insert_res = client.insert(args)

      expect(insert_res).to have_attributes(
        job: have_attributes(id: original_job.id),
        unique_skipped_as_duplicate?: be(true)
      )
    end
  end

  describe "#job_get_by_id" do
    let(:job_args) { SimpleArgs.new(job_num: 1) }

    it "gets a job by ID" do
      insert_res = client.insert(job_args)
      expect(driver.job_get_by_id(insert_res.job.id)).to_not be nil
    end

    it "returns nil on not found" do
      expect(driver.job_get_by_id(-1)).to be nil
    end
  end

  describe "#job_insert" do
    it "inserts a job" do
      insert_res = client.insert(SimpleArgs.new(job_num: 1))

      expect(insert_res).to have_attributes(
        job: have_attributes(
          args: {"job_num" => 1},
          attempt: 0,
          created_at: be_within(2).of(Time.now.getutc),
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          priority: River::PRIORITY_DEFAULT,
          queue: River::QUEUE_DEFAULT,
          scheduled_at: be_within(2).of(Time.now.getutc),
          state: River::JOB_STATE_AVAILABLE,
          tags: []
        ),
        unique_skipped_as_duplicate?: (be false)
      )

      # Make sure it made it to the database. Assert only minimally since we're
      # certain it's the same as what we checked above.
      job = driver.job_get_by_id(insert_res.job.id)

      expect(job).to have_attributes(
        kind: "simple"
      )
    end

    it "schedules a job" do
      target_time = Time.now.getutc + 1 * 3600

      insert_res = client.insert(
        SimpleArgs.new(job_num: 1),
        insert_opts: River::InsertOpts.new(scheduled_at: target_time)
      )

      expect(insert_res).to have_attributes(
        job: have_attributes(
          scheduled_at: be_within(2).of(target_time),
          state: River::JOB_STATE_SCHEDULED
        ),
        unique_skipped_as_duplicate?: (be false)
      )
    end

    it "inserts with job insert opts" do
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        max_attempts: 23,
        priority: 2,
        queue: "job_custom_queue",
        tags: ["job_custom"]
      )

      insert_res = client.insert(args)

      expect(insert_res).to have_attributes(
        job: have_attributes(
          max_attempts: 23,
          priority: 2,
          queue: "job_custom_queue",
          tags: ["job_custom"]
        ),
        unique_skipped_as_duplicate?: (be false)
      )
    end

    it "inserts with insert opts" do
      # We set job insert opts in this spec too so that we can verify that the
      # options passed at insertion time take precedence.
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        max_attempts: 23,
        priority: 2,
        queue: "job_custom_queue",
        tags: ["job_custom"]
      )

      insert_res = client.insert(args, insert_opts: River::InsertOpts.new(
        max_attempts: 17,
        priority: 3,
        queue: "my_queue",
        tags: ["custom"]
      ))

      expect(insert_res).to have_attributes(
        job: have_attributes(
          max_attempts: 17,
          priority: 3,
          queue: "my_queue",
          tags: ["custom"]
        ),
        unique_skipped_as_duplicate?: (be false)
      )
    end

    it "inserts with job args hash" do
      insert_res = client.insert(River::JobArgsHash.new("hash_kind", {
        job_num: 1
      }))
      expect(insert_res).to have_attributes(
        job: have_attributes(
          args: {"job_num" => 1},
          kind: "hash_kind"
        ),
        unique_skipped_as_duplicate?: (be false)
      )
    end

    it "inserts in a transaction" do
      insert_res = nil

      driver.transaction do
        insert_res = client.insert(SimpleArgs.new(job_num: 1))

        job = driver.job_get_by_id(insert_res.job.id)

        expect(job).to_not be_nil
        expect(insert_res.unique_skipped_as_duplicate?).to be false

        raise driver.rollback_exception
      end

      # Not present because the job was rolled back.
      job = driver.job_get_by_id(insert_res.job.id)

      expect(job).to be_nil
    end

    it "inserts a unique job" do
      insert_params = River::Driver::JobInsertParams.new(
        encoded_args: JSON.dump({"job_num" => 1}),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: Time.now.getutc,
        state: River::JOB_STATE_AVAILABLE,
        tags: nil,
        unique_key: "unique_key",
        unique_states: "00000001"
      )

      job_row, unique_skipped_as_duplicated = driver.job_insert(insert_params)

      expect(job_row).to have_attributes(
        args: {"job_num" => 1},
        attempt: 0,
        created_at: be_within(2).of(Time.now.getutc),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: be_within(2).of(Time.now.getutc),
        state: River::JOB_STATE_AVAILABLE,
        tags: [],
        unique_key: "unique_key",
        unique_states: [::River::JOB_STATE_AVAILABLE]
      )
      expect(unique_skipped_as_duplicated).to be false

      # second insertion should be skipped
      job_row, unique_skipped_as_duplicated = driver.job_insert(insert_params)

      expect(job_row).to have_attributes(
        args: {"job_num" => 1},
        attempt: 0,
        created_at: be_within(2).of(Time.now.getutc),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: be_within(2).of(Time.now.getutc),
        state: River::JOB_STATE_AVAILABLE,
        tags: [],
        unique_key: "unique_key",
        unique_states: [::River::JOB_STATE_AVAILABLE]
      )
      expect(unique_skipped_as_duplicated).to be true
    end
  end

  describe "#job_insert_many" do
    it "inserts multiple jobs" do
      inserted = client.insert_many([
        SimpleArgs.new(job_num: 1),
        SimpleArgs.new(job_num: 2)
      ])

      expect(inserted.length).to eq(2)
      expect(inserted[0]).to have_attributes(
        job: have_attributes(args: {"job_num" => 1}),
        unique_skipped_as_duplicate?: false
      )
      expect(inserted[1]).to have_attributes(
        job: have_attributes(args: {"job_num" => 2}),
        unique_skipped_as_duplicate?: false
      )

      jobs = driver.job_list

      expect(jobs.count).to be 2

      expect(jobs[0]).to have_attributes(
        args: {"job_num" => 1},
        attempt: 0,
        created_at: be_within(2).of(Time.now.getutc),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: be_within(2).of(Time.now.getutc),
        state: River::JOB_STATE_AVAILABLE,
        tags: []
      )

      expect(jobs[1]).to have_attributes(
        args: {"job_num" => 2},
        attempt: 0,
        created_at: be_within(2).of(Time.now.getutc),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: be_within(2).of(Time.now.getutc),
        state: River::JOB_STATE_AVAILABLE,
        tags: []
      )
    end

    it "inserts multiple jobs in a transaction" do
      jobs = nil

      driver.transaction do
        inserted = client.insert_many([
          SimpleArgs.new(job_num: 1),
          SimpleArgs.new(job_num: 2)
        ])

        expect(inserted.length).to eq(2)
        expect(inserted[0]).to have_attributes(
          job: have_attributes(args: {"job_num" => 1}),
          unique_skipped_as_duplicate?: false
        )
        expect(inserted[1]).to have_attributes(
          job: have_attributes(args: {"job_num" => 2}),
          unique_skipped_as_duplicate?: false
        )

        jobs = driver.job_list

        expect(jobs.count).to be 2

        raise driver.rollback_exception
      end

      # Not present because the jobs were rolled back.
      expect(driver.job_get_by_id(jobs[0].id)).to be nil
      expect(driver.job_get_by_id(jobs[1].id)).to be nil
    end
  end

  describe "#job_list" do
    let(:job_args) { SimpleArgs.new(job_num: 1) }

    it "gets a job by ID" do
      insert_res1 = client.insert(job_args)
      insert_res2 = client.insert(job_args)

      jobs = driver.job_list

      expect(jobs.count).to be 2

      expect(jobs[0].id).to be insert_res1.job.id
      expect(jobs[1].id).to be insert_res2.job.id
    end

    it "returns nil on not found" do
      expect(driver.job_get_by_id(-1)).to be nil
    end
  end

  describe "#transaction" do
    it "runs block in a transaction" do
      insert_res = nil

      driver.transaction do
        insert_res = client.insert(SimpleArgs.new(job_num: 1))

        job = driver.job_get_by_id(insert_res.job.id)

        expect(job).to_not be_nil

        raise driver.rollback_exception
      end

      # Not present because the job was rolled back.
      job = driver.job_get_by_id(insert_res.job.id)

      expect(job).to be_nil
    end
  end
end
