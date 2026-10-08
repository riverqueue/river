# frozen_string_literal: true

RSpec.shared_examples "historical attempt error decoding" do
  it "isolates non-object metadata across reads, unique inserts, claims, and finalization" do
    rows = [[], "invalid metadata", 7, true].map.with_index do |metadata, index|
      args = River::JobArgsHash.new(:decode, {index: index})
      options = {max_attempts: 1, unique_opts: River::UniqueOpts.new(by_args: true)}
      row = client.insert(args, **options).job
      encoded = driver.send(:runtime_json, JSON.generate(metadata))
      driver.send(:runtime_execute, "UPDATE river_job SET metadata = #{encoded} WHERE id = #{row.id}")
      expect { client.job_get(row.id) }.to raise_error(River::JobRowDecodeError, /metadata/)
      expect { client.job_list(ids: [row.id]) }.to raise_error(River::JobRowDecodeError, /metadata/)
      expect { client.insert(args, **options) }.to raise_error(River::JobRowDecodeError, /metadata/)
      row
    end
    healthy = client.insert(River::JobArgsHash.new(:decode, {})).job

    claimed = driver.job_get_available(queue: "default", max: 10, attempted_by: "test")
    expect(claimed.map(&:id)).to match_array([*rows.map(&:id), healthy.id])
    expect(claimed.reject(&:__decode_error).map(&:id)).to eq([healthy.id])
    claimed.select(&:__decode_error).each do |row|
      client.__finish_claimed_job(row, row.__decode_error)
      failed = driver.send(:runtime_read_job, row.id)
      expect(failed).to have_attributes(state: "discarded", errors: contain_exactly(have_attributes(error: include("metadata"))))
    end
  end

  it "reads, fetches, and returns duplicate jobs with unusually shaped error entries" do
    args = River::JobArgsHash.new(:decode, {})
    options = {unique_opts: River::UniqueOpts.new(by_args: true)}
    row = client.insert(args, **options).job
    errors = [{"at" => "unreadable timestamp", "attempt" => "2", "error" => {"message" => "failed"}, "trace" => ["frame"]}, "legacy", nil, '{"error":"literal JSON text"}']
    encoded = if driver.send(:runtime_postgres?)
      "ARRAY[#{errors.map { |error| driver.send(:runtime_json, JSON.generate(error)) }.join(",")}]::jsonb[]"
    else
      driver.send(:runtime_json, errors)
    end
    driver.send(:runtime_execute, "UPDATE river_job SET errors = #{encoded} WHERE id = #{row.id}")

    read = client.job_get(row.id)
    expect(read.errors.map(&:error)).to eq(['{"message":"failed"}', "legacy", "", '{"error":"literal JSON text"}'])
    expect(read.errors.first).to have_attributes(at: Time.utc(1), attempt: 2, trace: '["frame"]')
    expect(client.job_list.jobs.first.errors.map(&:to_h)).to eq(read.errors.map(&:to_h))
    expect(client.insert(args, **options).job.errors.map(&:to_h)).to eq(read.errors.map(&:to_h))
    expect(driver.job_get_available(queue: "default", max: 10, attempted_by: "test").first.errors.map(&:to_h)).to eq(read.errors.map(&:to_h))
  end
end

RSpec.shared_examples "SQLite corrupt job runtime" do
  it "fails only undecodable attempts before hooks and works healthy jobs in the same batch" do
    worked, hooks, handled, retry_hooks = Queue.new, Queue.new, Queue.new, Queue.new
    worker = Object.new
    worker.define_singleton_method(:work) { |job| worked << job.id }
    worker.define_singleton_method(:next_retry) { |*|
      retry_hooks << true
      Time.now.utc + 60
    }
    plugin = Object.new
    plugin.define_singleton_method(:work_begin) { |job| hooks << job.id }
    policy = Object.new
    policy.define_singleton_method(:next_retry) { |*, now:| now + 60 }
    client = River::Client.new(@driver, config: River::Config.new(
      queues: {default: 20}, workers: River::Workers.new.add(:decode, worker), plugins: [plugin],
      fetch_cooldown: 0.001, fetch_poll_interval: 0.005, leader_election_disabled: true,
      retry_policy: policy, error_handler: ->(error, job) { handled << [job.id, error] }
    ))
    args = River::JobArgsHash.new(:decode, {})
    first = client.insert(args).job
    corrupt_values = %w[args metadata attempted_by errors tags].product(["'{'", "X'1BFF'"])
    bad = corrupt_values.map.with_index do |(field, sql), index|
      row = client.insert(args, max_attempts: index.even? ? 1 : 2).job
      @driver.send(:runtime_execute, "UPDATE river_job SET #{field} = #{sql} WHERE id = #{row.id}")
      row
    end
    last = client.insert(args).job
    events = client.subscribe(:job_completed, :job_failed)
    client.start
    received = Timeout.timeout(5) { (bad.length + 2).times.map { events.pop } }
    client.stop

    expect(received.map { |event| event.job.id }).to match_array([first.id, *bad.map(&:id), last.id])
    expect([worked.pop(true), worked.pop(true)]).to match_array([first.id, last.id])
    expect(worked).to be_empty
    expect([hooks.pop(true), hooks.pop(true)]).to match_array([first.id, last.id])
    expect(hooks).to be_empty
    expect(retry_hooks).to be_empty
    failures = received.select { |event| event.kind == :job_failed }
    expect(failures.length).to eq(bad.length)
    expect(failures.map { |event| event.job.state }).to match_array(["discarded", "retryable"] * 5)
    expect(failures.map { |event| event.job.errors.last.error }).to all(include("couldn't be decoded"))
    expect(bad.length.times.map { handled.pop(true).last }).to all(be_a(River::JobRowDecodeError))
    expect(handled).to be_empty
  ensure
    client&.stop_and_cancel
    events&.close
  end
end

RSpec.shared_examples "SQLite corrupt job isolation" do
  def decoding_insert(**options)
    client.insert(River::JobArgsHash.new(:decode, {}), **options).job
  end

  def corrupt(row, field, sql)
    driver.send(:runtime_execute, "UPDATE river_job SET #{field} = #{sql} WHERE id = #{row.id}")
  end

  it "claims healthy jobs on both sides of malformed rows without repairing bad values" do
    first = decoding_insert
    bad = [[:args, "'{'"], [:metadata, "'{'"], [:tags, "'{'"], [:tags, "jsonb('[1]')"],
      [:attempted_by, "'{'"], [:attempted_by, "jsonb('{}')"], [:errors, "'{'"], [:errors, "jsonb('{}')"],
      [:args, "X'FF'"], *%i[args metadata attempted_by errors tags].map { |field| [field, "X'1BFF'"] },
      [:attempted_by, driver.send(:runtime_json, Array.new(100, "worker") + [{"unexpected" => true}])]].map do |field, sql|
      decoding_insert.tap { |row| corrupt(row, field, sql) }
    end
    last = decoding_insert
    bad.each { |row| expect { client.job_get(row.id) }.to raise_error(River::JobRowDecodeError) }

    rows = driver.job_get_available(queue: "default", max: 100, attempted_by: "test")
    expect(rows.map(&:id)).to match_array([first.id, *bad.map(&:id), last.id])
    expect(rows).to all(have_attributes(state: "running", attempt: 1))
    expect(rows.reject(&:__decode_error).map(&:id)).to eq([first.id, last.id])
    expect(rows.select(&:__decode_error).map(&:id)).to eq(bad.map(&:id))
    expect(driver.send(:runtime_query_rows, "SELECT attempted_by FROM river_job WHERE id = #{bad[4].id}").first.values).to eq(["{"])
  end

  ["jsonb('{}')", "jsonb('\"old\"')", "'{'", "X'1BFF'", "NULL"].each do |old_errors|
    it "preserves #{old_errors} when appending a failure and rescuing a stuck job" do
      now = Time.now.utc
      row = decoding_insert(scheduled_at: now - 120)
      corrupt(row, :errors, old_errors)
      corrupt(row, :metadata, "'{'")
      driver.job_claim(id: row.id, attempted_by: "test", now: now - 120)

      expect(driver.job_rescue_stuck(horizon: now - 60, now: now, retry_policy: River::DefaultClientRetryPolicy.new)).to eq(1)
      partial = driver.send(:runtime_read_job, row.id)
      expect(partial).to have_attributes(state: "retryable", __decode_error: be_a(River::JobRowDecodeError))
      expect(partial.errors.last.error).to eq("Stuck job rescued by River")
      expect(partial.errors.length).to eq((old_errors == "NULL") ? 1 : 2)
      expect(partial.errors.first.error).to eq("invalid JSONB: 1BFF") if old_errors == "X'1BFF'"
      expect(driver.send(:runtime_query_rows, "SELECT metadata FROM river_job WHERE id = #{row.id}").first.values).to eq(["{"])
    end
  end

  it "schedules malformed jobs and preserves malformed metadata on uniqueness conflicts" do
    now = Time.now.utc
    args = River::JobArgsHash.new(:unique_decode, {})
    unique = River::UniqueOpts.new(by_args: true, by_state: %w[available pending running scheduled])
    collision = client.insert(args, unique_opts: unique, scheduled_at: now - 60).job
    client.job_update(collision.id, state: :retryable)
    original = client.insert(args, unique_opts: unique).job
    [original, collision].each { |row| corrupt(row, :metadata, "'{'") }
    bad = decoding_insert(state: :scheduled, scheduled_at: now - 60)
    %i[args metadata tags attempted_by errors].each { |field| corrupt(bad, field, "'{'") }
    healthy = decoding_insert(state: :scheduled, scheduled_at: now - 60)

    expect(driver.job_schedule(now: now)).to eq(3)
    expect(driver.send(:runtime_read_job, collision.id).state).to eq("discarded")
    expect(driver.send(:runtime_read_job, bad.id).state).to eq("available")
    expect(client.job_get(healthy.id).state).to eq("available")
    expect(driver.send(:runtime_query_rows, "SELECT metadata FROM river_job WHERE id = #{collision.id}").first.values).to eq(["{"])
  end

  it "finds other jobs' cancellation requests when a running job's metadata is corrupt" do
    first, second = 2.times.map { decoding_insert }
    driver.job_get_available(queue: "default", max: 2, attempted_by: "test")
    client.job_cancel(second.id)
    ["'{'", "X'1BFF'"].each do |sql|
      corrupt(first, :metadata, sql)
      expect(driver.job_get_cancelled_ids([first.id, second.id])).to eq([second.id])
    end
  end
end
