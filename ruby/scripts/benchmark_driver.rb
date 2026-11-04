# frozen_string_literal: true

# Run from ruby/: bundle exec ruby scripts/benchmark_driver.rb > results.json
# RIVER_BENCH_ROOT can point at another checkout for an unchanged baseline.
require "json"
require "pg"
require "sqlite3"
require "tmpdir"

root = ENV.fetch("RIVER_BENCH_ROOT", File.expand_path("../..", __dir__))
$LOAD_PATH.unshift("#{root}/ruby/lib", "#{root}/ruby/driver/riverqueue-activerecord/lib", "#{root}/ruby/driver/riverqueue-sequel/lib")
require "riverqueue-activerecord"
require "riverqueue-sequel"

class DriverBenchmark
  class SQLRecorder
    attr_reader :statements

    def initialize
      @statements = []
    end

    def info(message)
      @statements << message.sub(/\A\([\d.]+s\) /, "")
    end
  end

  def initialize(adapter, backend)
    @adapter = adapter
    @backend = backend
    @iterations = Integer(ENV.fetch("ITERATIONS", "200"))
    @samples = Integer(ENV.fetch("SAMPLES", "5"))
  end

  def run
    Dir.mktmpdir("river-benchmark") do |directory|
      schema = "river_benchmark_#{Process.pid}_#{@adapter}"
      url = ENV.fetch("TEST_DATABASE_URL", "postgres://localhost/river_test")
      if @backend == :postgres
        admin = PG.connect(url)
        admin.exec("CREATE SCHEMA #{schema}")
      end
      if @adapter == :activerecord
        config = (@backend == :postgres) ? {url: url, schema_search_path: schema} : {adapter: "sqlite3", database: "#{directory}/river.db"}
        ActiveRecord::Base.establish_connection(config.merge(pool: 5))
        @driver = River::Driver::ActiveRecord.new
      else
        @db = (@backend == :postgres) ? Sequel.connect(url, search_path: schema) : Sequel.sqlite("#{directory}/river.db")
        @driver = River::Driver::Sequel.new(@db)
      end
      River::Migrator.new(@driver).migrate
      @client = River::Client.new(@driver)
      args = River::JobArgsHash.new("benchmark", {value: 123})
      job = @client.insert(args).job
      now = Time.now.utc
      results = {}
      results[:get] = measure { @driver.job_get_by_id(job.id) }
      results[:insert_available] = measure { @client.insert(args) }
      results[:insert_pending] = measure { @client.insert(args, state: :pending) }
      reset_leader = -> { @driver.send(:runtime_execute, "DELETE FROM river_leader") }
      results[:leader_acquire] = measure(setup: reset_leader) { @driver.leader_acquire("benchmark") }
      results[:metadata_merge] = measure { @driver.job_metadata_merge(job.id, {"output" => 42}) }
      results[:update] = measure { @client.job_update(job.id, max_attempts: 25) }
      results[:queue_upsert] = measure { @driver.queue_upsert("benchmark") }
      reset = -> { @driver.send(:runtime_execute, "UPDATE river_job SET state = 'available', attempt = 0, attempted_by = NULL, finalized_at = NULL WHERE id = #{job.id}") }
      results[:claim_complete] = measure(setup: reset) do
        @driver.job_claim(id: job.id, attempted_by: "benchmark", now: now + 60)
        @driver.job_complete(id: job.id, finalized_at: now)
      end
      batch = @client.insert_many(Array.new(50) { River::InsertManyParams.new(args, queue: "batch") }).map(&:job)
      reset_batch = -> { @driver.send(:runtime_execute, "UPDATE river_job SET state = 'available', attempt = 0, attempted_by = NULL WHERE queue = 'batch'") }
      results[:claim_batch_50] = measure(setup: reset_batch) do
        rows = @driver.job_get_available(queue: "batch", max: batch.length, attempted_by: "benchmark", now: now + 60)
        raise "wrong claim count" unless rows.length == 50
      end
      if @adapter == :activerecord
        pool = ActiveRecord::Base.connection_pool
        pool.release_connection
        Thread.new { @driver.job_metadata_merge(job.id, {"output" => 42}) }.value
        results[:dead_connections_after_thread] = pool.stat[:dead]
        pool.reap
        results[:thread_update_and_reap] = measure do
          Thread.new { @driver.job_metadata_merge(job.id, {"output" => 42}) }.value
          pool.reap
        end
        results[:thread_update_and_reap].delete(:statements)
      end
      results
    ensure
      ActiveRecord::Base.connection_pool.disconnect! if @adapter == :activerecord && @driver
      @db&.disconnect
      if admin
        admin.exec("DROP SCHEMA #{schema} CASCADE")
        admin.close
      end
    end
  end

  private def measure(setup: -> {}, &operation)
    10.times {
      setup.call
      operation.call
    }
    setup.call
    statements = record_sql(&operation)
    samples = Array.new(@samples) do
      elapsed = 0.0
      allocations = 0
      @iterations.times do
        setup.call
        before = GC.stat(:total_allocated_objects)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        operation.call
        elapsed += Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        allocations += GC.stat(:total_allocated_objects) - before
      end
      {microseconds: elapsed * 1_000_000 / @iterations, allocations: allocations.to_f / @iterations}
    end
    {median_us: samples.map { |sample| sample[:microseconds] }.sort[@samples / 2], samples: samples, statements: statements}
  end

  private def record_sql
    statements = []
    if @adapter == :sequel
      logger = SQLRecorder.new
      @db.loggers << logger
      begin
        yield
      ensure
        @db.loggers.delete(logger)
      end
      statements = logger.statements
    elsif @backend == :postgres
      callback = ->(_name, _start, _finish, _id, payload) { statements << payload[:sql] }
      ActiveSupport::Notifications.subscribed(callback, "sql.active_record") { yield }
    else
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.raw_connection.trace { |sql| statements << sql }
        begin
          yield
        ensure
          connection.raw_connection.trace(nil)
        end
      end
    end
    statements
  end
end

results = {source_root: root, ruby: RUBY_DESCRIPTION, active_record: ActiveRecord::VERSION::STRING, sequel: Sequel::VERSION, sqlite: SQLite3::SQLITE_VERSION, iterations: ENV.fetch("ITERATIONS", "200"), samples: ENV.fetch("SAMPLES", "5"), results: {}}
%i[activerecord sequel].each do |adapter|
  %i[postgres sqlite].each do |backend|
    warn "Benchmarking #{adapter}/#{backend}"
    results[:results]["#{adapter}/#{backend}"] = DriverBenchmark.new(adapter, backend).run
  end
end
puts JSON.pretty_generate(results)
