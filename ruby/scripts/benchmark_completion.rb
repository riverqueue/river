# frozen_string_literal: true

# Run from ruby/ with TEST_DATABASE_URL pointing at the Postgres version to test.
# RIVER_BENCH_ROOT selects an unchanged checkout for comparison.
require "json"
require "pg"
require "securerandom"

root = ENV.fetch("RIVER_BENCH_ROOT", File.expand_path("../..", __dir__))
$LOAD_PATH.unshift("#{root}/ruby/lib", "#{root}/ruby/driver/riverqueue-activerecord/lib", "#{root}/ruby/driver/riverqueue-sequel/lib")
require "riverqueue-activerecord"
require "riverqueue-sequel"

class CompletionBenchmark
  def initialize(adapter)
    @adapter = adapter
    @iterations = Integer(ENV.fetch("ITERATIONS", "200"))
    @jobs = Integer(ENV.fetch("JOBS", "50000"))
    @running = Integer(ENV.fetch("RUNNING_JOBS", "5000"))
    @samples = Integer(ENV.fetch("SAMPLES", "5"))
    raise "invalid workload size" unless @iterations.positive? && @samples.positive? && @running >= 0 && @jobs > @running + @iterations * @samples
  end

  def run
    schema = "river_completion_benchmark_#{SecureRandom.hex(8)}"
    url = ENV.fetch("TEST_DATABASE_URL", "postgres://localhost/river_test")
    admin = PG.connect(url)
    admin.exec("CREATE SCHEMA #{schema}")
    admin.exec("SET search_path = #{schema}")
    if @adapter == :activerecord
      ActiveRecord::Base.establish_connection(url: url, schema_search_path: schema)
      driver = River::Driver::ActiveRecord.new
    else
      db = Sequel.connect(url, search_path: schema)
      driver = River::Driver::Sequel.new(db)
    end
    River::Migrator.new(driver).migrate
    driver.init_driver
    # Keep the chosen statistics stable for both runs, in our disposable table.
    admin.exec("ALTER TABLE river_job SET (autovacuum_enabled = false)")
    admin.exec(<<~SQL)
      INSERT INTO river_job (id, kind, args)
      SELECT number, 'benchmark', '{}'::jsonb FROM generate_series(1, #{@jobs}) AS number
      ORDER BY (number::bigint * 7919) % #{@jobs}, number
    SQL
    admin.exec("ANALYZE river_job")
    # Claims made after ANALYZE are absent from the state histogram, as during
    # a burst of work. Physical ID order is deliberately unclustered above.
    admin.exec("UPDATE river_job SET state = 'running' WHERE id > #{@jobs - @running}")
    sql = nil
    driver.define_singleton_method(:runtime_query_rows) do |statement|
      sql = statement if statement.start_with?("UPDATE river_job SET state = 'completed'")
      super(statement)
    end
    next_id = 1
    samples = Array.new(@samples) do
      elapsed = 0.0
      @iterations.times do
        claimed = driver.job_claim(id: next_id, attempted_by: "benchmark")
        raise "claim failed" unless claimed
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        completed = driver.job_complete(id: next_id, finalized_at: Time.now.utc)
        elapsed += Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        raise "completion failed" unless completed&.state == "completed"
        next_id += 1
      end
      elapsed * 1_000_000 / @iterations
    end
    admin.exec("BEGIN")
    admin.exec("UPDATE river_job SET state = 'running', finalized_at = NULL WHERE id = #{next_id - 1}")
    plan = JSON.parse(admin.exec("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) #{sql}").getvalue(0, 0))
    admin.exec("ROLLBACK")
    {iterations: @iterations, jobs: @jobs, median_us: samples.sort[@samples / 2], plan: plan,
     postgres: admin.server_version, running_jobs: @running, samples_us: samples, sql: sql}
  ensure
    ActiveRecord::Base.connection_pool.disconnect! if @adapter == :activerecord && driver
    db&.disconnect
    if admin
      admin.exec("ROLLBACK") unless admin.transaction_status == PG::PQTRANS_IDLE
      admin.exec("DROP SCHEMA #{schema} CASCADE")
      admin.close
    end
  end
end

results = %i[activerecord sequel].to_h do |adapter|
  warn "Benchmarking #{adapter} completion"
  [adapter, CompletionBenchmark.new(adapter).run]
end
puts JSON.pretty_generate(source_root: root, ruby: RUBY_DESCRIPTION, results: results)
