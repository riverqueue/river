# frozen_string_literal: true

# Run from ruby/rails/riverqueue-rails with its bundle, against a disposable
# Postgres server started with shared_preload_libraries=pg_stat_statements:
#   bundle exec ruby ../../bench/drain.rb --database-url postgres://localhost:55432/river_bench
# To compare checkouts, use each checkout's Rails Gemfile as BUNDLE_GEMFILE
# while running this same script. Alternate baseline and candidate runs.
# Insertion, migration, and worker boot are outside the measured drain.
# SQL statistics are reset server-wide, so use a dedicated benchmark server.

require "bundler/setup"
require "json"
require "logger"
require "open3"
require "optparse"
require "rbconfig"
require "securerandom"
require "timeout"
require "pg"
require "riverqueue-rails"

class DrainJob < ActiveJob::Base
  class << self
    attr_accessor :executions, :mutex
  end

  # The only work is an in-memory audit of actual Active Job deliveries.
  def perform(number)
    self.class.mutex.synchronize { self.class.executions << number }
  end
end

def connect(options, schema)
  ActiveRecord::Base.establish_connection(
    url: options.fetch(:database_url), pool: options.fetch(:threads) + 6,
    schema_search_path: "#{schema},public"
  )
  River::Driver::ActiveRecord.new
end

def monotonic_now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def postgres_cpu(postmaster)
  output, status = Open3.capture2("ps", "-axo", "pid=,ppid=,time=")
  raise "ps failed" unless status.success?

  output.lines.sum do |line|
    pid, parent, elapsed = line.split
    next 0.0 unless pid.to_i == postmaster || parent.to_i == postmaster

    days, clock = elapsed.include?("-") ? elapsed.split("-", 2) : ["0", elapsed]
    days.to_i * 86_400 + clock.split(":").map(&:to_f).reduce { |total, part| total * 60 + part }
  end
end

def run_worker(options, schema, command_read, result_write)
  result_write.sync = true
  status = 0
  begin
    driver = connect(options, schema)
    DrainJob.executions = []
    DrainJob.mutex = Mutex.new
    client = River::Client.new(driver, config: River::Config.new(
      fetch_cooldown: options.fetch(:cooldown), fetch_poll_interval: 1.0,
      logger: Logger.new(File::NULL), queues: {"default" => options.fetch(:threads)},
      workers: River::Workers.new.add(River::Rails::Worker)
    ))
    result_write.puts("ready")
    raise "missing start command" unless command_read.gets&.strip == "start"

    before = Process.times
    client.start
    raise "missing stop command" unless command_read.gets&.strip == "stop"

    after = Process.times
    client.stop
    result_write.puts(JSON.generate(cpu_seconds: after.utime + after.stime - before.utime - before.stime,
      executions: DrainJob.executions))
  rescue => error
    result_write.puts(JSON.generate(error: error.full_message))
    status = 1
  ensure
    client&.stop_and_cancel
    ActiveRecord::Base.connection_pool.disconnect!
    result_write.close
    command_read.close
  end
  status
end

def start_worker(options, schema)
  command_read, command_write = IO.pipe
  command_write.sync = true
  result_read, result_write = IO.pipe
  # Boot fresh interpreters: forking a process that has used libpq can crash
  # in native connection setup on macOS, even after its connections close.
  pid = Process.spawn({"RIVER_DRAIN_WORKER" => JSON.generate(options: options, schema: schema)},
    RbConfig.ruby, File.expand_path(__FILE__), in: command_read, out: result_write)
  command_read.close
  result_write.close
  {command: command_write, pid: pid, result: result_read}
end

if ENV["RIVER_DRAIN_WORKER"]
  worker = JSON.parse(ENV.fetch("RIVER_DRAIN_WORKER"))
  ActiveJob::Base.logger = Logger.new(File::NULL)
  exit run_worker(worker.fetch("options").transform_keys(&:to_sym), worker.fetch("schema"), $stdin, $stdout)
end

options = {cooldown: 0.001, jobs: 50_000, label: "candidate", processes: 8, threads: 20, timeout: 600.0}
OptionParser.new do |parser|
  parser.on("--cooldown SECONDS", Float) { |value| options[:cooldown] = value }
  parser.on("--database-url URL") { |value| options[:database_url] = value }
  parser.on("--jobs COUNT", Integer) { |value| options[:jobs] = value }
  parser.on("--label LABEL") { |value| options[:label] = value }
  parser.on("--processes COUNT", Integer) { |value| options[:processes] = value }
  parser.on("--threads COUNT", Integer) { |value| options[:threads] = value }
  parser.on("--timeout SECONDS", Float) { |value| options[:timeout] = value }
end.parse!
raise "--database-url is required (use a dedicated benchmark server)" unless options[:database_url]
raise "jobs, processes, threads, and timeout must be positive" unless options.values_at(:jobs, :processes, :threads, :timeout).all?(&:positive?)

ActiveJob::Base.logger = Logger.new(File::NULL)
schema = "river_drain_#{SecureRandom.hex(8)}"
workers = []
begin
  admin = PG.connect(options.fetch(:database_url))
  admin.exec("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
  admin.exec("SET pg_stat_statements.track = 'none'")
  admin.exec("CREATE SCHEMA #{schema}")
  admin.exec("SET search_path = #{schema},public")
  driver = connect(options, schema)
  River::Migrator.new(driver).migrate
  client = River::Client.new(driver)
  adapter = ActiveJob::QueueAdapters::RiverAdapter.new(client: client)
  options.fetch(:jobs).times.each_slice(1_000) do |numbers|
    adapter.enqueue_all(numbers.map { |number| DrainJob.new(number) })
  end
  admin.exec("VACUUM ANALYZE river_job")
  ActiveRecord::Base.connection_pool.disconnect!
  admin.close

  options.fetch(:processes).times { workers << start_worker(options, schema) }
  Timeout.timeout(60) do
    workers.each do |worker|
      ready = worker.fetch(:result).gets&.strip
      raise "worker failed to start: #{ready}" unless ready == "ready"
    end
  end
  admin = PG.connect(options.fetch(:database_url))
  admin.exec("SET pg_stat_statements.track = 'none'")
  admin.exec("SET search_path = #{schema},public")
  backend = admin.exec("SELECT pg_backend_pid()").getvalue(0, 0)
  postmaster = Open3.capture2("ps", "-o", "ppid=", "-p", backend).first.to_i
  raise "could not find Postgres postmaster" unless postmaster.positive?

  admin.exec("SELECT pg_stat_statements_reset()")
  postgres_before = postgres_cpu(postmaster)
  started = monotonic_now
  workers.each { |worker| worker.fetch(:command).puts("start") }
  loop do
    if (ready = IO.select(workers.map { |worker| worker.fetch(:result) }, nil, nil, 0))
      raise "worker exited during drain: #{ready.first.first.gets}"
    end
    counts = admin.exec("SELECT state, count(*) FROM river_job GROUP BY state").to_h { |row| [row.fetch("state"), row.fetch("count").to_i] }
    break if counts["completed"] == options.fetch(:jobs)
    raise "jobs failed: #{counts}" if counts.keys.intersect?(["cancelled", "discarded", "retryable"])
    raise "drain timed out: #{counts}" if monotonic_now - started > options.fetch(:timeout)

    sleep 0.05
  end
  elapsed = monotonic_now - started
  postgres_seconds = postgres_cpu(postmaster) - postgres_before
  statements = admin.exec(<<~SQL).map do |row|
    SELECT query, calls, total_exec_time, shared_blks_hit, shared_blks_read
    FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    ORDER BY calls DESC
  SQL
    row.transform_values.with_index { |value, index| index.zero? ? value : value.to_f }
  end
  workers.each { |worker| worker.fetch(:command).puts("stop") }
  results = Timeout.timeout(60) do
    workers.map do |worker|
      result = JSON.parse(worker.fetch(:result).readline)
      raise result.fetch("error") if result.key?("error")

      Process.wait(worker.fetch(:pid))
      worker[:reaped] = true
      raise "worker exited unsuccessfully" unless $?.success?

      result
    end
  end
  executions = results.flat_map { |result| result.fetch("executions") }
  raise "missing or duplicate deliveries" unless executions.sort == (0...options.fetch(:jobs)).to_a
  raise "retried jobs" unless admin.exec("SELECT count(*) FROM river_job WHERE attempt <> 1").getvalue(0, 0) == "0"

  ruby_seconds = results.sum { |result| result.fetch("cpu_seconds") }
  calls = statements.sum { |row| row.fetch("calls") }
  puts JSON.generate(options.except(:database_url).merge(
    active_record_version: ActiveRecord.gem_version.to_s, ruby_version: RUBY_VERSION, postgres_version: admin.server_version,
    elapsed_seconds: elapsed, jobs_per_second: options.fetch(:jobs) / elapsed, exactly_once: true,
    ruby_cpu_ms_per_job: ruby_seconds * 1_000 / options.fetch(:jobs),
    postgres_cpu_ms_per_job: postgres_seconds * 1_000 / options.fetch(:jobs),
    statements_per_job: calls / options.fetch(:jobs), statements: statements
  ))
ensure
  workers.each do |worker|
    unless worker[:reaped]
      begin
        Process.kill("TERM", worker.fetch(:pid))
        Process.wait(worker.fetch(:pid))
      rescue Errno::ESRCH, Errno::ECHILD
        # The worker may have exited before cleanup.
      end
    end
    worker.fetch(:command).close
    worker.fetch(:result).close
  end
  ActiveRecord::Base.connection_handler.clear_all_connections!
  admin = PG.connect(options.fetch(:database_url)) if !admin || admin.finished?
  admin.exec("DROP SCHEMA IF EXISTS #{schema} CASCADE")
  admin.close
end
