# frozen_string_literal: true

require "open3"
require "timeout"
require "tmpdir"

RSpec.shared_examples "dedicated worker process" do
  def process_boot
    if @driver.respond_to?(:connection_class)
      options = @driver.connection_class.connection_db_config.configuration_hash
      <<~RUBY
        require "riverqueue-activerecord"
        ActiveRecord::Base.establish_connection(#{options.inspect})
        driver = River::Driver::ActiveRecord.new
      RUBY
    else
      options = @driver.instance_variable_get(:@db).opts.slice(:adapter, :database, :host, :port, :user, :password, :search_path, :max_connections, :timeout)
      <<~RUBY
        require "riverqueue-sequel"
        driver = River::Driver::Sequel.new(Sequel.connect(#{options.inspect}))
      RUBY
    end
  end

  def with_worker_process(stop_timeout: 30)
    Dir.mktmpdir("river-worker-process-") do |directory|
      path = File.join(directory, "river.rb")
      File.write(path, process_boot + <<~RUBY)
        class ProcessTestWorker
          def self.kind = "process_test"
          def timeout(_job) = nil
          def work(job)
            puts "attempt entered"
            $stdout.flush
            sleep(60) if job.args["block"]
            job.output = {"worked" => true}
          end
        end
        River::Client.new(driver, config: River::Config.new(
          fetch_cooldown: 0.001,
          fetch_poll_interval: 0.01,
          queues: {"process_test" => 1},
          workers: River::Workers.new.add(ProcessTestWorker)
        ))
      RUBY
      command = File.expand_path("../exe/river", __dir__)
      Open3.popen2e(RbConfig.ruby, command, "worker", "--config", path, "--stop-timeout", stop_timeout.to_s) do |input, output, process|
        input.close
        begin
          yield output, process
        ensure
          Process.kill("KILL", process.pid) if process.alive?
          process.join
        end
      end
    end
  end

  def await_output(output, text)
    seen = +""
    Timeout.timeout(10) do
      loop do
        line = output.gets
        raise "Worker exited before #{text.inspect}: #{seen}" unless line
        seen << line
        return seen if line.include?(text)
      end
    end
  end

  it "boots, executes a real job, handles TSTP, and exits cleanly after TERM" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new("process_test", {}), insert_opts: River::InsertOpts.new(queue: "process_test")).job
    with_worker_process do |output, process|
      await_output(output, "ready pid=")
      Timeout.timeout(10) do
        sleep(0.01) until client.job_get(row.id).state == River::JOB_STATE_COMPLETED
      end

      Process.kill("TSTP", process.pid)
      await_output(output, "stop requested")
      expect(process.alive?).to be true
      Process.kill("TERM", process.pid)
      expect(Timeout.timeout(10) { process.value.exitstatus }).to eq(0)
      expect(client.job_get(row.id)).to have_attributes(
        attempt: 1,
        metadata: include("output" => {"worked" => true}),
        state: River::JOB_STATE_COMPLETED
      )
    end
  end

  it "interrupts an active attempt at the deadline and makes it available again" do
    client = River::Client.new(@driver)
    row = client.insert(River::JobArgsHash.new("process_test", {"block" => true}), insert_opts: River::InsertOpts.new(queue: "process_test")).job
    with_worker_process(stop_timeout: 0) do |output, process|
      await_output(output, "attempt entered")
      Process.kill("INT", process.pid)
      expect(Timeout.timeout(10) { process.value.exitstatus }).to eq(1)
      expect(output.read).to include("interrupting active attempts", "stopped")
      expect(client.job_get(row.id)).to have_attributes(attempt: 0, state: River::JOB_STATE_AVAILABLE)
    end
  end
end
