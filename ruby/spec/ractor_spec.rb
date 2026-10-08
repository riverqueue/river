# frozen_string_literal: true

require "spec_helper"
require "open3"

RSpec.describe "Ractor compatibility" do
  it "keeps core constants shareable across Ractor boundaries" do
    values = [
      River::JOB_STATE_AVAILABLE,
      River::JOB_STATE_CANCELLED,
      River::JOB_STATE_COMPLETED,
      River::JOB_STATE_DISCARDED,
      River::JOB_STATE_PENDING,
      River::JOB_STATE_RETRYABLE,
      River::JOB_STATE_RUNNING,
      River::JOB_STATE_SCHEDULED,
      River::QUEUE_DEFAULT,
      River::RESUMABLE_CURSOR_METADATA_KEY,
      River::RESUMABLE_STEP_METADATA_KEY,
      River::Client.const_get(:DEFAULT_UNIQUE_STATES, false),
      River::Client.const_get(:REQUIRED_UNIQUE_STATES, false),
      River::Client.const_get(:EMPTY_INSERT_OPTS, false),
      River::Client.const_get(:TAG_RE, false),
      River::UniqueBitmask.const_get(:JOB_STATE_BIT_POSITIONS, false),
      River::Job.const_get(:RESUMABLE_CURSOR_UNSET, false),
      River::JobUpdateParams::UNSET,
      River::QUEUE_NAME_REGEX,
      River::EVENT_JOB_COMPLETED
    ]

    expect(values).to all(satisfy { |value| Ractor.shareable?(value) })
  end

  # A fresh process avoids test instrumentation and ensures a main-Ractor call
  # hasn't already warmed up a lazily initialized dependency. Bound the whole
  # subprocess so a broken producer or Ractor cannot hang the test suite.
  def in_ractor(body)
    script = <<~RUBY
      require_relative "spec/support/ractor_test_driver"
      begin
        ractors = Array.new(2) do
          Ractor.new do
            #{body}
          end
        end
        ractors.each do |ractor|
          result = ractor.respond_to?(:value) ? ractor.value : ractor.take
          raise "unexpected result: \#{result.inspect}" unless result == :ok
        end
      rescue Exception => error
        warn error.full_message
        Process.exit!(1)
      end

      # Ruby 4.0.2 can deadlock tearing down Ractors' Timeout helper threads.
      # Both results are verified before bypassing VM shutdown in this process.
      Process.exit!(0)
    RUBY
    Open3.popen2e(RbConfig.ruby, "-Ilib", "-e", script) do |input, output, process|
      input.close
      begin
        result = Timeout.timeout(15) { output.read }
        expect(process.value.success?).to be(true), result
      ensure
        Process.kill("KILL", process.pid) if process.alive?
        process.join
      end
    end
  end

  it "reports Ractor exceptions instead of treating subprocess exit as success" do
    expect { in_ractor('raise "ractor failure sentinel"') }
      .to raise_error(RSpec::Expectations::ExpectationNotMetError, /ractor failure sentinel/)
  end

  it "requires successful results before exiting the subprocess" do
    expect { in_ractor(":unexpected") }
      .to raise_error(RSpec::Expectations::ExpectationNotMetError, /unexpected result: :unexpected/)
  end

  it "inserts and computes unique keys on first use in a non-main Ractor" do
    in_ractor <<~RUBY
      client = River::Client.new(RactorTestDriver.new)
      args = River::JobArgsHash.new(:ractor_test, value: 1)
      row = client.insert(args, insert_opts: River::InsertOpts.new(
        unique_opts: River::UniqueOpts.new(by_args: true, by_queue: true)
      )).job
      raise "args" unless row.args == {"value" => 1}
      expected = Digest::SHA256.digest('&kind=ractor_test&args={"value":1}&queue=default')
      raise "unique key" unless row.unique_key == expected
      raise "unique states" unless row.unique_states == "11110101"
      raise "insert many" unless client.insert_many([args, args]).length == 2
      :ok
    RUBY
  end

  it "executes resumable work, retries, and events with Ractor-local state" do
    in_ractor <<~'RUBY'
      worker = Object.new
      def worker.work(job)
        job.resumable_step(:download) { job.update_metadata("downloaded" => true) }
        job.resumable_step_cursor(:rows, default: {"id" => 0}) do |cursor|
          if cursor.fetch("id") == 0
            job.resumable_checkpoint(cursor: {id: 1})
            job.resumable_set_cursor(id: 2)
            raise "retry me"
          end
          raise "cursor" unless cursor == {"id" => 2}
          job.output = "done"
        end
      end
      config = River::Config.new(workers: River::Workers.new.add(:ractor_test, worker), job_timeout: nil)
      client = River::Client.new(RactorTestDriver.new, config: config)
      events = client.subscribe(:job_failed, :job_completed)
      row = client.insert(River::JobArgsHash.new(:ractor_test, {})).job
      result = River::Testing.perform_job(client, row.id)
      raise "retry: #{result.error.inspect}" unless result.outcome == :retried && result.error.message == "retry me"
      raise "failed event" unless events.pop(true).kind == :job_failed
      result = River::Testing.perform_job(client, row.id, allow_scheduled: true)
      raise "completion: #{result.error.inspect}" unless result.outcome == :completed
      raise "output" unless result.job.metadata["output"] == "done"
      raise "completed event" unless events.pop(true).kind == :job_completed
      events.close
      :ok
    RUBY
  end

  it "starts producer/maintenance threads and stops within a non-main Ractor" do
    in_ractor <<~'RUBY'
      config = River::Config.new(
        workers: River::Workers.new.add(RactorTestWorker), queues: {default: 1},
        fetch_cooldown: 0.001, fetch_poll_interval: 0.01, job_timeout: nil,
        periodic_jobs: [River::PeriodicJob.new(
          schedule: River::PeriodicInterval.new(3600), run_on_start: true,
          constructor: -> { River::JobArgsHash.new(:ractor_test, value: 21) }
        )]
      )
      client = River::Client.new(RactorTestDriver.new, config: config)
      events = client.subscribe(:job_completed, :job_failed)
      begin
        client.start
        event = events.pop
        raise "work failed: #{event.job.errors.inspect}" unless event.kind == :job_completed
        raise "output" unless event.job.metadata["output"] == 42
      ensure
        client.stop
        events.close
      end
      raise "did not stop" unless client.stopped?
      :ok
    RUBY
  end

  it "persists attempt logs with Ractor-local loggers and buffers" do
    in_ractor <<~'RUBY'
      worker = Object.new
      def worker.work(job)
        job.logger.info "hello from a Ractor"
      end
      config = River::Config.new(
        workers: River::Workers.new.add(:ractor_test, worker), job_timeout: nil,
        plugins: [River::JobPersistedLogging::Plugin.new]
      )
      client = River::Client.new(RactorTestDriver.new, config: config)
      row = client.insert(River::JobArgsHash.new(:ractor_test, {})).job
      result = River::Testing.perform_job(client, row.id)
      raise "work failed: #{result.error.inspect}" unless result.outcome == :completed
      log = result.job.metadata.fetch("river:log").fetch(0)
      raise "attempt" unless log.fetch("attempt") == 1
      raise "log" unless log.fetch("log").include?("hello from a Ractor")
      :ok
    RUBY
  end

  it "enforces job timeouts inside a non-main Ractor on Ruby 4+" do
    skip "timeout gem's Ractor support requires Ruby 4+" if RUBY_VERSION.to_i < 4

    in_ractor <<~'RUBY'
      worker = Object.new
      def worker.work(_job) = sleep(60)
      config = River::Config.new(workers: River::Workers.new.add(:ractor_test, worker), job_timeout: 0.01)
      client = River::Client.new(RactorTestDriver.new, config: config)
      row = client.insert(River::JobArgsHash.new(:ractor_test, {})).job
      result = River::Testing.perform_job(client, row.id)
      raise "timeout: #{result.error.inspect}" unless result.error.is_a?(Timeout::Error) && result.outcome == :retried
      :ok
    RUBY
  end

  it "rejects the signal-handling runner outside the main Ractor" do
    in_ractor <<~RUBY
      client = River::Client.new(RactorTestDriver.new, config: River::Config.new(queues: {default: 1}))
      begin
        River::WorkerRunner.new(client).run
        raise "runner should reject a non-main Ractor"
      rescue ArgumentError => error
        raise unless error.message.include?("main Ractor")
      end
      :ok
    RUBY
  end
end
