# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe River::JobPersistedLogging::Plugin do
  let(:diagnostics) { StringIO.new }
  let(:client) { River::Client.new(Object.new, config: River::Config.new(logger: Logger.new(diagnostics))) }
  let(:row) do
    River::JobRow.new(id: 123, args: {}, attempt: 1, created_at: Time.now.utc,
      kind: "logging", max_attempts: 3, metadata: {}, priority: 1,
      queue: "default", scheduled_at: Time.now.utc, state: River::JOB_STATE_RUNNING)
  end
  let(:job) { River::Job.new(client, row) }

  def capture(text, job: self.job, **options)
    described_class.new(**options).work(job, -> { job.logger << text })
    job.metadata.fetch("river:log")
  end

  it "exposes a standard INFO logger without replacing the client's logger" do
    result = described_class.new.work(job, -> do
      expect(job.logger).to be_a(Logger)
      job.logger.debug { raise "debug should not be evaluated" }
      job.logger.info "hello"
      job.output = 42
      :result
    end)

    expect(result).to eq(:result)
    expect(job.metadata.fetch("river:log")).to match([
      {"attempt" => 1, "log" => a_string_including("INFO", "hello")}
    ])
    expect(job.metadata.fetch("output")).to eq(42)
    expect(row.metadata).to eq({})
    expect(diagnostics.string).to eq("")
  end

  it "requires the plugin and removes the logger after work" do
    expect { job.logger }.to raise_error(River::Error, /JobPersistedLogging::Plugin/)
    capture("hello")
    expect { job.logger }.to raise_error(River::Error, /JobPersistedLogging::Plugin/)
  end

  it "supports a fresh custom logger per attempt and restores a previous logger" do
    writers = []
    plugin = described_class.new do |writer|
      writers << writer
      Logger.new(writer, formatter: ->(_severity, _time, _progname, message) { JSON.generate(message: message) + "\n" })
    end
    outer = Object.new
    job.__with_logger(outer) do
      2.times { plugin.work(job, -> { job.logger.info "hello" }) }
      expect(job.logger).to equal(outer)
    end

    expect(writers.uniq.length).to eq(2)
    expect(job.metadata.fetch("river:log").map { |entry| entry.fetch("log") }).to eq(["{\"message\":\"hello\"}\n"] * 2)
  end

  it "persists logs without replacing the original exception" do
    [RuntimeError.new("broken"), River.job_cancel("cancel"), River.job_snooze(10),
      River::ClientRuntime::Interrupted.new, Timeout::Error.new, Exception.new("fatal")].each do |error|
      attempt = River::Job.new(client, row)
      expect do
        described_class.new.work(attempt, -> do
          attempt.logger << "before failure\n"
          raise error
        end)
      end.to raise_error { |raised| expect(raised).to equal(error) }

      expect(attempt.metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "before failure\n"}])
      expect { attempt.logger }.to raise_error(River::Error, /JobPersistedLogging::Plugin/)
    end
  end

  it "does not create or alter log metadata when nothing was logged" do
    described_class.new.work(job, -> {})
    expect(job.metadata_updates).to eq({})

    row.metadata["river:log"] = [{"attempt" => 0, "log" => "old"}]
    described_class.new.work(job, -> { job.logger.debug "filtered" })
    expect(job.metadata_updates).to eq({})
    expect(row.metadata.fetch("river:log")).to eq([{"attempt" => 0, "log" => "old"}])
  end

  it "keeps the first bytes up to the per-attempt limit" do
    expect(capture("123456789", max_size_bytes: 4)).to eq([{"attempt" => 1, "log" => "1234"}])
    expect(diagnostics.string).to include("truncated to 4 bytes", "job 123")
  end

  it "drops incomplete UTF-8, invalid byte sequences, and NULs" do
    expect(capture("aé", max_size_bytes: 2).last.fetch("log")).to eq("a")
    expect(capture("é\0\xFFz".b).last.fetch("log")).to eq("éz")
    expect(capture("é", max_size_bytes: 2).last.fetch("log")).to eq("é")
  end

  it "appends to Go-format history without mutating existing entries or unrelated metadata" do
    previous = {"attempt" => 1, "log" => "old"}.freeze
    row.metadata = {"river:log" => [previous].freeze, "other" => "keep"}.freeze
    row.attempt = 2

    expect(capture("new")).to eq([previous, {"attempt" => 2, "log" => "new"}])
    expect(row.metadata.fetch("river:log")).to eq([previous])
    expect(job.metadata.fetch("other")).to eq("keep")
  end

  it "drops the oldest entries based on serialized JSON bytes, including escaping" do
    history = (1..3).map { |attempt| {"attempt" => attempt, "log" => "é\"\n"} }
    row.metadata["river:log"] = history
    row.attempt = 4
    newest = {"attempt" => 4, "log" => "é\"\n"}
    expected = [history.last, newest]

    expect(capture(newest.fetch("log"), max_total_bytes: JSON.generate(expected).bytesize)).to eq(expected)
    expect(diagnostics.string).to include("dropped 2 oldest entries")
    expect(history.length).to eq(3)
  end

  it "retains the newest entry even if that entry alone exceeds the history cap" do
    row.metadata["river:log"] = [{"attempt" => 0, "log" => "old"}]
    expect(capture("latest", max_total_bytes: 1)).to eq([{"attempt" => 1, "log" => "latest"}])
  end

  it "validates limits and caps the total history setting at 64 MiB" do
    %i[max_size_bytes max_total_bytes].product([nil, 0, -1, 1.5, Float::INFINITY, "2"]).each do |option, value|
      expect { described_class.new(**{option => value}) }.to raise_error(ArgumentError, /positive integer/)
    end
    expect(described_class.new.instance_variable_get(:@max_size_bytes)).to eq(2 * 1024 * 1024)
    expect(described_class.new.instance_variable_get(:@max_total_bytes)).to eq(8 * 1024 * 1024)
    expect(described_class.new(max_total_bytes: 100 * 1024 * 1024).instance_variable_get(:@max_total_bytes)).to eq(64 * 1024 * 1024)
  end

  it "reports malformed history without losing worker errors or overwriting metadata" do
    nested = 150.times.reduce(nil) { |value, _| [value] }
    [nil, {}, "bad", [{"log" => Float::NAN}], [nested]].each do |history|
      row.metadata = {"river:log" => history}
      attempt = River::Job.new(client, row)
      error = RuntimeError.new("worker failed")
      expect do
        described_class.new.work(attempt, -> do
          attempt.logger << "hello"
          raise error
        end)
      end.to raise_error { |raised| expect(raised).to equal(error) }

      expect(attempt.metadata_updates).to eq({})
    end
    expect(diagnostics.string).to include("could not be persisted", "job 123")
  end

  it "does not capture writes made after an attempt finishes or its logger closes" do
    logger = nil
    described_class.new.work(job, -> do
      logger = job.logger
      logger << "hello"
      logger.close
      logger << "closed"
    end)
    logger << "late"
    expect(job.metadata.fetch("river:log")).to eq([{"attempt" => 1, "log" => "hello"}])
  end

  it "bounds capture memory and supports concurrent writes" do
    writer = described_class.const_get(:Buffer, false).new(100)
    threads = 4.times.map do
      Thread.new { 100.times { expect(writer.write("abc")).to eq(3) } }
    end
    threads.each(&:value)

    expect(writer.instance_variable_get(:@data).bytesize).to eq(100)
    expect(writer.finish).to eq([("abc" * 34).byteslice(0, 100), true])
    expect(writer.write("ignored")).to eq(7)
    expect(writer.finish.first.bytesize).to eq(100)
  end

  it "keeps simultaneous attempts isolated when sharing a plugin" do
    plugin = described_class.new
    attempts = Array.new(4) { River::Job.new(client, row) }
    threads = attempts.each_with_index.map do |attempt, index|
      Thread.new { plugin.work(attempt, -> { 10.times { attempt.logger << "job #{index}\n" } }) }
    end
    threads.each(&:value)

    attempts.each_with_index do |attempt, index|
      expect(attempt.metadata.fetch("river:log").last.fetch("log")).to eq("job #{index}\n" * 10)
    end
  end
end
