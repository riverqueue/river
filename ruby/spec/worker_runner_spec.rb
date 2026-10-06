# frozen_string_literal: true

require "spec_helper"
require "stringio"

require_relative "support/runner_test_client"

RSpec.describe River::WorkerRunner do
  let(:out) { StringIO.new }

  it "validates deadlines, client state, queues, and the main thread" do
    [-1, Float::INFINITY, Float::NAN].each do |invalid|
      expect { described_class.new(RunnerTestClient.new, stop_timeout: invalid) }.to raise_error(ArgumentError)
      expect { described_class.new(RunnerTestClient.new, finalization_timeout: invalid) }.to raise_error(ArgumentError)
    end

    client = RunnerTestClient.new
    client.define_singleton_method(:started?) { true }
    expect { described_class.new(client, out: out).run }.to raise_error(ArgumentError, /stopped client/)
    expect { described_class.new(River::Client.new(Object.new), out: out).run }.to raise_error(ArgumentError, /queue/)

    error = Thread.new do
      described_class.new(RunnerTestClient.new, out: out).run
    rescue ArgumentError => e
      e
    end.value
    expect(error.message).to include("main thread")
  end

  %w[INT TERM].each do |signal|
    it "drains after #{signal} and restores the previous signal handler" do
      handler = proc {}
      previous = Signal.trap(signal, handler)
      expect(described_class.new(RunnerTestClient.new(signals: [signal]), out: out).run).to eq(0)
      expect(Signal.trap(signal, handler)).to equal(handler)
      expect(out.string).to include("starting", "ready pid=", "draining", "stopped")
    ensure
      Signal.trap(signal, previous)
    end
  end

  it "handles TSTP followed by TERM through nonblocking stop" do
    client = RunnerTestClient.new(signals: %w[TSTP TERM])
    expect(described_class.new(client, out: out).run).to eq(0)
    expect(out.string).to include("stop requested", "stopped")
  end

  it "interrupts attempts when the grace period expires" do
    client = RunnerTestClient.new(stall: true)
    expect(described_class.new(client, out: out, stop_timeout: 0).run).to eq(1)
    expect(client.interruptions).to eq(1)
    expect(out.string).to include("interrupting active attempts", "stopped")
  end

  it "escalates a second stop signal before the deadline" do
    client = RunnerTestClient.new(signals: %w[TERM INT], stall: true)
    expect(described_class.new(client, out: out, stop_timeout: 60).run).to eq(1)
    expect(client.interruptions).to eq(1)
  end

  it "stops unsuccessfully when a runtime thread exits, without a signal" do
    client = RunnerTestClient.new(healthy: false, signals: [])
    expect(described_class.new(client, out: out).run).to eq(1)
    expect(out.string).to include("runtime thread exited unexpectedly")
  end

  it "continues waiting while healthy and no stop has been requested" do
    client = RunnerTestClient.new(signals: [])
    polls = 0
    client.define_singleton_method(:__runtime_healthy?) do
      polls += 1
      Process.kill("TERM", Process.pid) if polls == 2
      true
    end
    expect(described_class.new(client, out: out).run).to eq(0)
    expect(polls).to be >= 3
  end

  it "forces process exit if interrupted work cannot finalize" do
    client = RunnerTestClient.new(stall: true, unresponsive: true)
    exits = []
    forced_exit = Class.new(StandardError)
    original_exit = Process.method(:exit!)
    Process.define_singleton_method(:exit!) do |status|
      exits << status
      raise forced_exit
    end
    expect { described_class.new(client, finalization_timeout: 0, out: out, stop_timeout: 0).run }.to raise_error(forced_exit)
    expect(exits).to eq([1])
    expect(out.string).to include("forcing process exit")
  ensure
    Process.define_singleton_method(:exit!, original_exit)
    client.release
    client.stop_thread&.join
  end

  it "restores signal handlers on boot failure" do
    client = RunnerTestClient.new
    client.define_singleton_method(:job_list) { |_params| raise "database unavailable" }
    handler = proc {}
    previous = Signal.trap("TERM", handler)
    expect { described_class.new(client, out: out).run }.to raise_error("database unavailable")
    expect(Signal.trap("TERM", handler)).to equal(handler)
  ensure
    Signal.trap("TERM", previous)
  end
end
