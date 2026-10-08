# frozen_string_literal: true

require "spec_helper"
require "stringio"
require "tmpdir"
require_relative "../lib/cli"
require_relative "support/runner_test_client"

RSpec.describe River::CLI do
  let(:err) { StringIO.new }
  let(:out) { StringIO.new }

  def run(*args)
    described_class.run(args, err: err, out: out)
  end

  it "prints command and worker help without booting an application" do
    expect(run).to eq(0)
    expect(run("--help")).to eq(0)
    expect(run("-h")).to eq(0)
    expect(run("worker", "--help")).to eq(0)
    expect(out.string).to include("migrate-up", "--stop-timeout", "--rails")
    expect(err.string).to eq("")
  end

  it "dispatches migration commands to the migration CLI" do
    expect(run("migrate-status", "--help")).to eq(0)
    expect(out.string).to include("--database-url")
    expect(run("unknown")).to eq(1)
  end

  it "rejects missing, conflicting, and unknown worker options" do
    [[], ["--rails", "--config", "unused"], ["--rails", "extra"], ["--shutdown-timeout", "1"], ["--stop-timeout", "bad"]].each do |options|
      expect(run("worker", *options)).to eq(1)
    end
    expect(err.string).to include("Worker failed")
  end

  it "evaluates trusted config files and supports default and explicit stop timeouts" do
    Dir.mktmpdir("river-cli-") do |directory|
      path = File.join(directory, "river.rb")
      File.write(path, "RunnerTestClient.new")
      expect(run("worker", "--config", path)).to eq(0)
      expect(run("worker", "--config", path, "--stop-timeout", "2")).to eq(0)
      expect(run("worker", "--config", path, "--stop-timeout", "-1")).to eq(1)

      File.write(path, "Object.new")
      expect(run("worker", "--config", path)).to eq(1)
      expect(err.string).to include("configuration must return a River::Client")
      File.write(path, "invalid ruby (")
      expect(run("worker", "--config", path)).to eq(1)
      expect(err.string).to include("SyntaxError")
      expect(run("worker", "--config", File.join(directory, "missing.rb"))).to eq(1)
    end
  end

  it "boots the Rails environment before loading and invoking the optional integration" do
    Dir.mktmpdir("river-cli-rails-") do |directory|
      Dir.mkdir(File.join(directory, "config"))
      environment = File.join(directory, "config/environment.rb")
      integration = File.join(directory, "riverqueue-rails.rb")
      File.write(environment, "module River; module Rails; end; end")
      File.write(integration, <<~'RUBY')
        raise "environment not booted" unless defined?(River::Rails)
        class River::Rails::Runner
          def self.start(out:, stop_timeout:)
            out.puts("Rails timeout: #{stop_timeout.inspect}")
            0
          end
        end
      RUBY
      $LOAD_PATH.unshift(directory)
      Dir.chdir(directory) do
        expect(run("worker", "--rails")).to eq(0)
        expect(run("worker", "--rails", "--stop-timeout", "2")).to eq(0)
      end
      expect(out.string).to include("Rails timeout: nil", "Rails timeout: 2.0")
    ensure
      $LOAD_PATH.delete(directory)
      $LOADED_FEATURES.delete(environment)
      $LOADED_FEATURES.delete(integration)
      River.send(:remove_const, :Rails)
    end
  end
end
