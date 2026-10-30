# frozen_string_literal: true

require "spec_helper"
require "open3"
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
    expect(out.string).to include("migrate-up", "validate", "version", "--stop-timeout", "--rails")
    expect(err.string).to eq("")
  end

  describe "version" do
    it "includes the activated Pro version without loading Pro" do
      original = Gem.loaded_specs["riverqueue-pro"]
      Gem.loaded_specs["riverqueue-pro"] = Gem::Specification.new do |spec|
        spec.name = "riverqueue-pro"
        spec.version = "1.2.3"
      end

      expect(run("version")).to eq(0)
      expect(out.string).to eq("River version #{Gem.loaded_specs.fetch("riverqueue").version}\nRiver Pro version 1.2.3\nRuby #{RUBY_VERSION}\n")
      expect(err.string).to eq("")
    ensure
      Gem.loaded_specs.delete("riverqueue-pro")
      Gem.loaded_specs["riverqueue-pro"] = original if original
    end

    it "prints help without printing versions" do
      expect(run("version", "--help")).to eq(0)
      expect(run("version", "-h")).to eq(0)
      expect(out.string).to include("Usage: river version | river --version")
      expect(out.string).not_to include("River version")
      expect(err.string).to eq("")
    end

    it "prints the activated core gem and Ruby versions" do
      expect(run("version")).to eq(0)
      expect(out.string).to eq("River version #{Gem.loaded_specs.fetch("riverqueue").version}\nRuby #{RUBY_VERSION}\n")
      expect(err.string).to eq("")
    end

    it "reports an unknown gem version when loaded directly from source" do
      original = Gem.loaded_specs.delete("riverqueue")
      expect(run("--version")).to eq(0)
      expect(out.string).to eq("River version (unknown)\nRuby #{RUBY_VERSION}\n")
      expect(err.string).to eq("")
    ensure
      Gem.loaded_specs["riverqueue"] = original
    end

    %w[version --version].each do |command|
      it "runs #{command} through the executable without a database or adapters" do
        script = <<~RUBY
          at_exit do
            raise "loaded a database adapter" if defined?(Sequel) || defined?(ActiveRecord) || defined?(PG) || defined?(SQLite3)
          end
          load #{File.expand_path("../exe/river", __dir__).inspect}
        RUBY
        stdout, stderr, status = Open3.capture3({"DATABASE_URL" => "invalid://"}, Gem.ruby, "-e", script, "--", command)

        expect(status.success?).to be(true), stderr
        expect(stdout).to eq("River version #{Gem.loaded_specs.fetch("riverqueue").version}\nRuby #{RUBY_VERSION}\n")
        expect(stderr).to eq("")
      end

      [["extra"], ["--database-url", "unused"], ["--line", "pro"], ["--unknown"]].each do |args|
        it "rejects #{command} #{args.join(" ")}" do
          expect(run(command, *args)).to eq(1)
          expect(err.string).to include("Version failed:")
          expect(out.string).to eq("")
        end
      end
    end
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
