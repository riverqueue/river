# frozen_string_literal: true

require "spec_helper"
require "open3"
require "stringio"
require "tmpdir"
require_relative "../lib/cli"

RSpec.describe River::MigrationCLI do
  let(:err) { StringIO.new }
  let(:out) { StringIO.new }

  def run(*args)
    described_class.run(["migrate-get", *args], err: err, out: out)
  end

  def versions
    out.string.scan(/^-- River main migration (\d+)/).flatten.map(&:to_i)
  end

  it "exports SQL through the executable without loading database adapters or using DATABASE_URL" do
    script = <<~RUBY
      at_exit do
        raise "loaded a database adapter" if defined?(Sequel) || defined?(ActiveRecord) || defined?(PG) || defined?(SQLite3)
      end
      load #{File.expand_path("../exe/river", __dir__).inspect}
    RUBY
    stdout, stderr, status = Open3.capture3({"DATABASE_URL" => "sqlite://"}, Gem.ruby, "-e", script, "--",
      "migrate-get", "--version", "2", "--up")

    expect(status.success?).to be(true), stderr
    expect(stderr).to eq("")
    expect(stdout).to include("-- River main migration 002 [up]", "CREATE TYPE")
    expect(stdout).not_to include("TEMPLATE")
  end

  it "exports no SQL when every selected version is excluded" do
    expect(run("--version", "1", "--exclude-version", "1", "--up")).to eq(0)
    expect(out.string).to eq("")
  end

  it "exports selected versions in supplied order with schema substitution" do
    expect(run("--version", "3,1", "--version", "2", "--up", "--schema", "jobs")).to eq(0), err.string
    expect(versions).to eq([3, 1, 2])
    expect(out.string).to include('ALTER TABLE "jobs".river_job', 'CREATE TABLE "jobs".river_migration')
    expect(out.string).not_to include("TEMPLATE")
  end

  it "lists the export command and options in help" do
    expect(River::CLI.run(["--help"], err: err, out: out)).to eq(0)
    expect(run("--help")).to eq(0)
    expect(out.string).to include("migrate-get", "--all", "--up", "--down", "--version", "--exclude-version")
  end

  it "refuses to export main SQL for a Pro gem without offline export support" do
    Dir.mktmpdir("river-export-pro-") do |directory|
      integration = File.join(directory, "riverqueue-pro.rb")
      File.write(integration, <<~RUBY)
        module River
          module Pro
            class Migrator < River::Migrator
            end
          end
        end
      RUBY
      $LOAD_PATH.unshift(directory)

      expect(run("--line", "pro", "--version", "1", "--up")).to eq(1)
      expect(err.string).to include("migration files must contain contiguous versions starting at 1")
      expect(out.string).to eq("")
    ensure
      $LOAD_PATH.delete(directory)
      $LOADED_FEATURES.delete(integration)
      River.send(:remove_const, :Pro)
    end
  end

  %w[migrate-down migrate-status migrate-up].each do |command|
    [["--all"], ["--down"], ["--exclude-version", "1"], ["--up"], ["--version", "3"]].each do |args|
      it "rejects #{args.join(" ")} with #{command} before connecting or applying migrations" do
        expect(described_class.run([command, *args], err: err, out: out)).to eq(1)
        expect(err.string).to include("#{args.first} cannot be used with #{command}")
        expect(out.string).to eq("")
      end
    end
  end

  %w[postgres postgresql sqlite sqlite3].each do |scheme|
    %w[down up].each do |direction|
      it "exports canonical #{scheme} #{direction} SQL without connecting" do
        expect(run("--all", "--#{direction}", "--database-url", "#{scheme}://unreachable/unused")).to eq(0), err.string
        expected_versions = (1..8).to_a
        expected_versions.reverse! if direction == "down"
        expect(versions).to eq(expected_versions)
        backend = scheme.start_with?("sqlite") ? "sqlite" : "postgresql"
        expected = expected_versions.map do |version|
          path = Dir.glob(File.expand_path("../migration/#{backend}/main/#{version.to_s.rjust(3, "0")}_*.#{direction}.sql", __dir__)).fetch(0)
          "-- River main migration #{version.to_s.rjust(3, "0")} [#{direction}]\n" +
            File.read(path).gsub("/* TEMPLATE: schema */", "").strip + "\n"
        end.join("\n")
        expect(out.string).to eq(expected)
        expect(err.string).to eq("")
      end
    end
  end

  %w[down up].each do |direction|
    it "excludes comma-separated and repeated versions when exporting all #{direction} SQL" do
      expect(run("--all", "--#{direction}", "--exclude-version", "1,2", "--exclude-version", "8")).to eq(0), err.string
      expect(versions).to eq((direction == "up") ? [3, 4, 5, 6, 7] : [7, 6, 5, 4, 3])
    end
  end

  [
    [["--up"], "pass exactly one of --all or --version"],
    [["--all", "--version", "1", "--up"], "pass exactly one of --all or --version"],
    [["--all"], "pass exactly one of --up or --down"],
    [["--all", "--up", "--down"], "pass exactly one of --up or --down"],
    [["--version", "1,999", "--up"], "migration 999 does not exist"],
    [["--version", "1,999", "--exclude-version", "999", "--up"], "migration 999 does not exist"],
    [["--all", "--up", "--database-url", "mysql://user:secret@localhost"], "--database-url must use"],
    [["--all", "--up", "--database-url", "sqlite://", "--schema", "jobs"], "SQLite does not support"],
    [["--all", "--up", "--schema", 'bad"schema'], "schema must be a simple SQL identifier"],
    [["--all", "--up", "--dry-run"], "--dry-run cannot be used with migrate-get"],
    [["--all", "--up", "--steps", "1"], "--steps cannot be used with migrate-get"],
    [["--all", "--up", "--target", "1"], "--target cannot be used with migrate-get"],
    [["--all", "--up", "--yes"], "--yes cannot be used with migrate-get"],
    [["--all", "--up", "--line", "unknown"], "invalid argument: --line"],
    [["--all", "--up", "unexpected"], "Usage: river"]
  ].each do |args, message|
    it "rejects #{args.join(" ")} before printing SQL" do
      expect(run(*args)).to eq(1)
      expect(err.string).to include(message)
      expect(err.string).not_to include("secret")
      expect(out.string).to eq("")
    end
  end

  %w[--exclude-version --version].each do |flag|
    ["", "0", "-1", "1,x", "1,", "1,,2", "1.5"].each do |value|
      it "rejects #{flag}=#{value.inspect}" do
        expect(run(flag, value, "--up")).to eq(1)
        expect(err.string).to include("#{flag} must contain positive integers")
        expect(out.string).to eq("")
      end
    end
  end
end
