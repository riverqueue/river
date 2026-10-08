# frozen_string_literal: true

require "spec_helper"
require_relative "migration_cli_shared_examples"

RSpec.describe River::MigrationCLI, database: :sqlite do
  %w[activerecord sequel].each do |adapter|
    context "with only #{adapter} available" do
      around { |example| with_available_drivers([adapter]) { example.run } }

      it_behaves_like "migration command", adapter
    end
  end

  %w[activerecord sequel].each do |adapter|
    it "auto-detects #{adapter} and runs real SQLite migrations" do
      with_available_drivers([adapter]) do
        Dir.mktmpdir("river-cli-detection-") do |directory|
          err = StringIO.new
          out = StringIO.new
          scheme = (adapter == "sequel") ? "sqlite" : "sqlite3"
          url = "#{scheme}://#{File.join(directory, "river.sqlite3")}"

          expect(described_class.run(["migrate-up", "--database-url", url], err: err, out: out)).to eq(0), err.string
          expect(out.string).to include("applied 001")

          out.truncate(0)
          out.rewind
          expect(described_class.run(["migrate-status", "--database-url", url], err: err, out: out)).to eq(0), err.string
          expect(out.string).to include("applied 001")
          expect(out.string.lines).to all(start_with("applied "))
        end
      end
    end
  end

  it "prefers Sequel when both SQL adapters are available" do
    err = StringIO.new
    out = StringIO.new
    expect(described_class.run(["migrate-status", "--database-url", "sqlite::memory:"], err: err, out: out)).to eq(0), err.string
    expect(out.string).to include("pending 001")
  end

  it "explains which gems to install when neither SQL adapter is available" do
    with_available_drivers([]) do
      err = StringIO.new
      expect(described_class.run(["migrate-status", "--database-url", "sqlite::memory:"], err: err)).to eq(1)
      expect(err.string).to include("install riverqueue-activerecord or riverqueue-sequel in your bundle")

      out = StringIO.new
      expect(described_class.run(["--help"], err: err, out: out)).to eq(0)
      expect(out.string).not_to include("--driver")
    end
  end

  it "rejects the removed driver option" do
    err = StringIO.new
    expect(described_class.run(["migrate-status", "--driver", "sequel"], err: err)).to eq(1)
    expect(err.string).to include("invalid option: --driver")
  end

  it "validates database configuration and redacts unexpected connection errors" do
    previous_url = ENV.delete("DATABASE_URL")
    err = StringIO.new
    expect(described_class.run(["migrate-status"], err: err)).to eq(1)
    expect(err.string).to include("provide --database-url or DATABASE_URL")
    Dir.mktmpdir("river-cli-errors-") do |directory|
      ENV["DATABASE_URL"] = "sqlite://#{File.join(directory, "missing", "river.sqlite3")}"
      expect(described_class.run(["migrate-status"], err: err)).to eq(1)
      expect(err.string).to include("Sequel::DatabaseConnectionError")
      expect(err.string).not_to include(directory)
    end
  ensure
    ENV["DATABASE_URL"] = previous_url
  end

  it "loads the optional Pro migration entry point only when requested" do
    # Only the loading/dispatch contract is doubled here. Private Pro migration
    # behavior is exercised by that package's own real-database suite.
    Dir.mktmpdir("river-cli-pro-") do |directory|
      integration = File.join(directory, "riverqueue-pro.rb")
      File.write(integration, <<~RUBY)
        module River
          module Pro
            class Migrator
              def initialize(driver, schema:)
              end
              def status
                [River::Migrator::Status.new(1, "pro_test", false)]
              end
            end
          end
        end
      RUBY
      $LOAD_PATH.unshift(directory)
      out = StringIO.new
      err = StringIO.new
      url = "sqlite://#{File.join(directory, "river.sqlite3")}"
      expect(described_class.run(["migrate-status", "--database-url", url, "--line", "pro"], err: err, out: out)).to eq(0)
      expect(out.string).to include("pending 001 pro_test")
    ensure
      $LOAD_PATH.delete(directory)
      $LOADED_FEATURES.delete(integration)
      River.send(:remove_const, :Pro)
    end
  end

  # Change only gem discovery; adapter loading and database operations remain real.
  def with_available_drivers(drivers)
    original = Gem::Specification.method(:find_all_by_name)
    Gem::Specification.define_singleton_method(:find_all_by_name) do |name, *requirements|
      if %w[riverqueue-activerecord riverqueue-sequel].include?(name) && !drivers.include?(name.delete_prefix("riverqueue-"))
        []
      else
        original.call(name, *requirements)
      end
    end
    yield
  ensure
    Gem::Specification.define_singleton_method(:find_all_by_name, original)
  end
end
