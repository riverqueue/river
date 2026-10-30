# frozen_string_literal: true

require "spec_helper"
require "sequel"
require "securerandom"
require "stringio"
require "tmpdir"
require_relative "../lib/cli"

RSpec.describe "migration validation CLI" do
  let(:err) { StringIO.new }
  let(:out) { StringIO.new }

  def run(*args)
    err.truncate(0)
    err.rewind
    out.truncate(0)
    out.rewind
    River::CLI.run([*args, *@options], err: err, out: out)
  end

  it "prints help without a database" do
    expect(River::CLI.run(["validate", "--help"], err: err, out: out)).to eq(0)
    expect(out.string).to include("validate", "--database-url", "--line", "--schema")
    expect(err.string).to eq("")
  end

  [["--all"], ["--down"], ["--dry-run"], ["--exclude-version", "1"], ["--steps", "1"],
    ["--target", "1"], ["--up"], ["--version", "1"], ["--yes"]].each do |args|
    it "rejects #{args.join(" ")} before connecting" do
      expect(River::CLI.run(["validate", *args], err: err, out: out)).to eq(1)
      expect(err.string).to include("#{args.first} cannot be used with validate")
      expect(out.string).to eq("")
    end
  end

  %w[activerecord sequel].product([:postgres, :sqlite]).each do |driver_name, backend|
    context "with #{driver_name}/#{backend}", database: backend do
      around do |example|
        # Select the adapter only; CLI loading, connections, and SQL remain real.
        original = River::MigrationCLI.method(:detect_driver)
        River::MigrationCLI.define_singleton_method(:detect_driver) { driver_name }

        Dir.mktmpdir("river-validate-") do |directory|
          if backend == :postgres
            @url = ENV.fetch("TEST_DATABASE_URL", "postgres://localhost/river_test")
            schema = "river_validate_test_#{SecureRandom.hex(8)}"
            @database = Sequel.connect(@url, search_path: schema)
            @database.run("CREATE SCHEMA #{schema}")
            @schema_options = ["--schema", schema]
          else
            path = File.join(directory, "river.sqlite3")
            scheme = (driver_name == "activerecord") ? "sqlite3" : "sqlite"
            @url = "#{scheme}://#{path}"
            @database = Sequel.sqlite(path)
            @schema_options = []
          end
          @options = ["--database-url", @url, *@schema_options]

          example.run
        ensure
          @database.run("DROP SCHEMA #{schema} CASCADE") if schema
          @database&.disconnect
        end
      ensure
        River::MigrationCLI.define_singleton_method(:detect_driver, original)
      end

      it "checks empty, partial, current, and downgraded histories without changing them" do
        expect(run("validate")).to eq(1)
        expect(err.string).to eq("unapplied migrations: 1, 2, 3, 4, 5, 6, 7, 8\n")
        expect(out.string).to eq("")
        expect(@database.tables).to be_empty

        expect(run("migrate-up", "--target", "4")).to eq(0), err.string
        history = @database[:river_migration].order(:version).all
        expect(run("validate")).to eq(1)
        expect(err.string).to eq("unapplied migrations: 5, 6, 7, 8\n")
        expect(@database[:river_migration].order(:version).all).to eq(history)

        expect(run("migrate-up")).to eq(0), err.string
        # Migration 005 rebuilds the history table; discard cached SQLite statements.
        @database.disconnect
        history = @database[:river_migration].order(:version).all
        expect(run("validate")).to eq(0), err.string
        expect(err.string).to eq("")
        expect(out.string).to eq("")
        expect(@database[:river_migration].order(:version).all).to eq(history)

        expect(run("migrate-down", "--yes")).to eq(0), err.string
        expect(run("validate")).to eq(1)
        expect(err.string).to eq("unapplied migrations: 8\n")
        expect(@database[:river_migration].order(:version).select_map(:version)).to eq((1..7).to_a)
      end

      it "rejects incomplete migration histories" do
        expect(run("migrate-up")).to eq(0), err.string
        @database[:river_migration].where(version: 2).delete
        history = @database[:river_migration].order(:version).all

        expect(run("validate")).to eq(1)
        expect(err.string).to include("database migration history is incomplete or newer than this gem")
        expect(out.string).to eq("")
        expect(@database[:river_migration].order(:version).all).to eq(history)
      end

      it "rejects migration histories newer than the gem" do
        expect(run("migrate-up")).to eq(0), err.string
        @database[:river_migration].insert(line: "main", version: 9)

        expect(run("validate")).to eq(1)
        expect(err.string).to include("database migration history is incomplete or newer than this gem")
        expect(out.string).to eq("")
        expect(@database[:river_migration].where(version: 9).count).to eq(1)
      end

      it "requires a URL and accepts DATABASE_URL" do
        original = ENV.delete("DATABASE_URL")
        expect(River::CLI.run(["validate"], err: err, out: out)).to eq(1)
        expect(err.string).to include("provide --database-url or DATABASE_URL")

        expect(run("migrate-up")).to eq(0), err.string
        ENV["DATABASE_URL"] = @url
        @options = @schema_options
        expect(run("validate")).to eq(0), err.string
        expect(err.string).to eq("")
        expect(out.string).to eq("")
      ensure
        ENV["DATABASE_URL"] = original
      end
    end
  end
end
