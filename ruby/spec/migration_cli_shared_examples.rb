# frozen_string_literal: true

require "stringio"
require "tmpdir"
require_relative "../lib/migration_cli"

RSpec.shared_examples "migration command" do |adapter|
  it "supports help and reports usage errors" do
    out = StringIO.new
    err = StringIO.new

    expect(River::MigrationCLI.run(["--help"], err: err, out: out)).to eq(0)
    expect(out.string).to include("Usage: river ", "migrate-up", "migrate-down", "migrate-status")
    expect(River::MigrationCLI.run(["unknown"], err: err, out: out)).to eq(1)
    expect(River::MigrationCLI.run(["--unknown"], err: err, out: out)).to eq(1)
  end

  it "migrates SQLite through the #{adapter} command and guards destructive operations", database: :sqlite do
    Dir.mktmpdir("river-cli-test-") do |directory|
      url = "sqlite://#{File.join(directory, "river.sqlite3")}"
      # ActiveRecord accepts sqlite3 URLs; Sequel accepts sqlite URLs.
      url = url.sub("sqlite:", "sqlite3:") if adapter == "activerecord"

      options = ["--database-url", url]
      out = StringIO.new
      err = StringIO.new
      run = ->(*args) {
        out.truncate(0)
        out.rewind
        River::MigrationCLI.run(args + options, err: err, out: out)
      }

      expect(run.call("migrate-status")).to eq(0)
      expect(out.string).to include("pending 001")
      expect(run.call("migrate-up", "--dry-run")).to eq(0)
      expect(out.string).to include("planned 008")
      expect(run.call("migrate-up", "--steps", "2", "--target", "4")).to eq(0)
      expect(run.call("migrate-up", "--target", "1")).to eq(0)
      expect(out.string).not_to include("applied")
      expect(run.call("migrate-status")).to eq(0)
      expect(out.string).to include("pending 003")
      expect(run.call("migrate-up")).to eq(0)
      expect(run.call("migrate-status")).to eq(0)
      expect(out.string).to include("applied 008")
      expect(run.call("migrate-down")).to eq(1)
      expect(err.string).to include("--yes")
      expect(run.call("migrate-down", "--dry-run")).to eq(0)
      expect(run.call("migrate-down", "--yes", "--target", "0")).to eq(0)
      expect(run.call("migrate-status")).to eq(0)
      expect(out.string).to include("pending 001")
      expect(run.call("migrate-up", "--schema", "invalid")).to eq(1)
    end
  end
end
