# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"
require_relative "support/client_test_database"
require_relative "migrator_shared_examples"
require "digest"

RSpec.describe River::Migrator do
  it "ships exact files recorded by the upstream checksum manifest" do
    root = File.expand_path("../migration", __dir__)
    manifest = JSON.parse(File.read(File.join(root, "manifest.json")))

    expect(Dir.glob(File.join(root, "**/*.sql")).size).to eq(manifest.fetch("files").size)
    manifest.fetch("files").each do |path, digest|
      expect(Digest::SHA256.file(File.join(root, path)).hexdigest).to eq(digest)
    end
  end

  it "rejects unsupported backends, invalid lines, empty bundles, and SQLite schemas" do
    driver = Object.new
    driver.define_singleton_method(:migration_backend) { :unknown }

    expect { described_class.new(driver) }.to raise_error(ArgumentError, /backend/)
    driver.define_singleton_method(:migration_backend) { :sqlite }

    expect { described_class.new(driver, line: "../main") }.to raise_error(ArgumentError, /line/)
    expect { described_class.new(driver, line: "missing") }.to raise_error(ArgumentError, /contiguous/)
    expect { described_class.new(driver, schema: "other") }.to raise_error(ArgumentError, /SQLite/)
  end

  context "MySQL migration session without a server" do
    let(:connection) do
      Class.new do
        attr_accessor :fail_ddl, :locked, :schema
        attr_reader :statements, :versions

        def initialize
          @locked = 1
          @schema = "test_db"
          @statements = []
          @versions = []
        end

        def query(sql, **)
          @statements << sql
          case sql
          when /SELECT DATABASE/ then [{"name" => schema}]
          when /GET_LOCK/ then [{"locked" => locked}]
          when /information_schema.tables/ then versions.empty? ? [] : [{"table_name" => "river_migration"}]
          when /information_schema.columns/ then []
          when /SELECT version/ then versions.map { |version| {"version" => version} }
          when /CREATE TABLE/
            raise "DDL failed" if fail_ddl
            []
          when /INSERT INTO .*river_migration.*VALUES \((\d+)\)/
            versions << Regexp.last_match(1).to_i
            []
          else []
          end
        end
      end.new
    end
    let(:driver) do
      value = connection
      Object.new.tap do |driver|
        driver.define_singleton_method(:migration_backend) { :mysql }
        driver.define_singleton_method(:migration_connection) { |&block| block.call(value) }
      end
    end

    it "records history after DDL and releases the lock without transaction commands" do
      migrator = described_class.new(driver)
      expect(migrator.migrate(steps: 1).map(&:version)).to eq([1])
      expect(connection.versions).to eq([1])
      expect(connection.statements.last).to include("RELEASE_LOCK")
      expect(connection.statements).not_to include("BEGIN", "COMMIT", "ROLLBACK")
      expect(migrator.status.first.applied).to be true
    end

    it "releases its lock after a DDL failure without recording the failed version" do
      connection.fail_ddl = true
      expect { described_class.new(driver).migrate(steps: 1) }.to raise_error("DDL failed")
      expect(connection.versions).to be_empty
      expect(connection.statements.last).to include("RELEASE_LOCK")
    end

    it "leaves another migrator's lock alone" do
      connection.locked = 0
      expect { described_class.new(driver).migrate }.to raise_error(River::Error, /database lock/)
      expect(connection.statements.grep(/RELEASE_LOCK|CREATE TABLE/)).to be_empty
    end

    it "requires a selected safe database and supports an explicit schema" do
      [nil, "bad'name", "a" * 65].each do |schema|
        connection.schema = schema
        expect { described_class.new(driver).status }.to raise_error(ArgumentError, /identifier/)
      end
      described_class.new(driver, schema: "other").migrate(steps: 1)
      expect(connection.statements.grep(/CREATE TABLE/).first).to include("`other`.river_migration")
    end

    it "splits migration scripts while preserving quoted semicolons" do
      migrator = described_class.new(driver)
      expect(migrator.send(:mysql_statements, "; SELECT 'a;b'; -- comment;\n SELECT \"x;y\"; /* ; */ SELECT `a;b` # comment;\n")).to eq(["SELECT 'a;b'", "SELECT \"x;y\"", "SELECT `a;b`"])
    end
  end

  [:mysql, :postgres, :sqlite].each do |adapter|
    context "with #{adapter}", database: adapter do
      around do |example|
        skip "Postgres unavailable" if adapter == :postgres && !DB

        ClientTestDatabase.with_sequel(adapter, migrate: false) do |driver|
          @driver = driver
          example.run
        end
      end

      it_behaves_like "canonical migrations", adapter

      if adapter == :sqlite
        it "supports SQLite connections configured to return hashes" do
          @driver.migration_connection { |connection| connection.results_as_hash = true }
          expect(described_class.new(@driver).migrate.length).to eq(8)
        end
      end

      it "detects concurrent history changes before executing SQL" do
        migrator = described_class.new(@driver)
        calls = 0
        migrator.define_singleton_method(:existing_versions) { ((calls += 1) == 1) ? [] : [1] }

        expect { migrator.migrate }.to raise_error(River::Error, /changed concurrently/)
      end

      if adapter == :mysql
        it "supports explicit schemas and rejects unsafe identifiers" do
          schema = @driver.send(:runtime_query_rows, "SELECT DATABASE() AS name").first.fetch(:name)
          expect(described_class.new(@driver, schema: schema).migrate.length).to eq(8)
          expect { described_class.new(@driver, schema: "bad'name").status }.to raise_error(ArgumentError, /identifier/)
        end

        it "migrates databases with the maximum 64-character name" do
          schema = "river_migrate_#{SecureRandom.hex(16)}".ljust(64, "x")
          @driver.send(:runtime_execute, "CREATE DATABASE `#{schema}` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin")
          migrator = described_class.new(@driver, schema: schema)
          expect(migrator.migrate.length).to eq(8)
          expect(migrator.migrate(direction: :down, steps: 8).length).to eq(8)
        ensure
          @driver.send(:runtime_execute, "DROP DATABASE IF EXISTS `#{schema}`") if schema
        end

        it "refuses concurrent migrators and releases its named lock" do
          schema = @driver.send(:runtime_query_rows, "SELECT DATABASE() AS name").first.fetch(:name)
          admin = Sequel.connect(ENV.fetch("TEST_MYSQL_URL"))
          admin.fetch("SELECT GET_LOCK(CONCAT('river_migrate:',LEFT(SHA2('#{schema}',256),48)), 0)").all
          expect { described_class.new(@driver).migrate }.to raise_error(River::Error, /database lock/)
          admin.fetch("SELECT RELEASE_LOCK(CONCAT('river_migrate:',LEFT(SHA2('#{schema}',256),48)))").all
          expect(described_class.new(@driver).migrate.length).to eq(8)
        ensure
          admin&.disconnect
        end

        it "requires a selected database" do
          admin = Sequel.connect(ENV.fetch("TEST_MYSQL_URL").sub(%r{/[^/]*$}, "/"))
          expect { described_class.new(River::Driver::Sequel.new(admin)).status }.to raise_error(ArgumentError, /identifier/)
        ensure
          admin&.disconnect
        end

      end

      if adapter == :postgres
        it "rejects an empty search path" do
          @driver.migration_connection do |connection|
            connection.exec("SET search_path TO missing_river_schema")
            expect { described_class.new(@driver).status }.to raise_error(ArgumentError, /identifier/)
          end
        end

        it "supports explicit schemas and rejects unsafe identifiers" do
          schema = @driver.send(:runtime_query_rows, "SELECT current_schema() AS name").first.fetch(:name)

          expect(described_class.new(@driver, schema: schema).migrate.length).to eq(8)
          expect { described_class.new(@driver, schema: "bad'name").status }.to raise_error(ArgumentError, /identifier/)
        end

        it "refuses concurrent Ruby migrators using the same schema" do
          locked = Queue.new
          release = Queue.new
          holder = Thread.new do
            @driver.migration_connection do |connection|
              schema = connection.exec("SELECT current_schema() AS name").first.fetch("name")
              connection.exec("SELECT pg_advisory_lock(hashtext(current_database()), hashtext('river_migrate:#{schema}'))")
              locked << true
              release.pop
              connection.exec("SELECT pg_advisory_unlock(hashtext(current_database()), hashtext('river_migrate:#{schema}'))")
            end
          end

          Timeout.timeout(5) { locked.pop }

          expect { described_class.new(@driver).migrate }.to raise_error(River::Error, /schema lock/)
        ensure
          release << true
          holder&.join
        end
      end
    end
  end
end
