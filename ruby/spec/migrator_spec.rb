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

        it "refuses concurrent migrators and releases its named lock" do
          schema = @driver.send(:runtime_query_rows, "SELECT DATABASE() AS name").first.fetch(:name)
          admin = Sequel.connect(ENV.fetch("TEST_MYSQL_URL"))
          admin.fetch("SELECT GET_LOCK('river_migrate:#{schema}', 0)").all
          expect { described_class.new(@driver).migrate }.to raise_error(River::Error, /database lock/)
          admin.fetch("SELECT RELEASE_LOCK('river_migrate:#{schema}')").all
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

        it "splits migration scripts while preserving quoted semicolons" do
          migrator = described_class.new(@driver)
          expect(migrator.send(:mysql_statements, "; SELECT 'a;b'; -- comment;\n SELECT \"x;y\"; /* ; */ SELECT `a;b` # comment;\n")).to eq(["SELECT 'a;b'", "SELECT \"x;y\"", "SELECT `a;b`"])
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
