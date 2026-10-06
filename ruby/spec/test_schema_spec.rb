# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"
require_relative "support/client_test_database"

RSpec.describe RiverTestSchema do
  [:postgres, :sqlite].each do |adapter|
    context "with #{adapter}", database: adapter do
      around do |example|
        skip "PostgreSQL unavailable" if adapter == :postgres && !DB

        ClientTestDatabase.with_sequel(adapter, migrate: false) do |driver|
          @driver = driver
          example.run
        end
      end

      it "loads the canonical schema and complete migration history" do
        described_class.load(@driver)
        migrator = River::Migrator.new(@driver)
        expect(migrator.status.map(&:version)).to eq(migrator.migrations.map(&:version))
        expect(migrator.status).to all(have_attributes(applied: true))
        expect(migrator.migrate).to be_empty

        client = River::Client.new(@driver)
        args = River::JobArgsHash.new(:fixture, {})
        options = {state: :pending, unique_opts: River::UniqueOpts.new(by_args: true)}
        first = client.insert(args, **options)
        duplicate = client.insert(args, **options)
        expect(first.job.state).to eq("pending")
        expect(duplicate).to have_attributes(unique_skipped_as_duplicate?: true, job: have_attributes(id: first.job.id))

        expect(migrator.migrate(direction: :down, target: 0)).to eq(migrator.migrations.reverse)
        expect(migrator.status).to all(have_attributes(applied: false))
      end

      it "rolls back a failed setup so the connection remains usable for cleanup" do
        @driver.send(:runtime_execute, "CREATE TABLE river_job (id integer)")
        expect { described_class.load(@driver) }.to raise_error(/river_job.*already exists/)
        expect { @driver.send(:runtime_execute, "DROP TABLE river_job") }.not_to raise_error
      end
    end
  end
end
