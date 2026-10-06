# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/support/connection_class_test_database"

class SelectedRiverConnection < ActiveRecord::Base
  self.abstract_class = true
  # The internal River model must not inherit application default scopes.
  default_scope { where(application_only_column: "not a river column") }
end

RSpec.describe "Active Record connection selection" do
  [nil, Object, "ActiveRecord::Base", Class.new(ActiveRecord::Base)].each_with_index do |value, index|
    it "rejects invalid connection class #{index}" do
      expect { River::Driver::ActiveRecord.new(connection_class: value) }.to raise_error(ArgumentError)
    end
  end

  [:sqlite, :postgres].each do |backend|
    next if backend == :postgres && !PG_AVAILABLE

    context "with #{backend}", database: backend do
      around do |example|
        ClientTestDatabase.with_active_record(backend) do |primary|
          ConnectionClassTestDatabase.with_class(SelectedRiverConnection, backend) do |selected|
            @primary = primary
            @selected = selected
            River::Migrator.new(selected).migrate
            @client = River::Client.new(selected)
            example.run
          end
        end
      end

      it "isolates job models, writes, reads, and runtime operations" do
        first = @client.insert(River::JobArgsHash.new("selected", {})).job
        other = River::Client.new(@primary).insert(River::JobArgsHash.new("primary", {})).job

        expect(@selected.connection_class).to eq(SelectedRiverConnection)
        expect(@selected.job_list.map(&:kind)).to eq(["selected"])
        expect(@primary.job_list.map(&:kind)).to eq(["primary"])
        expect(@client.job_get(first.id).kind).to eq("selected")
        expect(@primary.job_get_by_id(other.id).kind).to eq("primary")
        @selected.queue_upsert("selected")

        expect(@primary.queue_get("selected")).to be_nil
        @client.job_cancel(first.id)

        expect(@client.job_get(first.id).state).to eq("cancelled")
        expect(@primary.job_get_by_id(other.id).state).to eq("available")
        @selected.transaction do
          @client.insert(River::JobArgsHash.new("rollback", {}))
          raise ActiveRecord::Rollback
        end

        expect(@selected.job_list.map(&:kind)).to eq(["selected"])
      end

      it "joins the selected connection's outer transaction" do
        SelectedRiverConnection.transaction do
          @client.insert(River::JobArgsHash.new("rollback", {}))

          expect(@selected.job_list.length).to eq(1)
          expect(@primary.job_list).to be_empty
          raise ActiveRecord::Rollback
        end

        expect(@selected.job_list).to be_empty
      end

      if backend == :postgres
        it "detects capabilities again when the selected class replaces its pool" do
          capabilities = @selected.postgres_capabilities
          configuration = SelectedRiverConnection.connection_db_config.configuration_hash
          SelectedRiverConnection.remove_connection
          SelectedRiverConnection.establish_connection(configuration)

          expect(@selected.postgres_capabilities).not_to equal(capabilities)
          expect(@selected.postgres_capabilities.unique_insert_mode).to eq(capabilities.unique_insert_mode)
          expect(@client.insert(River::JobArgsHash.new(:reconnected, {})).job.kind).to eq("reconnected")
        end
      end

      it "does not claim atomicity with an unrelated Base transaction" do
        ActiveRecord::Base.transaction do
          @client.insert(River::JobArgsHash.new("committed", {}))
          raise ActiveRecord::Rollback
        end

        expect(@selected.job_list.map(&:kind)).to eq(["committed"])
      end

      it "guards migrations against transactions on the selected connection only" do
        SelectedRiverConnection.transaction do
          expect { River::Migrator.new(@selected).migrate }.to raise_error(River::Error, /transaction/)
        end

        ActiveRecord::Base.transaction do
          expect { River::Migrator.new(@selected).migrate }.not_to raise_error
        end
      end
    end
  end
end
