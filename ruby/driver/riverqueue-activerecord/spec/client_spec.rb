# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/client_driver_shared_examples"
require_relative "../../../spec/driver_query_shared_examples"
require_relative "../../../spec/worker_process_shared_examples"
require_relative "../../../spec/insert_notification_shared_examples"
require_relative "../../../spec/row_decoding_shared_examples"
require_relative "../../../spec/notification_receiver_shared_examples"

RSpec.describe "ActiveRecord client integration" do
  [:postgres, :sqlite].each do |adapter|
    context "with #{adapter}", database: adapter do
      before { skip "Postgres unavailable" if adapter == :postgres && !PG_AVAILABLE }

      around do |example|
        if adapter == :postgres && !PG_AVAILABLE
          example.run
        else
          ClientTestDatabase.with_active_record(adapter) do |driver|
            @driver = driver
            example.run
          end
        end
      end

      it "releases connections after operations and errors without reclaiming dead threads" do
        client = River::Client.new(@driver)
        job = client.insert(River::JobArgsHash.new(:connection_test, {})).job
        pool = @driver.connection_class.connection_pool
        pool.release_connection
        operations = [
          -> { River::Driver::ActiveRecord.new },
          -> { @driver.init_driver },
          -> { client.job_get(job.id) },
          -> { client.job_list },
          -> { client.insert(River::JobArgsHash.new(:connection_test, {}), state: :pending) },
          -> { client.insert(River::JobArgsHash.new(:connection_test, {})) },
          -> { @driver.job_claim(id: job.id, attempted_by: "test") },
          -> { @driver.job_metadata_merge(job.id, {"value" => 123}) },
          -> { @driver.job_complete(id: job.id, finalized_at: Time.now.utc) },
          -> { @driver.queue_upsert("connection_test") },
          -> { @driver.transaction { raise @driver.rollback_exception } },
          -> { expect { @driver.transaction { raise "failed" } }.to raise_error("failed") },
          -> { expect { @driver.send(:runtime_query_rows, "SELECT * FROM nonexistent_river_table") }.to raise_error((adapter == :sqlite) ? SQLite3::SQLException : ActiveRecord::StatementInvalid, /nonexistent_river_table/) }
        ]
        operations.each do |operation|
          Thread.new do
            operation.call
            expect(pool.active_connection?).to be_falsey
          end.value
          expect(pool.stat[:dead]).to eq(0)
        end
      end

      it "retains a caller-owned connection lease" do
        pool = @driver.connection_class.connection_pool
        pool.release_connection
        pool.with_connection do |connection|
          client = River::Client.new(@driver)
          job = client.insert(River::JobArgsHash.new(:connection_test, {})).job
          client.job_get(job.id)
          @driver.job_metadata_merge(job.id, {"value" => 123})
          expect(pool.active_connection?).to equal(connection)
        end
        expect(pool.active_connection?).to be_falsey
      end

      it_behaves_like "client driver end to end"
      it_behaves_like "driver query efficiency"
      it_behaves_like "Postgres completion plans" if adapter == :postgres
      it_behaves_like "notification receiving", adapter
      it_behaves_like "Postgres state update races" if adapter == :postgres
      it_behaves_like "SQLite corrupt job runtime" if adapter == :sqlite
      it_behaves_like "Postgres insert notifications" if adapter == :postgres
      it_behaves_like "Postgres cancellation notifications" if adapter == :postgres
      it_behaves_like "Postgres leadership notifications" if adapter == :postgres
      it_behaves_like "Postgres queue control notifications" if adapter == :postgres
      it_behaves_like "SQLite cancellation notifications" if adapter == :sqlite
      it_behaves_like "SQL scheduling concurrency" if adapter == :postgres
      it_behaves_like "Postgres finalized job list plans" if adapter == :postgres
      it_behaves_like "Postgres rescue concurrency" if adapter == :postgres
      it_behaves_like "dedicated worker process"
    end
  end
end
