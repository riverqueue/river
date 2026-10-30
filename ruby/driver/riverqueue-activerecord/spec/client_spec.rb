# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/client_driver_shared_examples"
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

      it_behaves_like "client driver end to end"
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

      it "returns borrowed connections after each operation on a job thread" do
        leases = Thread.new do
          pool = ActiveRecord::Base.connection_pool
          observed = {}
          record = lambda do |operation, &block|
            result = block.call
            observed[operation] = !!pool.active_connection?
            result
          end

          driver = record.call(:initialize) { River::Driver::ActiveRecord.new }
          client = record.call(:client) { River::Client.new(driver) }
          row = record.call(:insert) { client.insert(River::JobArgsHash.new("borrowed", {})).job }
          claimed = record.call(:claim) { driver.job_claim(id: row.id, attempted_by: "borrower") }
          record.call(:complete) { driver.job_complete(id: row.id, expected_job: claimed, finalized_at: Time.now.utc) }
          record.call(:read) { driver.job_get_by_id(row.id) }
          record.call(:cancelled) { driver.job_get_cancelled_ids([row.id]) }
          record.call(:transaction) { driver.transaction { driver.job_get_by_id(row.id) } }
          record.call(:rollback) do
            driver.transaction do
              client.insert(River::JobArgsHash.new("rolled_back", {}))
              raise driver.rollback_exception
            end
          end
          record.call(:error) do
            driver.transaction { raise "transaction failed" }
          rescue RuntimeError
            # Check the lease after the exception leaves the transaction.
          end
          observed
        ensure
          pool&.release_connection
        end.value

        expect(leases).to eq(%i[initialize client insert claim complete read cancelled transaction rollback error].to_h { |operation| [operation, false] })
        expect(@driver.job_list.map(&:kind)).to eq(["borrowed"])
      end

      it "keeps the caller's leased connection through nested driver operations" do
        Thread.new do
          pool = ActiveRecord::Base.connection_pool
          connection = pool.lease_connection
          @driver.transaction do
            @driver.transaction { @driver.job_get_cancelled_ids([1]) }
            expect(pool.active_connection?).to equal(connection)
          end
          expect(pool.active_connection?).to equal(connection)
        ensure
          pool&.release_connection
        end.value
      end
    end
  end
end
