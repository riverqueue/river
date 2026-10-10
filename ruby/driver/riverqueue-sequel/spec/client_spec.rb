# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/client_driver_shared_examples"
require_relative "../../../spec/worker_process_shared_examples"
require_relative "../../../spec/insert_notification_shared_examples"
require_relative "../../../spec/row_decoding_shared_examples"

RSpec.describe "Sequel client integration" do
  [:postgres, :sqlite].each do |adapter|
    context "with #{adapter}", database: adapter do
      before { skip "Postgres unavailable" if adapter == :postgres && !DB }

      around do |example|
        if adapter == :postgres && !DB
          example.run
        else
          ClientTestDatabase.with_sequel(adapter) do |driver|
            @driver = driver
            example.run
          end
        end
      end

      it_behaves_like "client driver end to end"
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

      it "rejects job completion in an unrelated database's transaction" do
        other_database = Sequel.sqlite
        client = River::Client.new(@driver)
        row = client.insert(River::JobArgsHash.new(:transactional, {})).job
        job = River::Job.new(client, @driver.job_claim(id: row.id, attempted_by: "test"))

        other_database.transaction do
          expect { client.job_complete_tx(job) }.to raise_error(River::Error, /requires an active transaction/)
        end

        expect(client.job_get(row.id)).to have_attributes(finalized_at: nil, state: "running")
      ensure
        other_database&.disconnect
      end
    end
  end
end
