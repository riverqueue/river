# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/driver_shared_examples"
require_relative "../../../spec/row_decoding_shared_examples"

RSpec.describe River::Driver::Sequel do
  if DB
    context "with PostgreSQL", database: :postgres do
      around(:each) { |ex| test_transaction(&ex) }

      let!(:driver) { River::Driver::Sequel.new(DB) }
      let(:client) { River::Client.new(driver) }

      it_behaves_like "driver shared examples"
      it_behaves_like "historical attempt error decoding"

      describe "client inserts" do
        it "persists args as a JSON object rather than a JSON string" do
          insert_res = client.insert(SimpleArgs.new(job_num: 1))

          row = DB.fetch(<<~SQL, insert_res.job.id).first
            SELECT args, jsonb_typeof(args) AS args_type
            FROM river_job
            WHERE id = ?
          SQL

          expect(row[:args_type]).to eq("object")
          expect(row[:args].to_h).to eq({"job_num" => 1})
        end
      end

      describe "#to_job_row (PostgreSQL)" do
        it "converts a database record to `River::JobRow` with minimal properties" do
          river_job = DB[:river_job].returning.insert_select({
            id: 1,
            args: %({"job_num":1}),
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT
          })

          job_row = driver.send(:to_job_row, river_job)

          expect(job_row).to be_an_instance_of(River::JobRow)
          expect(job_row).to have_attributes(
            id: 1,
            args: {"job_num" => 1},
            attempt: 0,
            attempted_at: nil,
            attempted_by: nil,
            created_at: be_within(2).of(Time.now.getutc),
            finalized_at: nil,
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            priority: River::PRIORITY_DEFAULT,
            queue: River::QUEUE_DEFAULT,
            scheduled_at: be_within(2).of(Time.now.getutc),
            state: River::JOB_STATE_AVAILABLE,
            tags: []
          )
        end

        it "converts a database record to `River::JobRow` with all properties" do
          now = Time.now
          river_job = DB[:river_job].returning.insert_select({
            id: 1,
            args: %({"job_num":1}),
            attempt: 1,
            attempted_at: now,
            attempted_by: ::Sequel.pg_array(["client1"]),
            created_at: now,
            finalized_at: now,
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            priority: River::PRIORITY_DEFAULT,
            queue: River::QUEUE_DEFAULT,
            scheduled_at: now,
            state: River::JOB_STATE_COMPLETED,
            tags: ::Sequel.pg_array(["tag1"]),
            unique_key: ::Sequel.blob(Digest::SHA256.digest("unique_key_str"))
          })

          job_row = driver.send(:to_job_row, river_job)

          expect(job_row).to be_an_instance_of(River::JobRow)
          expect(job_row).to have_attributes(
            id: 1,
            args: {"job_num" => 1},
            attempt: 1,
            attempted_at: be_within(2).of(now.getutc),
            attempted_by: ["client1"],
            created_at: be_within(2).of(now.getutc),
            finalized_at: be_within(2).of(now.getutc),
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            priority: River::PRIORITY_DEFAULT,
            queue: River::QUEUE_DEFAULT,
            scheduled_at: be_within(2).of(now.getutc),
            state: River::JOB_STATE_COMPLETED,
            tags: ["tag1"],
            unique_key: Digest::SHA256.digest("unique_key_str")
          )
        end

        it "with errors" do
          now = Time.now.utc
          river_job = DB[:river_job].returning.insert_select({
            args: %({"job_num":1}),
            errors: ::Sequel.pg_array([
              ::Sequel.pg_json_wrap({
                at: now,
                attempt: 1,
                error: "job failure",
                trace: "error trace"
              })
            ]),
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            state: River::JOB_STATE_AVAILABLE
          })

          job_row = driver.send(:to_job_row, river_job)

          expect(job_row.errors.count).to be(1)
          expect(job_row.errors[0]).to be_an_instance_of(River::AttemptError)
          expect(job_row.errors[0]).to have_attributes(
            at: now.floor(0),
            attempt: 1,
            error: "job failure",
            trace: "error trace"
          )
        end
      end
    end
  end

  next unless SQLITE_DB

  context "with SQLite", database: :sqlite do
    around(:each) { |ex| sqlite_test_transaction(&ex) }

    let!(:driver) { River::Driver::Sequel.new(SQLITE_DB) }
    let(:client) { River::Client.new(driver) }

    it_behaves_like "driver shared examples"
    it_behaves_like "historical attempt error decoding"
    it_behaves_like "SQLite corrupt job isolation"

    describe "client inserts" do
      it "persists JSON columns as JSONB objects" do
        insert_res = client.insert(SimpleArgs.new(job_num: 1))

        row = SQLITE_DB.fetch(<<~SQL, insert_res.job.id).first
          SELECT
            json(args) AS args,
            json_type(args) AS args_type,
            typeof(args) AS args_storage_type,
            typeof(metadata) AS metadata_storage_type,
            typeof(tags) AS tags_storage_type,
            CAST(created_at AS text) AS created_at,
            CAST(scheduled_at AS text) AS scheduled_at
          FROM river_job
          WHERE id = ?
        SQL

        expect(row[:args_type]).to eq("object")
        expect(JSON.parse(row[:args])).to eq({"job_num" => 1})
        expect(row.values_at(:args_storage_type, :metadata_storage_type, :tags_storage_type)).to eq(["blob", "blob", "blob"])
        expect(row[:created_at]).to match(/\.\d{3}\z/)
        expect(row[:scheduled_at]).to match(/\.\d{3}\z/)
        expect(insert_res.job.errors).to eq([])
      end

      it "inserts a batch atomically" do
        expect do
          client.insert_many([
            SimpleArgs.new(job_num: 1),
            River::InsertManyParams.new(
              SimpleArgs.new(job_num: 2),
              insert_opts: River::InsertOpts.new(priority: 0)
            )
          ])
        end.to raise_error(Sequel::CheckConstraintViolation)

        expect(driver.job_list).to be_empty
      end

      it "notifies each available queue once per batch" do
        client.insert_many([
          SimpleArgs.new(job_num: 1),
          SimpleArgs.new(job_num: 2)
        ])

        rows = SQLITE_DB[:river_notification].order(:id).select(:payload, :topic).all

        expect(rows).to contain_exactly(
          {payload: JSON.dump({queue: River::QUEUE_DEFAULT}), topic: "river_insert"}
        )
      end

      it "handles an empty batch" do
        expect(driver.job_insert_many([])).to eq([])
      end

      it "defaults a missing scheduled_at" do
        params = River::Driver::JobInsertParams.new(
          encoded_args: JSON.dump({job_num: 1}),
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          priority: River::PRIORITY_DEFAULT,
          queue: River::QUEUE_DEFAULT,
          scheduled_at: nil,
          state: River::JOB_STATE_AVAILABLE,
          tags: []
        )

        job, = driver.job_insert(params)

        expect(job.scheduled_at).to be_within(2).of(Time.now.utc)
      end

      it "rounds timestamps to three fractional digits" do
        time = Time.utc(2026, 8, 31, 12, 34, 56) + 0.1236
        expect(driver.send(:format_time, time)).to eq("2026-08-31 12:34:56.124")
      end
    end

    describe "#to_job_row (SQLite)" do
      it "converts a database record to `River::JobRow` with minimal properties" do
        SQLITE_DB[:river_job].insert(
          args: Sequel.function(:jsonb, %({"job_num":1})),
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT
        )
        river_job = SQLITE_DB[:river_job].first

        job_row = driver.send(:to_job_row, river_job)

        expect(job_row).to be_an_instance_of(River::JobRow)
        expect(job_row).to have_attributes(
          id: be_a(Integer),
          args: {"job_num" => 1},
          attempt: 0,
          attempted_at: nil,
          attempted_by: nil,
          created_at: be_within(2).of(Time.now.getutc),
          finalized_at: nil,
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          priority: River::PRIORITY_DEFAULT,
          queue: River::QUEUE_DEFAULT,
          scheduled_at: be_within(2).of(Time.now.getutc),
          state: River::JOB_STATE_AVAILABLE,
          tags: []
        )
      end

      it "converts a database record to `River::JobRow` with all properties" do
        now = Time.now.utc
        now_str = now.iso8601(3)

        SQLITE_DB[:river_job].insert(
          args: Sequel.function(:jsonb, %({"job_num":1})),
          attempt: 1,
          attempted_at: now_str,
          attempted_by: Sequel.function(:jsonb, JSON.dump(["client1"])),
          created_at: now_str,
          finalized_at: now_str,
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          priority: River::PRIORITY_DEFAULT,
          queue: River::QUEUE_DEFAULT,
          scheduled_at: now_str,
          state: River::JOB_STATE_COMPLETED,
          tags: Sequel.function(:jsonb, JSON.dump(["tag1"])),
          unique_key: ::Sequel.blob(Digest::SHA256.digest("unique_key_str"))
        )
        river_job = SQLITE_DB[:river_job].first

        job_row = driver.send(:to_job_row, river_job)

        expect(job_row).to be_an_instance_of(River::JobRow)
        expect(job_row).to have_attributes(
          id: be_a(Integer),
          args: {"job_num" => 1},
          attempt: 1,
          attempted_at: be_within(2).of(now.getutc),
          attempted_by: ["client1"],
          created_at: be_within(2).of(now.getutc),
          finalized_at: be_within(2).of(now.getutc),
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          priority: River::PRIORITY_DEFAULT,
          queue: River::QUEUE_DEFAULT,
          scheduled_at: be_within(2).of(now.getutc),
          state: River::JOB_STATE_COMPLETED,
          tags: ["tag1"],
          unique_key: Digest::SHA256.digest("unique_key_str")
        )
      end

      it "with errors" do
        now = Time.now.utc

        SQLITE_DB[:river_job].insert(
          args: Sequel.function(:jsonb, %({"job_num":1})),
          errors: Sequel.function(:jsonb, JSON.dump([{
            at: now.iso8601,
            attempt: 1,
            error: "job failure",
            trace: "error trace"
          }])),
          kind: "simple",
          max_attempts: River::MAX_ATTEMPTS_DEFAULT,
          state: River::JOB_STATE_AVAILABLE
        )
        river_job = SQLITE_DB[:river_job].first

        job_row = driver.send(:to_job_row, river_job)

        expect(job_row.errors.count).to be(1)
        expect(job_row.errors[0]).to be_an_instance_of(River::AttemptError)
        expect(job_row.errors[0]).to have_attributes(
          at: now.floor(0),
          attempt: 1,
          error: "job failure",
          trace: "error trace"
        )
      end
    end
  end
end
