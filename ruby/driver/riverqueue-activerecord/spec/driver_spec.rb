# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/driver_shared_examples"
require_relative "../../../spec/row_decoding_shared_examples"

RSpec.describe River::Driver::ActiveRecord do
  before do
    if ENV["RIVER_DEBUG"] == "1" || ENV["RIVER_DEBUG"] == "true"
      ActiveRecord::Base.logger = Logger.new($stdout)
    end
  end

  {
    "Postgres" => {adapter: :postgres, available: PG_AVAILABLE},
    "SQLite" => {adapter: :sqlite, available: true}
  }.each do |name, config|
    next unless config[:available]

    context "with #{name}", database: config[:adapter] do
      before(:all) do
        if config[:adapter] == :sqlite
          switch_to_sqlite!
        else
          switch_to_postgres!
        end
      end

      after(:all) do
        switch_to_postgres! if config[:adapter] == :sqlite && PG_AVAILABLE
      end

      around(:each) { |ex| test_transaction(&ex) }

      let!(:driver) { River::Driver::ActiveRecord.new }
      let(:client) { River::Client.new(driver) }

      it_behaves_like "driver shared examples"
      it_behaves_like "historical attempt error decoding"
      it_behaves_like "SQLite corrupt job isolation" if config[:adapter] == :sqlite

      describe "client inserts" do
        it "persists SQLite JSON columns as JSONB objects" do
          next unless config[:adapter] == :sqlite

          insert_res = client.insert(SimpleArgs.new(job_num: 1))

          row = ActiveRecord::Base.connection.exec_query(<<~SQL).first
            SELECT
              json(args) AS args,
              json_type(args) AS args_type,
              typeof(args) AS args_storage_type,
              typeof(metadata) AS metadata_storage_type,
              typeof(tags) AS tags_storage_type,
              CAST(created_at AS text) AS created_at,
              CAST(scheduled_at AS text) AS scheduled_at
            FROM river_job
            WHERE id = #{insert_res.job.id}
          SQL

          expect(row["args_type"]).to eq("object")
          expect(JSON.parse(row["args"])).to eq({"job_num" => 1})
          expect(row.values_at("args_storage_type", "metadata_storage_type", "tags_storage_type")).to eq(["blob", "blob", "blob"])
          expect(row["created_at"]).to match(/\.\d{3}\z/)
          expect(row["scheduled_at"]).to match(/\.\d{3}\z/)
          expect(insert_res.job.errors).to eq([])
        end

        it "persists Postgres args as a JSON object rather than a JSON string" do
          next unless config[:adapter] == :postgres

          insert_res = client.insert(SimpleArgs.new(job_num: 1))

          row = ActiveRecord::Base.connection.exec_query(<<~SQL).first
            SELECT args, jsonb_typeof(args) AS args_type
            FROM river_job
            WHERE id = #{insert_res.job.id}
          SQL

          expect(row["args_type"]).to eq("object")
          expect(JSON.parse(row["args"])).to eq({"job_num" => 1})
        end

        it "inserts a SQLite batch atomically" do
          next unless config[:adapter] == :sqlite

          expect do
            client.insert_many([
              SimpleArgs.new(job_num: 1),
              River::InsertManyParams.new(
                SimpleArgs.new(job_num: 2),
                insert_opts: River::InsertOpts.new(priority: 0)
              )
            ])
          end.to raise_error(SQLite3::ConstraintException)

          expect(driver.job_list).to be_empty
        end

        it "notifies each available SQLite queue once per batch" do
          next unless config[:adapter] == :sqlite

          client.insert_many([
            SimpleArgs.new(job_num: 1),
            SimpleArgs.new(job_num: 2)
          ])

          rows = ActiveRecord::Base.connection.exec_query(<<~SQL).to_a
            SELECT payload, topic FROM river_notification ORDER BY id
          SQL
          expect(rows).to contain_exactly(
            {"payload" => JSON.dump({queue: River::QUEUE_DEFAULT}), "topic" => "river_insert"}
          )
        end

        it "handles an empty SQLite batch" do
          next unless config[:adapter] == :sqlite

          expect(driver.job_insert_many([])).to eq([])
        end

        it "defaults a missing SQLite scheduled_at" do
          next unless config[:adapter] == :sqlite

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

        it "rounds SQLite timestamps to three fractional digits" do
          next unless config[:adapter] == :sqlite

          time = Time.utc(2026, 8, 31, 12, 34, 56) + 0.1236

          expect(driver.send(:format_time, time)).to eq("2026-08-31 12:34:56.124")
        end
      end

      describe "#to_job_row_from_model" do
        it "converts a database record to `River::JobRow` with minimal properties" do
          if config[:adapter] == :sqlite
            ActiveRecord::Base.connection.execute(
              ActiveRecord::Base.sanitize_sql_array([
                "INSERT INTO river_job (args, kind, max_attempts) VALUES (?, ?, ?)",
                '{"job_num":1}', "simple", River::MAX_ATTEMPTS_DEFAULT
              ])
            )
          else
            driver.instance_variable_get(:@job_model).create(
              id: 1,
              args: {"job_num" => 1},
              kind: "simple",
              max_attempts: River::MAX_ATTEMPTS_DEFAULT,
              priority: River::PRIORITY_DEFAULT,
              queue: River::QUEUE_DEFAULT,
              state: River::JOB_STATE_AVAILABLE
            )
          end

          river_job = driver.instance_variable_get(:@job_model).first
          job_row = driver.send(:to_job_row_from_model, river_job)

          expect(job_row).to be_an_instance_of(River::JobRow)
          expect(job_row).to have_attributes(
            id: be_a(Integer),
            args: {"job_num" => 1},
            attempt: 0,
            attempted_at: nil,
            attempted_by: nil,
            created_at: river_job.created_at.getutc,
            finalized_at: nil,
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            priority: River::PRIORITY_DEFAULT,
            queue: River::QUEUE_DEFAULT,
            scheduled_at: river_job.scheduled_at.getutc,
            state: River::JOB_STATE_AVAILABLE,
            tags: []
          )
        end

        it "converts a database record to `River::JobRow` with all properties" do
          now = Time.now.utc
          now_str = (config[:adapter] == :sqlite) ? now.iso8601(3) : now.strftime("%Y-%m-%d %H:%M:%S.%3N")

          if config[:adapter] == :sqlite
            # Use raw SQLite connection to avoid binary encoding issues with
            # sanitize_sql_array.
            ActiveRecord::Base.connection.raw_connection.execute(
              "INSERT INTO river_job (attempt, attempted_at, attempted_by, created_at, args, finalized_at, kind, max_attempts, priority, queue, scheduled_at, state, tags, unique_key) VALUES (?, ?, jsonb(?), ?, jsonb(?), ?, ?, ?, ?, ?, ?, ?, jsonb(?), ?)",
              [1, now_str, JSON.dump(["client1"]), now_str, '{"job_num":1}', now_str, "simple",
                River::MAX_ATTEMPTS_DEFAULT, River::PRIORITY_DEFAULT, River::QUEUE_DEFAULT,
                now_str, River::JOB_STATE_COMPLETED, JSON.dump(["tag1"]),
                Digest::SHA256.digest("unique_key_str")]
            )
          else
            driver.instance_variable_get(:@job_model).create(
              id: 1,
              args: {"job_num" => 1},
              attempt: 1,
              attempted_at: now,
              attempted_by: ["client1"],
              created_at: now,
              finalized_at: now,
              kind: "simple",
              max_attempts: River::MAX_ATTEMPTS_DEFAULT,
              priority: River::PRIORITY_DEFAULT,
              queue: River::QUEUE_DEFAULT,
              scheduled_at: now,
              state: River::JOB_STATE_COMPLETED,
              tags: ["tag1"],
              unique_key: Digest::SHA256.digest("unique_key_str")
            )
          end

          river_job = driver.instance_variable_get(:@job_model).first
          job_row = driver.send(:to_job_row_from_model, river_job)

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

          if config[:adapter] == :sqlite
            ActiveRecord::Base.connection.execute(
              ActiveRecord::Base.sanitize_sql_array([
                "INSERT INTO river_job (args, errors, kind, max_attempts, state) VALUES (?, ?, ?, ?, ?)",
                '{"job_num":1}',
                JSON.dump([{at: now.iso8601, attempt: 1, error: "job failure", trace: "error trace"}]),
                "simple", River::MAX_ATTEMPTS_DEFAULT, River::JOB_STATE_AVAILABLE
              ])
            )
          else
            driver.instance_variable_get(:@job_model).create(
              args: {"job_num" => 1},
              errors: [{at: now.iso8601, attempt: 1, error: "job failure", trace: "error trace"}],
              kind: "simple",
              max_attempts: River::MAX_ATTEMPTS_DEFAULT,
              state: River::JOB_STATE_AVAILABLE
            )
          end

          river_job = driver.instance_variable_get(:@job_model).first
          job_row = driver.send(:to_job_row_from_model, river_job)

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

      # Postgres-only: test the raw row conversion used by upsert_all
      next unless config[:adapter] == :postgres

      describe "#postgres_to_job_row_from_raw" do
        it "converts a database record to `River::JobRow` with minimal properties" do
          res = driver.instance_variable_get(:@job_model).insert({
            id: 1,
            args: {"job_num" => 1},
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT
          }, returning: Arel.sql("*, false AS unique_skipped_as_duplicate"))

          job_row, skipped_as_duplicate = driver.send(:postgres_to_job_row_from_raw, res.rows[0], res.columns, res.column_types)

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
          expect(skipped_as_duplicate).to be(false)
        end

        it "converts a database record to `River::JobRow` with all properties" do
          now = Time.now
          res = driver.instance_variable_get(:@job_model).insert({
            id: 1,
            args: {"job_num" => 1},
            attempt: 1,
            attempted_at: now,
            attempted_by: ["client1"],
            created_at: now,
            finalized_at: now,
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            priority: River::PRIORITY_DEFAULT,
            queue: River::QUEUE_DEFAULT,
            scheduled_at: now,
            state: River::JOB_STATE_COMPLETED,
            tags: ["tag1"],
            unique_key: Digest::SHA256.digest("unique_key_str")
          }, returning: Arel.sql("*, true AS unique_skipped_as_duplicate"))

          job_row, skipped_as_duplicate = driver.send(:postgres_to_job_row_from_raw, res.rows[0], res.columns, res.column_types)

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
          expect(skipped_as_duplicate).to be(true)
        end

        it "with errors" do
          now = Time.now.utc
          res = driver.instance_variable_get(:@job_model).insert({
            args: {"job_num" => 1},
            errors: [
              {
                at: now.iso8601,
                attempt: 1,
                error: "job failure",
                trace: "error trace"
              }
            ],
            kind: "simple",
            max_attempts: River::MAX_ATTEMPTS_DEFAULT,
            state: River::JOB_STATE_AVAILABLE
          }, returning: Arel.sql("*, false AS unique_skipped_as_duplicate"))

          job_row, skipped_as_duplicate = driver.send(:postgres_to_job_row_from_raw, res.rows[0], res.columns, res.column_types)

          expect(job_row.errors.count).to be(1)
          expect(job_row.errors[0]).to be_an_instance_of(River::AttemptError)
          expect(job_row.errors[0]).to have_attributes(
            at: now.floor(0),
            attempt: 1,
            error: "job failure",
            trace: "error trace"
          )
          expect(skipped_as_duplicate).to be(false)
        end
      end
    end
  end
end
