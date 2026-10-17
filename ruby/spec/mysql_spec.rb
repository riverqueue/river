# frozen_string_literal: true

require "spec_helper"
require "active_record"
require "sequel"
require "riverqueue-activerecord"
require "riverqueue-sequel"
require_relative "driver_shared_examples"
require_relative "support/client_test_database"

RSpec.describe "MySQL drivers", database: :mysql do
  [:active_record, :sequel].each do |adapter|
    context adapter.to_s do
      before(:all) do
        @admin = Sequel.connect(ENV.fetch("TEST_MYSQL_URL", "mysql2://root@127.0.0.1/"))
        @schema = "river_ruby_mysql_#{SecureRandom.hex(8)}"
        @admin.run("CREATE DATABASE #{@schema}")
        config = @admin.opts.merge(database: @schema, max_connections: 10)
        if adapter == :sequel
          @database = Sequel.connect(config)
          @driver = River::Driver::Sequel.new(@database)
        else
          @original_connection = ClientTestDatabase.active_record_config
          ActiveRecord::Base.establish_connection(adapter: "mysql2", host: config[:host], port: config[:port],
            username: config[:user], password: config[:password], database: @schema, pool: 10)
          @driver = River::Driver::ActiveRecord.new
        end
        River::Migrator.new(@driver).migrate
      end

      after(:all) do
        @database&.disconnect
        if adapter == :active_record
          if @original_connection
            ActiveRecord::Base.establish_connection(@original_connection)
          else
            ActiveRecord::Base.remove_connection
          end
        end
        @admin&.run("DROP DATABASE #{@schema}") if @schema
        @admin&.disconnect
      end

      around(:each) do |example|
        driver.transaction do
          example.run
          raise driver.rollback_exception
        end
      end

      let(:driver) { (adapter == :sequel) ? River::Driver::Sequel.new(@database) : River::Driver::ActiveRecord.new }
      let(:client) { River::Client.new(driver) }

      it_behaves_like "driver shared examples"

      it "handles empty batches and uses the database clock for an omitted schedule" do
        expect(driver.job_insert_many([])).to eq([])
        params = River::Driver::JobInsertParams.new(encoded_args: "{}", kind: "simple", max_attempts: 25,
          priority: 1, queue: "default", scheduled_at: nil, state: "available", tags: nil)
        row, duplicate = driver.job_insert(params)
        expect(duplicate).to be(false)
        expect(row.scheduled_at).to be_within(2).of(Time.now.getutc)
        expect { driver.send(:runtime_time, nil) }.to raise_error(ArgumentError, /nil/)
      end

      it "appends scalar attempt errors without changing their JSON type" do
        row = client.insert(SimpleArgs.new(job_num: 1)).job
        sql = driver.send(:runtime_append_error, JSON.generate("a scalar error"))
        driver.send(:runtime_execute, "UPDATE river_job SET errors = #{sql} WHERE id = #{row.id}")
        expect(driver.job_get_by_id(row.id).errors).to contain_exactly(have_attributes(error: "a scalar error"))
      end

      it "matches kinds and queues exactly, including trailing spaces" do
        wanted = client.insert(SimpleArgs.new(job_num: 1)).job
        padded = client.insert(SimpleArgs.new(job_num: 2)).job
        driver.send(:runtime_execute, "UPDATE river_job SET kind = 'simple ', queue = 'default ' WHERE id = #{padded.id}")
        expect(client.job_list(River::JobListParams.new(kinds: ["simple"])).jobs.map(&:id)).to eq([wanted.id])
        expect(client.job_list(River::JobListParams.new(queues: ["default"])).jobs.map(&:id)).to eq([wanted.id])
        expect(driver.job_get_available(queue: "default", max: 10, attempted_by: "test").map(&:id)).to eq([wanted.id])
      end

      it "keeps adjacent large IDs distinct" do
        first = client.insert(SimpleArgs.new(job_num: 1)).job
        second = client.insert(SimpleArgs.new(job_num: 2)).job
        driver.send(:runtime_execute, "UPDATE river_job SET id = #{2**53} WHERE id = #{first.id}")
        driver.send(:runtime_execute, "UPDATE river_job SET id = #{2**53 + 1} WHERE id = #{second.id}")
        expect(client.job_list(River::JobListParams.new(ids: [2**53 + 1])).jobs.map(&:id)).to eq([2**53 + 1])
      end
    end
  end
end

require_relative "client_driver_shared_examples"

RSpec.describe "MySQL worker integration", database: :mysql do
  [:active_record, :sequel].each do |adapter|
    context adapter.to_s do
      around do |example|
        ClientTestDatabase.public_send("with_#{adapter}", :mysql) do |driver|
          @driver = driver
          example.run
        end
      end

      it_behaves_like "client driver end to end"

      it "rejects conflicts on an unrelated unique index" do
        @driver.send(:runtime_execute, "CREATE UNIQUE INDEX mysql_test_kind ON river_job(kind)")
        client = River::Client.new(@driver)
        client.insert(SimpleArgs.new(job_num: 1))
        expect { client.insert(SimpleArgs.new(job_num: 2)) }.to raise_error(River::Error, /other than River/)
        expect { client.insert(SimpleArgs.new(job_num: 3), unique_opts: River::UniqueOpts.new(by_args: true)) }.to raise_error(River::Error, /other than River/)
      end

      it "rejects unrelated conflicts with inactive unique keys" do
        @driver.send(:runtime_execute, "CREATE UNIQUE INDEX mysql_test_kind ON river_job(kind)")
        client = River::Client.new(@driver)
        unique = River::UniqueOpts.new(by_args: true, by_state: [:available, :pending, :running, :scheduled])
        params = client.send(:make_insert_params, SimpleArgs.new(job_num: 1), River::InsertOpts.new(unique_opts: unique))
        job, = @driver.job_insert(params)
        params.state = "retryable"
        expect { @driver.job_insert(params) }.to raise_error(River::Error, /other than River/)

        @driver.send(:runtime_execute, "UPDATE river_job SET unique_states = NULL WHERE id = #{job.id}")
        params.state = "available"
        expect { @driver.job_insert(params) }.to raise_error(River::Error, /other than River/)
      end

      it "migrates down and back up using the Go migration history" do
        migrator = River::Migrator.new(@driver)
        expect(migrator.status).to all(have_attributes(applied: true))
        migrator.migrate(direction: :down, target: 0)
        expect(migrator.status).to all(have_attributes(applied: false))
        migrator.migrate
        expect(migrator.status).to all(have_attributes(applied: true))
      end
    end
  end
end

RSpec.describe "MySQL cleaner", database: :mysql do
  [:active_record, :sequel].each do |adapter|
    it "skips locked jobs with #{adapter}" do
      ClientTestDatabase.public_send("with_#{adapter}", :mysql) do |driver|
        params = River::Driver::JobInsertParams.new(encoded_args: "{}", kind: "cleanup", max_attempts: 25,
          priority: 1, queue: "default", scheduled_at: nil, state: "available", tags: [])
        running, locked, *expired = 1002.times.map { driver.job_insert(params).first.id }
        driver.send(:runtime_execute, "UPDATE river_job SET state = 'completed', finalized_at = '1970-01-01' WHERE id >= #{locked}")
        driver.send(:runtime_execute, "UPDATE river_job SET state = 'running' WHERE id = #{running}")
        ready = Queue.new
        release = Queue.new
        locker = Thread.new do
          driver.transaction do
            [running, locked].each do |id|
              driver.send(:runtime_query_rows, "SELECT id FROM river_job WHERE id = #{id} FOR UPDATE")
            end
            ready << true
            release.pop
          end
        end
        begin
          Timeout.timeout(5) { ready.pop }
          deleted = Timeout.timeout(2) { driver.job_delete_finalized(retention: {completed: 0}, max: 1000) }
          expect(deleted).to eq(1000)
          expect(driver.job_get_by_id(expired.first)).to be_nil
          expect(driver.job_get_by_id(locked).state).to eq("completed")
          expect(driver.job_get_by_id(running).state).to eq("running")
        ensure
          release << true
          locker.join
        end
      end
    end
  end
end
