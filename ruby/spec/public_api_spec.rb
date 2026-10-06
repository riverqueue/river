# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"

RSpec.describe "Ruby client keyword APIs" do
  around { |example| available_test_transaction(&example) }

  let(:driver) { River::Driver::Sequel.new(available_test_database) }
  let(:client) { River::Client.new(driver) }
  let(:args) { River::JobArgsHash.new(:example, value: 1) }

  it "preserves the insertion result class name from earlier releases" do
    expect(River::InsertResult).to equal(River::JobInsertResult)

    result = client.insert(args)
    expect(result).to be_an_instance_of(River::InsertResult)
    expect(client.insert_many([args])).to all(be_an_instance_of(River::InsertResult))

    legacy_result = River::InsertResult.new(result.job, unique_skipped_as_duplicated: true)
    expect(legacy_result.job).to equal(result.job)
    expect(legacy_result).to be_unique_skipped_as_duplicate
    expect(legacy_result.unique_skipped_as_duplicated).to be true
  end

  it "accepts insertion keywords with the same defaults and uniqueness behavior" do
    unique = River::UniqueOpts.new(by_args: true)
    first = client.insert(args, queue: :critical, priority: 2, unique_opts: unique)
    duplicate = client.insert(args, queue: :critical, priority: 2, unique_opts: unique)

    expect(first.job).to have_attributes(queue: "critical", priority: 2)
    expect(first).not_to be_unique_skipped_as_duplicate
    expect(duplicate).to be_unique_skipped_as_duplicate
    expect(first.unique_skipped_as_duplicated).to be false
    expect(duplicate.unique_skipped_as_duplicated).to be true
    expect(duplicate.job.id).to eq(first.job.id)
  end

  it "merges keyword options with argument-level defaults without mutating them" do
    defaults = River::InsertOpts.new(queue: :default_queue, priority: 3, metadata: {"default" => true}).freeze
    args.define_singleton_method(:insert_opts) { defaults }

    result = client.insert(args, queue: :critical, metadata: {"call" => true})

    expect(result.job).to have_attributes(queue: "critical", priority: 3)
    expect(result.job.metadata).to include("default" => true, "call" => true)
    expect(defaults).to have_attributes(queue: :default_queue, metadata: {"default" => true})
  end

  it "accepts per-job keyword options in a mixed batch" do
    results = client.insert_many([
      args,
      River::InsertManyParams.new(args, queue: :critical, priority: 2)
    ])

    expect(results.map { |result| result.job.queue }).to eq(%w[default critical])
    expect(results.last.job.priority).to eq(2)
  end

  it "lists, paginates, and deletes with keyword filters" do
    first, second = client.insert_many([args, args]).map(&:job)
    page = client.job_list(kinds: [:example], limit: 1)
    next_page = client.job_list(kinds: [:example], after: page.last_cursor)

    expect(page.jobs.map(&:id)).to eq([first.id])
    expect(next_page.jobs.map(&:id)).to eq([second.id])
    expect(client.job_delete_many(ids: [first.id]).jobs.map(&:id)).to eq([first.id])
    expect(client.job_list.jobs.map(&:id)).to eq([second.id])
    expect { client.job_delete_many }.to raise_error(ArgumentError, /no filters/)
  end

  it "distinguishes omitted update fields from explicit nil" do
    row = client.insert(args).job
    at = Time.utc(2026, 1, 1)
    client.job_update(row.id, attempted_at: at, max_attempts: 7)

    expect(client.job_update(row.id)).to have_attributes(attempted_at: at, max_attempts: 7)
    expect(client.job_update(row.id, attempted_at: nil)).to have_attributes(attempted_at: nil, max_attempts: 7)
  end

  it "rejects mixed option forms instead of silently choosing precedence" do
    options = River::InsertOpts.new(queue: :default)
    filters = River::JobListParams.new(kinds: [:example])
    updates = River::JobUpdateParams.new(max_attempts: 7)

    expect { client.insert(args, insert_opts: options, queue: :critical) }.to raise_error(ArgumentError, /not both/)
    expect { River::InsertManyParams.new(args, insert_opts: options, queue: :critical) }.to raise_error(ArgumentError, /not both/)
    expect { client.job_list(filters, limit: 1) }.to raise_error(ArgumentError, /not both/)
    expect { client.job_delete_many(filters, limit: 1) }.to raise_error(ArgumentError, /not both/)
    expect { client.job_update(1, updates, max_attempts: 8) }.to raise_error(ArgumentError, /not both/)
    expect(driver.job_list).to be_empty
  end

  it "rejects unknown keywords before making changes" do
    expect { client.insert(args, quue: :critical) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { River::InsertManyParams.new(args, quue: :critical) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { client.job_list(knd: :example) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { client.job_delete_many(knd: :example) }.to raise_error(ArgumentError, /unknown keyword/)
    expect { client.job_update(1, priority: 2) }.to raise_error(ArgumentError, /unknown keyword/)
    expect(driver.job_list).to be_empty
  end
end
