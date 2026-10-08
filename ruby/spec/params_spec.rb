# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::JobListParams do
  it "uses stable pagination defaults" do
    params = described_class.new

    expect(params).to have_attributes(after_id: nil, filters?: be(false), limit: 100, sort_by: :id, sort_order: :asc)
  end

  it "accepts every supported filter" do
    params = described_class.new(
      after_id: 10,
      ids: [11],
      kinds: ["email"],
      metadata: {tenant: "one"},
      priorities: [2],
      queues: ["default"],
      states: [River::JOB_STATE_AVAILABLE],
      tags_all: ["one"],
      tags_any: ["two"]
    )

    expect(params).to have_attributes(after_id: 10, filters?: be(true), ids: [11], kinds: ["email"], priorities: [2])
  end

  it "does not consider empty collections to be filters" do
    params = described_class.new(ids: [], kinds: [], metadata: {}, queues: [], states: [], tags_all: [], tags_any: [])

    expect(params.filters?).to be false
  end

  it "normalizes symbol filters without modifying the supplied arrays" do
    kinds = [:email, "report"].freeze
    queues = [:default].freeze
    states = [:available].freeze
    params = described_class.new(kinds: kinds, queues: queues, states: states)

    expect(params).to have_attributes(kinds: %w[email report], queues: ["default"], states: ["available"])
    expect(kinds).to eq([:email, "report"])
    expect(queues).to eq([:default])
    expect(states).to eq([:available])
  end

  it "coerces limit and sorting values" do
    params = described_class.new(limit: "25", sort_by: "scheduled_at", sort_order: "desc")

    expect(params).to have_attributes(limit: 25, sort_by: :scheduled_at, sort_order: :desc)
  end

  [0, 10_001].each do |limit|
    it "rejects limit=#{limit}" do
      expect { described_class.new(limit: limit) }.to raise_error(ArgumentError, /limit must be between/)
    end
  end

  it "rejects an unsupported sort field" do
    expect { described_class.new(sort_by: :priority) }.to raise_error(ArgumentError, "invalid sort field")
  end

  it "rejects an unsupported sort direction" do
    expect { described_class.new(sort_order: :sideways) }.to raise_error(ArgumentError, "invalid sort order")
  end

  it "accepts a cursor as a filter and coerces the ID shortcut" do
    cursor = River::JobListCursor.new(id: 10, sort_by: :id, sort_order: :asc, value: 10)
    expect(described_class.new(after: cursor)).to have_attributes(after: cursor, filters?: be(true))
    expect(described_class.new(after_id: "10").after_id).to eq(10)
  end

  it "rejects ambiguous or incompatible pagination" do
    cursor = River::JobListCursor.new(id: 10, sort_by: :scheduled_at, sort_order: :asc, value: Time.now.utc)
    expect { described_class.new(after: cursor, after_id: 10) }.to raise_error(ArgumentError, /either after or after_id/)
    expect { described_class.new(after_id: 10, sort_by: :scheduled_at) }.to raise_error(ArgumentError, /after_id requires/)
    expect { described_class.new(after: 10) }.to raise_error(ArgumentError, /JobListCursor/)
    expect { described_class.new(after: cursor) }.to raise_error(ArgumentError, /same ordering/)
    expect { described_class.new(after: cursor, sort_by: :scheduled_at, sort_order: :desc) }.to raise_error(ArgumentError, /same ordering/)
    expect(described_class.new(after: cursor, sort_by: :scheduled_at).after).to eq(cursor)
  end
end

RSpec.describe River::JobUpdateParams do
  it "normalizes symbolic states without changing unset or explicitly nil states" do
    expect(described_class.new(state: :available).each.to_h).to eq(state: "available")
    expect(described_class.new(state: nil).each.to_h).to eq(state: nil)
    expect(described_class.new.each.to_h).to eq({})
  end

  it "enumerates only explicitly supplied fields" do
    params = described_class.new(attempt: 2, finalized_at: nil, metadata: {"updated" => true})

    expect(params.each.to_h).to eq(attempt: 2, finalized_at: nil, metadata: {"updated" => true})
  end

  it "distinguishes an explicit nil from an unset field" do
    params = described_class.new(attempted_at: nil)

    expect(params.each.to_a).to eq([[:attempted_at, nil]])
  end

  it "returns an enumerator without a block" do
    expect(described_class.new.each).to be_an(Enumerator)
  end

  it "is empty when no updates are supplied" do
    expect(described_class.new.each.to_a).to be_empty
  end
end
