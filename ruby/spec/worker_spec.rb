# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::Workers do
  it "registers and fetches a worker under an explicit kind" do
    worker = Object.new
    registry = described_class.new.add("email", worker)

    expect(registry.fetch("email")).to equal(worker)
    expect(registry).to include("email")
  end

  it "infers kind from a worker instance" do
    worker = Class.new { def kind = :instance_kind }.new
    registry = described_class.new.add(worker)

    expect(registry.fetch("instance_kind")).to equal(worker)
  end

  it "infers kind from an instance's worker class" do
    worker = Class.new { def self.kind = :class_kind }.new
    registry = described_class.new.add(worker)

    expect(registry.fetch("class_kind")).to equal(worker)
  end

  it "infers kind from a worker class" do
    worker_class = Class.new { def self.kind = :class_kind }
    registry = described_class.new.add(worker_class)

    expect(registry.fetch("class_kind")).to equal(worker_class)
  end

  it "registers aliases as strings" do
    worker = Object.new
    registry = described_class.new.add(:primary, worker, aliases: [:old_name, "legacy"])

    expect(registry.kinds).to contain_exactly("primary", "old_name", "legacy")
    expect(registry.fetch("old_name")).to equal(worker)
    expect(registry.fetch(:primary)).to equal(worker)
    expect(registry.fetch(:old_name)).to equal(worker)
    expect(registry).to include(:primary, :old_name, :legacy)
    expect(registry).not_to include(:missing)
  end

  it "rejects a duplicate primary kind" do
    registry = described_class.new.add("email", Object.new)

    expect { registry.add("email", Object.new) }
      .to raise_error(ArgumentError, 'worker for kind "email" is already registered')
  end

  it "rejects a duplicate alias without partially registering the worker" do
    registry = described_class.new.add("existing", Object.new)

    expect { registry.add("fresh", Object.new, aliases: ["existing"]) }
      .to raise_error(ArgumentError, 'worker for kind "existing" is already registered')
    expect(registry).not_to include("fresh")
  end

  it "returns nil for an unknown kind" do
    expect(described_class.new[:missing]).to be_nil
  end

  it "supports bracket lookup for registered kinds" do
    worker = Object.new
    registry = described_class.new.add(:known, worker)

    expect(registry[:known]).to equal(worker)
    expect(registry["known"]).to equal(worker)
  end

  it "fetches with Hash-like missing-key, default, and block behavior" do
    worker = Object.new
    registry = described_class.new.add(:known, worker)

    expect { registry.fetch(:missing) }.to raise_error(KeyError)
    expect(registry.fetch(:missing, nil)).to be_nil
    expect(registry.fetch(:missing, false)).to be false
    expect(registry.fetch(:missing) { |kind| "fallback for #{kind}" }).to eq("fallback for missing")
    expect(registry.fetch(:known) { raise "must not evaluate fallback" }).to equal(worker)
  end

  it "returns a frozen snapshot of registered kinds" do
    registry = described_class.new.add("one", Object.new)
    kinds = registry.kinds
    registry.add("two", Object.new)

    expect(kinds).to eq(["one"])
    expect(kinds).to be_frozen
  end
end

RSpec.describe River::Job do
  let(:row) do
    River::JobRow.new(
      id: 123,
      args: {"value" => 1},
      attempt: 1,
      created_at: Time.now.utc,
      kind: "example",
      max_attempts: 3,
      metadata: {"original" => true},
      priority: 1,
      queue: "default",
      scheduled_at: Time.now.utc,
      state: River::JOB_STATE_RUNNING
    )
  end

  let(:job) { described_class.new(Object.new, row) }

  it "exposes arguments and delegates persisted attributes" do
    expect(job).to have_attributes(
      id: 123,
      args: {"value" => 1},
      kind: "example"
    )
    expect(job).to respond_to(:scheduled_at)
  end

  it "merges metadata updates without mutating the persisted row" do
    expect(job.update_metadata(updated: 2)).to equal(job)

    expect(job.metadata).to eq("original" => true, "updated" => 2)
    expect(row.metadata).to eq("original" => true)
  end

  it "stores worker output in metadata" do
    job.output = {"answer" => 42}

    expect(job.metadata).to include("output" => {"answer" => 42})
  end

  it "returns a defensive copy of pending metadata updates" do
    job.update_metadata("one" => 1)
    copy = job.metadata_updates
    copy["two"] = 2

    expect(job.metadata_updates).to eq("one" => 1)
  end

  it "snapshots JSON metadata and prevents nested mutations from corrupting pending updates" do
    value = {"nested" => [1]}
    job.update_metadata(value: value)
    value["nested"] << Float::NAN
    job.metadata["value"]["nested"] << Float::NAN
    job.metadata_updates["value"]["nested"] << Float::NAN

    expect(job.metadata_updates).to eq("value" => {"nested" => [1]})
  end

  it "rejects invalid JSON without replacing previously valid metadata" do
    job.update_metadata(value: 1)
    expect { job.update_metadata(value: Float::INFINITY) }.to raise_error(JSON::GeneratorError)
    expect(job.metadata_updates).to eq("value" => 1)
  end

  it "raises normally for an unknown delegated method" do
    expect { job.not_a_job_attribute }.to raise_error(NoMethodError)
    expect(job).not_to respond_to(:not_a_job_attribute)
  end
end

RSpec.describe River::DefaultClientRetryPolicy do
  def random_returning(value)
    Object.new.tap { |random| random.define_singleton_method(:rand) { value } }
  end

  it "uses quartic backoff based on the next error count" do
    policy = described_class.new(random: random_returning(0.5))
    job = Struct.new(:errors).new([Object.new, Object.new])
    now = Time.utc(2026, 1, 1)

    expect(policy.next_retry(job, now: now)).to eq(now + 81)
  end

  it "applies up to ten percent negative jitter" do
    policy = described_class.new(random: random_returning(0.0))
    now = Time.utc(2026, 1, 1)

    expect(policy.next_retry(Struct.new(:errors).new([]), now: now)).to be_within(0.000001).of(now + 0.9)
  end

  it "applies up to ten percent positive jitter" do
    policy = described_class.new(random: random_returning(1.0))
    now = Time.utc(2026, 1, 1)

    expect(policy.next_retry(Struct.new(:errors).new(nil), now: now)).to be_within(0.000001).of(now + 1.1)
  end

  it "caps large error counts at Go's exact maximum duration without jitter" do
    random = Object.new
    random.define_singleton_method(:rand) { raise "capped retries must not apply jitter" }
    policy = described_class.new(random: random)
    now = Time.utc(2026, 1, 1)
    [309, 1_000].each do |count|
      job = Struct.new(:errors).new(Array.new(count))
      expect(policy.next_retry(job, now: now).to_r - now.to_r).to eq(Rational((1 << 63) - 1, 1_000_000_000))
    end
  end

  it "caps again if positive jitter takes the delay past the maximum" do
    policy = described_class.new(random: random_returning(1.0))
    now = Time.utc(2026, 1, 1)
    job = Struct.new(:errors).new(Array.new(308))
    expect(policy.next_retry(job, now: now).to_r - now.to_r).to eq(Rational((1 << 63) - 1, 1_000_000_000))
  end
end
