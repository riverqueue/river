# frozen_string_literal: true

require "spec_helper"

describe River::JobArgsHash do
  it "generates a job args based on a hash" do
    args = River::JobArgsHash.new("my_hash_kind", {job_num: 123})
    expect(args).to have_attributes(
      kind: "my_hash_kind",
      to_json: JSON.generate({job_num: 123})
    )
  end

  it "round-trips JSON-compatible arguments" do
    args = described_class.new(:example, values: [nil, true, false, 1, 1.5, "a\"b", "é", {nested: []}])

    expect(JSON.parse(args.to_json)).to eq("values" => [nil, true, false, 1, 1.5, "a\"b", "é", {"nested" => []}])
  end

  it "rejects nonfinite numbers when encoding arguments" do
    [Float::NAN, Float::INFINITY, -Float::INFINITY].each do |value|
      expect { described_class.new(:example, value: value).to_json }.to raise_error(JSON::GeneratorError)
    end
  end

  it "rejects arguments beyond the default JSON nesting limit" do
    nested = 150.times.reduce(nil) { |value, _| [value] }

    expect { described_class.new(:example, nested: nested).to_json }.to raise_error(JSON::NestingError)
  end

  it "does not depend on or modify application-wide JSON.dump options" do
    options = JSON.dump_default_options.dup
    JSON.dump_default_options[:max_nesting] = 1
    JSON.dump_default_options[:allow_nan] = true

    expect(described_class.new(:example, values: [1]).to_json).to eq('{"values":[1]}')
    expect { described_class.new(:example, value: Float::NAN).to_json }.to raise_error(JSON::GeneratorError)
    expect(JSON.dump_default_options).to include(max_nesting: 1, allow_nan: true)
  ensure
    JSON.dump_default_options.replace(options)
  end

  it "errors on a nil kind" do
    expect do
      River::JobArgsHash.new(nil, {job_num: 123})
    end.to raise_error(ArgumentError, "kind should be non-nil")
  end

  it "errors on a nil hash" do
    expect do
      River::JobArgsHash.new("my_hash_kind", nil)
    end.to raise_error(ArgumentError, "hash should be non-nil")
  end
end

describe River::AttemptError do
  it "initializes with parameters" do
    now = Time.now

    attempt_error = River::AttemptError.new(
      at: now,
      attempt: 1,
      error: "job failure",
      trace: "error trace"
    )

    expect(attempt_error).to have_attributes(
      at: now,
      attempt: 1,
      error: "job failure",
      trace: "error trace"
    )
  end

  it "serializes local and frozen timestamps without changing their timezones" do
    time = Time.new(2026, 1, 2, 3, 4, 5, "+05:30")
    error = described_class.new(at: time, attempt: 1, error: "failed", trace: "")

    expect(error.to_h[:at]).to eq("2026-01-01T21:34:05.000000Z")
    expect(time.utc_offset).to eq(19_800)
    time.freeze
    expect(error.to_h[:at]).to eq("2026-01-01T21:34:05.000000Z")
  end
end
