# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::Driver::JobRowDecoder do
  it "decodes canonical errors without changing their values" do
    value = {"at" => "2026-09-30T12:34:56.123456Z", "attempt" => 2, "error" => "failed", "trace" => "stack"}
    expect(described_class.attempt_error(value).to_h.transform_keys(&:to_s)).to eq(value)
  end

  it "tolerates historical error values without losing structured messages" do
    [nil, "legacy", 42, ["trace"], {"error" => {"message" => "failed"}, "trace" => ["frame"]}].each do |value|
      error = described_class.attempt_error(value)
      expect(error.at).to eq(Time.utc(1))
      expect(error.attempt).to eq(0)
      expected = value.is_a?(Hash) ? value["error"] : value
      expect(error.error).to eq(described_class.error_string(expected))
    end
    expect(described_class.attempt_error({"trace" => ["frame"]}).trace).to eq('["frame"]')
  end

  it "tolerates unreadable timestamps while retaining Ruby's existing timestamp formats" do
    [nil, 123, "invalid", "2026-99-30T12:34:56Z"].each do |value|
      expect(described_class.error_time(value)).to eq(Time.utc(1))
    end
    expect(described_class.error_time("2026-09-30T14:34:56+02:00")).to eq(Time.utc(2026, 9, 30, 12, 34, 56))
    expect(described_class.error_time("2026-09-30 12:34:56+00")).to eq(Time.utc(2026, 9, 30, 12, 34, 56))
  end

  it "accepts JSON null for nullable collections" do
    expect(described_class.new.json(:tags, "null", type: Array, strings: true)).to be_nil
  end

  it "reports non-string JSON input through the partial row error" do
    decoder = described_class.new
    expect(decoder.json(:metadata, 42, type: Hash, default: {})).to eq({})
    row = Struct.new(:id, :__decode_error).new(123)
    expect { decoder.finish(row) }.to raise_error(River::JobRowDecodeError, /metadata/)
  end

  it "accepts integral attempts and numeric strings without truncating fractions" do
    {1 => 1, " 2 " => 2, "3.0" => 3, "4e0" => 4, 5.0 => 5, 1.5 => 0,
     "1.5" => 0, "invalid" => 0, nil => 0, true => 0, Float::INFINITY => 0,
     1e20 => 0}.each do |value, expected|
      expect(described_class.error_integer(value)).to eq(expected)
    end
  end
end
