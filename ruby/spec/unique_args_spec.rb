# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::UniqueArgs do
  it "retains raw values and ignores outer whitespace" do
    expect(described_class.encode(" { \"z\": -0, \"a\": { \"nested\": [1e0, 2] } } ", true))
      .to eq('{"a":{ "nested": [1e0, 2] },"z":-0}')
  end

  it "preserves token offsets with JSON whitespace and multibyte characters" do
    json = "\t{\r\n \"é🌊\" \t:\n [ \"雪\", {\"escaped\": \"\\u003c\"} ],\r \"number\": -0\n} \r\n"
    expect(described_class.encode(json, true)).to eq('{"number":-0,"é🌊":[ "雪", {"escaped": "\u003c"} ]}')
  end

  it "preserves long strings containing escaped quotes and structural characters" do
    raw_value = JSON.generate("\"\\{},[]:\n雪" * 10_000)
    expect(described_class.encode("{\"value\":#{raw_value},\"a\":1}", true)).to eq("{\"a\":1,\"value\":#{raw_value}}")
  end

  it "stops at an unterminated string instead of searching past escaped quotes" do
    json = '{"' + '\"' * 8_000 + ":1}"
    # Recovering tokens after the opening quote both invents a member and makes
    # the scan quadratic. Check that it stops without relying on timing limits.
    expect(described_class.members(json)).to eq({})
    expect { described_class.encode(json, true) }.to raise_error(JSON::ParserError)
  end

  it "sorts all arguments for an empty selection" do
    expect(described_class.encode('{"z":0,"a":1}', [])).to eq('{"a":1,"z":0}')
  end

  it "distinguishes literal field names from nested paths" do
    expect(described_class.encode('{"user.id":1,"user":{"id":2}}', ["user.id", [:user, :id]]))
      .to eq('{"user":{"id":2},"user.id":1}')
  end

  it "omits missing fields but retains null and false" do
    expect(described_class.encode('{"null":null,"false":false,"scalar":1}', [:null, "false", [:scalar, :missing]]))
      .to eq('{"false":false,"null":null}')
  end

  it "keeps a selected whole object when a child is also selected" do
    expect(described_class.encode('{"account":{ "z":-0, "id":1 }}', [[:account, :id], :account]))
      .to eq('{"account":{ "z":-0, "id":1 }}')
  end

  it "rejects empty paths and invalid JSON" do
    expect { described_class.encode("{}", [[]]) }.to raise_error(ArgumentError, /paths must not be empty/)
    expect { described_class.encode("{", true) }.to raise_error(JSON::ParserError)
  end

  it "truncates periods against Go's year-one epoch without rounding across a boundary" do
    client = River::Client.new(Object.new)
    boundary = Time.utc(2026, 1, 2, 3, 4, 5)
    expect(client.send(:truncate_time, boundary - Rational(1, 1_000_000_000), 1)).to eq(boundary - 1)
    time = Time.at(0).utc
    # Unix epoch is not divisible by seven seconds measured from Go's epoch.
    expect(client.send(:truncate_time, time, 7)).to eq(Time.at(-4))
  end
end
