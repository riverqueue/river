# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"

RSpec.describe River::UniqueBitmask do
  describe ".from_states" do
    it "converts an array of states to a bitmask string" do
      expect(described_class.from_states(River::Client.const_get(:DEFAULT_UNIQUE_STATES))).to eq("11110101")
      expect(described_class.from_states([River::JOB_STATE_AVAILABLE, River::JOB_STATE_PENDING, River::JOB_STATE_RUNNING, River::JOB_STATE_SCHEDULED])).to eq("11010001")
      expect(described_class.from_states([River::JOB_STATE_AVAILABLE])).to eq("00000001")
    end
  end

  describe ".to_states" do
    it "converts a bitmask string to an array of states" do
      expect(described_class.to_states(0b11110101)).to eq(River::Client.const_get(:DEFAULT_UNIQUE_STATES))
      expect(described_class.to_states(0b11010001)).to eq([River::JOB_STATE_AVAILABLE, River::JOB_STATE_PENDING, River::JOB_STATE_RUNNING, River::JOB_STATE_SCHEDULED])
      expect(described_class.to_states(0b00000001)).to eq([River::JOB_STATE_AVAILABLE])
    end
  end
end
