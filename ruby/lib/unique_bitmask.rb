# frozen_string_literal: true

module River
  class UniqueBitmask
    def self.from_states(states)
      val = 0

      states.each do |state|
        bit_index = JOB_STATE_BIT_POSITIONS[state]

        bit_position = 7 - (bit_index % 8)
        val |= 1 << bit_position
      end

      format("%08b", val)
    end

    def self.to_states(mask)
      states = [] #: Array[jobStateAll] # rubocop:disable Layout/LeadingCommentSpace

      JOB_STATE_BIT_POSITIONS.each do |state, bit_index|
        bit_position = 7 - (bit_index % 8)
        if (mask & (1 << bit_position)) != 0
          states << state
        end
      end

      states.sort
    end

    JOB_STATE_BIT_POSITIONS = {
      ::River::JOB_STATE_AVAILABLE => 7,
      ::River::JOB_STATE_CANCELLED => 6,
      ::River::JOB_STATE_COMPLETED => 5,
      ::River::JOB_STATE_DISCARDED => 4,
      ::River::JOB_STATE_PENDING => 3,
      ::River::JOB_STATE_RETRYABLE => 2,
      ::River::JOB_STATE_RUNNING => 1,
      ::River::JOB_STATE_SCHEDULED => 0
    }.freeze
    private_constant :JOB_STATE_BIT_POSITIONS
  end
end
