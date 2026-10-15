# frozen_string_literal: true

module River
  # Contains an interface used by the top-level River module to interface with
  # its driver implementations. All types and methods in this module should be
  # considered to be for internal use only and subject to change. API stability
  # is not guaranteed.
  module Driver
    # Insert parameters for a job. This is sent to underlying drivers and is meant
    # for internal use only. Its interface is subject to change.
    class JobInsertParams
      attr_accessor :args
      attr_accessor :encoded_args
      attr_accessor :kind
      attr_accessor :max_attempts
      attr_accessor :metadata
      attr_accessor :priority
      attr_accessor :queue
      attr_accessor :scheduled_at
      attr_accessor :state
      attr_accessor :tags
      attr_accessor :unique_key
      attr_accessor :unique_states

      def initialize(
        encoded_args:,
        kind:,
        max_attempts:,
        priority:, queue:, scheduled_at:, state:, tags:, metadata: {},
        unique_key: nil,
        unique_states: nil,
        args: nil
      )
        self.args = args
        self.encoded_args = encoded_args
        self.kind = kind
        self.max_attempts = max_attempts
        self.metadata = metadata
        self.priority = priority
        self.queue = queue
        self.scheduled_at = scheduled_at
        self.state = state
        self.tags = tags
        self.unique_key = unique_key
        self.unique_states = unique_states
      end
    end
  end
end

require_relative "driver/postgres_capabilities"
require_relative "driver/job_row_decoder"
require_relative "driver/notification_listener"
require_relative "driver/runtime"
