# frozen_string_literal: true

require "time"

module River::Driver
  # Tolerance applies only to database reads, not application JSON encoding.
  # Historical error entries must not make an otherwise valid job unreadable.
  class JobRowDecoder
    ZERO_TIME = Time.utc(1).freeze
    private_constant :ZERO_TIME

    def self.attempt_error(value)
      value = value.__getobj__ if value.respond_to?(:__getobj__)
      fields = value.is_a?(Hash) ? value : {"error" => value}
      River::AttemptError.new(
        at: error_time(fields["at"]),
        attempt: error_integer(fields["attempt"]),
        error: error_string(fields["error"]),
        trace: error_string(fields["trace"])
      )
    end

    def self.error_time(value)
      return ZERO_TIME unless value.is_a?(String)

      # Keep Ruby's existing acceptance of Postgres-style timestamps too.
      Time.parse(value).utc
    rescue ArgumentError
      ZERO_TIME
    end

    def self.error_integer(value)
      return value if value.is_a?(Integer)

      if value.is_a?(String)
        integer = Integer(value, 10, exception: false)
        return integer if integer

        value = Float(value, exception: false)
      end
      (value.is_a?(Float) && value.finite? && value.abs <= 2**53 && value == value.truncate) ? value.to_i : 0
    end

    def self.error_string(value)
      case value
      when nil then ""
      when String then value
      else JSON.generate(value)
      end
    end

    def initialize
      @errors = []
    end

    def json(field, raw, type: nil, strings: false, default: nil)
      return default if raw.nil?

      decoded(field, JSON.parse(raw), type: type, strings: strings, default: default)
    rescue JSON::ParserError, TypeError => error
      @errors << "#{field}: #{error.message}"
      default
    end

    def decoded(field, value, type: nil, strings: false, default: nil)
      # Sequel's Postgres JSON and array wrappers delegate to Ruby values.
      value = value.__getobj__ if value.respond_to?(:__getobj__)
      return default if value.nil?

      raise TypeError, "expected #{type}" if type && !value.is_a?(type)
      raise TypeError, "expected an array of strings" if strings && !value.all? { |item| item.is_a?(String) }

      value
    rescue TypeError => error
      @errors << "#{field}: #{error.message}"
      default
    end

    def finish(job)
      raise River::JobRowDecodeError.new(job, @errors.join("; ")) unless @errors.empty?

      job
    end
  end
end
