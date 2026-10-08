# frozen_string_literal: true

module River
  # A cron schedule for PeriodicJob. Add the optional +fugit+ gem to use it.
  class PeriodicCron
    # Parses a cron expression once. +timezone+ defaults to UTC; use an IANA
    # name or a fixed offset for local schedules. A CRON_TZ= or TZ= prefix takes
    # precedence. Supports Go's @every durations, rounded down to whole seconds
    # with a one-second minimum. Fugit is loaded only on construction.
    def initialize(expression, timezone: "UTC")
      require "fugit"

      raise ArgumentError, "cron expression must be a String" unless expression.is_a?(String)
      raise ArgumentError, "timezone must be a nonempty name without whitespace" unless timezone.is_a?(String) && /\A\S+\z/.match?(timezone)

      expression = expression.strip
      if (prefix = /\A(?:CRON_TZ|TZ)=(\S+)\s+(.+)\z/.match(expression))
        timezone = prefix[1].to_s
        expression = prefix[2].to_s
      end
      # Validate the zone even for interval schedules, which don't use it.
      cron_class = Object.const_get(:Fugit).const_get(:Cron)
      cron_class.do_parse("* * * * * #{timezone}")
      @interval = nil
      if expression.start_with?("@every ")
        @interval = parse_interval(expression.delete_prefix("@every "))
        @cron = nil
      else
        @cron = cron_class.do_parse("#{expression.tr("?", "*")} #{timezone}")
      end
    end

    # Returns a UTC Time strictly after +time+. Interval schedules align to
    # whole seconds, as Go does. Calendar and daylight-saving rules use Fugit.
    def next(time)
      interval = @interval
      return Time.at(time.to_i + interval).utc if interval

      # Keep the reference in UTC so Fugit does not skip a repeated local wall
      # time when daylight saving ends. The schedule owns the calendar zone.
      @cron.next_time(time.getutc).to_t.getutc
    end

    private def parse_interval(duration)
      # Go's time.ParseDuration syntax, deliberately excluding Fugit's extra
      # units (days/months) and natural-language interval expressions.
      sign = duration.start_with?("-") ? -1 : 1
      duration = duration.sub(/\A[+-]/, "")
      parts = duration.scan(/((?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(ns|us|µs|μs|ms|s|m|h)/) #: Array[[String, String]] # rubocop:disable Layout/LeadingCommentSpace
      valid = duration == "0" || (!parts.empty? && parts.map(&:join).join == duration)
      raise ArgumentError, "invalid @every duration" unless valid
      units = {"ns" => 1, "us" => 1_000, "µs" => 1_000, "μs" => 1_000, "ms" => 1_000_000, "s" => 1_000_000_000, "m" => 60_000_000_000, "h" => 3_600_000_000_000}
      nanos = parts.sum { |number, unit| (number.to_r * units.fetch(unit)).to_i } * sign
      raise ArgumentError, "@every duration exceeds Go's duration range" unless (-(1 << 63)...(1 << 63)).cover?(nanos)

      [nanos.div(1_000_000_000), 1].max
    end
  end
end
