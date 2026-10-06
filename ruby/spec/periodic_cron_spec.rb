# frozen_string_literal: true

require "spec_helper"
require "open3"

RSpec.describe River::PeriodicCron do
  it "does not load Fugit when requiring River or using intervals" do
    output, status = Open3.capture2(RbConfig.ruby, "-Ilib", "-e", <<~RUBY)
      require "riverqueue"
      River::PeriodicInterval.new(60).next(Time.now)
      abort "Fugit was loaded" if defined?(Fugit)
    RUBY
    expect(status.success?).to be(true), output
  end

  it "returns the next UTC occurrence, excluding an exact boundary" do
    schedule = described_class.new("*/15 * * * *")
    now = Time.utc(2026, 1, 1, 9)

    expect(schedule.next(now)).to eq(Time.utc(2026, 1, 1, 9, 15))
    expect(schedule.next(now + 0.5)).to have_attributes(utc?: true, year: 2026)
    expect(now).to eq(Time.utc(2026, 1, 1, 9))
  end

  it "supports aliases and optional seconds" do
    now = Time.utc(2026, 1, 1)

    expect(described_class.new("@daily").next(now)).to eq(Time.utc(2026, 1, 2))
    expect(described_class.new("*/10 * * * * *").next(now)).to eq(now + 10)
  end

  it "uses UTC by default even when the supplied time has a different offset" do
    schedule = described_class.new("0 9 * * *")

    expect(schedule.next(Time.new(2026, 1, 1, 9, 0, 0, "+08:00"))).to eq(Time.utc(2026, 1, 1, 9))
  end

  it "calculates weekdays in the requested timezone across daylight-saving changes" do
    schedule = described_class.new("0 9 * * 1-5", timezone: "America/New_York")

    expect(schedule.next(Time.utc(2026, 3, 6, 14))).to eq(Time.utc(2026, 3, 9, 13))
    expect(schedule.next(Time.utc(2026, 10, 30, 13))).to eq(Time.utc(2026, 11, 2, 14))
  end

  it "skips a nonexistent spring-forward local time" do
    schedule = described_class.new("30 2 * * *", timezone: "America/New_York")

    expect(schedule.next(Time.utc(2026, 3, 7, 7, 30))).to eq(Time.utc(2026, 3, 9, 6, 30))
  end

  it "matches either restricted day-of-month or day-of-week" do
    schedule = described_class.new("0 0 13 * FRI")

    expect(schedule.next(Time.utc(2026, 1, 1))).to eq(Time.utc(2026, 1, 2))
    expect(schedule.next(Time.utc(2026, 1, 12))).to eq(Time.utc(2026, 1, 13))
  end

  it "finds leap days" do
    expect(described_class.new("0 0 29 2 *").next(Time.utc(2026, 1, 1))).to eq(Time.utc(2028, 2, 29))
  end

  it "honors a timezone prefix over the keyword timezone" do
    schedule = described_class.new("CRON_TZ=America/New_York 0 9 * * *", timezone: "UTC")
    expect(schedule.next(Time.utc(2026, 1, 1))).to eq(Time.utc(2026, 1, 1, 14))
  end

  it "returns both occurrences of a repeated wall time even from a local reference" do
    schedule = described_class.new("30 1 * * *", timezone: "America/New_York")
    first = Time.new(2026, 11, 1, 1, 30, 0, "-04:00")
    expect(schedule.next(first)).to eq(Time.utc(2026, 11, 1, 6, 30))
  end

  {"0" => 1, "-1h" => 1, "+1.5s" => 1, ".5h" => 1800,
   "1ns1us1µs1μs" => 1, "9223372036854775807ns" => 9_223_372_036,
   "-9223372036854775808ns" => 1}.each do |duration, seconds|
    it "matches Go's interval normalization for #{duration}" do
      now = Time.utc(2026, 1, 1) + Rational(9, 10)
      expect(described_class.new("@every #{duration}").next(now)).to eq(Time.at(now.to_i + seconds).utc)
    end
  end

  %w[1d -+1s ++1s 1e3s 1 0.0 1s? 9223372036854775808ns -9223372036854775809ns].each do |duration|
    it "rejects invalid Go duration #{duration}" do
      expect { described_class.new("@every #{duration}") }.to raise_error(ArgumentError)
    end
  end

  it "works through the existing periodic job and bundle interfaces" do
    job = River::PeriodicJob.new(constructor: -> {}, run_on_start: true, schedule: described_class.new("0 9 * * *"))
    jobs = River::PeriodicJobBundle.new([job], wake: -> {})
    now = Time.now.utc + 1

    expect(jobs.due(now)).to eq([job])
    expect(jobs.due(now)).to be_empty
    expect(jobs.due(job.next_at(now))).to eq([job])
  end

  [nil, 123, "", "not cron", "60 * * * *", "0 9 * * * America/New_York"].each do |expression|
    it "rejects invalid or timezone-suffixed expression #{expression.inspect}" do
      expect { described_class.new(expression) }.to raise_error(ArgumentError)
    end
  end

  [nil, "", "UTC extra", "Not/A_Zone"].each do |timezone|
    it "rejects invalid timezone #{timezone.inspect}" do
      expect { described_class.new("0 9 * * *", timezone: timezone) }.to raise_error(ArgumentError)
    end
  end
end
