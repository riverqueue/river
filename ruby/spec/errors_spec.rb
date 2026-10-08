# frozen_string_literal: true

require "spec_helper"

RSpec.describe ".job_cancel" do
  it "builds an error with the default message" do
    error = River.job_cancel

    expect(error).to be_a(River::JobCancelError)
    expect(error).to have_attributes(
      cause: be_nil,
      message: "job cancelled"
    )
  end

  it "builds an error from a message" do
    error = River.job_cancel("account closed")

    expect(error).to be_a(River::JobCancelError)
    expect(error).to have_attributes(
      cause: be_nil,
      message: "account closed"
    )
  end

  it "wraps an exception" do
    cause = RuntimeError.new("account closed")
    error = River.job_cancel(cause)

    expect(error).to be_a(River::JobCancelError)
    expect(error).to have_attributes(
      cause: equal(cause),
      message: "account closed"
    )
  end
end

RSpec.describe ".job_snooze" do
  it "builds a snooze error" do
    error = River.job_snooze(30)

    expect(error).to be_a(River::JobSnoozeError)
    expect(error.duration).to eq(30.0)
  end
end

RSpec.describe River::JobCancelError do
  it "has a default message" do
    expect(described_class.new.message).to eq("job cancelled")
  end

  it "retains a custom message and cause" do
    cause = RuntimeError.new("original")
    error = described_class.new("cancelled externally", cause: cause)

    expect(error).to have_attributes(
      cause: equal(cause),
      message: "cancelled externally"
    )
  end
end

RSpec.describe River::JobSnoozeError do
  [Float::NAN, Float::INFINITY, -Float::INFINITY].each do |seconds|
    it "rejects non-finite snoozes of #{seconds}" do
      expect { described_class.new(seconds) }.to raise_error(ArgumentError, "duration must be finite")
    end
  end

  it "coerces and exposes its duration" do
    error = described_class.new("1.5")

    expect(error).to have_attributes(
      duration: 1.5,
      message: "job snoozed for 1.5 seconds"
    )
  end

  it "allows an immediate retry" do
    expect(described_class.new(0).duration).to eq(0.0)
  end

  it "rejects a negative duration" do
    expect { described_class.new(-1) }
      .to raise_error(ArgumentError, "duration must be zero or greater")
  end

  it "rejects a nonnumeric duration" do
    expect { described_class.new("later") }.to raise_error(ArgumentError)
  end
end

RSpec.describe River::UnknownJobKindError do
  it "exposes the unknown kind" do
    error = described_class.new("missing")

    expect(error).to have_attributes(
      kind: "missing",
      message: "unknown job kind: missing"
    )
  end
end
