# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::Subscription do
  let(:matching) { River::Event.new(River::EVENT_JOB_COMPLETED, nil, nil, nil) }
  let(:other) { River::Event.new(River::EVENT_JOB_FAILED, nil, nil, nil) }

  it "publishes subscribed event kinds" do
    subscription = described_class.new(["job_completed"])
    subscription.publish(matching)

    expect(subscription.pop(true)).to equal(matching)
  end

  it "ignores event kinds that were not subscribed" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED])
    subscription.publish(other)

    expect { subscription.pop(true) }.to raise_error(ThreadError)
  end

  it "drops events when its bounded buffer is full" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED], buffer_size: 1)
    subscription.publish(matching)
    subscription.publish(River::Event.new(River::EVENT_JOB_COMPLETED, :second, nil, nil))

    expect(subscription.pop(true)).to equal(matching)
    expect { subscription.pop(true) }.to raise_error(ThreadError)
  end

  it "provides an enumerator and terminates iteration when closed" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED])
    subscription.publish(matching)
    subscription.close

    expect(subscription.each).to be_an(Enumerator)
    expect(subscription.each.to_a).to eq([matching])
  end

  it "detaches exactly once when closed more than once" do
    detached = []
    subscription = described_class.new(
      [River::EVENT_JOB_COMPLETED],
      on_close: ->(value) { detached << value }
    )

    expect(subscription.close).to be_nil
    expect(subscription.close).to be_nil
    expect(detached).to eq([subscription])
  end

  it "does not publish after close" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED])
    subscription.close
    subscription.publish(matching)

    expect(subscription.each.to_a).to be_empty
  end

  it "closes promptly even when its buffer is full" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED], buffer_size: 1)
    subscription.publish(matching)

    expect { Timeout.timeout(0.5) { subscription.close } }.not_to raise_error
    expect(subscription.each.to_a).to eq([matching])
  end

  it "wakes all readers and keeps returning nil after closure" do
    subscription = described_class.new([River::EVENT_JOB_COMPLETED])
    readers = Array.new(3) { Thread.new { subscription.pop } }
    subscription.close

    Timeout.timeout(1) { expect(readers.map(&:value)).to eq([nil, nil, nil]) }

    expect(subscription.pop).to be_nil
    expect(subscription.each.to_a).to be_empty
    expect { subscription.pop(true) }.to raise_error(ThreadError)
  end
end
