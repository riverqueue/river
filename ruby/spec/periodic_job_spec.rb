# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::PeriodicJob do
  it "uses an object schedule implementing next" do
    now = Time.utc(2026, 1, 1)
    job = described_class.new(constructor: -> {}, schedule: River::PeriodicInterval.new(60))

    expect(job.next_at(now)).to eq(now + 60)
  end

  it "uses a callable schedule" do
    now = Time.utc(2026, 1, 1)
    job = described_class.new(constructor: -> {}, schedule: ->(time) { time + 30 })

    expect(job.next_at(now)).to eq(now + 30)
  end

  it "retains registration attributes" do
    constructor = -> { :args }
    job = described_class.new(id: "cleanup", constructor: constructor, run_on_start: true, schedule: ->(time) { time })

    expect(job).to have_attributes(id: "cleanup", constructor: constructor, run_on_start: true)
  end

  it "normalizes symbolic IDs to strings" do
    job = described_class.new(id: :cleanup, constructor: -> {}, schedule: ->(time) { time })

    expect(job.id).to eq("cleanup")
  end

  it "accepts a factory block without running it during registration" do
    calls = 0
    job = described_class.new(schedule: River::PeriodicInterval.new(60)) do
      calls += 1
      :args
    end

    expect(calls).to eq(0)
    expect(job.constructor.call).to eq(:args)
    expect(calls).to eq(1)
  end

  it "requires exactly one callable factory" do
    schedule = River::PeriodicInterval.new(60)
    expect { described_class.new(schedule: schedule) }.to raise_error(ArgumentError, /factory/)
    expect { described_class.new(schedule: schedule, constructor: Object.new) }.to raise_error(ArgumentError, /callable/)
    expect { described_class.new(schedule: schedule, constructor: -> {}) {} }.to raise_error(ArgumentError, /not both/)
  end

  it "rejects invalid schedules and non-advancing or non-Time results" do
    expect { described_class.new(schedule: Object.new) {} }.to raise_error(ArgumentError, /schedule/)
    now = Time.utc(2026, 1, 1)
    [nil, 123, now, now - 1].each do |result|
      job = described_class.new(schedule: ->(_) { result }) {}
      expect { job.next_at(now) }.to raise_error(ArgumentError, /future Time/)
    end
  end
end

RSpec.describe River::PeriodicInterval do
  [Float::NAN, Float::INFINITY, -Float::INFINITY].each do |seconds|
    it "rejects non-finite interval #{seconds}" do
      expect { described_class.new(seconds) }.to raise_error(ArgumentError, "period must be finite")
    end
  end

  it "coerces seconds and advances a time" do
    now = Time.utc(2026, 1, 1)

    expect(described_class.new("1.5").next(now)).to eq(now + 1.5)
  end

  [0, -1].each do |seconds|
    it "rejects interval #{seconds}" do
      expect { described_class.new(seconds) }.to raise_error(ArgumentError, "period must be greater than zero")
    end
  end

  it "rejects a nonnumeric interval" do
    expect { described_class.new("daily") }.to raise_error(ArgumentError)
  end
end

RSpec.describe River::PeriodicJobBundle do
  let(:wake) { proc {} }
  let(:schedule) { ->(time) { time + 60 } }

  def periodic(id: nil, run_on_start: false, schedule: ->(time) { time + 60 })
    River::PeriodicJob.new(id: id, constructor: -> {}, run_on_start: run_on_start, schedule: schedule)
  end

  it "assigns increasing handles and wakes once per atomic addition" do
    wake_count = 0
    jobs = described_class.new([], wake: -> { wake_count += 1 })

    expect(jobs.add(periodic(id: "one"))).to eq(1)
    expect(jobs.add_many([periodic(id: "two"), periodic])).to eq([2, 3])
    expect(wake_count).to eq(2)
  end

  it "registers a batch atomically when an ID conflicts or a schedule fails" do
    jobs = described_class.new([periodic(id: "existing")], wake: wake)
    expect { jobs.add_many([periodic(id: "new"), periodic(id: "existing")]) }.to raise_error(ArgumentError, /already registered/)
    expect(jobs.remove_by_id("new")).to be false
    expect { jobs.add_many([periodic(id: "new"), periodic(id: "new")]) }.to raise_error(ArgumentError, /already registered/)
    expect(jobs.remove_by_id("new")).to be false
    failing = periodic(schedule: ->(_) { raise "bad schedule" })
    expect { jobs.add_many([periodic(id: "new"), failing]) }.to raise_error("bad schedule")
    expect(jobs.remove_by_id("new")).to be false
    expect(jobs.add(periodic)).to eq(2)
    expect(jobs.add_many([])).to eq([])
  end

  it "invokes schedule and wake callbacks outside the registry lock" do
    jobs = nil
    jobs = described_class.new([], wake: -> { jobs.remove_by_id("missing") })
    reentrant = periodic(schedule: ->(time) {
      jobs.remove_by_id("missing")
      time + 60
    })
    expect { jobs.add(reentrant) }.not_to raise_error
    expect(jobs.due(Time.now.utc + 120)).to eq([reentrant])
  end

  it "reports schedule errors without losing healthy due jobs" do
    broken = periodic(id: "broken", run_on_start: true, schedule: ->(_) { raise "bad schedule" })
    good = periodic(run_on_start: true)
    jobs = described_class.new([broken, good], wake: wake)
    errors = []
    expect(jobs.due(Time.now.utc + 1) { |job, error| errors << [job, error.message] }).to eq([good])
    expect(errors).to eq([[broken, "bad schedule"]])
    expect { jobs.due(Time.now.utc + 1) }.to raise_error("bad schedule")
  end

  it "does not return a job removed while its schedule is evaluated" do
    jobs = described_class.new([], wake: wake)
    job = periodic(id: "removed", run_on_start: true, schedule: ->(time) {
      jobs.remove_by_id("removed")
      time + 60
    })
    jobs.add(job)
    expect(jobs.due(Time.now.utc + 1)).to be_empty
  end

  it "does not return a job already claimed by a concurrent scheduler" do
    jobs = described_class.new([], wake: wake)
    nested = nil
    job = periodic(run_on_start: true, schedule: ->(time) {
      unless nested
        nested = []
        nested = jobs.due(time)
      end
      time + 60
    })
    jobs.add(job)
    expect(jobs.due(Time.now.utc + 1)).to be_empty
    expect(nested).to eq([job])
  end

  it "rejects duplicate non-nil IDs" do
    jobs = described_class.new([periodic(id: "same")], wake: wake)

    expect { jobs.add(periodic(id: "same")) }
      .to raise_error(ArgumentError, "periodic job ID is already registered: same")
  end

  it "allows multiple anonymous registrations" do
    jobs = described_class.new([], wake: wake)

    expect { jobs.add_many([periodic, periodic]) }.not_to raise_error
  end

  it "treats string and symbol IDs as the same registration" do
    jobs = described_class.new([periodic(id: :cleanup)], wake: wake)

    expect { jobs.add(periodic(id: "cleanup")) }
      .to raise_error(ArgumentError, "periodic job ID is already registered: cleanup")
    expect(jobs.remove_by_id(:cleanup)).to be true
    expect(jobs.remove_by_id("cleanup")).to be false
    expect(jobs.remove_by_id(nil)).to be false
  end

  it "makes run-on-start jobs immediately due" do
    job = periodic(id: "startup", run_on_start: true)
    jobs = described_class.new([job], wake: wake)

    expect(jobs.due(Time.now.utc + 1)).to eq([job])
  end

  it "does not return future jobs" do
    jobs = described_class.new([periodic], wake: wake)

    expect(jobs.due(Time.now.utc)).to be_empty
  end

  it "reschedules a due job from the supplied time" do
    calls = []
    schedule = ->(time) {
      calls << time
      time + 60
    }
    job = periodic(run_on_start: true, schedule: schedule)
    jobs = described_class.new([job], wake: wake)
    due_at = Time.now.utc + 1

    expect(jobs.due(due_at)).to eq([job])
    expect(jobs.due(due_at + 30)).to be_empty
    expect(calls.last).to eq(due_at)
  end

  it "removes a registration by handle" do
    jobs = described_class.new([], wake: wake)
    job = periodic(run_on_start: true)
    handle = jobs.add(job)

    expect(jobs.remove(handle)).to equal(job)
    expect(jobs.remove(handle)).to be_nil
    expect(jobs.due(Time.now.utc + 1)).to be_empty
  end

  it "removes a registration by ID" do
    jobs = described_class.new([periodic(id: "remove", run_on_start: true)], wake: wake)

    expect(jobs.remove_by_id("remove")).to be true
    expect(jobs.remove_by_id("remove")).to be false
  end

  it "clears all registrations" do
    jobs = described_class.new([periodic(run_on_start: true), periodic(run_on_start: true)], wake: wake)
    expect(jobs.clear).to equal(jobs)

    expect(jobs.due(Time.now.utc + 1)).to be_empty
  end
end
