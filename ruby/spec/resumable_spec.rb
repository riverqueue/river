# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::ResumableState do
  it "starts at the beginning without persisted progress" do
    state = described_class.new({})

    expect(state).to have_attributes(cursors_dirty: false, resume_matched: true, resume_step: nil)
    expect(state.cursors).to eq({})
  end

  it "loads persisted step and cursor progress defensively" do
    metadata = {
      River::RESUMABLE_STEP_METADATA_KEY => "items",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 4}
    }
    state = described_class.new(metadata)
    state.cursors["items"] = 5

    expect(state).to have_attributes(cursors_dirty: false, resume_matched: false, resume_step: "items")
    expect(metadata.fetch(River::RESUMABLE_CURSOR_METADATA_KEY)).to eq("items" => 4)
  end

  it "rejects duplicate step names" do
    state = described_class.new({})

    expect(state.register("same")).to be_truthy
    expect { state.register("same") }.to raise_error(River::Error, 'duplicate resumable step name "same"')
  end
end

RSpec.describe "resumable job execution" do
  def build_row(metadata: {}, state: River::JOB_STATE_RUNNING)
    River::JobRow.new(
      id: 123,
      args: {},
      attempt: 1,
      created_at: Time.now.utc,
      kind: "resumable",
      max_attempts: 3,
      metadata: metadata,
      priority: 1,
      queue: "default",
      scheduled_at: Time.now.utc,
      state: state
    )
  end

  def build_job(row: build_row, driver: Object.new)
    River::Job.new(Struct.new(:driver).new(driver), row)
  end

  it "runs named steps in order and returns their values" do
    job = build_job
    calls = []

    expect(job.resumable_step("first") {
      calls << "first"
      1
    }).to eq(1)
    expect(job.resumable_step("second") {
      calls << "second"
      2
    }).to eq(2)
    expect { job.__finish_resumable_work! }.not_to raise_error
    expect(calls).to eq(%w[first second])
  end

  it "supplies a default cursor and normalizes saved cursors through JSON" do
    job = build_job
    received = nil

    expect do
      job.resumable_step_cursor :items, default: {start: 1} do |cursor|
        received = cursor
        job.resumable_set_cursor(symbol_key: 2)
        raise "retry"
      end
    end.to raise_error("retry")

    job.__capture_resumable_metadata!

    expect(received).to eq(start: 1)
    expect(job.metadata_updates).to eq(
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => {"symbol_key" => 2}}
    )
  end

  %i[resumable_set_cursor resumable_checkpoint].each do |method|
    it "rejects unsupported JSON cursors before changing progress in #{method}" do
      nested = 150.times.reduce(nil) { |value, _| [value] }
      [[Float::NAN, JSON::GeneratorError], [Float::INFINITY, JSON::GeneratorError],
        [-Float::INFINITY, JSON::GeneratorError], [nested, JSON::NestingError]].each do |cursor, error|
        job = build_job

        expect do
          job.resumable_step_cursor :items do
            if method == :resumable_checkpoint
              job.resumable_checkpoint(cursor: cursor)
            else
              job.resumable_set_cursor(cursor)
            end
          end
        end.to raise_error(error)

        job.__capture_resumable_metadata!
        expect(job.metadata_updates).to eq({})
        expect(job.row.metadata).to eq({})
      end
    end
  end

  it "skips completed steps when resuming" do
    job = build_job(row: build_row(metadata: {River::RESUMABLE_STEP_METADATA_KEY => "download"}))
    calls = []

    job.resumable_step(:prepare) { calls << "prepare" }
    job.resumable_step(:download) { calls << "download" }
    job.resumable_step(:process) { calls << "process" }
    job.__finish_resumable_work!

    expect(calls).to eq(["process"])
  end

  it "resumes a cursor step from its persisted cursor" do
    metadata = {
      River::RESUMABLE_STEP_METADATA_KEY => "items",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 7}
    }
    job = build_job(row: build_row(metadata: metadata))
    received = nil

    job.resumable_step("prepare") { raise "must be skipped" }
    job.resumable_step_cursor(:items, default: 0) { |cursor| received = cursor }
    job.__finish_resumable_work!

    expect(received).to eq(7)
  end

  it "removes a completed persisted cursor on the next failed attempt" do
    metadata = {
      River::RESUMABLE_STEP_METADATA_KEY => "items",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 7}
    }
    job = build_job(row: build_row(metadata: metadata))
    job.resumable_step_cursor("items") { |_cursor| }
    job.__capture_resumable_metadata!

    expect(job.metadata_updates).to include(
      River::RESUMABLE_STEP_METADATA_KEY => "items",
      River::RESUMABLE_CURSOR_METADATA_KEY => nil
    )
  end

  it "captures a completed non-cursor step without cursor metadata" do
    job = build_job
    job.resumable_step(:done) {}

    job.__capture_resumable_metadata!

    expect(job.metadata_updates).to eq(River::RESUMABLE_STEP_METADATA_KEY => "done")
  end

  it "raises a step error immediately and restores step context" do
    job = build_job

    expect { job.resumable_step("fails") { raise "step failed" } }.to raise_error(RuntimeError, "step failed")
    expect { job.resumable_set_cursor(1) }.to raise_error(River::Error, /inside a resumable step/)
  end

  it "does not run later steps after a step error" do
    job = build_job
    later_ran = false
    expect do
      job.resumable_step("fails") { raise "step failed" }
      job.resumable_step("later") { later_ran = true }
    end.to raise_error("step failed")

    expect(later_ran).to be false
  end

  it "reports a persisted resume step missing from the worker" do
    job = build_job(row: build_row(metadata: {River::RESUMABLE_STEP_METADATA_KEY => "removed"}))
    job.resumable_step("current") {}

    expect { job.__finish_resumable_work! }
      .to raise_error(River::Error, 'resumable step "removed" not found in worker')
  end

  it "reports duplicate step names immediately" do
    job = build_job
    job.resumable_step("same") {}
    expect { job.resumable_step(:same) {} }
      .to raise_error(River::Error, 'duplicate resumable step name "same"')
  end

  it "rejects an empty step name" do
    expect { build_job.resumable_step("") {} }
      .to raise_error(ArgumentError, "resumable step name must be non-empty")
  end

  it "rejects nested steps without recording an unreachable resume point" do
    [:resumable_step, :resumable_step_cursor].each do |method|
      job = build_job
      expect do
        job.resumable_step :outer do
          job.public_send(method, :inner) { raise "must not run" }
        end
      end.to raise_error(River::Error, /cannot be nested/)
      job.__capture_resumable_metadata!
      expect(job.metadata_updates).to eq({})
      expect { job.resumable_set_cursor(1) }.to raise_error(River::Error, /inside a resumable step/)
    end
  end

  it "rejects setting a cursor outside a step" do
    expect { build_job.resumable_set_cursor(1) }
      .to raise_error(River::Error, "resumable cursor can only be set inside a resumable step")
  end

  it "rejects persisting outside a step" do
    expect { build_job.resumable_checkpoint }
      .to raise_error(River::Error, "resumable step can only be persisted inside a resumable step")
  end

  it "requires a running job for an immediate checkpoint" do
    job = build_job(row: build_row(state: River::JOB_STATE_AVAILABLE))
    captured = nil
    job.resumable_step("inside") do
      captured = begin
        job.resumable_checkpoint
      rescue => error
        error
      end
    end

    expect(captured).to be_a(River::Error).and have_attributes(message: "job must be running")
  end

  it "persists the current step and optional cursor immediately" do
    row = build_row
    received = nil
    driver = Object.new
    driver.define_singleton_method(:job_metadata_merge) do |id, updates|
      received = [id, updates]
      row.dup.tap { |updated| updated.metadata = row.metadata.merge(updates) }
    end

    job = build_job(driver: driver, row: row)

    job.resumable_step_cursor("items") { job.resumable_checkpoint(cursor: {last_id: 42}) }

    expect(received).to eq([
      123,
      {
        River::RESUMABLE_STEP_METADATA_KEY => "items",
        River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => {"last_id" => 42}}
      }
    ])
    expect(job.metadata).to include(River::RESUMABLE_STEP_METADATA_KEY => "items")
  end

  it "raises when the job disappears during an immediate checkpoint" do
    driver = Object.new
    driver.define_singleton_method(:job_metadata_merge) { |_id, _updates| nil }
    job = build_job(driver: driver)
    captured = nil
    job.resumable_step("inside") do
      captured = begin
        job.resumable_checkpoint
      rescue => error
        error
      end
    end

    expect(captured).to be_a(River::NotFoundError).and have_attributes(message: "job not found: 123")
  end

  it "does not advance progress when the checkpoint write fails" do
    driver = Object.new
    driver.define_singleton_method(:job_metadata_merge) { |_id, _updates| raise "database unavailable" }
    job = build_job(driver: driver)
    job.resumable_step(:prepare) {}

    expect do
      job.resumable_step_cursor(:items) { job.resumable_checkpoint(cursor: 42) }
    end.to raise_error("database unavailable")
    job.__capture_resumable_metadata!

    expect(job.metadata_updates).to eq(River::RESUMABLE_STEP_METADATA_KEY => "prepare")
  end

  it "clears a previous persisted cursor when checkpointing the next step" do
    row = build_row(metadata: {
      River::RESUMABLE_STEP_METADATA_KEY => "items",
      River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 42}
    })
    driver = Object.new
    driver.define_singleton_method(:job_metadata_merge) do |_id, updates|
      row.dup.tap { |updated| updated.metadata = row.metadata.merge(updates) }
    end
    job = build_job(driver: driver, row: row)
    job.resumable_step_cursor(:items) { |_cursor| }

    expect do
      job.resumable_step(:finish) do
        job.resumable_checkpoint
        raise "later failure"
      end
    end.to raise_error("later failure")
    job.__capture_resumable_metadata!

    expect(job.metadata).to include(River::RESUMABLE_STEP_METADATA_KEY => "finish", River::RESUMABLE_CURSOR_METADATA_KEY => nil)
    expect(job.metadata_updates).to be_empty
  end

  it "persists cursor changes made after an explicit checkpoint" do
    row = build_row
    driver = Object.new
    driver.define_singleton_method(:job_metadata_merge) do |_id, updates|
      row.dup.tap { |updated| updated.metadata = row.metadata.merge(updates) }
    end
    job = build_job(driver: driver, row: row)

    expect do
      job.resumable_step_cursor(:items) do
        job.resumable_checkpoint(cursor: 42)
        job.resumable_set_cursor 43
        raise "later failure"
      end
    end.to raise_error("later failure")
    job.__capture_resumable_metadata!

    expect(job.metadata_updates).to eq(River::RESUMABLE_CURSOR_METADATA_KEY => {"items" => 43})
    expect(job.metadata[River::RESUMABLE_STEP_METADATA_KEY]).to eq("items")
  end
end
