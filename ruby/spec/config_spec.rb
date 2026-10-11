# frozen_string_literal: true

require "spec_helper"

RSpec.describe River::QueueConfig do
  %i[fetch_cooldown fetch_poll_interval].product([Float::NAN, Float::INFINITY, -Float::INFINITY]).each do |name, value|
    it "rejects #{name}=#{value}" do
      expect { described_class.new(max_workers: 1, **{name => value}) }
        .to raise_error(ArgumentError, /finite/)
    end
  end

  let(:config) { River::Config.new(fetch_cooldown: 0.25, fetch_poll_interval: 1.5) }

  it "coerces numeric settings" do
    queue = described_class.new(fetch_cooldown: "0.5", fetch_poll_interval: "2", max_workers: "3")

    expect(queue).to have_attributes(fetch_cooldown: 0.5, fetch_poll_interval: 2.0, max_workers: 3)
  end

  it "inherits unspecified timing settings from the client configuration" do
    queue = described_class.new(max_workers: 1)

    expect(queue.resolved_fetch_cooldown(config)).to eq(0.25)
    expect(queue.resolved_fetch_poll_interval(config)).to eq(1.5)
  end

  it "uses queue-specific timing settings when provided" do
    queue = described_class.new(fetch_cooldown: 0.5, fetch_poll_interval: 0.75, max_workers: 1)

    expect(queue.resolved_fetch_cooldown(config)).to eq(0.5)
    expect(queue.resolved_fetch_poll_interval(config)).to eq(0.75)
  end

  [0, River::QUEUE_NUM_WORKERS_MAX + 1].each do |max_workers|
    it "rejects max_workers=#{max_workers}" do
      expect { described_class.new(max_workers: max_workers) }
        .to raise_error(ArgumentError, /max_workers must be between/)
    end
  end

  it "rejects a negative fetch cooldown" do
    expect { described_class.new(fetch_cooldown: -0.1, max_workers: 1) }
      .to raise_error(ArgumentError, "fetch_cooldown must be zero or greater")
  end

  it "rejects a negative fetch poll interval" do
    expect { described_class.new(fetch_poll_interval: -0.1, max_workers: 1) }
      .to raise_error(ArgumentError, "fetch_poll_interval must be zero or greater")
  end

  it "rejects an effective poll interval shorter than the cooldown" do
    queue = described_class.new(fetch_cooldown: 2, max_workers: 1)

    expect { queue.resolved_fetch_poll_interval(config) }
      .to raise_error(ArgumentError, "fetch_poll_interval cannot be less than fetch_cooldown")
  end
end

RSpec.describe River::Config do
  %i[fetch_cooldown fetch_poll_interval job_timeout].product([Float::NAN, Float::INFINITY, -Float::INFINITY]).each do |name, value|
    it "rejects #{name}=#{value}" do
      expect { described_class.new(**{name => value}) }.to raise_error(ArgumentError, /finite/)
    end
  end

  %i[cancelled_job_retention_period completed_job_retention_period discarded_job_retention_period].product([Float::NAN, Float::INFINITY, -2]).each do |name, value|
    it "rejects #{name}=#{value}" do
      expect { described_class.new(**{name => value}) }.to raise_error(ArgumentError, /retention must be finite and nonnegative/)
    end
  end

  it "copies its maintenance service list without freezing the caller's array" do
    service = Object.new
    services = [service]
    config = described_class.new(maintenance_services: services)
    services.clear

    expect(config.maintenance_services).to eq([service])
    expect(config.maintenance_services).to be_frozen
  end

  it "provides usable defaults" do
    config = described_class.new

    expect(config.id).to be_a(String).and have_attributes(length: be_between(1, 127))
    expect(config).to have_attributes(
      fetch_cooldown: River::FETCH_COOLDOWN_DEFAULT,
      fetch_only_known_kinds: false,
      fetch_poll_interval: River::FETCH_POLL_INTERVAL_DEFAULT,
      job_timeout: River::JOB_TIMEOUT_DEFAULT,
      leader_election_disabled: false,
      plugins: [],
      queues: {},
      workers: be_a(River::Workers)
    )
    expect(config.retry_policy).to respond_to(:next_retry)
  end

  it "normalizes queue names and integer worker counts" do
    config = described_class.new(queues: {:default => 2, "other" => River::QueueConfig.new(max_workers: 3)})

    expect(config.queues.keys).to eq(%w[default other])
    expect(config.queues.fetch("default").max_workers).to eq(2)
    expect(config.queues.fetch("other").max_workers).to eq(3)
  end

  it "normalizes disabled retention values" do
    config = described_class.new(
      cancelled_job_retention_period: nil,
      completed_job_retention_period: -1,
      discarded_job_retention_period: "60"
    )

    expect(config).to have_attributes(
      cancelled_job_retention_period: be_nil,
      completed_job_retention_period: be_nil,
      discarded_job_retention_period: 60.0
    )
  end

  it "copies configuration with overrides while preserving other values" do
    workers = River::Workers.new
    plugin = Object.new
    original = described_class.new(
      id: "client-one", job_timeout: nil, plugins: [plugin],
      fetch_only_known_kinds: true, leader_election_disabled: true,
      queues: {default: 2}, workers: workers
    )
    copy = original.with(id: "client-two", fetch_poll_interval: 2)

    expect(copy).to have_attributes(id: "client-two", fetch_poll_interval: 2.0, job_timeout: nil, workers: workers)
    expect(copy).to have_attributes(fetch_only_known_kinds: true, leader_election_disabled: true)
    expect(copy.queues.keys).to eq(["default"])
    expect(copy.plugins).to eq([plugin])
    expect(original.id).to eq("client-one")
  end

  it "copies and freezes its plugins list" do
    plugins = [Object.new]
    config = described_class.new(plugins: plugins)
    plugins.clear

    expect(config.plugins.length).to eq(1)
    expect(config.plugins).to be_frozen
  end

  it "rejects periodic jobs on a client that cannot become leader" do
    expect { described_class.new(leader_election_disabled: true, periodic_jobs: [Object.new]) }
      .to raise_error(ArgumentError, /periodic_jobs must be empty/)
  end

  describe "#id" do
    # Runs the block in a forked child and returns its result, marshaled back
    # through a pipe. The child exits with exit! to skip RSpec and coverage
    # at_exit hooks.
    def in_fork
      reader, writer = IO.pipe
      pid = Process.fork do
        reader.close
        writer.binmode.write(Marshal.dump(yield))
        writer.close
        exit!(0)
      end
      writer.close
      result = Marshal.load(reader.binmode.read) # rubocop:disable Security/MarshalLoad
      _, status = Process.wait2(pid)
      raise "forked child failed: #{status.inspect}" unless status.success?

      result
    ensure
      reader&.close
    end

    def skip_unless_fork_supported
      skip "Process.fork is unsupported on this platform" unless Process.respond_to?(:fork)
    end

    it "generates a distinct default ID for each config" do
      expect(described_class.new.id).not_to eq(described_class.new.id)
    end

    it "generates a default ID including the host name and process ID" do
      hostname = Socket.gethostname[0, 80]

      expect(described_class.new.id)
        .to match(/\A#{Regexp.escape(hostname)}-#{Process.pid}-\h{12}\z/)
    end

    it "truncates long host names to keep the default ID valid" do
      gethostname = Socket.method(:gethostname)
      Socket.define_singleton_method(:gethostname) { "h" * 200 }

      expect(described_class.new.id).to match(/\Ah{80}-#{Process.pid}-\h{12}\z/)
    ensure
      Socket.define_singleton_method(:gethostname, gethostname)
    end

    it "generates a new default ID in each forked process" do
      skip_unless_fork_supported

      config = described_class.new
      parent_id = config.id
      children = Array.new(2) do
        in_fork do
          client = River::Client.new(Object.new, config: config)
          {client_id: client.id, config_id: config.id, copy_id: config.with(job_timeout: 5).id, pid: Process.pid}
        end
      end

      children.each do |child|
        expect(child.fetch(:client_id)).to eq(child.fetch(:config_id))
        expect(child.fetch(:copy_id)).to eq(child.fetch(:config_id))
        expect(child.fetch(:config_id)).to include("-#{child.fetch(:pid)}-")
      end
      expect(children.map { |child| child.fetch(:config_id) }.push(parent_id).uniq.length).to eq(3)
      expect(config.id).to eq(parent_id)
    end

    it "generates a forked process's ID even when the parent never read one" do
      skip_unless_fork_supported

      config = described_class.new
      child_id = in_fork { config.id }

      expect(config.id).not_to eq(child_id)
    end

    it "keeps a default ID stable within a process" do
      config = described_class.new
      client = River::Client.new(Object.new, config: config)
      ids = Array.new(8) { Thread.new { Array.new(50) { config.id } } }.flat_map(&:value)

      expect(ids.uniq).to eq([client.id])
      expect(config.id).to be_frozen
    end

    it "regenerates a default ID left over from another process" do
      config = described_class.new
      config.instance_variable_get(:@default_id).instance_variable_set(:@state, {Process.pid + 1 => "inherited"}.freeze)

      expect(config.id).not_to eq("inherited")
      expect(config.id).to equal(config.id)
    end

    it "preserves an explicit ID across forks" do
      skip_unless_fork_supported

      config = described_class.new(id: "explicit-client")
      child = in_fork { [config.id, River::Client.new(Object.new, config: config).id] }

      expect(child).to eq(%w[explicit-client explicit-client])
      expect(config.id).to eq("explicit-client")
    end

    it "shares a default ID with copies unless overridden" do
      config = described_class.new
      copy = config.with(job_timeout: 5)
      reset = config.with(id: nil)

      expect(copy.id).to eq(config.id)
      expect(config.with(id: "explicit").id).to eq("explicit")
      expect(reset.id).not_to eq(config.id)
      expect(reset.with(job_timeout: 5).id).to eq(reset.id)
    end
  end

  ["", "a" * 128].each do |id|
    it "rejects an ID with length #{id.length}" do
      expect { described_class.new(id: id) }
        .to raise_error(ArgumentError, "id must be between 1 and 127 characters")
    end
  end

  it "rejects a fetch cooldown below one millisecond" do
    expect { described_class.new(fetch_cooldown: 0) }
      .to raise_error(ArgumentError, "fetch_cooldown must be at least 0.001 seconds")
  end

  it "rejects a poll interval shorter than the cooldown" do
    expect { described_class.new(fetch_cooldown: 1, fetch_poll_interval: 0.5) }
      .to raise_error(ArgumentError, "fetch_poll_interval cannot be less than fetch_cooldown")
  end

  it "allows a nil job timeout" do
    expect(described_class.new(job_timeout: nil).job_timeout).to be_nil
  end

  it "rejects non-positive job timeouts" do
    expect { described_class.new(job_timeout: 0) }
      .to raise_error(ArgumentError, "job_timeout must be greater than zero or nil")
  end

  it "rejects a retry policy without next_retry" do
    expect { described_class.new(retry_policy: Object.new) }
      .to raise_error(ArgumentError, "retry_policy must respond to next_retry")
  end

  it "rejects a non-Workers registry" do
    expect { described_class.new(workers: {}) }
      .to raise_error(ArgumentError, "workers must be a River::Workers")
  end

  ["", "not valid", "a" * 128].each do |name|
    it "rejects invalid queue name #{name.inspect}" do
      expect { described_class.new(queues: {name => 1}) }
        .to raise_error(ArgumentError, /invalid queue name/)
    end
  end

  it "validates queue-specific timing against client defaults" do
    queue = River::QueueConfig.new(fetch_poll_interval: 0.1, max_workers: 1)

    expect { described_class.new(fetch_cooldown: 0.2, queues: {default: queue}) }
      .to raise_error(ArgumentError, "fetch_poll_interval cannot be less than fetch_cooldown")
  end
end
