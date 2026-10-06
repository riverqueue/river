# frozen_string_literal: true

require "spec_helper"
require "generators/river/install_generator"

RSpec.describe River::Rails::Configuration do
  it "requires a configuration block" do
    expect { described_class.new.configure }.to raise_error(ArgumentError, /block/)
  end

  it "does not construct configuration until a client is requested" do
    calls = 0
    settings = described_class.new
    settings.configure {
      calls += 1
      River::Config.new
    }

    expect(calls).to eq(0)
    expect(settings.client).to equal(settings.client)
    expect(calls).to eq(1)
    expect(settings.client.started?).to be false
  end

  it "builds independent consumer registries without modifying the application registry" do
    registry = River::Workers.new
    settings = described_class.new
    settings.configure { River::Config.new(workers: registry) }

    expect(settings.build_client).to be_a(River::Client)
    expect(settings.build_client).to be_a(River::Client)
    expect(registry.kinds).to be_empty
  end

  it "rejects registration of the reserved Active Job kind" do
    settings = described_class.new
    settings.configure { River::Config.new(workers: River::Workers.new.add("active_job", Object.new)) }

    expect { settings.build_client }.to raise_error(ArgumentError, /already registered/)
  end

  it "rebuilds the insertion client after a fork" do
    settings = described_class.new
    original = settings.client
    reader, writer = IO.pipe
    pid = fork do
      reader.close
      writer.write((!settings.client.equal?(original)).to_s)
      writer.close
      exit! 0
    end

    writer.close

    expect(reader.read).to eq("true")
    Process.wait(pid)
    reader.close
  end
end

RSpec.describe River::Generators::InstallGenerator do
  it "installs a Ruby configuration and executable worker command" do
    Dir.mktmpdir("river-generator-") do |directory|
      described_class.start([], destination_root: directory, shell: Thor::Shell::Basic.new)

      expect(File.read(File.join(directory, "config/initializers/river.rb"))).to include("queue_adapter = :river")
      command = File.join(directory, "bin/jobs")

      expect(File.executable?(command)).to be true
      expect(File.read(command)).to include("River::Rails::Runner.start")
    end
  end
end
