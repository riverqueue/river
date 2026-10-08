# frozen_string_literal: true

require "spec_helper"
require "open3"

RSpec.describe River::Rails::Runner do
  ["TERM", "INT"].each do |signal|
    [true, false].each do |drains|
      it "handles #{signal} with #{drains ? "graceful draining" : "deadline cancellation"} and restores handlers" do
        # Isolate actual OS signals from RSpec. The fake client keeps this test
        # deterministic; database worker execution is exercised in integration_spec.
        output, error, status = Open3.capture3(RbConfig.ruby, "-Ilib", "-e", <<~RUBY)
          require "riverqueue"
          require "river/rails/runner"
          module Rails; end
          require #{File.expand_path("../../../spec/support/runner_test_client", __dir__).inspect}
          client = RunnerTestClient.new(signals: [#{signal.inspect}], stall: #{!drains})
          settings = Struct.new(:stop_timeout).new(0.01)
          settings.define_singleton_method(:build_client) { client }
          app = Struct.new(:config).new(Struct.new(:river).new(settings))
          Rails.define_singleton_method(:application) { app }
          original = proc {}
          Signal.trap(#{signal.inspect}, original)
          result = River::Rails::Runner.start
          abort "Wrong exit status: \#{result}" unless result == #{drains ? 0 : 1}
          abort "Handler not restored" unless Signal.trap(#{signal.inspect}, "DEFAULT").equal?(original)
        RUBY
        expect(status.success?).to be(true), error
        expect(output).to include(drains ? "stopped" : "interrupting active attempts")
      end
    end
  end
end
