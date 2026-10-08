# frozen_string_literal: true

module River
  module Rails
    # Runs one worker process; an external supervisor owns process restarts.
    class Runner
      # Boot Rails before calling. TERM/INT stop fetching and drain active jobs.
      def self.start(out: $stdout, stop_timeout: nil)
        config = ::Rails.application.config.river
        River::WorkerRunner.new(config.build_client, out: out,
          stop_timeout: stop_timeout || config.stop_timeout).run
      end
    end
  end
end
