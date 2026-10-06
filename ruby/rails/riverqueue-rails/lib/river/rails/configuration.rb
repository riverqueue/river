# frozen_string_literal: true

module River
  module Rails
    # Application-owned configuration and per-process insertion client.
    class Configuration
      # Seconds allowed for graceful termination before attempts are interrupted.
      attr_accessor :stop_timeout

      def initialize
        @connection_class = "ActiveRecord::Base"
        @factory = -> { River::Config.new(logger: ::Rails.logger, queues: {"default" => 10}) }
        @mutex = Mutex.new
        @pid = Process.pid
        @stop_timeout = 30
      end

      # Builds a consumer. Called after Rails boot, never in a web initializer.
      def build_client
        core = @factory.call
        workers = River::Workers.new
        core.workers.kinds.each { |kind| workers.add(kind, core.workers.fetch(kind)) }
        workers.add(Worker)
        periodic = core.periodic_jobs.map do |job|
          River::PeriodicJob.new(id: job.id,
            constructor: -> { ::Rails.application.reloader.wrap { job.constructor.call } },
            run_on_start: job.run_on_start, schedule: job.schedule)
        end

        River::Client.new(build_driver,
          config: core.with(periodic_jobs: periodic, plugins: [ExecutionPlugin.new] + core.plugins, workers: workers))
      end

      # Builds a driver for the configured connection class, including migrations.
      def build_driver
        River::Driver::ActiveRecord.new(connection_class: connection_class)
      end

      # Returns a lazy insertion-only client for this process.
      def client
        if @pid != Process.pid
          @client = nil
          @mutex = Mutex.new
          @pid = Process.pid
        end

        @mutex.synchronize do
          klass = connection_class
          @client = nil if @client && !@client.driver.connection_class.equal?(klass)

          @client ||= River::Client.new(River::Driver::ActiveRecord.new(connection_class: klass),
            config: @factory.call.with(periodic_jobs: [], queues: {}))
        end
      end

      # Resolves names lazily so Rails can reload application connection classes.
      def connection_class
        @connection_class.is_a?(String) ? @connection_class.constantize : @connection_class
      end

      # Selects an abstract Active Record class or its name. Prefer a name in
      # initializers to avoid retaining a reloadable class object.
      def connection_class=(value)
        @mutex.synchronize do
          @connection_class = value
          @client = nil
        end
      end

      # Supplies a block returning River::Config after application boot.
      def configure(&block)
        raise ArgumentError, "configuration block required" unless block

        @factory = block
        @client = nil
      end
    end

    # Wraps each attempt in Rails' reload-safe execution boundary.
    class ExecutionPlugin
      def work(_job, operation)
        ::Rails.application.reloader.wrap { operation.call }
      end
    end
  end
end
