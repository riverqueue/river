# frozen_string_literal: true

require "optparse"
require_relative "migration_cli"
require_relative "worker_runner"

module River
  # Shared command dispatch for migrations, version information, and workers.
  module CLI
    # Runs a River command and returns its process exit status.
    def self.run(argv, err: $stderr, out: $stdout)
      return version(argv.drop(1), err: err, out: out) if %w[version --version].include?(argv.first)
      return worker(argv.drop(1), out: out) if argv.first == "worker"

      if argv.empty? || %w[-h --help].include?(argv.first)
        out.puts("Usage: river worker|migrate-up|migrate-down|migrate-get|migrate-status|validate|version [options]")
        out.puts("Use river --version to print gem and Ruby versions.")
        return 0
      end

      MigrationCLI.run(argv, err: err, out: out)
    rescue StandardError, LoadError, SyntaxError => error
      err.puts("Worker failed: #{error.class}: #{error.message}")
      1
    end

    # Prints active gem versions and the Ruby runtime without loading adapters.
    def self.version(argv, err:, out:)
      parser = OptionParser.new do |args|
        args.banner = "Usage: river version | river --version"
        args.on("-h", "--help") do
          out.puts(args)
          return 0
        end
      end
      args = argv.dup
      parser.parse!(args)
      raise ArgumentError, parser.banner unless args.empty?

      out.puts("River version #{Gem.loaded_specs["riverqueue"]&.version || "(unknown)"}")
      pro = Gem.loaded_specs["riverqueue-pro"]
      out.puts("River Pro version #{pro.version}") if pro
      out.puts("Ruby #{RUBY_VERSION}")
      0
    rescue ArgumentError, OptionParser::ParseError => error
      err.puts("Version failed: #{error.message}")
      1
    end

    def self.worker(argv, out:)
      options = {} #: Hash[Symbol, untyped]
      parser = OptionParser.new do |args|
        args.banner = "Usage: river worker --config FILE | --rails [options]"
        args.on("--config FILE", "Ruby file returning an unstarted River client") { |value| options[:config] = value }
        args.on("--rails", "Boot config/environment.rb in the current directory") { options[:rails] = true }
        args.on("--stop-timeout SECONDS", Float) { |value| options[:stop_timeout] = value }
        args.on("-h", "--help") do
          out.puts(args)
          return 0
        end
      end

      args = argv.dup
      parser.parse!(args)
      raise ArgumentError, parser.banner unless args.empty? && (!!options[:config] ^ !!options[:rails])

      if options[:rails]
        require File.expand_path("config/environment.rb")
        require "riverqueue-rails"
        Object.const_get("River::Rails::Runner").start(out: out, stop_timeout: options[:stop_timeout])
      else
        path = File.expand_path(options[:config])
        client = TOPLEVEL_BINDING.eval(File.read(path), path)
        raise ArgumentError, "configuration must return a River::Client" unless client.is_a?(River::Client)

        WorkerRunner.new(client, out: out, stop_timeout: options.fetch(:stop_timeout, 30)).run
      end
    end
  end
end
