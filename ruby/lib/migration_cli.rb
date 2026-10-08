# frozen_string_literal: true

require "optparse"
require_relative "riverqueue"

module River
  # Minimal command-line interface to the Ruby migration API.
  module MigrationCLI
    def self.run(argv, err: $stderr, out: $stdout)
      # @type var database: untyped
      # @type var active_record_base: untyped
      options = {line: "main"} #: Hash[Symbol, untyped]
      parser = OptionParser.new do |args|
        args.banner = "Usage: river migrate-up|migrate-down|migrate-status [options]"
        args.on("--database-url URL", "Defaults to DATABASE_URL") { |value| options[:url] = value }
        args.on("--line NAME", %w[main pro]) { |value| options[:line] = value }
        args.on("--schema NAME") { |value| options[:schema] = value }
        args.on("--steps N", Integer) { |value| options[:steps] = value }
        args.on("--target VERSION", Integer) { |value| options[:target] = value }
        args.on("--dry-run") { options[:dry_run] = true }
        args.on("--yes", "Confirm destructive down migrations") { options[:yes] = true }
        args.on("-h", "--help") {
          out.puts(args)
          return 0
        }
      end

      args = argv.dup
      parser.parse!(args)
      command = args.shift
      raise ArgumentError, parser.banner unless %w[migrate-up migrate-down migrate-status].include?(command) && args.empty?

      url = options[:url] || ENV["DATABASE_URL"]
      raise ArgumentError, "provide --database-url or DATABASE_URL" if url.nil? || url.empty?

      if command == "migrate-down" && !options[:yes] && !options[:dry_run]
        raise ArgumentError, "down migrations may delete data; pass --yes to confirm"
      end

      adapter = detect_driver

      if options[:line] == "pro"
        require "riverqueue-pro"
      end

      migrator_class = (options[:line] == "pro") ? River.const_get(:Pro).const_get(:Migrator) : River::Migrator
      if adapter == "activerecord"
        require "riverqueue-activerecord"
        active_record_base = Object.const_get("ActiveRecord::Base")
        active_record_base.establish_connection(url)
        driver = River::Driver.const_get(:ActiveRecord).new
      else
        require "riverqueue-sequel"
        database = Object.const_get(:Sequel).connect(url)
        driver = River::Driver.const_get(:Sequel).new(database)
      end

      migrator = migrator_class.new(driver, schema: options[:schema])
      if command == "migrate-status"
        migrator.status.each { |migration| out.puts("#{migration.applied ? "applied" : "pending"} #{migration.version.to_s.rjust(3, "0")} #{migration.name}") }
      else
        migrations = migrator.migrate(direction: (command == "migrate-up") ? :up : :down,
          dry_run: !!options[:dry_run], steps: options[:steps], target: options[:target])
        migrations.each { |migration| out.puts("#{options[:dry_run] ? "planned" : "applied"} #{migration.version.to_s.rjust(3, "0")} #{migration.name}") }
      end

      0
    rescue StandardError, LoadError => error
      # Do not echo connection exception messages, which may contain credentials.
      message = (error.is_a?(ArgumentError) || error.is_a?(OptionParser::ParseError) || error.is_a?(River::Error)) ? error.message : error.class.name
      err.puts("Migration failed: #{message}")
      1
    ensure
      database&.disconnect
      active_record_base&.connection_pool&.disconnect!
    end

    def self.detect_driver
      driver = %w[sequel activerecord].find do |driver|
        !Gem::Specification.find_all_by_name("riverqueue-#{driver}").empty?
      end

      unless driver
        raise ArgumentError, "install riverqueue-activerecord or riverqueue-sequel in your bundle to run migrations"
      end
      driver
    end
    private_class_method :detect_driver
  end
end
