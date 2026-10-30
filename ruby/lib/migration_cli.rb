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
        args.banner = "Usage: river migrate-up|migrate-down|migrate-get|migrate-status|validate [options]"
        args.on("--database-url URL", "Defaults to DATABASE_URL; migrate-get uses only the dialect (default: Postgres)") { |value| options[:url] = value }
        args.on("--line NAME", %w[main pro]) { |value| options[:line] = value }
        args.on("--schema NAME") { |value| options[:schema] = value }
        args.on("--steps N", Integer) { |value| options[:steps] = value }
        args.on("--target VERSION", Integer) { |value| options[:target] = value }
        args.on("--dry-run") { options[:dry_run] = true }
        args.on("--yes", "Confirm destructive down migrations") { options[:yes] = true }
        args.separator ""
        args.separator "migrate-get: export SQL without a database connection or driver gem"
        args.on("--all", "Export all versions in migration order") { options[:all] = true }
        args.on("--up", "Export up SQL") { options[:up] = true }
        args.on("--down", "Export down SQL") { options[:down] = true }
        args.on("--version N[,N...]", "Versions in the supplied order; repeatable") { |value| (options[:version] ||= []).concat(parse_versions(value, "--version")) }
        args.on("--exclude-version N[,N...]", "Versions to omit; repeatable") { |value| (options[:exclude_version] ||= []).concat(parse_versions(value, "--exclude-version")) }
        args.on("-h", "--help") {
          out.puts(args)
          return 0
        }
      end

      args = argv.dup
      parser.parse!(args)
      command = args.shift
      raise ArgumentError, parser.banner unless %w[migrate-up migrate-down migrate-get migrate-status validate].include?(command) && args.empty?

      incompatible_options = options.keys & ((command == "migrate-get") ? %i[dry_run steps target yes] : %i[all down exclude_version up version])
      incompatible_options |= options.keys & %i[dry_run steps target yes] if command == "validate"
      unless incompatible_options.empty?
        raise ArgumentError, "#{incompatible_options.map { |key| "--#{key.to_s.tr("_", "-")}" }.join(", ")} cannot be used with #{command}"
      end

      if options[:line] == "pro"
        require "riverqueue-pro"
      end
      migrator_class = (options[:line] == "pro") ? River.const_get(:Pro).const_get(:Migrator) : River::Migrator
      return export(options, migrator_class, out: out) if command == "migrate-get"

      url = options[:url] || ENV["DATABASE_URL"]
      raise ArgumentError, "provide --database-url or DATABASE_URL" if url.nil? || url.empty?

      if command == "migrate-down" && !options[:yes] && !options[:dry_run]
        raise ArgumentError, "down migrations may delete data; pass --yes to confirm"
      end

      adapter = detect_driver

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
      if command == "validate"
        pending = migrator.status.reject(&:applied)
        unless pending.empty?
          err.puts("unapplied migrations: #{pending.map(&:version).join(", ")}")
          return 1
        end
      elsif command == "migrate-status"
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

    def self.export(options, migrator_class, out:)
      versions = options.fetch(:version, [])
      excluded = options.fetch(:exclude_version, [])
      raise ArgumentError, "pass exactly one of --all or --version" if !!options[:all] == versions.any?
      raise ArgumentError, "pass exactly one of --up or --down" if !!options[:up] == !!options[:down]

      backend = case options[:url] || "postgres://"
      when /\Apostgres(?:ql)?:/i then :postgresql
      when /\Asqlite3?:/i then :sqlite
      else raise ArgumentError, "--database-url must use postgres:, postgresql:, sqlite:, or sqlite3:"
      end
      schema = options[:schema]
      raise ArgumentError, "SQLite does not support a migration schema option" if backend == :sqlite && schema
      raise ArgumentError, "schema must be a simple SQL identifier" if schema && !schema.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/)

      migrations = migrator_class.migrations(backend: backend, line: options[:line])
      selected = if options[:all]
        options[:down] ? migrations.reverse : migrations
      else
        versions.map do |version|
          migrations.find { |migration| migration.version == version } || raise(ArgumentError, "migration #{version} does not exist")
        end
      end
      selected = selected.reject { |migration| excluded.include?(migration.version) }
      direction = options[:down] ? "down" : "up"
      selected.each_with_index do |migration, index|
        out.puts if index.positive?
        out.puts("-- River #{options[:line]} migration #{migration.version.to_s.rjust(3, "0")} [#{direction}]")
        sql = options[:down] ? migration.sql_down : migration.sql_up
        out.puts(sql.gsub("/* TEMPLATE: schema */", schema ? %("#{schema}".) : "").strip)
      end
      0
    end
    private_class_method :export

    def self.parse_versions(value, flag)
      raise ArgumentError, "#{flag} must contain positive integers separated by commas" unless value.match?(/\A[0-9]+(?:,[0-9]+)*\z/)

      value.split(",").map do |entry|
        version = Integer(entry, 10)
        raise ArgumentError, "#{flag} must contain positive integers" unless version.positive?

        version
      end
    end
    private_class_method :parse_versions
  end
end
