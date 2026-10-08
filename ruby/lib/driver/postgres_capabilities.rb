# frozen_string_literal: true

module River::Driver
  # PostgreSQL-compatible servers differ in conflict detection and notification
  # support. Keep these rules aligned with Go's PostgresCapabilities.
  class PostgresCapabilities
    attr_reader :supports_listen_notify, :unique_insert_mode, :unique_insert_sql

    def initialize(product:, version_num:, yb_listen_notify_enabled:)
      product = product.downcase
      yugabyte = product.include?("-yb") || product.include?("yugabyte")
      @supports_listen_notify = !yugabyte || yb_listen_notify_enabled
      @unique_insert_mode, @unique_insert_sql = if yugabyte
        [:metadata_nonce, "false"]
      elsif version_num >= 180_000
        [:returning_old, "(OLD.id IS NOT NULL)"]
      else
        [:xmax, "(xmax != 0)"]
      end
      freeze
    end

    # Scope detection to a pool: Active Record may route one driver to different
    # roles/shards. Failures are never cached, and no lock spans database I/O:
    # another caller may already hold the pool's only connection.
    class Cache
      def initialize
        @mutex = Mutex.new
        @values = {}
      end

      def fetch(pool)
        cached = @mutex.synchronize { @values[pool] }
        return cached if cached

        detected = yield
        @mutex.synchronize { @values[pool] ||= detected }
      end
    end
  end
end
