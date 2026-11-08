# frozen_string_literal: true

module River::Driver
  # Internal notification transports. Each client owns its listener; SQLite
  # cursors never consume or delete another client's notifications.
  module NotificationListener
    TOPICS = %w[river_control river_insert river_leadership].freeze

    class Postgres
      def initialize(connection, schema)
        @connection = connection
        @channels = TOPICS.to_h { |topic| ["#{schema}.#{topic}", topic] }
        @channels.each_key { |channel| @connection.exec("LISTEN #{@connection.escape_identifier(channel)}") }
      rescue
        close
        raise
      end

      def close
        @connection.close unless @connection.finished?
      end

      def poll(timeout)
        notifications = [] #: Array[untyped]
        @connection.wait_for_notify(timeout) do |channel, _pid, payload|
          notifications << [@channels[channel], payload]
        end
        # Bound each batch so a busy sender cannot prevent shutdown.
        while notifications.length < 1_000 && (notification = @connection.notifies)
          notifications << [@channels[notification[:relname]], notification[:extra]]
        end
        notifications
      end
    end

    class SQLite
      def initialize(query)
        @query = query
        # Historical requests must not resign a newly started client.
        @last_id = initial_cursor
      end

      def close
      end

      def poll(timeout)
        rows = read_batch
        if rows.empty?
          sleep(timeout)
          return []
        end

        rows.map do |row|
          row = row.transform_keys(&:to_sym)
          @last_id = row.fetch(:id).to_i
          [row.fetch(:topic), row.fetch(:payload)]
        end
      end

      private def initial_cursor
        @query.call("SELECT coalesce(max(id), 0) AS id FROM river_notification").first.transform_keys(&:to_sym).fetch(:id).to_i
      end

      private def read_batch
        @query.call("SELECT id, topic, payload FROM river_notification WHERE id > #{@last_id} ORDER BY id LIMIT 1000")
      end
    end

    class MySQL < SQLite
      # Locking scans wait for uncommitted lower IDs before advancing the cursor.
      # A snapshot MAX(id) could otherwise permanently skip a later commit.
      private def initial_cursor
        row = @query.call("SELECT id FROM river_notification ORDER BY id FOR UPDATE").last
        row ? row.transform_keys(&:to_sym).fetch(:id).to_i : 0
      end

      private def read_batch
        @query.call("SELECT id, topic, payload FROM river_notification WHERE id > #{@last_id} ORDER BY id LIMIT 1000 FOR SHARE")
      end
    end
  end
end
