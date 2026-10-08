# frozen_string_literal: true

require "logger"

module River
  module JobPersistedLogging
    # Captures job.logger output and persists it with the attempt's state change,
    # including failed attempts. Uses Go riverlog's metadata format for River UI.
    # Register through Config#plugins, preferably before other work middleware.
    class Plugin
      # Limits are positive byte counts. History is capped at 64 MiB, dropping
      # oldest entries first, but always retaining the newest attempt. An optional
      # block receives a bounded writer and must return a fresh logger per attempt.
      # By default this is a standard Ruby Logger at INFO level.
      def initialize(max_size_bytes: 2 * 1024 * 1024, max_total_bytes: 8 * 1024 * 1024, &logger_factory)
        {max_size_bytes: max_size_bytes, max_total_bytes: max_total_bytes}.each do |name, value|
          raise ArgumentError, "#{name} must be a positive integer" unless value.is_a?(Integer) && value.positive?
        end

        @logger_factory = logger_factory
        @max_size_bytes = max_size_bytes
        @max_total_bytes = [max_total_bytes, 64 * 1024 * 1024].min
      end

      # Wraps the work hooks and worker without changing their return value or
      # exception. No extra database round trip is needed to save the log.
      def work(job, operation)
        buffer = Buffer.new(@max_size_bytes)
        factory = @logger_factory
        logger = factory ? factory.call(buffer) : Logger.new(buffer, level: Logger::INFO)
        begin
          job.__with_logger(logger) { operation.call }
        ensure
          persist_log(job, buffer)
        end
      end

      private def persist_log(job, buffer)
        log, truncated = buffer.finish
        return if log.empty?

        history = job.metadata.fetch("river:log", [])
        raise TypeError, '"river:log" value is not an array' unless history.is_a?(Array)

        entries = [{"attempt" => job.row.attempt, "log" => log}]
        size = JSON.generate(entries.first).bytesize + 2 # Array brackets.
        # Walk only the newest suffix that fits; never repeatedly serialize or
        # shift the entire history. Keep the latest entry even if it exceeds the cap.
        history.reverse_each do |entry|
          entry_size = JSON.generate(entry).bytesize + 1 # Separator comma.
          break if size + entry_size > @max_total_bytes

          entries << entry
          size += entry_size
        end
        job.update_metadata("river:log" => entries.reverse)

        job.client.config.logger.warn("River job log truncated to #{@max_size_bytes} bytes for job #{job.row.id}") if truncated
        dropped = history.length + 1 - entries.length
        job.client.config.logger.warn("River job log dropped #{dropped} oldest entries for job #{job.row.id}") if dropped.positive?
      rescue JSON::JSONError, TypeError => error
        # Corrupt history must not hide the worker's original error or turn
        # otherwise successful work into a failure.
        job.client.config.logger.error("River job log could not be persisted for job #{job.row.id}: #{error.message}")
      end

      # A per-attempt, thread-safe writer. Bound memory while writing, rather than
      # accumulating an unbounded StringIO and truncating only when work finishes.
      class Buffer
        def initialize(limit)
          @closed = false
          @data = String.new(encoding: Encoding::BINARY)
          @limit = limit
          @mutex = Mutex.new
          @truncated = false
        end

        def close
          @mutex.synchronize { @closed = true }
          nil
        end

        def finish
          @mutex.synchronize do
            @closed = true
            # Drop invalid/incomplete UTF-8 sequences (including a character cut
            # at the byte limit) and NULs, which Postgres JSONB cannot represent.
            [@data.dup.force_encoding(Encoding::UTF_8).scrub("").delete("\0"), @truncated]
          end
        end

        def write(message)
          message = message.to_s
          @mutex.synchronize do
            unless @closed
              remaining = @limit - @data.bytesize
              chunk = message.byteslice(0, remaining) #: String
              @data << chunk.force_encoding(Encoding::BINARY)
              @truncated ||= message.bytesize > remaining
            end
          end
          message.bytesize
        end
      end
      private_constant :Buffer
    end
  end
end
