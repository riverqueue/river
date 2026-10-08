# frozen_string_literal: true

require "riverqueue"

module River
  # Database-backed test helpers. Require explicitly; no test framework is loaded.
  module Testing
    class AssertionError < StandardError; end
    class DrainLimitError < StandardError; end

    JOB_ATTRIBUTES = %i[id args attempt attempted_at attempted_by created_at errors finalized_at kind max_attempts metadata priority queue scheduled_at state tags unique_key unique_states].freeze

    ExecutionResult = Data.define(:id, :error, :job, :outcome)

    # Framework-neutral assertions, also used by the optional integrations.
    module Assertions
      # Asserts that a synchronous attempt was cancelled and returns its result.
      def assert_job_cancelled(result)
        river_assert(result.outcome == :cancelled, "Expected cancelled River job, got #{result.outcome}: #{result.error.inspect}")
        result
      end

      # Asserts that a synchronous attempt completed and returns its result.
      def assert_job_completed(result)
        river_assert(result.outcome == :completed, "Expected completed River job, got #{result.outcome}: #{result.error.inspect}")
        result
      end

      # Asserts that a synchronous attempt exhausted retries and returns its result.
      def assert_job_discarded(result)
        river_assert(result.outcome == :discarded, "Expected discarded River job, got #{result.outcome}: #{result.error.inspect}")
        result
      end

      # Asserts that exactly one matching new row was inserted by the block and
      # returns it. Attributes (including args) use exact equality.
      def assert_job_inserted(client, **attributes, &block)
        assert_jobs_inserted(client, count: 1, **attributes, &block).fetch(0)
      end

      # Asserts the number of matching new rows, returning those rows. Existing
      # rows returned by uniqueness checks do not count as insertions.
      def assert_jobs_inserted(client, count:, **attributes, &block)
        raise ArgumentError, "count must be a nonnegative integer" unless count.is_a?(Integer) && count >= 0

        rows = Testing.inserted_jobs(client, **attributes, &block)
        message = "Expected #{count} new River jobs matching #{attributes.inspect}, got #{rows.length}: #{rows.map(&:id).inspect}"
        river_assert(rows.length == count, message)
        rows
      end

      # Asserts that the block inserted no matching rows.
      def assert_no_jobs_inserted(client, **attributes, &block)
        assert_jobs_inserted(client, count: 0, **attributes, &block)
      end

      private def river_assert(condition, message)
        raise AssertionError, message unless condition
      end
    end

    class << self
      # Runs eligible jobs in priority order, including jobs inserted by workers.
      # Never sleeps or starts maintenance. Raises if runnable work remains after
      # max_jobs attempts. Use an isolated database with no background consumers.
      def drain(client, queue:, max_jobs: 100)
        raise ArgumentError, "max_jobs must be a positive integer" unless max_jobs.is_a?(Integer) && max_jobs.positive?
        queue = queue.to_s
        # @type var results: Array[ExecutionResult]
        results = []
        loop do
          now = Time.now.utc
          row = jobs(client).select { |job| job.queue == queue && %w[available retryable scheduled].include?(job.state) && job.scheduled_at <= now }
            .min_by { |job| [job.priority, job.scheduled_at, job.id] }

          return results unless row
          raise DrainLimitError, "River drain reached #{max_jobs} attempts with runnable jobs remaining in #{queue.inspect}" if results.length == max_jobs

          results << perform_job(client, row.id)
        end
      end

      # Returns matching rows newly persisted by a block, using an ID snapshot
      # rather than a count or sequence high-water mark. Rolls back no data.
      def inserted_jobs(client, **attributes)
        raise ArgumentError, "a block is required" unless block_given?

        attributes = normalize_attributes(attributes)

        before = jobs(client).to_h { |job| [job.id, true] }
        yield
        jobs(client).select do |job|
          !before.key?(job.id) && attributes.all? { |key, value| job.public_send(key) == value }
        end
      end

      # Returns all persisted jobs, in every state, using paginated reads.
      # Requires an isolated test database without concurrent consumers.
      def jobs(client)
        # @type var result: Array[JobRow]
        result = []
        # @type var cursor: Integer?
        cursor = nil
        loop do
          page = client.job_list(JobListParams.new(after_id: cursor, limit: 100)).jobs
          result.concat(page)
          return result if page.length < 100

          cursor = page.fetch(-1).id
        end
      end

      # Internal shared normalization for assertions and RSpec matchers. Only
      # symbolic identifiers are coerced; JSON values and matchers stay intact.
      def normalize_attributes(attributes)
        unknown = attributes.keys - JOB_ATTRIBUTES
        raise ArgumentError, "unknown job attributes: #{unknown.join(", ")}" unless unknown.empty?

        attributes.to_h do |key, value|
          [key, (value.is_a?(Symbol) && %i[kind queue state].include?(key)) ? value.to_s : value]
        end
      end

      # Performs one attempt on the calling thread using the actual runtime,
      # returning the persisted row, original exception, and symbolic outcome.
      # Future jobs require allow_scheduled: true; terminal/running/pending jobs
      # cannot be claimed. This intentionally bypasses queue pause and capacity.
      def perform_job(client, id, allow_scheduled: false)
        job, error, outcome = client.__perform_job(id, allow_scheduled: allow_scheduled)
        ExecutionResult.new(id: id, error: error, job: job, outcome: outcome)
      end
    end
  end
end
