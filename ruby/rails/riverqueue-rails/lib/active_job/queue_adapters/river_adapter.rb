# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    # Persists Active Job envelopes using the application's River client.
    class RiverAdapter < AbstractAdapter
      # An explicit client also allows using the adapter without a Rails app.
      def initialize(client: nil)
        @client = client
      end

      # Inserts an immediate Active Job delivery.
      def enqueue(job)
        enqueue_at(job, nil)
      end

      # Inserts the whole batch atomically and assigns provider IDs on success.
      def enqueue_all(jobs)
        results = client.insert_many(jobs.map { |job| params(job, job.scheduled_at) })
        jobs.zip(results).each do |job, result|
          job.provider_job_id = result.job.id.to_s
          job.successfully_enqueued = true
          record_retry(job)
        end

        results.length
      end

      # Inserts a delivery scheduled at an epoch timestamp (nil means now).
      def enqueue_at(job, timestamp)
        insertion = params(job, timestamp && Time.at(timestamp).utc)
        result = client.insert(insertion.args, insert_opts: insertion.insert_opts)
        job.provider_job_id = result.job.id.to_s
        record_retry(job)
        result
      end

      # Rails 7.2 adapter default: preserve same-connection atomic insertion.
      def enqueue_after_transaction_commit? = false

      private def client
        @client || ::Rails.application.config.river.client
      end

      private def params(job, scheduled_at)
        priority = job.priority
        valid_priority = priority.nil? || (priority.is_a?(Integer) && (1..4).cover?(priority))
        unless valid_priority
          raise ArgumentError, "River Active Job priority must be nil or an integer from 1 to 4"
        end

        River::InsertManyParams.new(
          River::JobArgsHash.new("active_job", {"job" => job.serialize, "version" => 1}),
          max_attempts: 25, priority: priority || 1,
          queue: job.queue_name, scheduled_at: scheduled_at
        )
      end

      private def record_retry(job)
        current = ActiveSupport::IsolatedExecutionState[:river_active_job]
        if current && current.args.fetch("job").fetch("job_id") == job.job_id
          current.update_metadata("active_job_outcome" => "retried", "active_job_retry_id" => job.provider_job_id)
        end
      end
    end
  end
end
