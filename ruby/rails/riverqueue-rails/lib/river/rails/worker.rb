# frozen_string_literal: true

module River
  module Rails
    # Keep River's control-flow exceptions out of Active Job retry/discard
    # handlers, including broad retry_on StandardError declarations.
    module ExecutionControl
      def rescue_with_handler(exception)
        if ActiveSupport::IsolatedExecutionState[:river_active_job] &&
            [River::ClientRuntime::Interrupted, River::JobCancelError, River::JobSnoozeError].any? { |type| exception.is_a?(type) }
          raise exception
        end

        super
      end
    end

    ActiveJob::Base.prepend(ExecutionControl)

    # Dispatches serialized jobs through Active Job's own execution machinery.
    class Worker
      def self.kind = "active_job"

      # Active Job owns retries for reported errors; River still rescues crashes.
      def retry?(_job, _error) = false

      def work(job)
        previous = ActiveSupport::IsolatedExecutionState[:river_active_job]
        ActiveSupport::IsolatedExecutionState[:river_active_job] = job
        raise ArgumentError, "Unsupported Active Job envelope version" unless job.args.fetch("version") == 1

        payload = job.args.fetch("job").merge("provider_job_id" => job.id.to_s)
        ActiveJob::Base.execute(payload)
      ensure
        ActiveSupport::IsolatedExecutionState[:river_active_job] = previous
      end
    end

    ActiveSupport::Notifications.subscribe("discard.active_job") do |*, payload|
      job = ActiveSupport::IsolatedExecutionState[:river_active_job]
      if job && payload.fetch(:job).job_id == job.args.fetch("job").fetch("job_id")
        job.update_metadata("active_job_outcome" => "discarded")
      end
    end
  end
end
