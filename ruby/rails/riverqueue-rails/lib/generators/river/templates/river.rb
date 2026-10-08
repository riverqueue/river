# frozen_string_literal: true

Rails.application.configure do
  # Optional: select an abstract connection class by name, resolved after boot.
  # config.river.connection_class = "ApplicationRecord"
  config.active_job.queue_adapter = :river unless Rails.env.test?

  config.river.configure do
    River::Config.new(
      job_timeout: 300,
      logger: Rails.logger,
      queues: {default: 10, mailers: 5}
    )
  end
end

# For atomic enqueueing on the same Active Record connection, set
# self.enqueue_after_transaction_commit = false in ApplicationJob (Rails 8.x).
# Explicit after-commit deferral is respected, but is not an atomic SQL write.
