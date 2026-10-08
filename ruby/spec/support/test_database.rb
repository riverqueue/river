# frozen_string_literal: true

# Like the Go suite, default to both backends and allow CI to test them separately.
module RiverTestDatabase
  BACKEND = ENV.fetch("TEST_DATABASE", "all")
  raise ArgumentError, "TEST_DATABASE must be all, postgres, or sqlite" unless %w[all postgres sqlite].include?(BACKEND)

  def self.enabled?(backend)
    BACKEND == "all" || backend.to_s == BACKEND
  end
end

RSpec.configure do |config|
  config.filter_run_excluding database: :postgres unless RiverTestDatabase.enabled?(:postgres)
  config.filter_run_excluding database: :sqlite unless RiverTestDatabase.enabled?(:sqlite)
end
