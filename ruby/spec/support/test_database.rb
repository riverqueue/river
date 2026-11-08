# frozen_string_literal: true

# Like the Go suite, default to both backends and allow CI to test them separately.
module RiverTestDatabase
  BACKEND = ENV.fetch("TEST_DATABASE", "all")
  raise ArgumentError, "TEST_DATABASE must be all, mysql, postgres, or sqlite" unless %w[all mysql postgres sqlite].include?(BACKEND)

  def self.enabled?(backend)
    return BACKEND == "mysql" || (BACKEND == "all" && ENV.key?("TEST_MYSQL_URL")) if backend == :mysql

    BACKEND == "all" || backend.to_s == BACKEND
  end
end

RSpec.configure do |config|
  config.filter_run_excluding database: ->(backend) { backend && !RiverTestDatabase.enabled?(backend) }
end
