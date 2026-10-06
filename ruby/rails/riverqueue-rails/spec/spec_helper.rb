# frozen_string_literal: true

require_relative "../../../spec/support/test_database"
require "rails"
require "active_record/railtie"
require "active_job/railtie"
require "action_mailer/railtie"
require "riverqueue-rails"
require "tmpdir"
require_relative "../../../spec/support/client_test_database"

class RiverTestApplication < Rails::Application
  config.root = File.expand_path("dummy", __dir__)
  config.eager_load = false
  config.secret_key_base = "river-test-secret"
  config.logger = Logger.new(File::NULL)
  config.active_support.deprecation = :stderr
  config.action_mailer.delivery_method = :test
  config.action_mailer.perform_deliveries = true
  config.action_mailer.default_url_options = {host: "example.test"}
end

Rails.application.initialize!
ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: ":memory:")
GlobalID.app = "river-test"
ActiveJob::Base.logger = Logger.new(File::NULL)

module RiverIntegrationHelpers
  def wait_until
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10
    until yield
      raise "Timed out waiting for River" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.01
    end
  end

  def rows
    @driver.job_list
  end

  def work_until
    @consumer = Rails.application.config.river.build_client.start
    wait_until { yield }
  ensure
    @consumer&.stop_and_cancel
  end
end

RSpec.configure do |config|
  config.include RiverIntegrationHelpers
end
