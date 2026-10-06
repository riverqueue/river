# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/support/yugabyte_test_database"
require_relative "../../../spec/yugabyte_shared_examples"
require_relative "../../../spec/insert_notification_shared_examples"

RSpec.describe "ActiveRecord Yugabyte compatibility", database: :postgres do
  let(:new_driver) { -> { River::Driver::ActiveRecord.new } }

  if ENV["RIVER_YUGABYTE_TEST"] == "1"
    around do |example|
      ClientTestDatabase.with_active_record(:postgres) do |driver|
        @driver = driver
        example.run
      end
    end
    it_behaves_like "Yugabyte driver compatibility", ENV["YUGABYTE_LISTEN_NOTIFY_ENABLED"] == "1"
    if ENV["YUGABYTE_LISTEN_NOTIFY_ENABLED"] == "1"
      it_behaves_like "PostgreSQL insert notifications"
      it_behaves_like "PostgreSQL cancellation notifications"
    end
  elsif PG_AVAILABLE
    [nil, false, true].each do |notifications|
      context "with notification setting #{notifications.inspect}" do
        around do |example|
          ClientTestDatabase.with_active_record(:postgres, pg_catalog_last: true) do |driver|
            @driver = driver
            YugabyteTestDatabase.simulate(driver, notifications: notifications)
            example.run
          end
        end
        it_behaves_like "Yugabyte driver compatibility", notifications
        if notifications
          it_behaves_like "PostgreSQL insert notifications"
          it_behaves_like "PostgreSQL cancellation notifications"
        end
      end
    end
  end
end
