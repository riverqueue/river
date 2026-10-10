# frozen_string_literal: true

require "spec_helper"
require "riverqueue-activerecord"
require "riverqueue-sequel"
require_relative "notification_receiver_shared_examples"
require_relative "support/client_test_database"

RSpec.describe "River notification receiver" do
  %i[active_record sequel].each do |adapter|
    %i[postgres sqlite].each do |backend|
      context "#{adapter} with #{backend}", database: backend do
        around do |example|
          ClientTestDatabase.public_send(:"with_#{adapter}", backend) do |driver|
            @driver = driver
            example.run
          end
        end

        it_behaves_like "notification receiving", backend
      end
    end
  end
end
