# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/client_test_database"
require_relative "../../../spec/migrator_shared_examples"
require_relative "../../../spec/migration_cli_shared_examples"

RSpec.describe "Sequel migrations" do
  it_behaves_like "migration command", "sequel"
  [:postgres, :sqlite].each do |adapter|
    context "with #{adapter}", database: adapter do
      around do |example|
        skip "PostgreSQL unavailable" if adapter == :postgres && !DB

        ClientTestDatabase.with_sequel(adapter, migrate: false) do |driver|
          @driver = driver
          example.run
        end
      end

      it_behaves_like "canonical migrations", adapter
    end
  end
end
