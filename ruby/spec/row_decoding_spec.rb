# frozen_string_literal: true

require "spec_helper"
require_relative "../driver/riverqueue-sequel/spec/spec_helper"
require_relative "row_decoding_shared_examples"

RSpec.describe "SQLite row decoding", database: :sqlite do
  around { |example| sqlite_test_transaction(&example) }
  let(:driver) { River::Driver::Sequel.new(SQLITE_DB) }
  let(:client) { River::Client.new(driver) }

  it_behaves_like "historical attempt error decoding"
  it_behaves_like "SQLite corrupt job isolation"
end
