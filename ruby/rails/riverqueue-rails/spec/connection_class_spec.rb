# frozen_string_literal: true

require "spec_helper"
require_relative "../../../spec/support/connection_class_test_database"

class RailsSelectedConnection < ActiveRecord::Base
  self.abstract_class = true
end

class SelectedConnectionJob < ActiveJob::Base
  self.enqueue_after_transaction_commit = (ActiveJob.gem_version >= Gem::Version.new("8.0")) ? false : :never
  def perform = nil
end

RSpec.describe "Rails connection class configuration", database: :sqlite do
  around do |example|
    ClientTestDatabase.with_active_record(:sqlite) do |primary|
      ConnectionClassTestDatabase.with_class(RailsSelectedConnection, :sqlite) do |selected|
        previous = Rails.application.config.river
        @primary = primary
        @selected = selected
        @settings = River::Rails::Configuration.new
        @settings.connection_class = "RailsSelectedConnection"
        Rails.application.config.river = @settings
        ActiveJob::Base.queue_adapter = :river
        example.run
      ensure
        @consumer&.stop_and_cancel
        Rails.application.config.river = previous
      end
    end
  end

  it "routes migrations, producers, and consumers to the selected database" do
    require "rake"
    Rails.application.load_tasks unless Rake::Task.task_defined?("river:migrate")

    Rake::Task["river:migrate"].execute

    expect { Rake::Task["river:status"].execute }.to output(/applied 7/).to_stdout
    expect(@settings.connection_class).to eq(RailsSelectedConnection)
    SelectedConnectionJob.perform_later
    @consumer = @settings.build_client.start
    wait_until { @selected.job_list.first.state == "completed" }

    expect(@primary.job_list).to be_empty
  end

  it "rolls back Active Job insertion with the selected transaction" do
    River::Migrator.new(@settings.build_driver).migrate
    RailsSelectedConnection.transaction do
      SelectedConnectionJob.perform_later

      expect(@selected.job_list.length).to eq(1)
      raise ActiveRecord::Rollback
    end

    expect(@selected.job_list).to be_empty
  end

  it "invalidates the insertion client when connection selection changes" do
    original = @settings.client
    @settings.connection_class = ActiveRecord::Base

    expect(@settings.client).not_to equal(original)
    expect(@settings.client.driver.connection_class).to eq(ActiveRecord::Base)
  end

  it "re-resolves a named class when Rails replaces its constant" do
    original = @settings.client
    old_class = RailsSelectedConnection
    Object.send(:remove_const, :RailsSelectedConnection)
    Object.const_set(:RailsSelectedConnection, Class.new(ActiveRecord::Base) { self.abstract_class = true })

    expect(@settings.client).not_to equal(original)
    expect(@settings.client.driver.connection_class).to equal(RailsSelectedConnection)
  ensure
    Object.send(:remove_const, :RailsSelectedConnection)
    Object.const_set(:RailsSelectedConnection, old_class)
  end
end
