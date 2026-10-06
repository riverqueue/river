# frozen_string_literal: true

class ReloadableJob < ActiveJob::Base
  def perform
    IntegrationJob.seen << self.class.object_id
  end
end
