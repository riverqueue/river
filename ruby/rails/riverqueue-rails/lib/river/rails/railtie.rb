# frozen_string_literal: true

require "rails/railtie"

module River
  module Rails
    class Railtie < ::Rails::Railtie
      config.river = Configuration.new

      rake_tasks do
        namespace :river do
          desc "Apply River's canonical database migrations"
          task migrate: :environment do
            River::Migrator.new(::Rails.application.config.river.build_driver).migrate
          end

          desc "Show River migration status"
          task status: :environment do
            River::Migrator.new(::Rails.application.config.river.build_driver).status.each do |migration|
              puts "#{migration.applied ? "applied" : "pending"} #{migration.version} #{migration.name}"
            end
          end
        end
      end
    end
  end
end
