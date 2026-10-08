# frozen_string_literal: true

require "rails/generators"

module River
  module Generators
    # Installs configuration and the explicitly started worker entry point.
    class InstallGenerator < ::Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      def install
        copy_file "river.rb", "config/initializers/river.rb"
        copy_file "jobs", "bin/jobs"
        chmod "bin/jobs", 0o755
      end
    end
  end
end
