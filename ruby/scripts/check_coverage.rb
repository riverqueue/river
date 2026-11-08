# frozen_string_literal: true

require "simplecov"

# All backend jobs must supply every package's report; a missing artifact must
# not turn into an apparently complete report for only one package or backend.
reports = %w[postgres sqlite].product([".", "driver/riverqueue-activerecord", "driver/riverqueue-sequel"]).map do |backend, package|
  path = "coverage/ci/ruby-coverage-#{backend}/#{package}/coverage/.resultset.json"
  abort "Missing coverage report: #{path}" unless File.file?(path)
  path
end

mysql_report = "coverage/ci/ruby-coverage-mysql/.resultset.json"
abort "Missing coverage report: #{mysql_report}" unless File.file?(mysql_report)
reports << mysql_report

SimpleCov.collate(reports) do
  enable_coverage :branch
  minimum_coverage branch: 100, line: 100
end
