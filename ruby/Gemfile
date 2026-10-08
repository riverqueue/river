# frozen_string_literal: true

source "https://rubygems.org"

gemspec

group :development, :test do
  gem "standard"
  gem "steep"
end

group :test do
  gem "debug"
  gem "fugit", "~> 1.13", require: false
  gem "minitest", require: false
  gem "pg"
  gem "rspec-core"
  gem "rspec-expectations"
  gem "riverqueue-activerecord", path: "driver/riverqueue-activerecord"
  gem "riverqueue-sequel", path: "driver/riverqueue-sequel"
  gem "simplecov", require: false
  gem "sqlite3"
end
