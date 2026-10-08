# frozen_string_literal: true

require "json"

module RiverConformanceFixtures
  def self.read(name)
    File.read(File.expand_path("../../../conformance/testdata/#{name}.json", __dir__))
  rescue Errno::ENOENT
    raise "Missing Go-generated #{name} fixtures; run `make generate/fixtures` from the repository root"
  end

  def self.load(name)
    JSON.parse(read(name))
  end
end
