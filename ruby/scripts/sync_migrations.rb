# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"

# Copies upstream SQL verbatim. --check verifies both file names and bytes.
check = ARGV.delete("--check")

source = File.expand_path(ARGV.fetch(0, File.expand_path("../..", __dir__)))
root = File.expand_path("..", __dir__)
destination = File.join(root, "migration")
line = "main"
drivers = {"postgresql" => "riverdriver/riverpgxv5", "sqlite" => "riverdriver/riversqlite"}

files = {}
drivers.each do |backend, directory|
  upstream = Dir.glob(File.join(source, directory, "migration", line, "*.sql")).sort
  abort "No migrations found for #{backend}" if upstream.empty?

  upstream.each do |path|
    relative = File.join(backend, line, File.basename(path))
    target = File.join(destination, relative)

    if check
      abort "Migration differs: #{relative}" unless File.file?(target) && File.binread(target) == File.binread(path)
    else
      FileUtils.mkdir_p(File.dirname(target))
      FileUtils.cp(path, target)
    end

    files[relative] = Digest::SHA256.file(path).hexdigest
  end
end

extra = Dir.glob(File.join(destination, "**/*.sql")).map { |path| path.delete_prefix("#{destination}/") } - files.keys
abort "Unexpected migrations: #{extra.join(", ")}" unless extra.empty?

license = File.join(source, "LICENSE")
target = File.join(destination, "LICENSE")

if check
  abort "Upstream migration license differs" unless File.binread(target) == File.binread(license)
else
  FileUtils.cp(license, target)
end

manifest = {"files" => files, "source" => "riverdriver/"}
manifest_path = File.join(destination, "manifest.json")
if check
  abort "Manifest differs" unless JSON.parse(File.read(manifest_path)) == manifest
else
  File.write(manifest_path, JSON.pretty_generate(manifest) + "\n")
end

puts "#{check ? "Verified" : "Copied"} #{files.size} #{line} migration files"
