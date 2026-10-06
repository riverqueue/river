# frozen_string_literal: true

#
# Updates the version in a gemspec file since doing it from the shell is a total
# pain.
#

file = ARGV[0] || abort("failure: need one argument, which is a gemspec filename")
version = ENV["VERSION"] || abort("failure: need VERSION")

file_data = File.read(file)

version = version[1..] # strip `v` from the beginning of the string

updated_file_data = file_data.gsub(%r{^(\W+)s\.version = "[\d.]+"$}, %(\\1s.version = "#{version}"))

abort("failure: nothing changed in file") if file_data == updated_file_data

File.write(file, updated_file_data)
