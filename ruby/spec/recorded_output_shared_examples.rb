# frozen_string_literal: true

require "riverqueue/testing"

RSpec.shared_examples "recorded output size limit" do
  ["x", "é", '"'].each do |character|
    it "limits recorded output by encoded UTF-8 bytes for #{character.inspect}" do
      limit = 32 * 1024 * 1024
      encoded_character_size = JSON.generate(character).bytesize - 2
      output_of_size = ->(bytes) do
        character * ((bytes - 2) / encoded_character_size) + "x" * ((bytes - 2) % encoded_character_size)
      end
      at_limit = output_of_size.call(limit)
      worker = Object.new
      check_output = ->(job) do
        job.update_metadata("kept" => true)
        job.output = output_of_size.call(limit - 1)
        job.output = at_limit
        expect { job.output = output_of_size.call(limit + 1) }
          .to raise_error(ArgumentError, "output is too large: 33554433 bytes (max 32 MiB)")
      end
      worker.define_singleton_method(:work) { |job| check_output.call(job) }
      client = River::Client.new(@driver, config: River::Config.new(workers: River::Workers.new.add(:output, worker)))
      row = client.insert(River::JobArgsHash.new(:output, {})).job

      result = River::Testing.perform_job(client, row.id)

      expect(result).to have_attributes(error: nil, outcome: :completed)
      expect(client.job_get(row.id).metadata).to include("kept" => true, "output" => at_limit)
    end
  end
end
