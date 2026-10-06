# frozen_string_literal: true

require "rspec/expectations"
require "riverqueue/testing"

module River
  module Testing
    # Include through RSpec.configure { |c| c.include River::Testing::RSpec }.
    module RSpec
      # Matches a client with at least one persisted job matching the attributes.
      # Includes all job states unless state: is specified.
      def have_job(**attributes)
        JobMatcher.new(1, attributes).at_least(1)
      end

      # Matches a block inserting exactly one matching persisted job.
      def insert_job(client, **attributes)
        insert_jobs(client, **attributes)
      end

      # Matches a block inserting count matching rows (one by default).
      # Attributes accept composable RSpec matchers. Negation always requires zero
      # matching rows, regardless of the configured positive count.
      def insert_jobs(client, count: 1, **attributes)
        InsertionMatcher.new(client, count, attributes)
      end

      class JobMatcher
        include ::RSpec::Matchers::Composable

        def initialize(count, attributes)
          @attributes = Testing.normalize_attributes(attributes)
          exactly(count)
        end

        # Requires at least count matching jobs.
        def at_least(count)
          set_count(count, :>=, "at least")
        end

        # Requires at most count matching jobs.
        def at_most(count)
          set_count(count, :<=, "at most")
        end

        def description = "#{verb} #{@count_description} #{@count} River jobs matching #{surface_descriptions_in(@attributes).inspect}"

        def does_not_match?(actual)
          matching_rows(actual).empty?
        end

        # Requires exactly count matching jobs.
        def exactly(count)
          set_count(count, :==, "exactly")
        end

        def failure_message = "Expected to #{description}, got #{@rows.length}: #{@rows.map(&:id).inspect}"

        def failure_message_when_negated = "Expected no matching River jobs for #{surface_descriptions_in(@attributes).inspect}, got #{@rows.length}: #{@rows.map(&:id).inspect}"

        def matches?(actual)
          matching_rows(actual).length.public_send(@comparison, @count)
        end

        def supports_block_expectations? = false

        def supports_value_expectations? = true

        private def candidates(actual)
          Testing.jobs(actual)
        end

        private def matching_rows(actual)
          @rows = candidates(actual).select do |row|
            @attributes.all? { |key, expected| values_match?(expected, row.public_send(key)) }
          end
        end

        private def set_count(count, comparison, description)
          raise ArgumentError, "count must be a nonnegative integer" unless count.is_a?(Integer) && count >= 0

          @comparison = comparison
          @count = count
          @count_description = description
          self
        end

        private def verb = "have"
      end

      class InsertionMatcher < JobMatcher
        def initialize(client, count, attributes)
          @client = client
          super(count, attributes)
        end

        def supports_block_expectations? = true

        def supports_value_expectations? = false

        private def candidates(operation)
          Testing.inserted_jobs(@client, &operation)
        end

        private def verb = "insert"
      end
    end
  end
end
