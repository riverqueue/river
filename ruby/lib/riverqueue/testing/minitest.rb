# frozen_string_literal: true

require "minitest"
require "riverqueue/testing"

module River
  module Testing
    # Include in Minitest::Test (or an individual test class) to use River
    # assertions with Minitest's failure reporting and assertion count.
    module Minitest
      include Assertions

      private def river_assert(condition, message)
        assert(condition, message)
      end
    end
  end
end
