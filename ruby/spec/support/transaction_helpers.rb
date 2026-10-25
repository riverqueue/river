# frozen_string_literal: true

require "logger"
require "stringio"

# Helpers for examples that run inside a caller's transaction, which River's
# drivers join instead of opening a savepoint of their own.
module TransactionHelpers
  # Runs the block in an application-owned savepoint, the way an application
  # recovers from a failed River call and keeps its transaction. Re-raises
  # the block's error after rolling back. Raising it outside the savepoint
  # keeps Sequel's SQLite adapter from wrapping an ArgumentError.
  def savepoint(driver)
    error = nil
    options = driver.respond_to?(:connection_class) ? [driver.connection_class, {requires_new: true}] : [driver.instance_variable_get(:@db), {savepoint: true}]
    result = options[0].transaction(**options[1]) do
      yield
    rescue driver.rollback_exception
      raise
    rescue => block_error
      error = block_error
      raise driver.rollback_exception
    end
    raise error if error

    result
  end

  # Returns the savepoints that the driver's adapter creates, releases, or
  # rolls back to while the block runs.
  def savepoint_statements(driver, &)
    return sql_statements(driver, &).grep(/\A\s*(RELEASE\s+|ROLLBACK\s+TO\s+)?SAVEPOINT\b/i) unless driver.respond_to?(:connection_class)

    # Record Active Record's savepoint calls directly. Lazily created
    # savepoints are not always logged before the block returns.
    connection = driver.connection_class.connection
    connection.singleton_class.prepend(ActiveRecordSavepointRecorder) unless connection.singleton_class <= ActiveRecordSavepointRecorder
    statements = [] #: Array[String]
    connection.river_savepoint_statements = statements
    begin
      yield
    ensure
      connection.river_savepoint_statements = nil
    end
    statements
  end

  # Returns the SQL that the driver's adapter sends while the block runs.
  def sql_statements(driver)
    db = driver.instance_variable_get(:@db)
    output = StringIO.new
    logger = Logger.new(output, formatter: ->(_severity, _time, _program, message) { "#{message}\n" })
    db.loggers << logger
    begin
      yield
    ensure
      db.loggers.delete(logger)
    end
    output.string.lines.map { |line| line.sub(/\A\([\d.]+s\) /, "").strip }
  end

  module ActiveRecordSavepointRecorder
    attr_accessor :river_savepoint_statements

    def create_savepoint(name = current_savepoint_name)
      river_savepoint_statements&.push("SAVEPOINT #{name}")
      super
    end

    def exec_rollback_to_savepoint(name = current_savepoint_name)
      river_savepoint_statements&.push("ROLLBACK TO SAVEPOINT #{name}")
      super
    end

    def release_savepoint(name = current_savepoint_name)
      river_savepoint_statements&.push("RELEASE SAVEPOINT #{name}")
      super
    end
  end
end

RSpec.configure { |config| config.include TransactionHelpers }
