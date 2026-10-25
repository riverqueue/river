# frozen_string_literal: true

module River
  RESUMABLE_CURSOR_METADATA_KEY = "river:resumable_cursor"
  RESUMABLE_STEP_METADATA_KEY = "river:resumable_step"

  # Execution state for resumable steps. Applications normally interact with
  # this through Job#resumable_step and Job#resumable_step_cursor.
  class ResumableState
    attr_reader :all_step_names
    attr_accessor :completed_step
    attr_reader :cursors
    attr_accessor :cursors_dirty
    attr_reader :pending_cursor_steps
    attr_accessor :resume_matched
    attr_reader :resume_step
    attr_accessor :step_name

    def initialize(metadata)
      @all_step_names = {}
      @completed_step = nil
      @cursors = (metadata[RESUMABLE_CURSOR_METADATA_KEY] || {}).dup
      @cursors_dirty = false
      @resume_step = metadata[RESUMABLE_STEP_METADATA_KEY]
      @step_name = nil

      @resume_matched = @resume_step.to_s.empty?
      # Step markers may accompany stale cursors from older clients.
      @pending_cursor_steps = @resume_matched ? @cursors.keys.sort : []
    end

    def register(name)
      raise Error, "duplicate resumable step name #{name.inspect}" if all_step_names.key?(name)

      all_step_names[name] = true
    end
  end

  class Job
    # Internal runtime boundary: attach progress only to attempts that did not
    # complete, matching River Go's persisted metadata format.
    def __capture_resumable_metadata!
      # Keep missing cursor steps reachable when the worker restores them.
      return unless @resumable_state.pending_cursor_steps.empty?

      if @resumable_state.cursors_dirty
        @metadata_updates[RESUMABLE_CURSOR_METADATA_KEY] = @resumable_state.cursors.empty? ? nil : @resumable_state.cursors.dup
      end

      if @resumable_state.completed_step
        @metadata_updates[RESUMABLE_STEP_METADATA_KEY] = @resumable_state.completed_step
      end
    end

    # Internal runtime boundary: validate that the recorded resume point still
    # exists in the worker after it has returned.
    def __finish_resumable_work!
      if (pending = @resumable_state.pending_cursor_steps.first)
        raise Error, "resumable cursor step #{pending.inspect} not found in worker"
      end
      if @resumable_state.resume_step && !@resumable_state.resume_matched
        raise Error, "resumable step #{@resumable_state.resume_step.inspect} not found in worker"
      end
    end

    # Immediately checkpoints the current step and cursor progress. Wrap the
    # call in Driver#transaction alongside application writes when an atomic
    # checkpoint is needed. When resuming cursor-only progress, reach its saved
    # cursor step before checkpointing another step.
    #
    # Commit the transaction before the step block returns
    # successfully, and let transaction and commit errors propagate out of it.
    # A successful block marks the step complete even if an enclosing transaction
    # later rolls back.
    def resumable_checkpoint(cursor: RESUMABLE_CURSOR_UNSET)
      step_name = @resumable_state.step_name
      raise Error, "resumable step can only be persisted inside a resumable step" unless step_name
      raise Error, "job must be running" unless row.state == JOB_STATE_RUNNING

      if (pending = @resumable_state.pending_cursor_steps.first)
        raise Error, "resumable checkpoint cannot precede saved cursor step #{pending.inspect}"
      end

      cursors = @resumable_state.cursors.dup
      cursors[step_name] = JSON.parse(JSON.generate(cursor)) unless cursor.equal?(RESUMABLE_CURSOR_UNSET)
      updates = {
        RESUMABLE_STEP_METADATA_KEY => step_name,
        RESUMABLE_CURSOR_METADATA_KEY => cursors.empty? ? nil : cursors
      }

      updated = client.driver.job_metadata_merge(row.id, updates) || raise(NotFoundError, "job not found: #{row.id}")
      # The database owns checkpointed progress, including rollback of an
      # enclosing application transaction. Only progress made after this write
      # belongs in the attempt's deferred updates; replaying the checkpoint on
      # failure could otherwise commit progress whose application writes rolled back.
      @resumable_state.cursors.replace(cursors)
      @resumable_state.cursors_dirty = false
      @resumable_state.completed_step = nil
      @row = updated
    end

    # Records JSON-compatible cursor data for the current step. It is persisted
    # with the failed attempt so that its retry can continue from this value.
    def resumable_set_cursor(cursor)
      step_name = @resumable_state.step_name
      raise Error, "resumable cursor can only be set inside a resumable step" unless step_name

      @resumable_state.cursors[step_name] = JSON.parse(JSON.generate(cursor))
      @resumable_state.cursors_dirty = true
      cursor
    end

    # Runs a named step, skipping it on retry when a previous attempt already
    # completed it. Names accept symbols or strings and must be unique within a
    # worker invocation. Steps cannot be nested: a nested resume point would be
    # unreachable when its enclosing step is skipped on retry. Persisted
    # checkpoints always use string names.
    # Exceptions propagate immediately, just as they do outside a step.
    def resumable_step(name, &block)
      run_resumable_step(name.to_s, cursor: false, default: nil, &block)
    end

    # Runs a named step with the cursor saved by #resumable_set_cursor during a
    # previous failed attempt. Names accept symbols or strings.
    def resumable_step_cursor(name, default: nil, &block)
      run_resumable_step(name.to_s, cursor: true, default: default, &block)
    end

    RESUMABLE_CURSOR_UNSET = Object.new.freeze
    private_constant :RESUMABLE_CURSOR_UNSET

    private def initialize_resumable_state
      @resumable_state = ResumableState.new(row.metadata)
    end

    private def run_resumable_step(name, cursor:, default:)
      raise Error, "resumable steps cannot be nested" if @resumable_state.step_name
      raise ArgumentError, "resumable step name must be non-empty" if name.empty?
      if !cursor && @resumable_state.pending_cursor_steps.include?(name)
        raise Error, "resumable cursor step #{name.inspect} requires a cursor step"
      end
      @resumable_state.register(name)

      unless @resumable_state.resume_matched
        if name == @resumable_state.resume_step
          @resumable_state.completed_step = name
          @resumable_state.resume_matched = true
          return unless cursor && @resumable_state.cursors.key?(name)
        else
          return
        end
      end

      @resumable_state.step_name = name
      begin
        value = (cursor && @resumable_state.cursors.key?(name)) ? @resumable_state.cursors[name] : default
        @resumable_state.pending_cursor_steps.delete(name) if cursor
        result = cursor ? yield(value) : yield
        @resumable_state.completed_step = name
        if cursor && @resumable_state.cursors.key?(name)
          @resumable_state.cursors.delete(name)
          @resumable_state.cursors_dirty = true
        end

        result
      ensure
        @resumable_state.step_name = nil
      end
    end
  end
end
