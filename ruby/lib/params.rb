# frozen_string_literal: true

module River
  # Result of Client#job_delete_many.
  JobDeleteManyResult = Data.define(:jobs)

  # Position in a job listing, including the ordering value so pagination still
  # works after the job at the end of the previous page has been deleted.
  JobListCursor = Data.define(:id, :sort_by, :sort_order, :value)

  # A page of jobs and the cursor for fetching the next page.
  JobListResult = Data.define(:jobs, :last_cursor)

  # A River queue record persisted in the database.
  Queue = Data.define(:name, :created_at, :metadata, :paused_at, :updated_at)

  # Result of Client#queue_list.
  QueueListResult = Data.define(:queues)

  # Filtering, ordering, and pagination parameters for Client#job_list and
  # Client#job_delete_many.
  class JobListParams
    # Normalized filtering, pagination, and ordering values.
    attr_reader :after, :after_id, :ids, :kinds, :limit, :metadata, :priorities, :queues, :sort_by,
      :sort_order, :states, :tags_all, :tags_any

    # Creates job-list parameters. Pass JobListResult#last_cursor as +after+ and
    # keep the same filters and ordering for subsequent pages. +after_id+ is a
    # shortcut for ID-ordered listings. Null timestamps always sort last.
    # Kind, queue, and state filters accept symbols or strings.
    def initialize(after: nil, after_id: nil, ids: nil, kinds: nil, limit: 100, priorities: nil,
      metadata: nil, queues: nil, sort_by: :id, sort_order: :asc, states: nil, tags_all: nil, tags_any: nil)
      @after = after
      @after_id = after_id.nil? ? nil : Integer(after_id)
      @ids = ids
      @kinds = kinds&.map(&:to_s)
      @limit = Integer(limit)
      @metadata = metadata
      @priorities = priorities
      @queues = queues&.map(&:to_s)
      @sort_by = sort_by.to_sym
      @sort_order = sort_order.to_sym
      @states = states&.map(&:to_s)
      @tags_all = tags_all
      @tags_any = tags_any

      raise ArgumentError, "limit must be between 1 and 10,000" unless (1..10_000).cover?(@limit)
      raise ArgumentError, "invalid sort field" unless [:id, :scheduled_at, :finalized_at].include?(@sort_by)
      raise ArgumentError, "invalid sort order" unless [:asc, :desc].include?(@sort_order)
      raise ArgumentError, "use either after or after_id" if after && after_id
      raise ArgumentError, "after_id requires sorting by id; use after for timestamp ordering" if after_id && @sort_by != :id
      if after && (!after.is_a?(JobListCursor) || after.sort_by != @sort_by || after.sort_order != @sort_order)
        raise ArgumentError, "after must be a JobListCursor with the same ordering"
      end
    end

    def filters?
      [after, after_id, ids, kinds, metadata, priorities, queues, states, tags_all, tags_any].any? do |value|
        value.respond_to?(:empty?) ? !value.empty? : !value.nil?
      end
    end
  end

  # Fields to change with Client#job_update. Omitted fields are left unchanged;
  # explicitly passing nil clears nullable fields.
  class JobUpdateParams
    UNSET = Object.new.freeze

    # Values to apply, with omitted fields represented internally by UNSET.
    attr_reader :attempt, :attempted_at, :attempted_by, :errors, :finalized_at,
      :max_attempts, :metadata, :state

    # Creates a partial set of updates for a persisted job.
    def initialize(attempt: UNSET, attempted_at: UNSET, attempted_by: UNSET,
      errors: UNSET, finalized_at: UNSET, max_attempts: UNSET, metadata: UNSET,
      state: UNSET)
      @attempt = attempt
      @attempted_at = attempted_at
      @attempted_by = attempted_by
      @errors = errors
      @finalized_at = finalized_at
      @max_attempts = max_attempts
      @metadata = metadata
      @state = state.is_a?(Symbol) ? state.to_s : state
    end

    def each
      return enum_for(:each) unless block_given?

      instance_variables.each do |ivar|
        value = instance_variable_get(ivar)
        yield ivar.to_s.delete_prefix("@").to_sym, value unless value.equal?(UNSET)
      end
    end
  end
end
