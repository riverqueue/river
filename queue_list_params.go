package river

// QueueListParams specifies the parameters for a QueueList query. It must be
// initialized with NewQueueListParams. Params can be built by chaining methods
// on the QueueListParams object:
//
//	params := NewQueueListParams().First(100)
type QueueListParams struct {
	paginationCount int32
}

// NewQueueListParams creates a new QueueListParams to return available jobs
// sorted by time in ascending order, returning 100 jobs at most.
func NewQueueListParams() *QueueListParams {
	return &QueueListParams{
		paginationCount: 100,
	}
}

func (p *QueueListParams) copy() *QueueListParams {
	return &QueueListParams{
		paginationCount: p.paginationCount,
	}
}

// First returns an updated filter set that will only return the first count
// queues.
//
// Count must be between 1 and 10000, inclusive, or this will panic.
func (p *QueueListParams) First(count int) *QueueListParams {
	if count <= 0 {
		panic("count must be > 0")
	}
	if count > 10000 {
		panic("count must be <= 10000")
	}
	result := p.copy()
	result.paginationCount = int32(count)
	return result
}
