package river

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
	"github.com/riverqueue/river/rivertype"
)

const jobWaitBatchSize = 1_000

// jobWaiter polls only while callers are waiting. Its lifetime is independent of
// worker services so it also works on clients that only insert and inspect jobs.
type jobWaiter struct {
	getExecutorFunc func() riverdriver.Executor
	pollInterval    time.Duration
	schema          string

	mu        sync.Mutex
	activeRun *jobWaiterRun
}

func newJobWaiter(getExecutorFunc func() riverdriver.Executor, schema string) *jobWaiter {
	return &jobWaiter{
		getExecutorFunc: getExecutorFunc,
		pollInterval:    250 * time.Millisecond,
		schema:          schema,
	}
}

func (w *jobWaiter) pollOnce(ctx context.Context, run *jobWaiterRun, pendingOnly bool) {
	batch := func() []jobWaiterBatchEntry {
		w.mu.Lock()
		defer w.mu.Unlock()

		batch := make([]jobWaiterBatchEntry, 0, len(run.requests))
		for id, requests := range run.requests {
			if _, pending := run.pending[id]; pendingOnly && !pending {
				continue
			}
			delete(run.pending, id)
			entry := jobWaiterBatchEntry{id: id, requests: make([]*jobWaiterRequest, 0, len(requests))}
			for request := range requests {
				entry.requests = append(entry.requests, request)
			}
			batch = append(batch, entry)
		}
		return batch
	}()

	for chunk := range slices.Chunk(batch, jobWaitBatchSize) {
		if ctx.Err() != nil {
			return
		}
		ids := make([]int64, len(chunk))
		for i, entry := range chunk {
			ids[i] = entry.id
		}
		jobs, err := timeoututil.WithTimeoutV(ctx, rivercommon.HotOperationTimeout, "JobWaitFinalized", func(ctx context.Context) ([]*rivertype.JobRow, error) {
			return w.getExecutorFunc().JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{ID: ids, Schema: w.schema})
		})
		if err != nil {
			err = fmt.Errorf("error waiting for jobs to finalize: %w", err)
		}
		jobsByID := make(map[int64]*rivertype.JobRow, len(jobs))
		for _, job := range jobs {
			jobsByID[job.ID] = job
		}

		func() {
			w.mu.Lock()
			defer w.mu.Unlock()

			for _, entry := range chunk {
				result := jobWaiterResult{err: err, job: jobsByID[entry.id]}
				if err == nil {
					if result.job == nil {
						result.err = rivertype.ErrNotFound
					} else if !slices.Contains([]rivertype.JobState{
						rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded,
					}, result.job.State) {
						continue
					}
				}
				for _, request := range entry.requests {
					// Only notify callers in the snapshot that are still registered.
					// A new caller must get a read initiated after it registered.
					if _, registered := run.requests[entry.id][request]; registered {
						request.resultChan <- result
						w.removeLocked(run, request)
					}
				}
			}
		}()
	}
}

func (w *jobWaiter) register(id int64) (*jobWaiterRun, *jobWaiterRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.activeRun == nil {
		// No individual caller owns this context: cancelling one wait must not
		// interrupt a shared query that other callers still need.
		ctx, cancel := context.WithCancel(context.Background())
		w.activeRun = &jobWaiterRun{
			cancelFunc: cancel,
			pending:    make(map[int64]struct{}),
			requests:   make(map[int64]map[*jobWaiterRequest]struct{}),
			wakeChan:   make(chan struct{}, 1),
		}
		go w.run(ctx, w.activeRun)
	}

	run := w.activeRun
	request := &jobWaiterRequest{id: id, resultChan: make(chan jobWaiterResult, 1)}
	if run.requests[id] == nil {
		run.requests[id] = make(map[*jobWaiterRequest]struct{})
	}
	run.requests[id][request] = struct{}{}
	run.pending[id] = struct{}{}
	select {
	case run.wakeChan <- struct{}{}:
	default:
	}
	return run, request
}

func (w *jobWaiter) remove(run *jobWaiterRun, request *jobWaiterRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.removeLocked(run, request)
}

func (w *jobWaiter) removeLocked(run *jobWaiterRun, request *jobWaiterRequest) {
	delete(run.requests[request.id], request)
	if len(run.requests[request.id]) == 0 {
		delete(run.requests, request.id)
		delete(run.pending, request.id)
	}
	if len(run.requests) == 0 {
		run.cancelFunc()
		if w.activeRun == run {
			w.activeRun = nil
		}
	}
}

func (w *jobWaiter) run(ctx context.Context, run *jobWaiterRun) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-run.wakeChan:
			// New waits are checked promptly without polling every existing ID
			// again each time a caller registers.
			w.pollOnce(ctx, run, true)
		case <-ticker.C:
			w.pollOnce(ctx, run, false)
		}
	}
}

func (w *jobWaiter) wait(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run, request := w.register(id) //nolint:contextcheck // Shared polling has its own lifetime, independent of any caller's context.
	defer w.remove(run, request)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-request.resultChan:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if result.err != nil {
			return nil, result.err
		}
		return result.jobCopy(), nil
	}
}

type jobWaiterBatchEntry struct {
	id       int64
	requests []*jobWaiterRequest
}

type jobWaiterRequest struct {
	id         int64
	resultChan chan jobWaiterResult
}

type jobWaiterResult struct {
	err error
	job *rivertype.JobRow
}

// Each caller owns its result, including slices and optional timestamps. The
// database row may otherwise be shared by many concurrent waits for the same ID.
func (r jobWaiterResult) jobCopy() *rivertype.JobRow {
	job := *r.job
	if job.AttemptedAt != nil {
		job.AttemptedAt = new(*job.AttemptedAt)
	}
	job.AttemptedBy = slices.Clone(job.AttemptedBy)
	job.EncodedArgs = slices.Clone(job.EncodedArgs)
	job.Errors = slices.Clone(job.Errors)
	if job.FinalizedAt != nil {
		job.FinalizedAt = new(*job.FinalizedAt)
	}
	job.Metadata = slices.Clone(job.Metadata)
	job.Tags = slices.Clone(job.Tags)
	job.UniqueKey = slices.Clone(job.UniqueKey)
	job.UniqueStates = slices.Clone(job.UniqueStates)
	return &job
}

type jobWaiterRun struct {
	cancelFunc context.CancelFunc
	pending    map[int64]struct{}                       // guarded by jobWaiter.mu
	requests   map[int64]map[*jobWaiterRequest]struct{} // guarded by jobWaiter.mu
	wakeChan   chan struct{}
}
