package river

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

func TestJobWaiter(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		exec   *jobWaiterExecutorStub
		waiter *jobWaiter
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		exec := &jobWaiterExecutorStub{}
		waiter := newJobWaiter(func() riverdriver.Executor { return exec }, "custom_schema")
		waiter.pollInterval = 10 * time.Millisecond
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				waiter.mu.Lock()
				defer waiter.mu.Unlock()
				return waiter.activeRun == nil
			}, riversharedtest.WaitTimeout(), time.Millisecond)
		})
		return &testBundle{exec: exec, waiter: waiter}
	}

	t.Run("BatchesAndDeduplicates", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.waiter.pollInterval = time.Hour
		bundle.exec.testSignals.Init(t)
		var numCalls atomic.Int32
		bundle.exec.getByIDManyFunc = func(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			require.Equal(t, "custom_schema", params.Schema)
			require.LessOrEqual(t, len(params.ID), jobWaitBatchSize)
			if numCalls.Add(1) == 1 {
				bundle.exec.testSignals.LookupStarted.Signal(params.ID)
				select {
				case <-bundle.exec.testSignals.LookupContinue.WaitC():
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			jobs := make([]*rivertype.JobRow, len(params.ID))
			for i, id := range params.ID {
				jobs[i] = &rivertype.JobRow{ID: id, State: rivertype.JobStateCompleted}
			}
			return jobs, nil
		}

		// Hold the first query so all remaining registrations share one poll.
		run, first := bundle.waiter.register(0)
		t.Cleanup(func() { bundle.waiter.remove(run, first) })
		bundle.exec.testSignals.LookupStarted.WaitOrTimeout()
		requests := make([]*jobWaiterRequest, 0, 1+2*(jobWaitBatchSize*2+1))
		requests = append(requests, first)
		for id := range int64(jobWaitBatchSize*2 + 1) {
			for range 2 {
				_, request := bundle.waiter.register(id + 1)
				t.Cleanup(func() { bundle.waiter.remove(run, request) })
				requests = append(requests, request)
			}
		}
		bundle.exec.testSignals.LookupContinue.Signal(struct{}{})
		for _, request := range requests {
			result := riversharedtest.WaitOrTimeout(t, request.resultChan)
			require.NoError(t, result.err)
			require.Equal(t, request.id, result.job.ID)
		}
		require.EqualValues(t, 4, numCalls.Load())
	})

	t.Run("CancelledContextDoesNotQuery", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		job, err := bundle.waiter.wait(ctx, 1)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, job)
	})

	t.Run("CancelledWaitDoesNotCancelOtherWaits", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.exec.testSignals.Init(t)
		var finalized atomic.Bool
		bundle.exec.getByIDManyFunc = func(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			state := rivertype.JobStateRunning
			if finalized.Load() {
				state = rivertype.JobStateCompleted
			}
			bundle.exec.testSignals.LookupStarted.Signal(params.ID)
			return []*rivertype.JobRow{{ID: 1, State: state}}, nil
		}

		run, other := bundle.waiter.register(1)
		t.Cleanup(func() { bundle.waiter.remove(run, other) })
		bundle.exec.testSignals.LookupStarted.WaitOrTimeout()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		resultChan := make(chan jobWaiterResult, 1)
		go func() {
			job, err := bundle.waiter.wait(ctx, 1)
			resultChan <- jobWaiterResult{err: err, job: job}
		}()
		require.Eventually(t, func() bool {
			bundle.waiter.mu.Lock()
			defer bundle.waiter.mu.Unlock()
			return len(run.requests[1]) == 2
		}, riversharedtest.WaitTimeout(), time.Millisecond)
		cancel()
		require.ErrorIs(t, riversharedtest.WaitOrTimeout(t, resultChan).err, context.Canceled)

		finalized.Store(true)
		result := riversharedtest.WaitOrTimeout(t, other.resultChan)
		require.NoError(t, result.err)
		require.Equal(t, rivertype.JobStateCompleted, result.job.State)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		expectedErr := errors.New("database unavailable")
		bundle.exec.getByIDManyFunc = func(context.Context, *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			return nil, expectedErr
		}

		job, err := bundle.waiter.wait(t.Context(), 1)
		require.ErrorIs(t, err, expectedErr)
		require.Nil(t, job)
	})

	t.Run("DeadlineCancelsLastQueryAndCanRestart", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.exec.testSignals.Init(t)
		var numCalls atomic.Int32
		bundle.exec.getByIDManyFunc = func(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			if numCalls.Add(1) == 1 {
				bundle.exec.testSignals.LookupStarted.Signal(params.ID)
				<-ctx.Done()
				bundle.exec.testSignals.QueryCancelled.Signal(struct{}{})
				return nil, ctx.Err()
			}
			return []*rivertype.JobRow{{ID: 1, State: rivertype.JobStateCompleted}}, nil
		}

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		job, err := bundle.waiter.wait(ctx, 1)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, job)
		bundle.exec.testSignals.QueryCancelled.WaitOrTimeout()

		job, err = bundle.waiter.wait(t.Context(), 1)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
	})

	t.Run("NewCallerDoesNotReceiveAnEarlierSnapshot", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.waiter.pollInterval = time.Hour
		bundle.exec.testSignals.Init(t)
		var numCalls atomic.Int32
		bundle.exec.getByIDManyFunc = func(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			attempt := numCalls.Add(1)
			if attempt == 1 {
				bundle.exec.testSignals.LookupStarted.Signal(params.ID)
				select {
				case <-bundle.exec.testSignals.LookupContinue.WaitC():
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return []*rivertype.JobRow{{ID: 1, Attempt: int(attempt), State: rivertype.JobStateCompleted}}, nil
		}

		run, first := bundle.waiter.register(1)
		t.Cleanup(func() { bundle.waiter.remove(run, first) })
		bundle.exec.testSignals.LookupStarted.WaitOrTimeout()
		_, second := bundle.waiter.register(1)
		t.Cleanup(func() { bundle.waiter.remove(run, second) })
		bundle.exec.testSignals.LookupContinue.Signal(struct{}{})

		require.Equal(t, 1, riversharedtest.WaitOrTimeout(t, first.resultChan).job.Attempt)
		require.Equal(t, 2, riversharedtest.WaitOrTimeout(t, second.resultChan).job.Attempt)
	})

	t.Run("NonFinalizedStatesKeepWaiting", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		states := []rivertype.JobState{
			rivertype.JobStateAvailable, rivertype.JobStatePending, rivertype.JobStateRunning,
			rivertype.JobStateRetryable, rivertype.JobStateScheduled, rivertype.JobStateCompleted,
		}
		var numCalls atomic.Int32
		bundle.exec.getByIDManyFunc = func(context.Context, *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			state := states[numCalls.Add(1)-1]
			return []*rivertype.JobRow{{ID: 1, State: state}}, nil
		}

		job, err := bundle.waiter.wait(t.Context(), 1)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
		require.EqualValues(t, len(states), numCalls.Load())
	})

	t.Run("NotFoundAfterDeletion", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		var numCalls atomic.Int32
		bundle.exec.getByIDManyFunc = func(context.Context, *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
			if numCalls.Add(1) == 1 {
				return []*rivertype.JobRow{{ID: 1, State: rivertype.JobStateAvailable}}, nil
			}
			return nil, nil
		}

		job, err := bundle.waiter.wait(t.Context(), 1)
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		require.Nil(t, job)
	})
}

func TestJobWaiterResultJobCopy(t *testing.T) {
	t.Parallel()

	original := &rivertype.JobRow{
		ID:           1,
		AttemptedAt:  new(time.Now()),
		AttemptedBy:  []string{"client"},
		EncodedArgs:  []byte(`{}`),
		Errors:       []rivertype.AttemptError{{Error: "error"}},
		FinalizedAt:  new(time.Now()),
		Metadata:     []byte(`{}`),
		Tags:         []string{"tag"},
		UniqueKey:    []byte("key"),
		UniqueStates: []rivertype.JobState{rivertype.JobStateCompleted},
	}
	result := jobWaiterResult{job: original}
	first, second := result.jobCopy(), result.jobCopy()
	require.Equal(t, original, first)

	first.ID++
	*first.AttemptedAt = time.Time{}
	first.AttemptedBy[0] = "changed"
	first.EncodedArgs[0] = 'x'
	first.Errors[0].Error = "changed"
	*first.FinalizedAt = time.Time{}
	first.Metadata[0] = 'x'
	first.Tags[0] = "changed"
	first.UniqueKey[0] = 'x'
	first.UniqueStates[0] = rivertype.JobStateAvailable
	require.Equal(t, original, second)
	require.False(t, slices.Equal(first.Metadata, second.Metadata))
}

type jobWaiterExecutorStub struct {
	riverdriver.Executor

	getByIDManyFunc func(context.Context, *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error)
	testSignals     jobWaiterExecutorTestSignals
}

func (e *jobWaiterExecutorStub) JobGetByIDMany(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
	return e.getByIDManyFunc(ctx, params)
}

type jobWaiterExecutorTestSignals struct {
	LookupContinue testsignal.TestSignal[struct{}]
	LookupStarted  testsignal.TestSignal[[]int64]
	QueryCancelled testsignal.TestSignal[struct{}]
}

func (s *jobWaiterExecutorTestSignals) Init(t *testing.T) {
	t.Helper()
	s.LookupContinue.Init(t)
	s.LookupStarted.Init(t)
	s.QueryCancelled.Init(t)
}
