package jobcompleter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbadaptertest"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/riverinternaltest"
)

func TestInlineJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	var attempt int
	expectedErr := errors.New("an error from the completer")
	adapter := &dbadaptertest.TestAdapter{
		JobSetCompletedIfRunningFunc: func(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error) {
			require.Equal(t, int64(1), job.ID)
			attempt++
			return nil, expectedErr
		},
	}

	completer := NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), adapter)
	t.Cleanup(completer.Wait)

	err := completer.JobSetCompleted(1, &jobstats.JobStatistics{}, time.Now())
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}

	require.True(t, adapter.JobSetCompletedIfRunningCalled)
	require.Equal(t, numRetries, attempt)
}

func TestInlineJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(a dbadapter.Adapter) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), a)
	})
}

func TestInlineJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(a dbadapter.Adapter) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), a)
	})
}

func TestAsyncJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	type jobInput struct {
		// TODO: Try to get rid of containing the context in struct. It'd be
		// better to pass it forward instead.
		ctx   context.Context //nolint:containedctx
		jobID int64
	}
	inputCh := make(chan jobInput)
	resultCh := make(chan error)

	expectedErr := errors.New("an error from the completer")

	go func() {
		riverinternaltest.WaitOrTimeout(t, inputCh)
		resultCh <- expectedErr
	}()

	adapter := &dbadaptertest.TestAdapter{
		JobSetCompletedIfRunningFunc: func(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error) {
			inputCh <- jobInput{ctx: ctx, jobID: job.ID}
			err := <-resultCh
			return nil, err
		},
	}
	completer := NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), adapter, 2)
	t.Cleanup(completer.Wait)

	// launch 4 completions, only 2 can be inline due to the concurrency limit:
	for i := int64(0); i < 2; i++ {
		if err := completer.JobSetCompleted(i, &jobstats.JobStatistics{}, time.Now()); err != nil {
			t.Errorf("expected nil err, got %v", err)
		}
	}
	bgCompletionsStarted := make(chan struct{})
	go func() {
		for i := int64(2); i < 4; i++ {
			if err := completer.JobSetCompleted(i, &jobstats.JobStatistics{}, time.Now()); err != nil {
				t.Errorf("expected nil err, got %v", err)
			}
		}
		close(bgCompletionsStarted)
	}()

	expectCompletionInFlight := func() {
		select {
		case input := <-inputCh:
			t.Logf("completion for %d in-flight", input.jobID)
		case <-time.After(time.Second):
			t.Fatalf("expected a completion to be in-flight")
		}
	}
	expectNoCompletionInFlight := func() {
		select {
		case input := <-inputCh:
			t.Fatalf("unexpected completion for %d in-flight", input.jobID)
		case <-time.After(500 * time.Millisecond):
		}
	}

	// two completions should be in-flight:
	expectCompletionInFlight()
	expectCompletionInFlight()

	// A 3rd one shouldn't be in-flight due to the concurrency limit:
	expectNoCompletionInFlight()

	// Finish the first two completions:
	resultCh <- nil
	resultCh <- nil

	// The final two completions should now be in-flight:
	<-bgCompletionsStarted
	expectCompletionInFlight()
	expectCompletionInFlight()

	// A 5th one shouldn't be in-flight because we only started 4:
	expectNoCompletionInFlight()

	// Finish the final two completions:
	resultCh <- nil
	resultCh <- nil
}

func TestAsyncJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(a dbadapter.Adapter) JobCompleter {
		return NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), a, 4)
	})
}

func TestAsyncJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(a dbadapter.Adapter) JobCompleter {
		return NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), a, 4)
	})
}

func testCompleterSubscribe(t *testing.T, constructor func(dbadapter.Adapter) JobCompleter) {
	t.Helper()

	adapter := &dbadaptertest.TestAdapter{
		JobSetCompletedIfRunningFunc: func(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error) {
			return &dbsqlc.RiverJob{
				State: dbsqlc.JobStateCompleted,
			}, nil
		},
	}

	completer := constructor(adapter)

	jobUpdates := make(chan CompleterJobUpdated, 10)
	completer.Subscribe(func(update CompleterJobUpdated) {
		jobUpdates <- update
	})

	for i := 0; i < 4; i++ {
		require.NoError(t, completer.JobSetCompleted(int64(i), &jobstats.JobStatistics{}, time.Now()))
	}

	completer.Wait()

	updates := riverinternaltest.WaitOrTimeoutN(t, jobUpdates, 4)
	for i := 0; i < 4; i++ {
		require.Equal(t, dbsqlc.JobStateCompleted, updates[0].Job.State)
	}
}

func testCompleterWait(t *testing.T, constructor func(dbadapter.Adapter) JobCompleter) {
	t.Helper()

	resultCh := make(chan error)
	completeStartedCh := make(chan struct{})
	adapter := &dbadaptertest.TestAdapter{
		JobSetCompletedIfRunningFunc: func(ctx context.Context, job dbadapter.JobToComplete) (*dbsqlc.RiverJob, error) {
			completeStartedCh <- struct{}{}
			err := <-resultCh
			return nil, err
		},
	}

	completer := constructor(adapter)

	// launch 4 completions:
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			require.NoError(t, completer.JobSetCompleted(int64(i), &jobstats.JobStatistics{}, time.Now()))
		}()
		<-completeStartedCh // wait for func to actually start
	}

	// Give one completion a signal to finish, there should be 3 remaining in-flight:
	resultCh <- nil

	waitDone := make(chan struct{})
	go func() {
		completer.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatalf("expected Wait to block until all jobs are complete, but it returned when there should be three remaining")
	case <-time.After(100 * time.Millisecond):
	}

	// Get us down to one in-flight completion:
	resultCh <- nil
	resultCh <- nil

	select {
	case <-waitDone:
		t.Fatalf("expected Wait to block until all jobs are complete, but it returned when there should be one remaining")
	case <-time.After(100 * time.Millisecond):
	}

	// Finish the last one:
	resultCh <- nil

	select {
	case <-waitDone:
	case <-time.After(100 * time.Millisecond):
		t.Errorf("expected Wait to return after all jobs are complete")
	}
}
