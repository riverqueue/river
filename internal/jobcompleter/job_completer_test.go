package jobcompleter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

type executorMock struct {
	JobSetStateIfRunningCalled bool
	JobSetStateIfRunningFunc   func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error)
	mu                         sync.Mutex
}

func (m *executorMock) JobSetStateIfRunning(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
	m.mu.Lock()
	m.JobSetStateIfRunningCalled = true
	m.mu.Unlock()

	return m.JobSetStateIfRunningFunc(ctx, params)
}

func TestInlineJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	var attempt int
	expectedErr := errors.New("an error from the completer")
	adapter := &executorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			require.Equal(t, int64(1), params.ID)
			attempt++
			return nil, expectedErr
		},
	}

	completer := NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), adapter)
	t.Cleanup(completer.Wait)

	err := completer.JobSetStateIfRunning(&jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now()))
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}

	require.True(t, adapter.JobSetStateIfRunningCalled)
	require.Equal(t, numRetries, attempt)
}

func TestInlineJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(exec PartialExecutor) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), exec)
	})
}

func TestInlineJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec PartialExecutor) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), exec)
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

	adapter := &executorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			inputCh <- jobInput{ctx: ctx, jobID: params.ID}
			err := <-resultCh
			return nil, err
		},
	}
	completer := NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), adapter, 2)
	t.Cleanup(completer.Wait)

	// launch 4 completions, only 2 can be inline due to the concurrency limit:
	for i := int64(0); i < 2; i++ {
		if err := completer.JobSetStateIfRunning(&jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now())); err != nil {
			t.Errorf("expected nil err, got %v", err)
		}
	}
	bgCompletionsStarted := make(chan struct{})
	go func() {
		for i := int64(2); i < 4; i++ {
			if err := completer.JobSetStateIfRunning(&jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now())); err != nil {
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

	testCompleterSubscribe(t, func(exec PartialExecutor) JobCompleter {
		return NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), exec, 4)
	})
}

func TestAsyncJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec PartialExecutor) JobCompleter {
		return NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), exec, 4)
	})
}

func testCompleterSubscribe(t *testing.T, constructor func(PartialExecutor) JobCompleter) {
	t.Helper()

	exec := &executorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			return &rivertype.JobRow{
				State: rivertype.JobStateCompleted,
			}, nil
		},
	}

	completer := constructor(exec)

	jobUpdates := make(chan CompleterJobUpdated, 10)
	completer.Subscribe(func(update CompleterJobUpdated) {
		jobUpdates <- update
	})

	for i := 0; i < 4; i++ {
		require.NoError(t, completer.JobSetStateIfRunning(&jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now())))
	}

	completer.Wait()

	updates := riverinternaltest.WaitOrTimeoutN(t, jobUpdates, 4)
	for i := 0; i < 4; i++ {
		require.Equal(t, rivertype.JobStateCompleted, updates[0].Job.State)
	}
}

func testCompleterWait(t *testing.T, constructor func(PartialExecutor) JobCompleter) {
	t.Helper()

	resultCh := make(chan error)
	completeStartedCh := make(chan struct{})
	exec := &executorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			completeStartedCh <- struct{}{}
			err := <-resultCh
			return nil, err
		},
	}

	completer := constructor(exec)

	// launch 4 completions:
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			require.NoError(t, completer.JobSetStateIfRunning(&jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now())))
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
