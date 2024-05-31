package jobcompleter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

type partialExecutorMock struct {
	JobSetCompleteIfRunningManyCalled bool
	JobSetCompleteIfRunningManyFunc   func(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error)
	JobSetStateIfRunningCalled        bool
	JobSetStateIfRunningFunc          func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error)
	mu                                sync.Mutex
}

// NewPartialExecutorMock returns a new mock with all mock functions set to call
// down into the given real executor.
func NewPartialExecutorMock(exec riverdriver.Executor) *partialExecutorMock {
	return &partialExecutorMock{
		JobSetCompleteIfRunningManyFunc: exec.JobSetCompleteIfRunningMany,
		JobSetStateIfRunningFunc:        exec.JobSetStateIfRunning,
	}
}

func (m *partialExecutorMock) JobSetCompleteIfRunningMany(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
	m.setCalled(func() { m.JobSetCompleteIfRunningManyCalled = true })
	return m.JobSetCompleteIfRunningManyFunc(ctx, params)
}

func (m *partialExecutorMock) JobSetStateIfRunning(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
	m.setCalled(func() { m.JobSetStateIfRunningCalled = true })
	return m.JobSetStateIfRunningFunc(ctx, params)
}

func (m *partialExecutorMock) setCalled(setCalledFunc func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	setCalledFunc()
}

func TestInlineJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var attempt int
	expectedErr := errors.New("an error from the completer")
	execMock := &partialExecutorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			require.Equal(t, int64(1), params.ID)
			attempt++
			return nil, expectedErr
		},
	}

	subscribeCh := make(chan []CompleterJobUpdated, 10)
	t.Cleanup(riverinternaltest.DiscardContinuously(subscribeCh))

	completer := NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), execMock, subscribeCh)
	t.Cleanup(completer.Stop)
	completer.disableSleep = true

	err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now()))
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}

	require.True(t, execMock.JobSetStateIfRunningCalled)
	require.Equal(t, numRetries, attempt)
}

func TestInlineJobCompleter_Subscribe(t *testing.T) {
	t.Parallel()

	testCompleterSubscribe(t, func(exec PartialExecutor, subscribeCh SubscribeChan) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeCh)
	})
}

func TestInlineJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec PartialExecutor, subscribeChan SubscribeChan) JobCompleter {
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeChan)
	})
}

func TestAsyncJobCompleter_Complete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

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

	adapter := &partialExecutorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			inputCh <- jobInput{ctx: ctx, jobID: params.ID}
			err := <-resultCh
			return nil, err
		},
	}
	subscribeChan := make(chan []CompleterJobUpdated, 10)
	completer := newAsyncCompleterWithConcurrency(riverinternaltest.BaseServiceArchetype(t), adapter, 2, subscribeChan)
	completer.disableSleep = true
	require.NoError(t, completer.Start(ctx))
	t.Cleanup(completer.Stop)

	// launch 4 completions, only 2 can be inline due to the concurrency limit:
	for i := int64(0); i < 2; i++ {
		if err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now())); err != nil {
			t.Errorf("expected nil err, got %v", err)
		}
	}
	bgCompletionsStarted := make(chan struct{})
	go func() {
		for i := int64(2); i < 4; i++ {
			if err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(i, time.Now())); err != nil {
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

	testCompleterSubscribe(t, func(exec PartialExecutor, subscribeCh SubscribeChan) JobCompleter {
		return newAsyncCompleterWithConcurrency(riverinternaltest.BaseServiceArchetype(t), exec, 4, subscribeCh)
	})
}

func TestAsyncJobCompleter_Wait(t *testing.T) {
	t.Parallel()

	testCompleterWait(t, func(exec PartialExecutor, subscribeCh SubscribeChan) JobCompleter {
		return newAsyncCompleterWithConcurrency(riverinternaltest.BaseServiceArchetype(t), exec, 4, subscribeCh)
	})
}

func testCompleterSubscribe(t *testing.T, constructor func(PartialExecutor, SubscribeChan) JobCompleter) {
	t.Helper()

	ctx := context.Background()

	exec := &partialExecutorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			return &rivertype.JobRow{
				State: rivertype.JobStateCompleted,
			}, nil
		},
	}

	subscribeChan := make(chan []CompleterJobUpdated, 10)
	completer := constructor(exec, subscribeChan)
	require.NoError(t, completer.Start(ctx))

	// Flatten the slice results from subscribeChan into jobUpdateChan:
	jobUpdateChan := make(chan CompleterJobUpdated, 10)
	go func() {
		defer close(jobUpdateChan)
		for update := range subscribeChan {
			for _, u := range update {
				jobUpdateChan <- u
			}
		}
	}()

	for i := 0; i < 4; i++ {
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now())))
	}

	completer.Stop() // closes subscribeChan

	updates := riverinternaltest.WaitOrTimeoutN(t, jobUpdateChan, 4)
	for i := 0; i < 4; i++ {
		require.Equal(t, rivertype.JobStateCompleted, updates[0].Job.State)
	}
	go completer.Stop()
	// drain all remaining jobs
	for range jobUpdateChan {
	}
}

func testCompleterWait(t *testing.T, constructor func(PartialExecutor, SubscribeChan) JobCompleter) {
	t.Helper()

	ctx := context.Background()

	resultCh := make(chan error)
	completeStartedCh := make(chan struct{})
	exec := &partialExecutorMock{
		JobSetStateIfRunningFunc: func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			completeStartedCh <- struct{}{}
			err := <-resultCh
			return nil, err
		},
	}
	subscribeCh := make(chan []CompleterJobUpdated, 100)

	completer := constructor(exec, subscribeCh)
	require.NoError(t, completer.Start(ctx))

	// launch 4 completions:
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(int64(i), time.Now())))
		}()
		<-completeStartedCh // wait for func to actually start
	}

	// Give one completion a signal to finish, there should be 3 remaining in-flight:
	resultCh <- nil

	waitDone := make(chan struct{})
	go func() {
		completer.Stop()
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

func TestAsyncCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) *AsyncCompleter {
		t.Helper()
		return NewAsyncCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeCh)
	},
		func(completer *AsyncCompleter) { completer.disableSleep = true },
		func(completer *AsyncCompleter, exec PartialExecutor) { completer.exec = exec })
}

func TestBatchCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) *BatchCompleter {
		t.Helper()
		return NewBatchCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeCh)
	},
		func(completer *BatchCompleter) { completer.disableSleep = true },
		func(completer *BatchCompleter, exec PartialExecutor) { completer.exec = exec })

	ctx := context.Background()

	type testBundle struct {
		exec        riverdriver.Executor
		subscribeCh <-chan []CompleterJobUpdated
	}

	setup := func(t *testing.T) (*BatchCompleter, *testBundle) {
		t.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, t))
			exec        = driver.GetExecutor()
			subscribeCh = make(chan []CompleterJobUpdated, 10)
			completer   = NewBatchCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeCh)
		)

		require.NoError(t, completer.Start(ctx))
		t.Cleanup(completer.Stop)

		riverinternaltest.WaitOrTimeout(t, completer.WaitStarted())

		return completer, &testBundle{
			exec:        exec,
			subscribeCh: subscribeCh,
		}
	}

	t.Run("CompletionsCompletedInSubBatches", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)
		completer.completionMaxSize = 10 // set to something artificially low

		jobUpdateChan := make(chan CompleterJobUpdated, 100)
		go func() {
			defer close(jobUpdateChan)
			for update := range bundle.subscribeCh {
				for _, u := range update {
					jobUpdateChan <- u
				}
			}
		}()

		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Wait for some jobs to come through, giving lots of opportunity for
		// the completer to have pooled some completions and being forced to
		// work them in sub-batches with our diminished sub-batch size.
		riverinternaltest.WaitOrTimeoutN(t, jobUpdateChan, 100)

		stopInsertion()
		go completer.Stop()
		// drain all remaining jobs
		for range jobUpdateChan {
		}
	})

	t.Run("BacklogWaitAndContinue", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)
		completer.maxBacklog = 10 // set to something artificially low

		jobUpdateChan := make(chan CompleterJobUpdated, 100)
		go func() {
			defer close(jobUpdateChan)
			for update := range bundle.subscribeCh {
				for _, u := range update {
					jobUpdateChan <- u
				}
			}
		}()

		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Wait for some jobs to come through. Waiting for these jobs to come
		// through will provide plenty of opportunity for the completer to back
		// up with our small configured backlog.
		riverinternaltest.WaitOrTimeoutN(t, jobUpdateChan, 100)

		stopInsertion()
		go completer.Stop()
		// drain all remaining jobs
		for range jobUpdateChan {
		}
	})
}

func TestInlineCompleter(t *testing.T) {
	t.Parallel()

	testCompleter(t, func(t *testing.T, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) *InlineCompleter {
		t.Helper()
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(t), exec, subscribeCh)
	},
		func(completer *InlineCompleter) { completer.disableSleep = true },
		func(completer *InlineCompleter, exec PartialExecutor) { completer.exec = exec })
}

func testCompleter[TCompleter JobCompleter](
	t *testing.T,
	newCompleter func(t *testing.T, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) TCompleter,

	// These functions are here to help us inject test behavior that's not part
	// of the JobCompleter interface. We could alternatively define a second
	// interface like jobCompleterWithTestFacilities to expose the additional
	// functionality, although that's not particularly beautiful either.
	disableSleep func(completer TCompleter),
	setExec func(completer TCompleter, exec PartialExecutor),
) {
	t.Helper()

	ctx := context.Background()

	type testBundle struct {
		exec        riverdriver.Executor
		subscribeCh <-chan []CompleterJobUpdated
	}

	setup := func(t *testing.T) (TCompleter, *testBundle) {
		t.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, t))
			exec        = driver.GetExecutor()
			subscribeCh = make(chan []CompleterJobUpdated, 10)
			completer   = newCompleter(t, exec, subscribeCh)
		)

		require.NoError(t, completer.Start(ctx))
		t.Cleanup(completer.Stop)

		return completer, &testBundle{
			exec:        exec,
			subscribeCh: subscribeCh,
		}
	}

	requireJob := func(t *testing.T, exec riverdriver.Executor, jobID int64) *rivertype.JobRow {
		t.Helper()

		job, err := exec.JobGetByID(ctx, jobID)
		require.NoError(t, err)
		return job
	}

	requireState := func(t *testing.T, exec riverdriver.Executor, jobID int64, state rivertype.JobState) *rivertype.JobRow {
		t.Helper()

		job := requireJob(t, exec, jobID)
		require.Equal(t, state, job.State)
		return job
	}

	t.Run("CompletesJobs", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		var (
			finalizedAt1 = time.Now().UTC().Add(-1 * time.Minute)
			finalizedAt2 = time.Now().UTC().Add(-2 * time.Minute)
			finalizedAt3 = time.Now().UTC().Add(-3 * time.Minute)

			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		)

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job1.ID, finalizedAt1)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job2.ID, finalizedAt2)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job3.ID, finalizedAt3)))

		completer.Stop()

		job1Updated := requireState(t, bundle.exec, job1.ID, rivertype.JobStateCompleted)
		job2Updated := requireState(t, bundle.exec, job2.ID, rivertype.JobStateCompleted)
		job3Updated := requireState(t, bundle.exec, job3.ID, rivertype.JobStateCompleted)

		require.WithinDuration(t, finalizedAt1, *job1Updated.FinalizedAt, time.Microsecond)
		require.WithinDuration(t, finalizedAt2, *job2Updated.FinalizedAt, time.Microsecond)
		require.WithinDuration(t, finalizedAt3, *job3Updated.FinalizedAt, time.Microsecond)
	})

	// Some completers like BatchCompleter have special logic for when they're
	// handling enormous numbers of jobs, so make sure we're covered for cases
	// like that.
	t.Run("CompletesManyJobs", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		const (
			kind    = "many_jobs_kind"
			numJobs = 4_400
		)

		var (
			insertParams = make([]*riverdriver.JobInsertFastParams, numJobs)
			stats        = make([]jobstats.JobStatistics, numJobs)
		)
		for i := 0; i < numJobs; i++ {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{}`),
				Kind:        kind,
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateRunning,
			}
		}

		_, err := bundle.exec.JobInsertFastMany(ctx, insertParams)
		require.NoError(t, err)

		jobs, err := bundle.exec.JobGetByKindMany(ctx, []string{kind})
		require.NoError(t, err)

		t.Cleanup(riverinternaltest.DiscardContinuously(bundle.subscribeCh))

		for i := range jobs {
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &stats[i], riverdriver.JobSetStateCompleted(jobs[i].ID, time.Now())))
		}

		completer.Stop()

		updatedJobs, err := bundle.exec.JobGetByKindMany(ctx, []string{kind})
		require.NoError(t, err)
		for i := range updatedJobs {
			require.Equal(t, rivertype.JobStateCompleted, updatedJobs[i].State)
		}
	})

	// The minimum time to wait go guarantee a batch of completions from the
	// batch completer. Unless jobs are above a threshold it'll wait a number of
	// ticks before starting completions. 5 ticks @ 50 milliseconds.
	const minBatchCompleterPassDuration = 5 * 50 * time.Millisecond

	t.Run("FastContinuousCompletion", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		t.Cleanup(riverinternaltest.DiscardContinuously(bundle.subscribeCh))
		stopInsertion := doContinuousInsertion(ctx, t, completer, bundle.exec)

		// Give some time for some jobs to be inserted, and a guaranteed pass by
		// the batch completer.
		time.Sleep(minBatchCompleterPassDuration)

		// Signal to stop insertion and wait for the goroutine to return.
		numInserted := stopInsertion()

		require.Greater(t, numInserted, 0)

		numCompleted, err := bundle.exec.JobCountByState(ctx, rivertype.JobStateCompleted)
		require.NoError(t, err)
		t.Logf("Counted %d jobs as completed", numCompleted)
		require.Greater(t, numCompleted, 0)
	})

	t.Run("SlowerContinuousCompletion", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		// Number here is chosen to be a little higher than the batch
		// completer's tick interval so we can make sure that the right thing
		// happens even on an empty tick.
		stopInsertion := doContinuousInsertionInterval(ctx, t, completer, bundle.exec, 30*time.Millisecond)

		// Give some time for some jobs to be inserted, and a guaranteed pass by
		// the batch completer.
		time.Sleep(minBatchCompleterPassDuration)

		// Signal to stop insertion and wait for the goroutine to return.
		numInserted := stopInsertion()

		require.Greater(t, numInserted, 0)

		numCompleted, err := bundle.exec.JobCountByState(ctx, rivertype.JobStateCompleted)
		require.NoError(t, err)
		t.Logf("Counted %d jobs as completed", numCompleted)
		require.Greater(t, numCompleted, 0)
	})

	t.Run("AllJobStates", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job5 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job6 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			job7 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		)

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCancelled(job1.ID, time.Now(), []byte("{}"))))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job2.ID, time.Now())))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateDiscarded(job3.ID, time.Now(), []byte("{}"))))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateErrorAvailable(job4.ID, time.Now(), []byte("{}"))))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateErrorRetryable(job5.ID, time.Now(), []byte("{}"))))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateSnoozed(job6.ID, time.Now(), 10)))
		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateSnoozedAvailable(job7.ID, time.Now(), 10)))

		completer.Stop()

		requireState(t, bundle.exec, job1.ID, rivertype.JobStateCancelled)
		requireState(t, bundle.exec, job2.ID, rivertype.JobStateCompleted)
		requireState(t, bundle.exec, job3.ID, rivertype.JobStateDiscarded)
		requireState(t, bundle.exec, job4.ID, rivertype.JobStateAvailable)
		requireState(t, bundle.exec, job5.ID, rivertype.JobStateRetryable)
		requireState(t, bundle.exec, job6.ID, rivertype.JobStateScheduled)
		requireState(t, bundle.exec, job7.ID, rivertype.JobStateAvailable)
	})

	t.Run("Subscription", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now())))

		completer.Stop()

		jobUpdate := riverinternaltest.WaitOrTimeout(t, bundle.subscribeCh)
		require.Len(t, jobUpdate, 1)
		require.Equal(t, rivertype.JobStateCompleted, jobUpdate[0].Job.State)
	})

	t.Run("MultipleCycles", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		{
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now())))

			completer.Stop()

			requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
		}

		// Completer closes the subscribe channel on stop, so we need to reset it between runs.
		completer.ResetSubscribeChan(make(SubscribeChan, 10))

		{
			require.NoError(t, completer.Start(ctx))

			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now())))

			completer.Stop()

			requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
		}
	})

	t.Run("CompletionFailure", func(t *testing.T) {
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		var numCalls int
		maybeError := func() error {
			numCalls++
			switch numCalls {
			case 1:
				fallthrough
			case 2:
				return fmt.Errorf("error from executor %d", numCalls)
			}
			return nil
		}

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetCompleteIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
			if err := maybeError(); err != nil {
				return nil, err
			}
			return bundle.exec.JobSetCompleteIfRunningMany(ctx, params)
		}
		execMock.JobSetStateIfRunningFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			if err := maybeError(); err != nil {
				return nil, err
			}
			return bundle.exec.JobSetStateIfRunning(ctx, params)
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now())))

		completer.Stop()

		// Make sure our mocks were really called. The specific function called
		// will depend on the completer under test, so okay as long as one or
		// the other was.
		require.True(t, execMock.JobSetCompleteIfRunningManyCalled || execMock.JobSetStateIfRunningCalled)

		// Job still managed to complete despite the errors.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateCompleted)
	})

	t.Run("CompletionImmediateFailureOnContextCanceled", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetCompleteIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
			return nil, context.Canceled
		}
		execMock.JobSetStateIfRunningFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			return nil, context.Canceled
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now()))

		// The error returned will be nil for asynchronous completers, but
		// returned immediately for synchronous ones.
		require.True(t, err == nil || errors.Is(err, context.Canceled))

		completer.Stop()

		// Make sure our mocks were really called. The specific function called
		// will depend on the completer under test, so okay as long as one or
		// the other was.
		require.True(t, execMock.JobSetCompleteIfRunningManyCalled || execMock.JobSetStateIfRunningCalled)

		// Job is still running because the completer is forced to give up
		// immediately on certain types of errors like where a pool is closed.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateRunning)
	})

	t.Run("CompletionImmediateFailureOnErrClosedPool", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		completer, bundle := setup(t)

		// The completers will do an exponential backoff sleep while retrying.
		// Make sure to disable it for this test case so the tests stay fast.
		disableSleep(completer)

		execMock := NewPartialExecutorMock(bundle.exec)
		execMock.JobSetCompleteIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
			return nil, puddle.ErrClosedPool
		}
		execMock.JobSetStateIfRunningFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
			return nil, puddle.ErrClosedPool
		}
		setExec(completer, execMock)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now()))

		// The error returned will be nil for asynchronous completers, but
		// returned immediately for synchronous ones.
		require.True(t, err == nil || errors.Is(err, puddle.ErrClosedPool))

		completer.Stop()

		// Make sure our mocks were really called. The specific function called
		// will depend on the completer under test, so okay as long as one or
		// the other was.
		require.True(t, execMock.JobSetCompleteIfRunningManyCalled || execMock.JobSetStateIfRunningCalled)

		// Job is still running because the completer is forced to give up
		// immediately on certain types of errors like where a pool is closed.
		requireState(t, bundle.exec, job.ID, rivertype.JobStateRunning)
	})

	// The batch completer supports an interface that lets caller wait for it to
	// start. Make sure this works as expected.
	t.Run("WithStartedWaitsForStarted", func(t *testing.T) {
		t.Parallel()

		completer, _ := setup(t)

		var completerInterface JobCompleter = completer
		if withWait, ok := completerInterface.(withWaitStarted); ok {
			riverinternaltest.WaitOrTimeout(t, withWait.WaitStarted())
		}
	})
}

func BenchmarkAsyncCompleter_Concurrency10(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return newAsyncCompleterWithConcurrency(riverinternaltest.BaseServiceArchetype(b), exec, 10, subscribeCh)
	})
}

func BenchmarkAsyncCompleter_Concurrency100(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return newAsyncCompleterWithConcurrency(riverinternaltest.BaseServiceArchetype(b), exec, 100, subscribeCh)
	})
}

func BenchmarkBatchCompleter(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return NewBatchCompleter(riverinternaltest.BaseServiceArchetype(b), exec, subscribeCh)
	})
}

func BenchmarkInlineCompleter(b *testing.B) {
	benchmarkCompleter(b, func(b *testing.B, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) JobCompleter {
		b.Helper()
		return NewInlineCompleter(riverinternaltest.BaseServiceArchetype(b), exec, subscribeCh)
	})
}

func benchmarkCompleter(
	b *testing.B,
	newCompleter func(b *testing.B, exec riverdriver.Executor, subscribeCh chan<- []CompleterJobUpdated) JobCompleter,
) {
	b.Helper()

	ctx := context.Background()

	type testBundle struct {
		exec  riverdriver.Executor
		jobs  []*rivertype.JobRow
		stats []jobstats.JobStatistics
	}

	setup := func(b *testing.B) (JobCompleter, *testBundle) {
		b.Helper()

		var (
			driver      = riverpgxv5.New(riverinternaltest.TestDB(ctx, b))
			exec        = driver.GetExecutor()
			subscribeCh = make(chan []CompleterJobUpdated, 100)
			completer   = newCompleter(b, exec, subscribeCh)
		)

		b.Cleanup(riverinternaltest.DiscardContinuously(subscribeCh))

		require.NoError(b, completer.Start(ctx))
		b.Cleanup(completer.Stop)

		if withWait, ok := completer.(withWaitStarted); ok {
			riverinternaltest.WaitOrTimeout(b, withWait.WaitStarted())
		}

		insertParams := make([]*riverdriver.JobInsertFastParams, b.N)
		for i := 0; i < b.N; i++ {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(`{}`),
				Kind:        "benchmark_kind",
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateRunning,
			}
		}

		_, err := exec.JobInsertFastMany(ctx, insertParams)
		require.NoError(b, err)

		jobs, err := exec.JobGetByKindMany(ctx, []string{"benchmark_kind"})
		require.NoError(b, err)

		return completer, &testBundle{
			exec:  exec,
			jobs:  jobs,
			stats: make([]jobstats.JobStatistics, b.N),
		}
	}

	b.Run("Completion", func(b *testing.B) {
		completer, bundle := setup(b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCompleted(bundle.jobs[i].ID, time.Now()))
			require.NoError(b, err)
		}

		completer.Stop()
	})

	b.Run("RotatingStates", func(b *testing.B) {
		completer, bundle := setup(b)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			switch i % 7 {
			case 0:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCancelled(bundle.jobs[i].ID, time.Now(), []byte("{}")))
				require.NoError(b, err)

			case 1:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateCompleted(bundle.jobs[i].ID, time.Now()))
				require.NoError(b, err)

			case 2:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateDiscarded(bundle.jobs[i].ID, time.Now(), []byte("{}")))
				require.NoError(b, err)

			case 3:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateErrorAvailable(bundle.jobs[i].ID, time.Now(), []byte("{}")))
				require.NoError(b, err)

			case 4:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateErrorRetryable(bundle.jobs[i].ID, time.Now(), []byte("{}")))
				require.NoError(b, err)

			case 5:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateSnoozed(bundle.jobs[i].ID, time.Now(), 10))
				require.NoError(b, err)

			case 6:
				err := completer.JobSetStateIfRunning(ctx, &bundle.stats[i], riverdriver.JobSetStateSnoozedAvailable(bundle.jobs[i].ID, time.Now(), 10))
				require.NoError(b, err)

			default:
				panic("unexpected modulo result (did you update cases without changing the modulo divider or vice versa?")
			}
		}

		completer.Stop()
	})
}

// Performs continuous job insertion from a background goroutine. Returns a
// function that should be invoked to stop insertion, which will block until
// insertion stops, then return the total number of jobs that were inserted.
func doContinuousInsertion(ctx context.Context, t *testing.T, completer JobCompleter, exec riverdriver.Executor) func() int {
	t.Helper()

	return doContinuousInsertionInterval(ctx, t, completer, exec, 1*time.Millisecond)
}

func doContinuousInsertionInterval(ctx context.Context, t *testing.T, completer JobCompleter, exec riverdriver.Executor, insertInterval time.Duration) func() int {
	t.Helper()

	var (
		insertionStopped = make(chan struct{})
		numInserted      atomic.Int64
		stopInsertion    = make(chan struct{})
		ticker           = time.NewTicker(insertInterval)
	)
	go func() {
		defer close(insertionStopped)

		defer ticker.Stop()

		defer func() {
			t.Logf("Inserted %d jobs", numInserted.Load())
		}()

		for {
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
			require.NoError(t, completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(job.ID, time.Now())))
			numInserted.Add(1)

			select {
			case <-stopInsertion:
				return
			case <-ticker.C:
			}
		}
	}()

	return func() int {
		close(stopInsertion)
		<-insertionStopped
		return int(numInserted.Load())
	}
}
