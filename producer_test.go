package river

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func Test_Producer_CanSafelyCompleteJobsWhileFetchingNewOnes(t *testing.T) {
	// We have encountered previous data races with the list of active jobs on
	// Producer because we need to know the count of active jobs in order to
	// determine how many we can fetch for the next batch, while we're managing
	// the map of active jobs in a different goroutine.
	//
	// This test attempts to exercise that race condition so that the race
	// detector can tell us if we're protected against it.
	t.Parallel()

	ctx := context.Background()
	require := require.New(t)
	dbPool := riverinternaltest.TestDB(ctx, t)

	const maxJobCount = 10000
	// This doesn't strictly mean that there are no more jobs left to process,
	// merely that the final job we inserted is now being processed, which is
	// close enough for our purposes here.
	lastJobRun := make(chan struct{})

	archetype := riverinternaltest.BaseServiceArchetype(t)

	dbDriver := riverpgxv5.New(dbPool)
	exec := dbDriver.GetExecutor()
	listener := dbDriver.GetListener()

	completer := jobcompleter.NewInlineCompleter(archetype, exec)
	t.Cleanup(completer.Wait)

	type WithJobNumArgs struct {
		JobArgsReflectKind[WithJobNumArgs]
		JobNum int `json:"job_num"`
	}

	workers := NewWorkers()
	AddWorker(workers, WorkFunc(func(ctx context.Context, job *Job[WithJobNumArgs]) error {
		var jobArgs WithJobNumArgs
		require.NoError(json.Unmarshal(job.EncodedArgs, &jobArgs))

		if jobArgs.JobNum == maxJobCount-1 {
			select {
			case <-ctx.Done():
			case lastJobRun <- struct{}{}:
			}
		}
		return nil
	}))

	ignoreNotifierStatusUpdates := func(componentstatus.Status) {}
	notifier := notifier.New(archetype, listener, ignoreNotifierStatusUpdates, riverinternaltest.Logger(t))

	config := &producerConfig{
		ErrorHandler: newTestErrorHandler(),
		// Fetch constantly to more aggressively trigger the potential data race:
		FetchCooldown:     time.Millisecond,
		FetchPollInterval: time.Millisecond,
		JobTimeout:        JobTimeoutDefault,
		MaxWorkerCount:    1000,
		Notifier:          notifier,
		Queue:             rivercommon.QueueDefault,
		RetryPolicy:       &DefaultClientRetryPolicy{},
		SchedulerInterval: maintenance.SchedulerIntervalDefault,
		ClientID:          "fakeWorkerNameTODO",
		Workers:           workers,
	}
	producer, err := newProducer(archetype, exec, completer, config)
	require.NoError(err)

	params := make([]*riverdriver.JobInsertFastParams, maxJobCount)
	for i := range params {
		insertParams, _, err := insertParamsFromArgsAndOptions(WithJobNumArgs{JobNum: i}, nil)
		require.NoError(err)

		params[i] = insertParams
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	go func() {
		// The producer should never exceed its MaxWorkerCount. If it does, panic so
		// we can get a trace.
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			numActiveJobs := producer.numJobsActive.Load()
			if numActiveJobs > int32(config.MaxWorkerCount) {
				panic(fmt.Sprintf("producer exceeded MaxWorkerCount=%d, actual count=%d", config.MaxWorkerCount, numActiveJobs))
			}
		}
	}()

	_, err = exec.JobInsertFastMany(ctx, params)
	require.NoError(err)

	ignoreStatusUpdates := func(queue string, status componentstatus.Status) {}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		producer.Run(ctx, ctx, ignoreStatusUpdates)
		wg.Done()
	}()

	select {
	case <-lastJobRun:
		t.Logf("Last job reported in; cancelling context")
		cancel()
	case <-ctx.Done():
		t.Error("timed out waiting for last job to run")
	}
	wg.Wait()
}

func Test_Producer_Run(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		completer  jobcompleter.JobCompleter
		exec       riverdriver.Executor
		jobUpdates chan jobcompleter.CompleterJobUpdated
		workers    *Workers
	}

	setup := func(t *testing.T) (*producer, *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		driver := riverpgxv5.New(dbPool)
		exec := driver.GetExecutor()
		listener := driver.GetListener()

		archetype := riverinternaltest.BaseServiceArchetype(t)

		completer := jobcompleter.NewInlineCompleter(archetype, exec)

		jobUpdates := make(chan jobcompleter.CompleterJobUpdated, 10)
		completer.Subscribe(func(update jobcompleter.CompleterJobUpdated) {
			jobUpdates <- update
		})

		workers := NewWorkers()

		notifier := notifier.New(archetype, listener, func(componentstatus.Status) {}, riverinternaltest.Logger(t))

		config := &producerConfig{
			ErrorHandler:      newTestErrorHandler(),
			FetchCooldown:     FetchCooldownDefault,
			FetchPollInterval: 50 * time.Millisecond, // more aggressive than normal so in case we miss the event, tests still pass quickly
			JobTimeout:        JobTimeoutDefault,
			MaxWorkerCount:    1000,
			Notifier:          notifier,
			Queue:             rivercommon.QueueDefault,
			RetryPolicy:       &DefaultClientRetryPolicy{},
			SchedulerInterval: riverinternaltest.SchedulerShortInterval,
			ClientID:          "fakeWorkerNameTODO",
			Workers:           workers,
		}
		producer, err := newProducer(archetype, exec, completer, config)
		require.NoError(t, err)

		return producer, &testBundle{
			completer:  completer,
			exec:       exec,
			jobUpdates: jobUpdates,
			workers:    workers,
		}
	}

	mustInsert := func(ctx context.Context, t *testing.T, exec riverdriver.Executor, args JobArgs) {
		t.Helper()

		insertParams, _, err := insertParamsFromArgsAndOptions(args, nil)
		require.NoError(t, err)

		_, err = exec.JobInsertFast(ctx, insertParams)
		require.NoError(t, err)
	}

	t.Run("NoOp", func(t *testing.T) {
		t.Parallel()

		producer, _ := setup(t)

		fetchCtx, fetchCtxDone := context.WithCancel(ctx)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			producer.Run(fetchCtx, ctx, func(queue string, status componentstatus.Status) {})
			wg.Done()
		}()

		fetchCtxDone()
		wg.Wait()
	})

	t.Run("SimpleJob", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)

		fetchCtx, fetchCtxDone := context.WithCancel(ctx)

		AddWorker(bundle.workers, &noOpWorker{})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			producer.Run(fetchCtx, ctx, func(queue string, status componentstatus.Status) {})
			wg.Done()
		}()

		// LIFO, so guarantee run loop finishes and producer exits, even in the
		// event of a test failure.
		t.Cleanup(wg.Wait)
		t.Cleanup(fetchCtxDone)

		mustInsert(ctx, t, bundle.exec, &noOpArgs{})

		update := riverinternaltest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
	})

	t.Run("UnknownJobKind", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)

		fetchCtx, fetchCtxDone := context.WithCancel(ctx)

		AddWorker(bundle.workers, &noOpWorker{})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			producer.Run(fetchCtx, ctx, func(queue string, status componentstatus.Status) {})
			wg.Done()
		}()

		// LIFO, so guarantee run loop finishes and producer exits, even in the
		// event of a test failure.
		t.Cleanup(wg.Wait)
		t.Cleanup(fetchCtxDone)

		mustInsert(ctx, t, bundle.exec, &noOpArgs{})
		mustInsert(ctx, t, bundle.exec, &callbackArgs{}) // not registered

		updates := riverinternaltest.WaitOrTimeoutN(t, bundle.jobUpdates, 2)

		// Print updated jobs for debugging.
		for _, update := range updates {
			t.Logf("Job: %+v", update.Job)
		}

		// Order jobs come back in is not guaranteed, which is why this is
		// written somewhat strangely.
		findJob := func(kind string) *rivertype.JobRow {
			index := slices.IndexFunc(updates, func(u jobcompleter.CompleterJobUpdated) bool { return u.Job.Kind == kind })
			require.NotEqualf(t, -1, index, "Job update not found", "Job update not found for kind: %s", kind)
			return updates[index].Job
		}

		{
			job := findJob((&callbackArgs{}).Kind())
			require.Equal(t, rivertype.JobStateRetryable, job.State)
			require.Equal(t, (&UnknownJobKindError{Kind: (&callbackArgs{}).Kind()}).Error(), job.Errors[0].Error)
		}
		{
			job := findJob((&noOpArgs{}).Kind())
			require.Equal(t, rivertype.JobStateCompleted, job.State)
		}
	})
}
