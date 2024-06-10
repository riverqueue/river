package river

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

const testClientID = "test-client-id"

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

	config := newTestConfig(t, nil)
	dbDriver := riverpgxv5.New(dbPool)
	exec := dbDriver.GetExecutor()
	listener := dbDriver.GetListener()

	subscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 100)
	t.Cleanup(riverinternaltest.DiscardContinuously(subscribeCh))

	completer := jobcompleter.NewInlineCompleter(archetype, exec, subscribeCh)
	t.Cleanup(completer.Stop)

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
	notifier := notifier.New(archetype, listener, ignoreNotifierStatusUpdates)

	ignoreStatusUpdates := func(queue string, status componentstatus.Status) {}

	producer := newProducer(archetype, exec, &producerConfig{
		ClientID:     testClientID,
		Completer:    completer,
		ErrorHandler: newTestErrorHandler(),
		// Fetch constantly to more aggressively trigger the potential data race:
		FetchCooldown:       time.Millisecond,
		FetchPollInterval:   time.Millisecond,
		JobTimeout:          JobTimeoutDefault,
		MaxWorkers:          1000,
		Notifier:            notifier,
		Queue:               rivercommon.QueueDefault,
		QueuePollInterval:   queuePollIntervalDefault,
		QueueReportInterval: queueReportIntervalDefault,
		RetryPolicy:         &DefaultClientRetryPolicy{},
		SchedulerInterval:   maintenance.JobSchedulerIntervalDefault,
		StatusFunc:          ignoreStatusUpdates,
		Workers:             workers,
	})

	params := make([]*riverdriver.JobInsertFastParams, maxJobCount)
	for i := range params {
		insertParams, _, err := insertParamsFromConfigArgsAndOptions(config, WithJobNumArgs{JobNum: i}, nil)
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
			if numActiveJobs > int32(producer.config.MaxWorkers) {
				panic(fmt.Sprintf("producer exceeded MaxWorkerCount=%d, actual count=%d", producer.config.MaxWorkers, numActiveJobs))
			}
		}
	}()

	_, err := exec.JobInsertFastMany(ctx, params)
	require.NoError(err)

	require.NoError(producer.StartWorkContext(ctx, ctx))
	t.Cleanup(producer.Stop)

	select {
	case <-lastJobRun:
		t.Logf("Last job reported in; cancelling context")
		cancel()
	case <-ctx.Done():
		t.Error("timed out waiting for last job to run")
	}
}

func TestProducer_PollOnly(t *testing.T) {
	t.Parallel()

	testProducer(t, func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated) {
		t.Helper()

		var (
			archetype = riverinternaltest.BaseServiceArchetype(t)
			driver    = riverpgxv5.New(nil)
			tx        = riverinternaltest.TestTx(ctx, t)
		)

		// Wrap with a shared transaction because the producer fetching jobs may
		// conflict with jobs being inserted in test cases.
		tx = sharedtx.NewSharedTx(tx)

		var (
			exec       = driver.UnwrapExecutor(tx)
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
		)

		completer := jobcompleter.NewInlineCompleter(archetype, exec, jobUpdates)
		{
			require.NoError(t, completer.Start(ctx))
			t.Cleanup(completer.Stop)
		}

		return newProducer(archetype, exec, &producerConfig{
			ClientID:            testClientID,
			Completer:           completer,
			ErrorHandler:        newTestErrorHandler(),
			FetchCooldown:       FetchCooldownDefault,
			FetchPollInterval:   50 * time.Millisecond, // more aggressive than normal because we have no notifier
			JobTimeout:          JobTimeoutDefault,
			MaxWorkers:          1_000,
			Notifier:            nil, // no notifier
			Queue:               rivercommon.QueueDefault,
			QueuePollInterval:   queuePollIntervalDefault,
			QueueReportInterval: queueReportIntervalDefault,
			RetryPolicy:         &DefaultClientRetryPolicy{},
			SchedulerInterval:   riverinternaltest.SchedulerShortInterval,
			StatusFunc:          func(queue string, status componentstatus.Status) {},
			Workers:             NewWorkers(),
		}), jobUpdates
	})
}

func TestProducer_WithNotifier(t *testing.T) {
	t.Parallel()

	testProducer(t, func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated) {
		t.Helper()

		var (
			archetype  = riverinternaltest.BaseServiceArchetype(t)
			dbPool     = riverinternaltest.TestDB(ctx, t)
			driver     = riverpgxv5.New(dbPool)
			exec       = driver.GetExecutor()
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
			listener   = driver.GetListener()
		)

		completer := jobcompleter.NewInlineCompleter(archetype, exec, jobUpdates)
		{
			require.NoError(t, completer.Start(ctx))
			t.Cleanup(completer.Stop)
		}

		notifier := notifier.New(archetype, listener, func(componentstatus.Status) {})
		{
			require.NoError(t, notifier.Start(ctx))
			t.Cleanup(notifier.Stop)
		}

		return newProducer(archetype, exec, &producerConfig{
			ClientID:            testClientID,
			Completer:           completer,
			ErrorHandler:        newTestErrorHandler(),
			FetchCooldown:       FetchCooldownDefault,
			FetchPollInterval:   50 * time.Millisecond, // more aggressive than normal so in case we miss the event, tests still pass quickly
			JobTimeout:          JobTimeoutDefault,
			MaxWorkers:          1_000,
			Notifier:            notifier,
			Queue:               rivercommon.QueueDefault,
			QueuePollInterval:   queuePollIntervalDefault,
			QueueReportInterval: queueReportIntervalDefault,
			RetryPolicy:         &DefaultClientRetryPolicy{},
			SchedulerInterval:   riverinternaltest.SchedulerShortInterval,
			StatusFunc:          func(queue string, status componentstatus.Status) {},
			Workers:             NewWorkers(),
		}), jobUpdates
	})
}

func testProducer(t *testing.T, makeProducer func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated)) {
	t.Helper()

	ctx := context.Background()

	type testBundle struct {
		completer  jobcompleter.JobCompleter
		config     *Config
		exec       riverdriver.Executor
		jobUpdates chan jobcompleter.CompleterJobUpdated
		workers    *Workers
	}

	setup := func(t *testing.T) (*producer, *testBundle) {
		t.Helper()

		producer, jobUpdates := makeProducer(ctx, t)
		producer.testSignals.Init()
		config := newTestConfig(t, nil)

		jobUpdatesFlattened := make(chan jobcompleter.CompleterJobUpdated, 10)
		go func() {
			for updates := range jobUpdates {
				for _, update := range updates {
					jobUpdatesFlattened <- update
				}
			}
		}()

		return producer, &testBundle{
			completer:  producer.completer,
			config:     config,
			exec:       producer.exec,
			jobUpdates: jobUpdatesFlattened,
			workers:    producer.workers,
		}
	}

	mustInsert := func(ctx context.Context, t *testing.T, bundle *testBundle, args JobArgs) {
		t.Helper()

		insertParams, _, err := insertParamsFromConfigArgsAndOptions(bundle.config, args, nil)
		require.NoError(t, err)

		_, err = bundle.exec.JobInsertFast(ctx, insertParams)
		require.NoError(t, err)
	}

	startProducer := func(t *testing.T, fetchCtx, workCtx context.Context, producer *producer) {
		t.Helper()

		require.NoError(t, producer.StartWorkContext(fetchCtx, workCtx))
		t.Cleanup(producer.Stop)
	}

	t.Run("NoOp", func(t *testing.T) {
		t.Parallel()

		producer, _ := setup(t)

		startProducer(t, ctx, ctx, producer)
	})

	t.Run("SimpleJob", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		AddWorker(bundle.workers, &noOpWorker{})

		mustInsert(ctx, t, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		update := riverinternaltest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
	})

	t.Run("RegistersQueueStatus", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueueReportInterval = 50 * time.Millisecond

		now := time.Now().UTC()
		startProducer(t, ctx, ctx, producer)

		queue, err := bundle.exec.QueueGet(ctx, rivercommon.QueueDefault)
		require.NoError(t, err)
		require.WithinDuration(t, now, queue.CreatedAt, 2*time.Second)
		require.Equal(t, []byte("{}"), queue.Metadata)
		require.Equal(t, rivercommon.QueueDefault, queue.Name)
		require.WithinDuration(t, now, queue.UpdatedAt, 2*time.Second)
		require.Equal(t, queue.CreatedAt, queue.UpdatedAt)

		// Queue status should be updated quickly:
		producer.testSignals.ReportedQueueStatus.WaitOrTimeout()
	})

	t.Run("UnknownJobKind", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		AddWorker(bundle.workers, &noOpWorker{})

		mustInsert(ctx, t, bundle, &noOpArgs{})
		mustInsert(ctx, t, bundle, &callbackArgs{}) // not registered

		startProducer(t, ctx, ctx, producer)

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

	t.Run("CancelledWorkContextCancelsJob", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			producer.Logger.InfoContext(ctx, "Job started")
			<-ctx.Done()
			producer.Logger.InfoContext(ctx, "Job stopped after context cancelled")
			return ctx.Err()
		}))

		workCtx, workCancel := context.WithCancel(ctx)
		defer workCancel()

		mustInsert(ctx, t, bundle, &JobArgs{})

		startProducer(t, ctx, workCtx, producer)

		workCancel()

		update := riverinternaltest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateRetryable, update.Job.State)
	})

	t.Run("MaxWorkers", func(t *testing.T) {
		t.Parallel()

		const (
			maxWorkers = 5
			numJobs    = 10
		)

		producer, bundle := setup(t)
		producer.config.MaxWorkers = maxWorkers

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		unpauseWorkers := make(chan struct{})
		defer close(unpauseWorkers)

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			t.Logf("Job paused")
			<-unpauseWorkers
			t.Logf("Job unpaused")
			return ctx.Err()
		}))

		for i := 0; i < numJobs; i++ {
			mustInsert(ctx, t, bundle, &JobArgs{})
		}

		startProducer(t, ctx, ctx, producer)

		producer.testSignals.StartedExecutors.WaitOrTimeout()

		// Jobs are still paused as we fetch updated job states.
		updatedJobs, err := bundle.exec.JobGetByKindMany(ctx, []string{(&JobArgs{}).Kind()})
		require.NoError(t, err)

		jobStateCounts := make(map[rivertype.JobState]int)

		for _, updatedJob := range updatedJobs {
			jobStateCounts[updatedJob.State]++
		}

		require.Equal(t, maxWorkers, jobStateCounts[rivertype.JobStateRunning])
		require.Equal(t, numJobs-maxWorkers, jobStateCounts[rivertype.JobStateAvailable])

		require.Equal(t, maxWorkers, int(producer.numJobsActive.Load()))
		require.Zero(t, producer.maxJobsToFetch()) // zero because all slots are occupied
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		producer, _ := setup(t)
		producer.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		producer.testSignals = producerTestSignals{}      // deinit so channels don't fill

		startstoptest.Stress(ctx, t, producer)
	})

	t.Run("QueuePausedBeforeStart", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		AddWorker(bundle.workers, &noOpWorker{})

		testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{
			Name:     ptrutil.Ptr(rivercommon.QueueDefault),
			PausedAt: ptrutil.Ptr(time.Now()),
		})

		mustInsert(ctx, t, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		select {
		case update := <-bundle.jobUpdates:
			t.Fatalf("Unexpected job update: job=%+v stats=%+v", update.Job, update.JobStats)
		case <-time.After(500 * time.Millisecond):
		}
	})

	testQueuePause := func(t *testing.T, queueNameToPause string) {
		t.Helper()
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueuePollInterval = 50 * time.Millisecond
		AddWorker(bundle.workers, &noOpWorker{})

		mustInsert(ctx, t, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		// First job should be executed immediately while resumed:
		update := riverinternaltest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)

		// Pause the queue and wait for confirmation:
		require.NoError(t, bundle.exec.QueuePause(ctx, queueNameToPause))
		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, queueNameToPause, "pause")
		}
		producer.testSignals.Paused.WaitOrTimeout()

		// Job should not be executed while paused:
		mustInsert(ctx, t, bundle, &noOpArgs{})

		select {
		case update := <-bundle.jobUpdates:
			t.Fatalf("Unexpected job update: %+v", update)
		case <-time.After(500 * time.Millisecond):
		}

		// Resume the queue and wait for confirmation:
		require.NoError(t, bundle.exec.QueueResume(ctx, queueNameToPause))
		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, queueNameToPause, "resume")
		}
		producer.testSignals.Resumed.WaitOrTimeout()

		// Now the 2nd job should execute:
		update = riverinternaltest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
	}

	t.Run("QueuePausedDuringOperation", func(t *testing.T) {
		testQueuePause(t, rivercommon.QueueDefault)
	})

	t.Run("QueuePausedAndResumedDuringOperationUsing*", func(t *testing.T) {
		testQueuePause(t, rivercommon.AllQueuesString)
	})

	t.Run("QueueDeletedFromRiverQueueTableDuringOperation", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueuePollInterval = time.Second
		producer.config.QueueReportInterval = time.Second

		startProducer(t, ctx, ctx, producer)

		// Delete the queue by using a future-dated horizon:
		_, err := bundle.exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{
			Max:              100,
			UpdatedAtHorizon: time.Now().Add(time.Minute),
		})
		require.NoError(t, err)

		producer.testSignals.ReportedQueueStatus.WaitOrTimeout()
		if producer.config.Notifier == nil {
			producer.testSignals.PolledQueueConfig.WaitOrTimeout()
		}
	})
}

func emitQueueNotification(t *testing.T, ctx context.Context, exec riverdriver.Executor, queue, action string) {
	t.Helper()
	err := exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Topic: string(notifier.NotificationTopicControl),
		Payload: []string{
			fmt.Sprintf(`{"queue":"%s","action":"%s"}`, queue, action),
		},
	})
	require.NoError(t, err)
}
