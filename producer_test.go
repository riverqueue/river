package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

const testClientID = "test-client-id"

// beforeJobGetAvailablePilot calls a hook before delegating JobGetAvailable to
// the wrapped pilot.
type beforeJobGetAvailablePilot struct {
	riverpilot.Pilot

	beforeJobGetAvailableFunc func(params *riverdriver.JobGetAvailableParams)
}

func (p *beforeJobGetAvailablePilot) JobGetAvailable(
	ctx context.Context,
	exec riverdriver.Executor,
	state riverpilot.ProducerState,
	params *riverdriver.JobGetAvailableParams,
) ([]*rivertype.JobRow, error) {
	if p.beforeJobGetAvailableFunc != nil {
		p.beforeJobGetAvailableFunc(params)
	}

	return p.Pilot.JobGetAvailable(ctx, exec, state, params)
}

type blockingJobCompleter struct {
	jobcompleter.JobCompleter

	releaseCh chan struct{}
	startedCh chan struct{}
}

func (c *blockingJobCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	close(c.startedCh)
	<-c.releaseCh
	return nil
}

type jobSetStateIfRunningManyErrorPilot struct {
	riverpilot.Pilot

	err error
}

func (p *jobSetStateIfRunningManyErrorPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return nil, p.err
}

type jobSetStateIfRunningManyRecordingPilot struct {
	riverpilot.Pilot

	paramsCh chan *riverdriver.JobSetStateIfRunningManyParams
}

func (p *jobSetStateIfRunningManyRecordingPilot) JobSetStateIfRunningMany(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	p.paramsCh <- params
	return p.Pilot.JobSetStateIfRunningMany(ctx, exec, params)
}

type recordingJobCompleter struct {
	jobcompleter.JobCompleter

	paramsCh chan *riverdriver.JobSetStateIfRunningParams
}

func (c *recordingJobCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	c.paramsCh <- params
	return nil
}

func TestProducer_MetricEmitHook(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		archetype *baseservice.Archetype
		config    *Config
		exec      riverdriver.Executor
		metrics   chan *rivertype.HookMetricEmitParams
		producer  *producer
		queue     string
		schema    string
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var (
			archetype  = riversharedtest.BaseServiceArchetype(t)
			driver     = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			exec       = driver.GetExecutor()
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
			metrics    = make(chan *rivertype.HookMetricEmitParams, 10)
			pilot      = &riverpilot.StandardPilot{}
			queueName  = "test_producer_metric_hook"
			schema     = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		t.Cleanup(riverinternaltest.DiscardContinuously(jobUpdates))

		completer := jobcompleter.NewInlineCompleter(archetype, schema, exec, pilot, jobUpdates)
		t.Cleanup(completer.Stop)

		metricHook := HookMetricEmitFunc(func(ctx context.Context, params *rivertype.HookMetricEmitParams) {
			paramsCopy := *params
			metrics <- &paramsCopy
		})
		pluginLookup := pluginlookup.NewPluginLookup([]any{metricHook})

		producer := newProducer(archetype, exec, pilot, &producerConfig{
			ClientID:                     testClientID,
			Completer:                    completer,
			ErrorHandler:                 newTestErrorHandler(),
			FetchCooldown:                FetchCooldownDefault,
			FetchPollInterval:            50 * time.Millisecond,
			JobTimeout:                   JobTimeoutDefault,
			MaxWorkers:                   1_000,
			PluginLookupByJob:            pluginlookup.NewJobPluginLookup(nil),
			PluginLookupGlobal:           pluginLookup,
			Queue:                        queueName,
			QueuePollInterval:            queuePollIntervalDefault,
			QueueReportInterval:          queueReportIntervalDefault,
			RetryPolicy:                  &DefaultClientRetryPolicy{},
			SchedulerInterval:            riverinternaltest.SchedulerShortInterval,
			Schema:                       schema,
			StaleProducerRetentionPeriod: time.Minute,
			Workers:                      NewWorkers(),
		})

		return &testBundle{
			archetype: archetype,
			config:    newTestConfig(t, schema),
			exec:      exec,
			metrics:   metrics,
			producer:  producer,
			queue:     queueName,
			schema:    schema,
		}
	}

	t.Run("EmitsMetricsForFetch", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		scheduledAt := time.Now().UTC().Add(-time.Second)
		insertParams := make([]*riverdriver.JobInsertFastParams, 2)
		for i := range insertParams {
			params, err := insertParamsFromConfigArgsAndOptions(bundle.archetype, bundle.config, noOpArgs{}, &InsertOpts{
				Queue: bundle.queue,
			})
			require.NoError(t, err)
			params.ScheduledAt = &scheduledAt
			insertParams[i] = (*riverdriver.JobInsertFastParams)(params)
		}

		_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		fetchResultCh := make(chan producerFetchResult, 1)
		bundle.producer.dispatchWork(ctx, 2, fetchResultCh)

		fetchResult := riversharedtest.WaitOrTimeout(t, fetchResultCh)
		require.NoError(t, fetchResult.err)
		require.Len(t, fetchResult.jobs, 2)
		require.Len(t, bundle.producer.metricEmitHooks, 1)

		metricsByName := make(map[rivertype.MetricName]rivertype.Metric)
		for _, metric := range riversharedtest.WaitOrTimeoutN(t, bundle.metrics, 2) {
			metricsByName[metric.Metric.Name()] = metric.Metric
		}

		durationMetric, durationMetricFound := metricsByName[rivertype.MetricNameJobGetAvailableDuration].(*rivertype.JobGetAvailableDurationMetric)
		require.True(t, durationMetricFound)
		require.Equal(t, bundle.queue, durationMetric.Queue)
		require.GreaterOrEqual(t, durationMetric.Duration, time.Duration(0))

		countMetric, countMetricFound := metricsByName[rivertype.MetricNameJobGetAvailableCount].(*rivertype.JobGetAvailableCountMetric)
		require.True(t, countMetricFound)
		require.Equal(t, bundle.queue, countMetric.Queue)
		require.Equal(t, 2, countMetric.Count)
	})

	t.Run("SkipsMetricsWhenNoFetchAttempted", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		fetchResultCh := make(chan producerFetchResult, 1)
		bundle.producer.dispatchWork(ctx, 0, fetchResultCh)

		fetchResult := riversharedtest.WaitOrTimeout(t, fetchResultCh)
		require.NoError(t, fetchResult.err)
		require.Empty(t, fetchResult.jobs)
		require.Len(t, bundle.producer.metricEmitHooks, 1)
		require.Empty(t, bundle.metrics)
	})
}

func TestProducer_AbandonActiveJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec       riverdriver.Executor
		jobUpdates chan []jobcompleter.CompleterJobUpdated
		producer   *producer
		schema     string
	}

	setup := func(t *testing.T, pilot riverpilot.Pilot) *testBundle {
		t.Helper()

		var (
			archetype = riversharedtest.BaseServiceArchetype(t)
			driver    = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			exec      = driver.GetExecutor()
			schema    = riverdbtest.TestSchema(ctx, t, driver, nil)
		)
		if pilot == nil {
			pilot = &riverpilot.StandardPilot{}
		}

		jobUpdates := make(chan []jobcompleter.CompleterJobUpdated, 1)
		completer := jobcompleter.NewInlineCompleter(archetype, schema, exec, pilot, make(chan []jobcompleter.CompleterJobUpdated, 10))
		producer := newProducer(archetype, exec, pilot, &producerConfig{
			ClientID:                     testClientID,
			Completer:                    completer,
			ErrorHandler:                 newTestErrorHandler(),
			FetchCooldown:                FetchCooldownDefault,
			FetchPollInterval:            FetchPollIntervalDefault,
			JobTimeout:                   JobTimeoutDefault,
			JobUpdateCallback:            func(ctx context.Context, updates []jobcompleter.CompleterJobUpdated) { jobUpdates <- updates },
			MaxWorkers:                   10,
			PluginLookupByJob:            pluginlookup.NewJobPluginLookup(nil),
			PluginLookupGlobal:           pluginlookup.NewPluginLookup(nil),
			Queue:                        rivercommon.QueueDefault,
			QueuePollInterval:            queuePollIntervalDefault,
			QueueReportInterval:          queueReportIntervalDefault,
			RetryPolicy:                  &DefaultClientRetryPolicy{},
			SchedulerInterval:            riverinternaltest.SchedulerShortInterval,
			Schema:                       schema,
			StaleProducerRetentionPeriod: time.Minute,
			Workers:                      NewWorkers(),
		})

		return &testBundle{exec: exec, jobUpdates: jobUpdates, producer: producer, schema: schema}
	}

	t.Run("AbandonDoesNotWinAfterWorkerFinalizationStarts", func(t *testing.T) {
		t.Parallel()

		finalizationReleaseCh := make(chan struct{})
		releaseFinalization := sync.OnceFunc(func() { close(finalizationReleaseCh) })
		t.Cleanup(releaseFinalization)
		finalizationStartedCh := make(chan struct{})
		executor := &jobexecutor.JobExecutor{
			Completer: &blockingJobCompleter{
				releaseCh: finalizationReleaseCh,
				startedCh: finalizationStartedCh,
			},
		}
		activeJob := newProducerActiveJob(executor)

		finalizationResultCh := make(chan error, 1)
		go func() {
			finalizationResultCh <- executor.Completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now(), nil))
		}()

		riversharedtest.WaitOrTimeout(t, finalizationStartedCh)
		require.False(t, activeJob.tryAbandon())

		releaseFinalization()
		require.NoError(t, riversharedtest.WaitOrTimeout(t, finalizationResultCh))
		require.False(t, activeJob.isAbandoned())
	})

	t.Run("AbandonWinsBeforeWorkerFinalizationStarts", func(t *testing.T) {
		t.Parallel()

		paramsCh := make(chan *riverdriver.JobSetStateIfRunningParams, 1)
		executor := &jobexecutor.JobExecutor{
			Completer: &recordingJobCompleter{paramsCh: paramsCh},
		}
		activeJob := newProducerActiveJob(executor)

		require.True(t, activeJob.tryAbandon())
		require.NoError(t, executor.Completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now(), nil)))
		require.Empty(t, paramsCh)
		require.False(t, activeJob.tryAbandon())
		require.False(t, executor.ShouldReportResultFunc())
	})

	t.Run("DatabaseErrorAbandonsExecutor", func(t *testing.T) {
		t.Parallel()

		pilotErr := errors.New("database error")
		bundle := setup(t, &jobSetStateIfRunningManyErrorPilot{Pilot: &riverpilot.StandardPilot{}, err: pilotErr})

		runningState := rivertype.JobStateRunning
		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			Schema:      bundle.schema,
			State:       &runningState,
		})
		executor := &jobexecutor.JobExecutor{JobRow: job}
		bundle.producer.addActiveJob(job.ID, executor)
		activeJob := bundle.producer.activeJobs[job.ID]

		bundle.producer.abandon()
		bundle.producer.executorShutdownLoop(ctx)

		require.Empty(t, bundle.producer.activeJobs)
		require.Zero(t, bundle.producer.numJobsActive.Load())
		require.Empty(t, bundle.jobUpdates)
		require.True(t, activeJob.isAbandoned())
		require.False(t, activeJob.tryAbandon())

		jobAfter, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: bundle.schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
		require.Empty(t, jobAfter.Errors)
	})

	t.Run("UpdatesJobsInSingleBatchAndEmitsEvents", func(t *testing.T) {
		t.Parallel()

		recordingPilot := &jobSetStateIfRunningManyRecordingPilot{
			Pilot:    &riverpilot.StandardPilot{},
			paramsCh: make(chan *riverdriver.JobSetStateIfRunningManyParams, 10),
		}
		bundle := setup(t, recordingPilot)

		availableState := rivertype.JobStateAvailable
		runningState := rivertype.JobStateRunning
		alreadyAvailableJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			Schema:      bundle.schema,
			State:       &availableState,
		})
		retryableJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(1),
			MaxAttempts: ptrutil.Ptr(3),
			Schema:      bundle.schema,
			State:       &runningState,
		})
		discardedJob := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Attempt:     ptrutil.Ptr(3),
			MaxAttempts: ptrutil.Ptr(3),
			Schema:      bundle.schema,
			State:       &runningState,
		})

		bundle.producer.addActiveJob(alreadyAvailableJob.ID, &jobexecutor.JobExecutor{JobRow: alreadyAvailableJob})
		bundle.producer.addActiveJob(retryableJob.ID, &jobexecutor.JobExecutor{JobRow: retryableJob})
		bundle.producer.addActiveJob(discardedJob.ID, &jobexecutor.JobExecutor{JobRow: discardedJob})

		bundle.producer.abandon()
		bundle.producer.executorShutdownLoop(ctx)

		require.Empty(t, bundle.producer.activeJobs)
		require.Zero(t, bundle.producer.numJobsActive.Load())
		batchParams := riversharedtest.WaitOrTimeout(t, recordingPilot.paramsCh)
		require.ElementsMatch(t, []int64{alreadyAvailableJob.ID, retryableJob.ID, discardedJob.ID}, batchParams.ID)
		require.Empty(t, recordingPilot.paramsCh)

		updates := riversharedtest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Len(t, updates, 2)
		require.ElementsMatch(t, []int64{retryableJob.ID, discardedJob.ID}, []int64{updates[0].Job.ID, updates[1].Job.ID})
		for _, update := range updates {
			require.NotNil(t, update.JobStats)
			require.Equal(t, riverdriver.JobSetStateReasonFailed, update.Reason)
		}
		require.Empty(t, bundle.jobUpdates)

		alreadyAvailableJobAfter, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: alreadyAvailableJob.ID, Schema: bundle.schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, alreadyAvailableJobAfter.State)
		require.Empty(t, alreadyAvailableJobAfter.Errors)

		retryableJobAfter, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: retryableJob.ID, Schema: bundle.schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, retryableJobAfter.State)
		require.Len(t, retryableJobAfter.Errors, 1)
		require.Equal(t, producerJobAbandonedError, retryableJobAfter.Errors[0].Error)
		require.Equal(t, retryableJob.Attempt, retryableJobAfter.Errors[0].Attempt)
		require.Empty(t, retryableJobAfter.Errors[0].Trace)
		require.Nil(t, retryableJobAfter.FinalizedAt)

		discardedJobAfter, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: discardedJob.ID, Schema: bundle.schema})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateDiscarded, discardedJobAfter.State)
		require.Len(t, discardedJobAfter.Errors, 1)
		require.Equal(t, producerJobAbandonedError, discardedJobAfter.Errors[0].Error)
		require.Equal(t, discardedJob.Attempt, discardedJobAfter.Errors[0].Attempt)
		require.Empty(t, discardedJobAfter.Errors[0].Trace)
		require.NotNil(t, discardedJobAfter.FinalizedAt)
	})
}

func TestProducer_PollOnly(t *testing.T) {
	t.Parallel()

	testProducer(t, func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated) {
		t.Helper()

		var (
			archetype = riversharedtest.BaseServiceArchetype(t)
			driver    = riverpgxv5.New(nil)
			pilot     = &riverpilot.StandardPilot{}
			queueName = fmt.Sprintf("test-producer-poll-only-%05d", randutil.IntBetween(1, 100_000))
			tx        = riverdbtest.TestTxPgx(ctx, t)
		)

		// Wrap with a shared transaction because the producer fetching jobs may
		// conflict with jobs being inserted in test cases.
		tx = sharedtx.NewSharedTx(tx)

		var (
			exec       = driver.UnwrapExecutor(tx)
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
		)

		completer := jobcompleter.NewInlineCompleter(archetype, "", exec, &riverpilot.StandardPilot{}, jobUpdates)
		{
			require.NoError(t, completer.Start(ctx))
			t.Cleanup(completer.Stop)
		}

		return newProducer(archetype, exec, pilot, &producerConfig{
			ClientID:                     testClientID,
			Completer:                    completer,
			ErrorHandler:                 newTestErrorHandler(),
			FetchCooldown:                FetchCooldownDefault,
			FetchPollInterval:            50 * time.Millisecond, // more aggressive than normal because we have no notifier
			PluginLookupByJob:            pluginlookup.NewJobPluginLookup(nil),
			PluginLookupGlobal:           pluginlookup.NewPluginLookup(nil),
			JobTimeout:                   JobTimeoutDefault,
			MaxWorkers:                   1_000,
			Notifier:                     nil, // no notifier
			Queue:                        queueName,
			QueuePollInterval:            queuePollIntervalDefault,
			QueueReportInterval:          queueReportIntervalDefault,
			RetryPolicy:                  &DefaultClientRetryPolicy{},
			SchedulerInterval:            riverinternaltest.SchedulerShortInterval,
			Schema:                       "",
			StaleProducerRetentionPeriod: time.Minute,
			Workers:                      NewWorkers(),
		}), jobUpdates
	})
}

func TestProducer_WithNotifier(t *testing.T) {
	t.Parallel()

	testProducer(t, func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated) {
		t.Helper()

		var (
			archetype  = riversharedtest.BaseServiceArchetype(t)
			dbPool     = riversharedtest.DBPool(ctx, t)
			driver     = riverpgxv5.New(dbPool)
			exec       = driver.GetExecutor()
			jobUpdates = make(chan []jobcompleter.CompleterJobUpdated, 10)
			schema     = riverdbtest.TestSchema(ctx, t, driver, nil)
			listener   = driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema})
			pilot      = &riverpilot.StandardPilot{}
			queueName  = fmt.Sprintf("test-producer-with-notifier-%05d", randutil.IntBetween(1, 100_000))
		)

		completer := jobcompleter.NewInlineCompleter(archetype, schema, exec, &riverpilot.StandardPilot{}, jobUpdates)
		{
			require.NoError(t, completer.Start(ctx))
			t.Cleanup(completer.Stop)
		}

		notifier := notifier.New(archetype, listener)
		{
			require.NoError(t, notifier.Start(ctx))
			t.Cleanup(notifier.Stop)
		}

		return newProducer(archetype, exec, pilot, &producerConfig{
			ClientID:                     testClientID,
			Completer:                    completer,
			ErrorHandler:                 newTestErrorHandler(),
			FetchCooldown:                FetchCooldownDefault,
			FetchPollInterval:            50 * time.Millisecond, // more aggressive than normal so in case we miss the event, tests still pass quickly
			PluginLookupByJob:            pluginlookup.NewJobPluginLookup(nil),
			PluginLookupGlobal:           pluginlookup.NewPluginLookup(nil),
			JobTimeout:                   JobTimeoutDefault,
			MaxWorkers:                   1_000,
			Notifier:                     notifier,
			Queue:                        queueName,
			QueuePollInterval:            queuePollIntervalDefault,
			QueueReportInterval:          queueReportIntervalDefault,
			RetryPolicy:                  &DefaultClientRetryPolicy{},
			SchedulerInterval:            riverinternaltest.SchedulerShortInterval,
			Schema:                       schema,
			StaleProducerRetentionPeriod: time.Minute,
			Workers:                      NewWorkers(),
		}), jobUpdates
	})
}

func testProducer(t *testing.T, makeProducer func(ctx context.Context, t *testing.T) (*producer, chan []jobcompleter.CompleterJobUpdated)) {
	t.Helper()

	ctx := context.Background()

	type testBundle struct {
		archetype       *baseservice.Archetype
		completer       jobcompleter.JobCompleter
		config          *Config
		exec            riverdriver.Executor
		jobUpdates      chan jobcompleter.CompleterJobUpdated
		queue           string
		timeBeforeStart time.Time
		workers         *Workers
	}

	setup := func(t *testing.T) (*producer, *testBundle) {
		t.Helper()

		timeBeforeStart := time.Now().UTC()

		producer, jobUpdates := makeProducer(ctx, t)
		producer.testSignals.Init(t)
		config := newTestConfig(t, producer.config.Schema)

		jobUpdatesFlattened := make(chan jobcompleter.CompleterJobUpdated, 10)
		go func() {
			for updates := range jobUpdates {
				for _, update := range updates {
					jobUpdatesFlattened <- update
				}
			}
		}()

		return producer, &testBundle{
			archetype:       &producer.Archetype,
			completer:       producer.completer,
			config:          config,
			exec:            producer.exec,
			jobUpdates:      jobUpdatesFlattened,
			queue:           producer.config.Queue,
			timeBeforeStart: timeBeforeStart,
			workers:         producer.workers,
		}
	}

	mustInsert := func(ctx context.Context, t *testing.T, producer *producer, bundle *testBundle, args JobArgs) {
		t.Helper()

		insertParams, err := insertParamsFromConfigArgsAndOptions(bundle.archetype, bundle.config, args, &InsertOpts{
			Queue: bundle.queue,
		})
		require.NoError(t, err)
		if insertParams.ScheduledAt == nil {
			// Without this, newly inserted jobs will pick up a scheduled_at time
			// that's the current Go time at the time of insertion. If the test is
			// using a transaction, this will be after the `now()` time in the
			// transaction that gets used by default in `JobGetAvailable`, so new jobs
			// won't be visible.
			//
			// To work around this, set all inserted jobs to a time before the start
			// of the test to ensure they're visible.
			insertParams.ScheduledAt = &bundle.timeBeforeStart
		}

		_, err = bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   []*riverdriver.JobInsertFastParams{(*riverdriver.JobInsertFastParams)(insertParams)},
			Schema: producer.config.Schema,
		})
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

		mustInsert(ctx, t, producer, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		update := riversharedtest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
	})

	t.Run("RegistersQueueStatus", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueueReportInterval = 50 * time.Millisecond

		now := producer.Time.StubNow(time.Now().UTC())

		startProducer(t, ctx, ctx, producer)

		queue, err := bundle.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name:   producer.config.Queue,
			Schema: producer.config.Schema,
		})
		require.NoError(t, err)
		require.WithinDuration(t, now, queue.CreatedAt, time.Microsecond)
		require.Equal(t, []byte("{}"), queue.Metadata)
		require.Equal(t, producer.config.Queue, queue.Name)
		require.WithinDuration(t, now, queue.UpdatedAt, time.Microsecond)
		require.Equal(t, queue.CreatedAt, queue.UpdatedAt)

		// Queue status should be updated quickly:
		producer.testSignals.ReportedQueueStatus.WaitOrTimeout()
	})

	t.Run("UnknownJobKind", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		AddWorker(bundle.workers, &noOpWorker{})

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		mustInsert(ctx, t, producer, bundle, &noOpArgs{})
		mustInsert(ctx, t, producer, bundle, &JobArgs{}) // not registered

		startProducer(t, ctx, ctx, producer)

		updates := riversharedtest.WaitOrTimeoutN(t, bundle.jobUpdates, 2)

		// Print updated jobs for debugging.
		for _, update := range updates {
			t.Logf("Job: %+v", update.Job)
		}

		// Order jobs come back in is not guaranteed, which is why this is
		// written somewhat strangely.
		findJob := func(kind string) *rivertype.JobRow {
			index := slices.IndexFunc(updates, func(u jobcompleter.CompleterJobUpdated) bool { return u.Job.Kind == kind })
			require.NotEqualf(t, -1, index, "Job update not found for kind: %s", kind)
			return updates[index].Job
		}

		{
			job := findJob((&JobArgs{}).Kind())
			require.Equal(t, rivertype.JobStateRetryable, job.State)
			require.Equal(t, (&UnknownJobKindError{Kind: (&JobArgs{}).Kind()}).Error(), job.Errors[0].Error)
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
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			producer.Logger.InfoContext(ctx, "Job started")
			<-ctx.Done()
			producer.Logger.InfoContext(ctx, "Job stopped after context cancelled")
			return ctx.Err()
		}))

		workCtx, workCancel := context.WithCancel(ctx)
		defer workCancel()

		mustInsert(ctx, t, producer, bundle, &JobArgs{})

		startProducer(t, ctx, workCtx, producer)

		workCancel()

		update := riversharedtest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateRetryable, update.Job.State)
	})

	t.Run("CompletesJobWhileFetchingNewOnes", func(t *testing.T) {
		t.Parallel()

		// Exercise the case where a job finishes while the producer is fetching
		// more jobs. One of two worker slots is occupied, so the next fetch
		// computes a limit of one before the test holds it pending.
		// Completing the active job must be processed without waiting for the
		// fetch, reopening both slots before the fetched job occupies one.
		// A third queued job verifies that the producer continues fetching and
		// runs the remaining work.
		//
		// The test also reads capacity throughout these active-job changes,
		// exercising the shared-state boundary so the race detector can catch
		// unsynchronized access.

		producer, bundle := setup(t)
		producer.config.FetchCooldown = time.Millisecond
		producer.config.FetchPollInterval = time.Hour
		producer.config.MaxWorkers = 2

		const numJobs = 3

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			JobIndex int `json:"job_index"`
		}

		capacityReadCtx, stopCapacityRead := context.WithCancel(ctx)

		var (
			capacityChanged    = make(chan int)
			capacityReadDone   = make(chan struct{})
			fetchCount         atomic.Int32
			jobStarted         = make(chan int, numJobs)
			releaseJob         = make([]chan struct{}, numJobs)
			secondFetchStarted = make(chan int, 1)
			thirdFetchStarted  = make(chan int, 1)
			unblockSecondFetch = make(chan struct{}, 1)
			unblockThirdFetch  = make(chan struct{}, 1)
		)

		for jobIndex := range numJobs {
			releaseJob[jobIndex] = make(chan struct{}, 1)
		}

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobIndex := job.Args.JobIndex
			if jobIndex < 0 || jobIndex >= numJobs {
				return fmt.Errorf("unexpected job index: %d", jobIndex)
			}

			jobStarted <- jobIndex
			<-releaseJob[jobIndex]
			return nil
		}))

		producer.pilot = &beforeJobGetAvailablePilot{
			Pilot: producer.pilot,
			beforeJobGetAvailableFunc: func(params *riverdriver.JobGetAvailableParams) {
				switch fetchCount.Add(1) {
				case 2:
					secondFetchStarted <- params.MaxToLock
					<-unblockSecondFetch
				case 3:
					thirdFetchStarted <- params.MaxToLock
					<-unblockThirdFetch
				}
			},
		}

		mustInsert(ctx, t, producer, bundle, &JobArgs{JobIndex: 0})

		startProducer(t, ctx, ctx, producer)
		t.Cleanup(func() {
			for _, release := range releaseJob {
				close(release)
			}
			close(unblockSecondFetch)
			close(unblockThirdFetch)
			stopCapacityRead()
		})

		require.Equal(t, 0, riversharedtest.WaitOrTimeout(t, jobStarted))

		mustInsert(ctx, t, producer, bundle, &JobArgs{JobIndex: 1})
		mustInsert(ctx, t, producer, bundle, &JobArgs{JobIndex: 2})
		producer.TriggerJobFetch()

		secondFetchLimit := riversharedtest.WaitOrTimeout(t, secondFetchStarted)
		require.Equal(t, 1, secondFetchLimit)

		go func() {
			defer close(capacityReadDone)

			lastCapacity := -1
			for {
				capacity := producer.maxJobsToFetch()
				if capacity != lastCapacity {
					select {
					case capacityChanged <- capacity:
						lastCapacity = capacity
					case <-capacityReadCtx.Done():
						return
					}
				}

				select {
				case <-capacityReadCtx.Done():
					return
				default:
				}
			}
		}()
		t.Cleanup(func() {
			stopCapacityRead()
			riversharedtest.WaitOrTimeout(t, capacityReadDone)
		})

		// Keep reading maxJobsToFetch between transitions so reads can overlap
		// active-job changes. Wait for each observed transition before causing
		// the next.
		waitForCapacity := func(expected int) {
			t.Helper()

			require.Equal(t, expected, riversharedtest.WaitOrTimeout(t, capacityChanged))
			require.Equal(t, producer.config.MaxWorkers-expected, int(producer.numJobsActive.Load()))
			require.Equal(t, expected, producer.maxJobsToFetch())
		}

		// The second fetch is blocked with a limit of one while job 0 occupies a
		// slot. Complete job 0 and verify that the producer processes its result,
		// reopening both slots before the fetch resumes.
		waitForCapacity(1)
		releaseJob[0] <- struct{}{}

		waitForCapacity(2)
		unblockSecondFetch <- struct{}{}

		// The fetch keeps its original limit of one, so it starts only job 1 and
		// leaves job 2 queued. The third fetch may calculate its limit while job 1
		// is still active (one slot) or after its completion is processed (two
		// slots).
		require.Equal(t, 1, riversharedtest.WaitOrTimeout(t, jobStarted))
		waitForCapacity(1)

		releaseJob[1] <- struct{}{}
		waitForCapacity(2)

		thirdFetchLimit := riversharedtest.WaitOrTimeout(t, thirdFetchStarted)
		require.Contains(t, []int{1, 2}, thirdFetchLimit)
		unblockThirdFetch <- struct{}{}

		// After job 2 starts, wait for the reader to observe the final transition
		// back to capacity one. Stop it before releasing job 2, then require all
		// three jobs to complete.
		require.Equal(t, 2, riversharedtest.WaitOrTimeout(t, jobStarted))
		waitForCapacity(1)
		stopCapacityRead()
		riversharedtest.WaitOrTimeout(t, capacityReadDone)

		releaseJob[2] <- struct{}{}
		updates := riversharedtest.WaitOrTimeoutN(t, bundle.jobUpdates, numJobs)
		for _, update := range updates {
			require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
		}
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
			testutil.JobArgsReflectKind[JobArgs]
		}

		unpauseWorkers := make(chan struct{})
		defer close(unpauseWorkers)

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			t.Logf("Job paused")
			<-unpauseWorkers
			t.Logf("Job unpaused")
			return ctx.Err()
		}))

		for range numJobs {
			mustInsert(ctx, t, producer, bundle, &JobArgs{})
		}

		startProducer(t, ctx, ctx, producer)

		producer.testSignals.StartedExecutors.WaitOrTimeout()

		// Jobs are still paused as we fetch updated job states.
		updatedJobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(&JobArgs{}).Kind()},
			Schema: producer.config.Schema,
		})
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

	t.Run("JobStuckHandler", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.JobTimeout = 10 * time.Millisecond
		producer.config.JobStuckThreshold = time.Millisecond
		producer.config.MaxWorkers = 2
		producer.jobTimeout = producer.config.JobTimeout

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			Num int `json:"num"`
		}

		releaseJobs := make(chan struct{})
		defer close(releaseJobs)

		handlerParamsCh := make(chan JobStuckHandlerParams, 2)
		producer.config.JobStuckHandler = func(ctx context.Context, params JobStuckHandlerParams) JobStuckHandlerResult {
			handlerParamsCh <- params
			return JobStuckHandlerResult{}
		}

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			<-releaseJobs
			return nil
		}))

		mustInsert(ctx, t, producer, bundle, &JobArgs{Num: 1})
		mustInsert(ctx, t, producer, bundle, &JobArgs{Num: 2})

		startProducer(t, ctx, ctx, producer)

		handlerParams := riversharedtest.WaitOrTimeoutN(t, handlerParamsCh, 2)
		require.ElementsMatch(t, []int{1, 2}, []int{handlerParams[0].TotalStuckJobs, handlerParams[1].TotalStuckJobs})
		for _, params := range handlerParams {
			require.NotZero(t, params.ID)
			require.Equal(t, (&JobArgs{}).Kind(), params.Kind)
			require.Equal(t, producer.config.Queue, params.Queue)
		}
	})

	t.Run("JobStuckHandlerOpensExecutorSlot", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.JobTimeout = 20 * time.Millisecond
		producer.config.JobStuckThreshold = time.Millisecond
		producer.config.MaxWorkers = 1
		producer.jobTimeout = producer.config.JobTimeout

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			Num int `json:"num"`
		}

		handlerParamsCh := make(chan JobStuckHandlerParams, 2)
		producer.config.JobStuckHandler = func(ctx context.Context, params JobStuckHandlerParams) JobStuckHandlerResult {
			handlerParamsCh <- params

			// Only replace the first stuck job. The second job may also pass the
			// short test timeout before the assertion runs on a busy machine, but
			// it should continue occupying its executor slot in that case.
			return JobStuckHandlerResult{AddWorkerSlot: params.TotalStuckJobs == 1}
		}

		var (
			firstStarted  = make(chan struct{})
			releaseJobs   = make(chan struct{})
			secondStarted = make(chan struct{})
		)
		defer close(releaseJobs)

		AddWorker(bundle.workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			switch job.Args.Num {
			case 1:
				close(firstStarted)
			case 2:
				close(secondStarted)
			default:
				require.FailNow(t, "unexpected job num", "num=%d", job.Args.Num)
			}

			<-releaseJobs
			return nil
		}))

		mustInsert(ctx, t, producer, bundle, &JobArgs{Num: 1})
		mustInsert(ctx, t, producer, bundle, &JobArgs{Num: 2})

		startProducer(t, ctx, ctx, producer)

		riversharedtest.WaitOrTimeout(t, firstStarted)

		handlerParams := riversharedtest.WaitOrTimeout(t, handlerParamsCh)
		require.Equal(t, 1, handlerParams.TotalStuckJobs)

		riversharedtest.WaitOrTimeout(t, secondStarted)
		require.Equal(t, int32(1), producer.numJobsActive.Load())
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		producer, _ := setup(t)
		producer.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		producer.testSignals = producerTestSignals{}    // deinit so channels don't fill

		startstoptest.Stress(ctx, t, producer)
	})

	t.Run("QueuePausedBeforeStart", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		AddWorker(bundle.workers, &noOpWorker{})

		testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{
			Name:     new(producer.config.Queue),
			PausedAt: new(time.Now()),
			Schema:   producer.config.Schema,
		})

		mustInsert(ctx, t, producer, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		select {
		case update := <-bundle.jobUpdates:
			t.Fatalf("Unexpected job update: job=%+v stats=%+v", update.Job, update.JobStats)
		case <-time.After(500 * time.Millisecond):
		}
	})

	testQueuePause := func(t *testing.T, pauseAll bool) {
		t.Helper()
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueuePollInterval = 50 * time.Millisecond
		AddWorker(bundle.workers, &noOpWorker{})

		mustInsert(ctx, t, producer, bundle, &noOpArgs{})

		startProducer(t, ctx, ctx, producer)

		// First job should be executed immediately while resumed:
		update := riversharedtest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)

		// Pause the queue and wait for confirmation:
		queueNameToPause := producer.config.Queue
		if pauseAll {
			queueNameToPause = rivercommon.AllQueuesString
		}
		require.NoError(t, bundle.exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
			Name:   queueNameToPause,
			Schema: producer.config.Schema,
		}))
		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, producer.config.Schema, queueNameToPause, "pause", nil)
		}
		producer.testSignals.Paused.WaitOrTimeout()

		// Job should not be executed while paused:
		mustInsert(ctx, t, producer, bundle, &noOpArgs{})

		select {
		case update := <-bundle.jobUpdates:
			t.Fatalf("Unexpected job update: %+v", update)
		case <-time.After(500 * time.Millisecond):
		}

		// Resume the queue and wait for confirmation:
		require.NoError(t, bundle.exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
			Name:   queueNameToPause,
			Schema: producer.config.Schema,
		}))
		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, producer.config.Schema, queueNameToPause, "resume", nil)
		}
		producer.testSignals.Resumed.WaitOrTimeout()

		// Now the 2nd job should execute:
		update = riversharedtest.WaitOrTimeout(t, bundle.jobUpdates)
		require.Equal(t, rivertype.JobStateCompleted, update.Job.State)
	}

	t.Run("QueuePausedDuringOperation", func(t *testing.T) {
		testQueuePause(t, false)
	})

	t.Run("QueuePausedAndResumedDuringOperationUsing*", func(t *testing.T) {
		testQueuePause(t, true)
	})

	t.Run("QueueDeletedFromRiverQueueTableDuringOperation", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueuePollInterval = 100 * time.Millisecond
		producer.config.QueueReportInterval = 100 * time.Millisecond
		producer.config.ProducerReportInterval = 100 * time.Millisecond

		startProducer(t, ctx, ctx, producer)
		producer.testSignals.ReportedProducerStatus.WaitOrTimeout()

		// Delete the queue by using a future-dated horizon:
		_, err := bundle.exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{
			Max:              100,
			Schema:           producer.config.Schema,
			UpdatedAtHorizon: time.Now().Add(time.Minute),
		})
		require.NoError(t, err)

		producer.testSignals.ReportedQueueStatus.WaitOrTimeout()
		if producer.config.Notifier == nil {
			producer.testSignals.PolledQueueConfig.WaitOrTimeout()
		}
	})

	t.Run("QueueMetadataChangedDuringOperation", func(t *testing.T) {
		t.Parallel()

		producer, bundle := setup(t)
		producer.config.QueuePollInterval = 50 * time.Millisecond

		startProducer(t, ctx, ctx, producer)

		updateMetadata := func(newMetadata []byte) {
			t.Helper()

			_, err := bundle.exec.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
				Metadata:         newMetadata,
				MetadataDoUpdate: true,
				Name:             producer.config.Queue,
				Schema:           producer.config.Schema,
			})
			require.NoError(t, err)
		}

		// Update the queue's metadata:
		updateMetadata([]byte(`{"foo":"bar","baz":123}`))

		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, producer.config.Schema, producer.config.Queue, "metadata_changed", []byte(`{"foo":"bar","baz":123}`))
		}

		producer.testSignals.MetadataChanged.WaitOrTimeout()

		// Update with equivalent metadata but different field ordering:
		reorderedMetadata := []byte(`{"baz":123,"foo":"bar"}`)
		updateMetadata(reorderedMetadata)
		// do not emit a notification here because this isn't a "real" update and
		// notifier mode doesn't check for metadata equivalence.

		// Should not receive a metadata changed signal since the JSON is equivalent:
		select {
		case <-producer.testSignals.MetadataChanged.WaitC():
			t.Fatal("Received unexpected metadata changed signal for equivalent JSON")
		case <-time.After(100 * time.Millisecond):
			// Expected - no signal received
		}

		// Verify that the producer's comparison logic is working correctly by updating with different metadata:
		differentMetadata := []byte(`{"foo":"bar","baz":456}`)
		updateMetadata(differentMetadata)
		if producer.config.Notifier != nil {
			// also emit notification:
			emitQueueNotification(t, ctx, bundle.exec, producer.config.Schema, producer.config.Queue, "metadata_changed", differentMetadata)
		}

		// Should receive a metadata changed signal since the JSON is different:
		producer.testSignals.MetadataChanged.WaitOrTimeout()
	})
}

func TestProducer_jitteredFetchPollInterval(t *testing.T) {
	t.Parallel()

	prod := &producer{}
	prod.config = &producerConfig{
		FetchPollInterval: 1 * time.Second,
	}

	// Run enough iterations to catch any out-of-bounds values without being
	// flaky. The jitter range is [FetchPollInterval, FetchPollInterval +
	// 10% of FetchPollInterval), so [1s, 1.1s).
	for range 100 {
		d := prod.jitteredFetchPollInterval()
		require.GreaterOrEqual(t, d, prod.config.FetchPollInterval)
		require.Less(t, d, prod.config.FetchPollInterval+prod.config.FetchPollInterval/10)
	}
}

func emitQueueNotification(t *testing.T, ctx context.Context, exec riverdriver.Executor, schema, queue, action string, metadata []byte) {
	t.Helper()

	payload := map[string]any{
		"queue":  queue,
		"action": action,
	}
	if metadata != nil {
		payload["metadata"] = metadata
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	err = exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Topic:   string(notifier.NotificationTopicControl),
		Payload: []string{string(payloadBytes)},
		Schema:  schema,
	})
	require.NoError(t, err)
}
