package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/retrypolicytest"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

type invalidKindArgs struct{}

func (a invalidKindArgs) Kind() string { return "this kind is invalid" }

type invalidKindWorker struct {
	WorkerDefaults[invalidKindArgs]
}

func (w *invalidKindWorker) Work(ctx context.Context, job *Job[invalidKindArgs]) error { return nil }

type noOpArgs struct {
	Name string `json:"name"`
}

func (noOpArgs) Kind() string { return "noOp" }

type noOpWorker struct {
	WorkerDefaults[noOpArgs]
}

func (w *noOpWorker) Work(ctx context.Context, job *Job[noOpArgs]) error { return nil }

type periodicJobArgs struct{}

func (periodicJobArgs) Kind() string { return "periodic_job" }

type periodicJobWorker struct {
	WorkerDefaults[periodicJobArgs]
}

func (w *periodicJobWorker) Work(ctx context.Context, job *Job[periodicJobArgs]) error {
	return nil
}

func makeAwaitWorker[T JobArgs](startedCh chan<- int64, doneCh chan struct{}) Worker[T] {
	return WorkFunc(func(ctx context.Context, job *Job[T]) error {
		client := ClientFromContext[pgx.Tx](ctx)
		client.config.Logger.InfoContext(ctx, "callback job started with id="+strconv.FormatInt(job.ID, 10))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case startedCh <- job.ID:
		}

		// await done signal, or context cancellation:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-doneCh:
			return nil
		}
	})
}

// A small wrapper around Client that gives us a struct that corrects the
// client's Stop function so that it can implement startstop.Service.
type clientWithSimpleStop[TTx any] struct {
	*Client[TTx]
}

func (c *clientWithSimpleStop[TTx]) Started() <-chan struct{} {
	return c.baseStartStop.Started()
}

func (c *clientWithSimpleStop[TTx]) Stop() {
	_ = c.Client.Stop(context.Background())
}

func newTestConfig(t *testing.T, schema string) *Config {
	t.Helper()

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	return &Config{
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		Logger:            riversharedtest.Logger(t),
		MaxAttempts:       MaxAttemptsDefault,
		Queues:            map[string]QueueConfig{QueueDefault: {MaxWorkers: 50}},
		Schema:            schema,
		Test: TestConfig{
			Time: &riversharedtest.TimeStub{},
		},
		TestOnly:          true, // disables staggered start in maintenance services
		Workers:           workers,
		schedulerInterval: riverinternaltest.SchedulerShortInterval,
	}
}

func newTestClient(t *testing.T, dbPool *pgxpool.Pool, config *Config) *Client[pgx.Tx] {
	t.Helper()

	client, err := NewClient(riverpgxv5.New(dbPool), config)
	require.NoError(t, err)

	return client
}

func startClient[TTx any](ctx context.Context, t *testing.T, client *Client[TTx]) {
	t.Helper()

	require.NoError(t, client.Start(ctx))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, client.Stop(ctx))
	})
}

func runNewTestClient(ctx context.Context, t *testing.T, config *Config) *Client[pgx.Tx] {
	t.Helper()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)
	config.Schema = schema

	client, err := NewClient(driver, config)
	require.NoError(t, err)

	startClient(ctx, t, client)

	return client
}

func subscribe[TTx any](t *testing.T, client *Client[TTx]) <-chan *Event {
	t.Helper()

	subscribeChan, cancel := client.Subscribe(
		EventKindJobCancelled,
		EventKindJobCompleted,
		EventKindJobFailed,
		EventKindJobSnoozed,
		EventKindQueuePaused,
		EventKindQueueResumed,
	)
	t.Cleanup(cancel)
	return subscribeChan
}

func Test_Client_Common(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		config *Config
		dbPool *pgxpool.Pool
		driver *riverpgxv5.Driver
		schema string
	}

	// Alternate setup returning only client Config rather than a full Client.
	setupConfig := func(t *testing.T) (*Config, *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPoolClone(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		return config, &testBundle{
			config: config,
			dbPool: dbPool,
			driver: driver,
			schema: schema,
		}
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config, bundle := setupConfig(t)

		client, err := NewClient(bundle.driver, config)
		require.NoError(t, err)

		return client, bundle
	}

	t.Run("StartInsertAndWork", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		workedChan := make(chan struct{})

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			workedChan <- struct{}{}
			return nil
		}))

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, workedChan)
	})

	t.Run("Queues_Add_WhenClientWontExecuteJobs", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		config.Queues = nil
		config.Workers = nil
		client := newTestClient(t, bundle.dbPool, config)

		err := client.Queues().Add("new_queue", QueueConfig{MaxWorkers: 2})
		require.Error(t, err)
		require.Contains(t, err.Error(), "client is not configured to execute jobs, cannot add queue")
	})

	t.Run("Queues_Add_BeforeStart", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		workedChan := make(chan struct{})

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			workedChan <- struct{}{}
			return nil
		}))

		queueName := "new_queue"
		err := client.Queues().Add(queueName, QueueConfig{
			MaxWorkers: 2,
		})
		require.NoError(t, err)

		startClient(ctx, t, client)

		_, err = client.Insert(ctx, &JobArgs{}, &InsertOpts{
			Queue: queueName,
		})
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, workedChan)
	})

	t.Run("Queues_Add_AfterStart", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		workedChan := make(chan struct{})

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			workedChan <- struct{}{}
			return nil
		}))

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		queueName := "new_queue"
		err := client.Queues().Add(queueName, QueueConfig{
			MaxWorkers: 2,
		})
		require.NoError(t, err)

		_, err = client.Insert(ctx, &JobArgs{}, &InsertOpts{
			Queue: queueName,
		})
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, workedChan)
	})

	t.Run("Queues_Add_AlreadyAdded", func(t *testing.T) {
		t.Parallel()

		// Test two scenarios: when the queue was already added before the client
		// was created, and when the queue was added after the client was created.
		// Both should error.

		config, bundle := setupConfig(t)
		config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 2}}
		client := newTestClient(t, bundle.dbPool, config)

		err := client.Queues().Add(QueueDefault, QueueConfig{MaxWorkers: 2})
		require.Error(t, err)
		var alreadyAddedErr *QueueAlreadyAddedError
		require.ErrorAs(t, err, &alreadyAddedErr)
		require.Equal(t, QueueDefault, alreadyAddedErr.Name)

		require.NoError(t, client.Queues().Add("new_queue", QueueConfig{MaxWorkers: 2}))

		err = client.Queues().Add("new_queue", QueueConfig{MaxWorkers: 2})
		require.Error(t, err)
		require.ErrorAs(t, err, &alreadyAddedErr)
		require.Equal(t, "new_queue", alreadyAddedErr.Name)
	})

	t.Run("Queues_Add_Stress", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		var wg sync.WaitGroup

		// Uses a smaller number of workers and iterations than most stress
		// tests because there's quite a lot of mutex contention so that too
		// many can make the test run long.
		for i := range 3 {
			wg.Add(1)
			workerNum := i
			go func() {
				defer wg.Done()

				for j := range 5 {
					err := client.Queues().Add(fmt.Sprintf("new_queue_%d_%d_before", workerNum, j), QueueConfig{MaxWorkers: 1})
					require.NoError(t, err)

					require.NoError(t, client.Start(ctx))

					err = client.Queues().Add(fmt.Sprintf("new_queue_%d_%d_after", workerNum, j), QueueConfig{MaxWorkers: 1})
					require.NoError(t, err)

					stopped := make(chan struct{})

					go func() {
						defer close(stopped)
						require.NoError(t, client.Stop(ctx))
					}()

					select {
					case <-stopped:
					case <-time.After(5 * time.Second):
						require.FailNow(t, "Timed out waiting for service to stop")
					}
				}
			}()
		}

		wg.Wait()
	})

	t.Run("JobCancelErrorReturned", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobCancel(errors.New("a persisted internal error"))
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		updatedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
		require.WithinDuration(t, time.Now(), *updatedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobCancelErrorReturnedWithNilErr", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobCancel(nil)
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		updatedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
		require.WithinDuration(t, time.Now(), *updatedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobSnoozeErrorReturned", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobSnooze(15 * time.Minute)
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobSnoozed, event.Kind)
		require.Equal(t, rivertype.JobStateScheduled, event.Job.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), event.Job.ScheduledAt, 2*time.Second)

		updatedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, 0, updatedJob.Attempt)
		require.Equal(t, rivertype.JobStateScheduled, updatedJob.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), updatedJob.ScheduledAt, 2*time.Second)
	})

	t.Run("JobSnoozeWithZeroDurationSetsAvailableImmediately", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobSnooze(0)
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, EventKindJobSnoozed, event.Kind)
		require.Equal(t, rivertype.JobStateAvailable, event.Job.State)
		require.WithinDuration(t, time.Now(), event.Job.ScheduledAt, 2*time.Second)
		require.Equal(t, 0, event.Job.Attempt)
	})

	// This helper is used to test cancelling a job both _in_ a transaction and
	// _outside of_ a transaction. The exact same test logic applies to each case,
	// the only difference is a different cancelFunc provided by the specific
	// subtest.
	cancelRunningJobTestHelper := func(t *testing.T, configMutate func(config *Config), cancelFunc func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error)) { //nolint:thelper
		config, bundle := setupConfig(t)

		if configMutate != nil {
			configMutate(config)
		}

		client := newTestClient(t, bundle.dbPool, config)

		jobStartedChan := make(chan int64)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			<-ctx.Done()
			return ctx.Err()
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riversharedtest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertRes.Job.ID, startedJobID)

		// Cancel the job:
		updatedJob, err := cancelFunc(ctx, bundle.dbPool, client, insertRes.Job.ID)
		require.NoError(t, err)
		require.NotNil(t, updatedJob)
		// Job is still actively running at this point because the query wouldn't
		// modify that column for a running job:
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		jobAfterCancel, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, jobAfterCancel.State)
		require.WithinDuration(t, time.Now(), *jobAfterCancel.FinalizedAt, 2*time.Second)
	}

	t.Run("CancelRunningJob", func(t *testing.T) {
		t.Parallel()

		cancelRunningJobTestHelper(t, nil, func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error) {
			return client.JobCancel(ctx, jobID)
		})
	})

	t.Run("CancelRunningJobWithLongPollInterval", func(t *testing.T) {
		t.Parallel()

		configMutate := func(config *Config) {
			config.FetchPollInterval = 60 * time.Second
		}

		cancelRunningJobTestHelper(t, configMutate, func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error) {
			return client.JobCancel(ctx, jobID)
		})
	})

	t.Run("CancelRunningJobInTx", func(t *testing.T) {
		t.Parallel()

		cancelRunningJobTestHelper(t, nil, func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error) {
			var (
				job *rivertype.JobRow
				err error
			)
			txErr := pgx.BeginFunc(ctx, dbPool, func(tx pgx.Tx) error {
				job, err = client.JobCancelTx(ctx, tx, jobID)
				return err
			})
			require.NoError(t, txErr)
			return job, err
		})
	})

	t.Run("CancelScheduledJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(5 * time.Minute)})
		require.NoError(t, err)

		// Cancel the job:
		updatedJob, err := client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.NotNil(t, updatedJob)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
		require.WithinDuration(t, time.Now(), *updatedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("CancelNonExistentJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		startClient(ctx, t, client)

		// Cancel an unknown job ID:
		jobAfter, err := client.JobCancel(ctx, 0)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, jobAfter)

		// Cancel an unknown job ID, within a transaction:
		err = dbutil.WithTx(ctx, client.driver.GetExecutor(), func(ctx context.Context, exec riverdriver.ExecutorTx) error {
			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{ID: 0, Schema: client.config.Schema})
			require.ErrorIs(t, err, ErrNotFound)
			require.Nil(t, jobAfter)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("CancelProducerControlEventSent", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPoolClone(ctx, t)
			driver = NewDriverWithoutListenNotify(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client, err := NewClient(driver, config)
		require.NoError(t, err)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		_, err = client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		controlEvent := client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.WaitOrTimeout()
		require.NotNil(t, controlEvent)
		require.Equal(t, controlActionCancel, controlEvent.Action)
	})

	t.Run("CancelProducerControlEventNotSent", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		_, err = client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.RequireEmpty()
	})

	t.Run("AlternateSchema", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		// Reconfigure the pool with an alternate schema, initialize a new pool
		dbPoolConfig := bundle.dbPool.Config() // a copy of the original config
		dbPoolConfig.ConnConfig.RuntimeParams["search_path"] = "alternate_schema"

		dbPool, err := pgxpool.NewWithConfig(ctx, dbPoolConfig)
		require.NoError(t, err)
		t.Cleanup(dbPool.Close)

		bundle.config.Schema = ""
		client, err := NewClient(riverpgxv5.New(dbPool), bundle.config)
		require.NoError(t, err)

		// We don't actually verify that River's functional on another schema so
		// that we don't have to raise and migrate it. We cheat a little by
		// configuring a different schema and then verifying that we can't find
		// a `river_job` to confirm we're point there.
		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		// PgError has SchemaName and TableName properties, but unfortunately
		// neither contain a useful value in this case.
		require.Equal(t, `relation "river_job" does not exist`, pgErr.Message)
	})

	t.Run("WithGlobalInsertBeginHook", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		insertBeginHookCalled := false

		bundle.config.Hooks = []rivertype.Hook{
			HookInsertBeginFunc(func(ctx context.Context, params *rivertype.JobInsertParams) error {
				insertBeginHookCalled = true
				return nil
			}),
		}

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		_, err = client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		require.True(t, insertBeginHookCalled)
	})

	t.Run("HookArchetypeInitialized", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		type HookWithBaseService struct {
			baseservice.BaseService
			HookInsertBeginFunc
		}

		var (
			hook       = &HookWithBaseService{}
			hookCalled = false
		)
		hook.HookInsertBeginFunc = func(ctx context.Context, params *rivertype.JobInsertParams) error {
			hookCalled = true
			require.NotEmpty(t, hook.Name) // if name is non-empty, it means the base service was initialized properly
			return nil
		}

		bundle.config.Hooks = []rivertype.Hook{hook}
		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		_, err = client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		require.True(t, hookCalled)
	})

	t.Run("WithGlobalWorkBeginHook", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		workBeginHookCalled := false

		bundle.config.Hooks = []rivertype.Hook{
			HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error {
				workBeginHookCalled = true
				return nil
			}),
		}

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)

		require.True(t, workBeginHookCalled)
	})

	t.Run("WithGlobalWorkEndHook", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		workEndHookCalled := false

		bundle.config.Hooks = []rivertype.Hook{
			HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
				workEndHookCalled = true
				return err
			}),
		}

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)

		require.True(t, workEndHookCalled)
	})

	t.Run("WithInsertBeginHookOnJobArgs", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		var hookInsertBeginCalled atomic.Bool

		jobArgs := JobArgsWithHooksFunc(func() []rivertype.Hook {
			return []rivertype.Hook{
				HookInsertBeginFunc(func(ctx context.Context, params *rivertype.JobInsertParams) error {
					hookInsertBeginCalled.Store(true)
					return nil
				}),
			}
		})

		AddWorkerArgs(bundle.config.Workers, jobArgs, WorkFunc(func(ctx context.Context, job *Job[JobArgsWithHooksFunc]) error {
			return nil
		}))

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		_, err = client.Insert(ctx, jobArgs, nil)
		require.NoError(t, err)

		require.True(t, hookInsertBeginCalled.Load())
	})

	t.Run("WithWorkBeginHookOnJobArgs", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		var hookWorkBeginCalled atomic.Bool

		jobArgs := JobArgsWithHooksFunc(func() []rivertype.Hook {
			return []rivertype.Hook{
				HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error {
					hookWorkBeginCalled.Store(true)
					return nil
				}),
			}
		})

		AddWorkerArgs(bundle.config.Workers, jobArgs, WorkFunc(func(ctx context.Context, job *Job[JobArgsWithHooksFunc]) error {
			return nil
		}))

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, jobArgs, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)

		require.True(t, hookWorkBeginCalled.Load())
	})

	t.Run("WithWorkEndHookOnJobArgs", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		var hookWorkEndCalled atomic.Bool

		jobArgs := JobArgsWithHooksFunc(func() []rivertype.Hook {
			return []rivertype.Hook{
				HookWorkEndFunc(func(ctx context.Context, job *rivertype.JobRow, err error) error {
					hookWorkEndCalled.Store(true)
					return nil
				}),
			}
		})

		AddWorkerArgs(bundle.config.Workers, jobArgs, WorkFunc(func(ctx context.Context, job *Job[JobArgsWithHooksFunc]) error {
			return nil
		}))

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, jobArgs, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)

		require.True(t, hookWorkEndCalled.Load())
	})

	t.Run("WithGlobalWorkerMiddleware", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		middlewareCalled := false

		type privateKey string

		middleware := &overridableJobMiddleware{
			workFunc: func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
				ctx = context.WithValue(ctx, privateKey("middleware"), "called")
				middlewareCalled = true
				return doInner(ctx)
			},
		}
		bundle.config.Middleware = []rivertype.Middleware{middleware}

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(bundle.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			require.Equal(t, "called", ctx.Value(privateKey("middleware")))
			return nil
		}))

		driver := riverpgxv5.New(bundle.dbPool)
		client, err := NewClient(driver, bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		result, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, result.Job.ID, event.Job.ID)
		require.True(t, middlewareCalled)
	})

	t.Run("WithWorkerMiddlewareOnWorker", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		middlewareCalled := false

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		type privateKey string

		worker := &workerWithMiddleware[JobArgs]{
			workFunc: func(ctx context.Context, job *Job[JobArgs]) error {
				require.Equal(t, "called", ctx.Value(privateKey("middleware")))
				return nil
			},
			middlewareFunc: func(job *rivertype.JobRow) []rivertype.WorkerMiddleware {
				require.Equal(t, (JobArgs{}).Kind(), job.Kind)

				return []rivertype.WorkerMiddleware{
					&overridableJobMiddleware{
						workFunc: func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
							ctx = context.WithValue(ctx, privateKey("middleware"), "called")
							middlewareCalled = true
							return doInner(ctx)
						},
					},
				}
			},
		}

		AddWorker(bundle.config.Workers, worker)

		driver := riverpgxv5.New(bundle.dbPool)
		client, err := NewClient(driver, bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		result, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, result.Job.ID, event.Job.ID)
		require.True(t, middlewareCalled)
	})

	t.Run("MiddlewareModifiesEncodedArgs", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)
		middlewareCalled := false

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			Name string `json:"name"`
		}

		worker := &workerWithMiddleware[JobArgs]{
			workFunc: func(ctx context.Context, job *Job[JobArgs]) error {
				require.Equal(t, "middleware name", job.Args.Name)
				return nil
			},
			middlewareFunc: func(job *rivertype.JobRow) []rivertype.WorkerMiddleware {
				return []rivertype.WorkerMiddleware{
					&overridableJobMiddleware{
						workFunc: func(ctx context.Context, job *rivertype.JobRow, doInner func(ctx context.Context) error) error {
							middlewareCalled = true
							require.JSONEq(t, `{"name": "inserted name"}`, string(job.EncodedArgs))
							job.EncodedArgs = []byte(`{"name": "middleware name"}`)
							return doInner(ctx)
						},
					},
				}
			},
		}

		AddWorker(bundle.config.Workers, worker)

		driver := riverpgxv5.New(bundle.dbPool)
		client, err := NewClient(driver, bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		result, err := client.Insert(ctx, JobArgs{Name: "inserted name"}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, result.Job.ID, event.Job.ID)
		require.True(t, middlewareCalled)
	})

	t.Run("NotifyRequestResign", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		client.testSignals.Init(t)

		startClient(ctx, t, client)

		client.config.Logger.InfoContext(ctx, "Test waiting for client to be elected leader for the first time")
		client.testSignals.electedLeader.WaitOrTimeout()
		client.config.Logger.InfoContext(ctx, "Client was elected leader for the first time")

		// We test the function with a forced resignation, but this is a general
		// Notify test case so this could be changed to any notification.
		require.NoError(t, client.Notify().RequestResign(ctx))

		client.config.Logger.InfoContext(ctx, "Test waiting for client to be elected leader after forced resignation")
		client.testSignals.electedLeader.WaitOrTimeout()
		client.config.Logger.InfoContext(ctx, "Client was elected leader after forced resignation")
	})

	t.Run("NotifyRequestResignTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)
		client.testSignals.Init(t)

		startClient(ctx, t, client)

		client.config.Logger.InfoContext(ctx, "Test waiting for client to be elected leader for the first time")
		client.testSignals.electedLeader.WaitOrTimeout()
		client.config.Logger.InfoContext(ctx, "Client was elected leader for the first time")

		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		require.NoError(t, client.Notify().RequestResignTx(ctx, tx))

		require.NoError(t, tx.Commit(ctx))

		client.config.Logger.InfoContext(ctx, "Test waiting for client to be elected leader after forced resignation")
		client.testSignals.electedLeader.WaitOrTimeout()
		client.config.Logger.InfoContext(ctx, "Client was elected leader after forced resignation")
	})

	t.Run("OutputRoundTrip", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			require.NoError(t, RecordOutput(ctx, "my job output"))
			return nil
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		_, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, `"my job output"`, string(event.Job.Output()))
	})

	t.Run("PauseAndResumeSingleQueue", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		client := newTestClient(t, bundle.dbPool, config)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes1, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes1.Job.ID, event.Job.ID)

		require.NoError(t, client.QueuePause(ctx, QueueDefault, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueuePaused, Queue: &rivertype.Queue{Name: QueueDefault}}, event)

		insertRes2, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		select {
		case <-subscribeChan:
			t.Fatal("expected job 2 to not start on paused queue")
		case <-time.After(500 * time.Millisecond):
		}

		require.NoError(t, client.QueueResume(ctx, QueueDefault, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueueResumed, Queue: &rivertype.Queue{Name: QueueDefault}}, event)

		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes2.Job.ID, event.Job.ID)
	})

	t.Run("PauseAndResumeMultipleQueues", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		config.Queues["alternate"] = QueueConfig{MaxWorkers: 10}
		client := newTestClient(t, bundle.dbPool, config)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes1, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes1.Job.ID, event.Job.ID)

		// Pause only the default queue:
		require.NoError(t, client.QueuePause(ctx, QueueDefault, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueuePaused, Queue: &rivertype.Queue{Name: QueueDefault}}, event)

		insertRes2, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		select {
		case <-subscribeChan:
			t.Fatal("expected job 2 to not start on paused queue")
		case <-time.After(500 * time.Millisecond):
		}

		// alternate queue should still be running:
		insertResAlternate1, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{Queue: "alternate"})
		require.NoError(t, err)

		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertResAlternate1.Job.ID, event.Job.ID)

		// Pause all queues:
		require.NoError(t, client.QueuePause(ctx, rivercommon.AllQueuesString, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueuePaused, Queue: &rivertype.Queue{Name: "alternate"}}, event)

		insertResAlternate2, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{Queue: "alternate"})
		require.NoError(t, err)

		select {
		case <-subscribeChan:
			t.Fatal("expected alternate job 2 to not start on paused queue")
		case <-time.After(500 * time.Millisecond):
		}

		// Resume only the alternate queue:
		require.NoError(t, client.QueueResume(ctx, "alternate", nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueueResumed, Queue: &rivertype.Queue{Name: "alternate"}}, event)

		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertResAlternate2.Job.ID, event.Job.ID)

		// Resume all queues:
		require.NoError(t, client.QueueResume(ctx, rivercommon.AllQueuesString, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &Event{Kind: EventKindQueueResumed, Queue: &rivertype.Queue{Name: QueueDefault}}, event)

		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes2.Job.ID, event.Job.ID)
	})

	t.Run("PauseAndResumeSingleQueueTx", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		client := newTestClient(t, bundle.dbPool, config)

		queue := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{
			Schema: config.Schema,
		})

		tx, err := bundle.dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		require.NoError(t, client.QueuePauseTx(ctx, tx, queue.Name, nil))

		queueRes, err := client.QueueGetTx(ctx, tx, queue.Name)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), *queueRes.PausedAt, 2*time.Second)

		// Not paused outside transaction.
		queueRes, err = client.QueueGet(ctx, queue.Name)
		require.NoError(t, err)
		require.Nil(t, queueRes.PausedAt)

		require.NoError(t, client.QueueResumeTx(ctx, tx, queue.Name, nil))

		queueRes, err = client.QueueGetTx(ctx, tx, queue.Name)
		require.NoError(t, err)
		require.Nil(t, queueRes.PausedAt)
	})

	t.Run("PausedBeforeStart", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		jobStartedChan := make(chan int64)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			return nil
		}))

		// Ensure queue record exists:
		queue := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{
			Schema: bundle.schema,
		})

		// Pause only the default queue:
		require.NoError(t, client.QueuePause(ctx, queue.Name, nil))

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, &JobArgs{}, &InsertOpts{Queue: queue.Name})
		require.NoError(t, err)

		select {
		case <-jobStartedChan:
			t.Fatal("expected job to not start on paused queue")
		case <-time.After(500 * time.Millisecond):
		}
	})

	t.Run("QueuePauseAndResumeProducerControlEventSent", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPoolClone(ctx, t)
			driver = NewDriverWithoutListenNotify(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client, err := NewClient(driver, config)
		require.NoError(t, err)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		require.NoError(t, client.QueuePause(ctx, QueueDefault, nil))

		controlEvent := client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.WaitOrTimeout()
		require.NotNil(t, controlEvent)
		require.Equal(t, controlActionPause, controlEvent.Action)

		require.NoError(t, client.QueueResume(ctx, QueueDefault, nil))

		controlEvent = client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.WaitOrTimeout()
		require.NotNil(t, controlEvent)
		require.Equal(t, controlActionResume, controlEvent.Action)
	})

	t.Run("QueuePauseAndResumeProducerControlEventNotSent", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		require.NoError(t, client.QueuePause(ctx, QueueDefault, nil))

		client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.RequireEmpty()

		require.NoError(t, client.QueueResume(ctx, QueueDefault, nil))

		client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.RequireEmpty()
	})

	t.Run("PollOnlyDriver", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		bundle.config.PollOnly = true

		client, err := NewClient(NewDriverPollOnly(bundle.dbPool), config)
		require.NoError(t, err)
		client.testSignals.Init(t)

		// Notifier should not have been initialized at all.
		require.Nil(t, client.notifier)

		insertRes, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		// Despite no notifier, the client should still be able to elect itself
		// leader.
		client.testSignals.electedLeader.WaitOrTimeout()

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
	})

	t.Run("PollOnlyOption", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		bundle.config.PollOnly = true

		client := newTestClient(t, bundle.dbPool, config)
		client.testSignals.Init(t)

		// Notifier should not have been initialized at all.
		require.Nil(t, client.notifier)

		insertRes, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		// Despite no notifier, the client should still be able to elect itself
		// leader.
		client.testSignals.electedLeader.WaitOrTimeout()

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
	})

	t.Run("KindAliases", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		AddWorker(bundle.config.Workers, WorkFunc(func(ctx context.Context, job *Job[withKindAliasesArgs]) error {
			return nil
		}))

		client, err := NewClient(riverpgxv5.New(bundle.dbPool), bundle.config)
		require.NoError(t, err)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		// withKindAliasesCollisionArgs returns withKindAliasesArgs's
		// KindAliases as its primary Kind
		_, err = client.Insert(ctx, withKindAliasesCollisionArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, []string{event.Job.Kind}, (withKindAliasesArgs{}).KindAliases())
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		clientWithStop := &clientWithSimpleStop[pgx.Tx]{Client: client}

		startstoptest.Stress(ctx, t, clientWithStop)
	})
}

type workerWithMiddleware[T JobArgs] struct {
	WorkerDefaults[T]

	workFunc       func(context.Context, *Job[T]) error
	middlewareFunc func(*rivertype.JobRow) []rivertype.WorkerMiddleware
}

func (w *workerWithMiddleware[T]) Middleware(job *rivertype.JobRow) []rivertype.WorkerMiddleware {
	return w.middlewareFunc(job)
}

func (w *workerWithMiddleware[T]) Work(ctx context.Context, job *Job[T]) error {
	return w.workFunc(ctx, job)
}

func Test_Client_Stop_Common(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Performs continual job insertion on a number of background goroutines.
	// Returns a `finish` function that should be deferred to stop insertion and
	// safely stop goroutines.
	doParallelContinualInsertion := func(ctx context.Context, t *testing.T, client *Client[pgx.Tx], args JobArgs) func() {
		t.Helper()

		ctx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					_, err := client.Insert(ctx, args, nil)
					// A cancelled context may produce a variety of underlying
					// errors in pgx, so rather than comparing the return error,
					// first check if context is cancelled, and ignore an error
					// return if it is.
					if ctx.Err() != nil {
						return
					}
					require.NoError(t, err)

					// Sleep a brief time between inserts.
					serviceutil.CancellableSleep(ctx, randutil.DurationBetween(1*time.Microsecond, 10*time.Millisecond))
				}
			})
		}

		return func() {
			cancel()
			wg.Wait()
		}
	}

	t.Run("NoJobsInProgress", func(t *testing.T) {
		t.Parallel()

		client := runNewTestClient(ctx, t, newTestConfig(t, ""))

		// Should shut down quickly:
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("JobsInProgressCompletingPromptly", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client := newTestClient(t, dbPool, config)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var (
			doneCh    = make(chan struct{})
			startedCh = make(chan int64)
		)
		AddWorker(client.config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

		// enqueue job:
		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		startClient(ctx, t, client)

		startedJobID := riversharedtest.WaitOrTimeout(t, startedCh)
		require.Equal(t, insertRes.Job.ID, startedJobID)

		// Should not shut down immediately, not until jobs are given the signal to
		// complete:
		go func() {
			<-time.After(50 * time.Millisecond)
			close(doneCh)
		}()

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("JobsInProgressFailingToCompleteBeforeStopContext", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		jobDoneChan := make(chan struct{})
		jobStartedChan := make(chan int64)
		jobCanceledChan := make(chan struct{}, 1)

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobStartedChan <- job.ID:
			}

			select {
			case <-ctx.Done():
				// Graceful Stop should wait for in-progress work rather than
				// cancelling worker context (that's StopAndCancel behavior).
				select {
				case jobCanceledChan <- struct{}{}:
				default:
				}
				return ctx.Err()
			case <-jobDoneChan:
			}

			return nil
		}))

		client := runNewTestClient(ctx, t, config)

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riversharedtest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertRes.Job.ID, startedJobID)

		t.Logf("Shutting down client with timeout, but while jobs are still in progress")

		// Context should expire while jobs are still in progress:
		stopCtx, stopCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		t.Cleanup(stopCancel)

		// Keep the job blocked while waiting for Stop so this assertion doesn't
		// depend on relative scheduling of short timers in CI.
		err = client.Stop(stopCtx)
		require.Equal(t, context.DeadlineExceeded, err)

		select {
		case <-jobCanceledChan:
			require.FailNow(t, "Did not expect job to be cancelled")
		default:
		}

		// The first Stop timed out by design. Now release the worker and stop
		// again so test cleanup doesn't race an in-progress shutdown.
		close(jobDoneChan)
		require.NoError(t, client.Stop(ctx))
	})

	t.Run("WithContinualInsertionNoJobsLeftRunning", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		startedCh := make(chan int64)
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			select {
			case startedCh <- job.ID:
			default:
			}
			return nil
		}))

		client := runNewTestClient(ctx, t, config)

		finish := doParallelContinualInsertion(ctx, t, client, JobArgs{})
		t.Cleanup(finish)

		// Wait for at least one job to start
		riversharedtest.WaitOrTimeout(t, startedCh)

		require.NoError(t, client.Stop(ctx))

		listRes, err := client.JobList(ctx, NewJobListParams().States(rivertype.JobStateRunning))
		require.NoError(t, err)
		require.Empty(t, listRes.Jobs, "expected no jobs to be left running")
	})

	t.Run("WithSubscriber", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		client := runNewTestClient(ctx, t, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		defer cancel()

		finish := doParallelContinualInsertion(ctx, t, client, JobArgs{})
		defer finish()

		// Arbitrarily wait for 100 jobs to come through.
		for range 100 {
			riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		require.NoError(t, client.Stop(ctx))
	})
}

func Test_Client_Stop_ContextImmediatelyCancelled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		config = newTestConfig(t, schema)
	)
	config.Schema = schema

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	// doneCh will never close, job will exit due to context cancellation:
	var (
		doneCh    = make(chan struct{})
		startedCh = make(chan int64)
	)
	AddWorker(config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

	client := newTestClient(t, dbPool, config)

	// Doesn't use newTestClient because it turns out that the test will fail on
	// waiting for stop to timeout if a non-background context is used. I added
	// the test below (Test_Client_Stop_AfterContextCancelled, currently
	// skipped) to reproduce the problem and which we should fix.
	require.NoError(t, client.Start(ctx))
	t.Cleanup(func() { require.NoError(t, client.Stop(context.Background())) })

	insertRes, err := client.Insert(ctx, JobArgs{}, nil)
	require.NoError(t, err)
	startedJobID := riversharedtest.WaitOrTimeout(t, startedCh)
	require.Equal(t, insertRes.Job.ID, startedJobID)

	cancel()

	require.ErrorIs(t, client.Stop(ctx), context.Canceled)
}

// Added this failing test case as skipped while working on another project,
// detecting a problem that the test case above was glossing over, but finding
// it not easy to fix so decided to punt.
//
// An initial Stop is called on the client with a cancelled context, but then
// Stop is called again with a non-cancelled one through startClient. Because
// the client already semi-stopped (I think this is the reason anyway), it
// doesn't cancel jobs in progress, so doneCh not being closed ends up hanging
// the test until it's eventually killed by the context timeout in startClient.
//
// TODO: Remove the Skip below and fix the problem.
func Test_Client_Stop_AfterContextCancelled(t *testing.T) {
	t.Parallel()

	t.Skip("this test case was added broken and should be fixed")

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		config = newTestConfig(t, schema)
	)
	config.Schema = schema

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	// doneCh will never close, job will exit due to context cancellation:
	var (
		doneCh    = make(chan struct{})
		startedCh = make(chan int64)
	)
	AddWorker(config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

	client := newTestClient(t, dbPool, config)
	subscribeChan := subscribe(t, client)

	startClient(ctx, t, client)

	insertRes, err := client.Insert(ctx, JobArgs{}, nil)
	require.NoError(t, err)
	event := riversharedtest.WaitOrTimeout(t, subscribeChan)
	require.Equal(t, insertRes.Job.ID, event.Job.ID)

	ctx, cancel := context.WithCancel(ctx)
	cancel()

	require.ErrorIs(t, client.Stop(ctx), context.Canceled)
}

func Test_Client_StopAndCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	type testBundle struct {
		jobDoneChan    chan struct{}
		jobStartedChan chan int64
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, "")

		var (
			jobStartedChan = make(chan int64)
			jobDoneChan    = make(chan struct{})
		)
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			t.Logf("Job waiting for context cancellation")
			defer t.Logf("Job finished")
			<-ctx.Done()
			t.Logf("Job context done, closing chan and returning")
			close(jobDoneChan)
			return nil
		}))

		client := runNewTestClient(ctx, t, config)

		return client, &testBundle{
			jobDoneChan:    jobDoneChan,
			jobStartedChan: jobStartedChan,
		}
	}

	t.Run("OnItsOwn", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, bundle.jobStartedChan)

		require.NoError(t, client.StopAndCancel(ctx))
		riversharedtest.WaitOrTimeout(t, client.Stopped())
	})

	t.Run("BeforeStart", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		require.NoError(t, client.StopAndCancel(ctx))
		riversharedtest.WaitOrTimeout(t, client.Stopped()) // this works because Stopped is nil
	})

	t.Run("AfterStop", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, bundle.jobStartedChan)

		go func() {
			require.NoError(t, client.Stop(ctx))
		}()

		select {
		case <-client.Stopped():
			t.Fatal("expected client to not be stopped yet")
		case <-time.After(500 * time.Millisecond):
		}

		require.NoError(t, client.StopAndCancel(ctx))
		riversharedtest.WaitOrTimeout(t, client.Stopped())

		select {
		case <-bundle.jobDoneChan:
		default:
			t.Fatal("expected job to have exited")
		}
	})
}

type callbackWithCustomTimeoutArgs struct {
	TimeoutValue time.Duration `json:"timeout"`
}

func (callbackWithCustomTimeoutArgs) Kind() string { return "callbackWithCustomTimeout" }

type callbackWorkerWithCustomTimeout struct {
	WorkerDefaults[callbackWithCustomTimeoutArgs]

	fn func(context.Context, *Job[callbackWithCustomTimeoutArgs]) error
}

func (w *callbackWorkerWithCustomTimeout) Work(ctx context.Context, job *Job[callbackWithCustomTimeoutArgs]) error {
	return w.fn(ctx, job)
}

func (w *callbackWorkerWithCustomTimeout) Timeout(job *Job[callbackWithCustomTimeoutArgs]) time.Duration {
	return job.Args.TimeoutValue
}

func Test_Client_JobContextInheritsFromProvidedContext(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(2 * time.Minute)

	jobCtxCh := make(chan context.Context)
	doneCh := make(chan struct{})
	close(doneCh)

	callbackFunc := func(ctx context.Context, job *Job[callbackWithCustomTimeoutArgs]) error {
		// indicate the job has started, unless context is already done:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case jobCtxCh <- ctx:
		}
		return nil
	}
	config := newTestConfig(t, "")
	AddWorker(config.Workers, &callbackWorkerWithCustomTimeout{fn: callbackFunc})

	// Set a deadline and a value on the context for the client so we can verify
	// it's propagated through to the job:
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancel)

	type customContextKey string
	ctx = context.WithValue(ctx, customContextKey("BestGoPostgresQueue"), "River")
	client := runNewTestClient(ctx, t, config)

	insertCtx, insertCancel := context.WithTimeout(ctx, time.Second)
	t.Cleanup(insertCancel)

	// enqueue job:
	_, err := client.Insert(insertCtx, callbackWithCustomTimeoutArgs{TimeoutValue: 5 * time.Minute}, nil)
	require.NoError(t, err)

	var jobCtx context.Context
	select {
	case jobCtx = <-jobCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}

	require.Equal(t, "River", jobCtx.Value(customContextKey("BestGoPostgresQueue")), "job should persist the context value from the client context")
	jobDeadline, ok := jobCtx.Deadline()
	require.True(t, ok, "job should have a deadline")
	require.Equal(t, deadline, jobDeadline, "job should have the same deadline as the client context (shorter than the job's timeout)")
}

func Test_Client_ClientFromContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	config := newTestConfig(t, "")

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	var (
		clientResult *Client[pgx.Tx]
		jobDoneChan  = make(chan struct{})
	)
	AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
		clientResult = ClientFromContext[pgx.Tx](ctx)
		close(jobDoneChan)
		return nil
	}))

	client := runNewTestClient(ctx, t, config)

	_, err := client.Insert(ctx, JobArgs{}, nil)
	require.NoError(t, err)

	riversharedtest.WaitOrTimeout(t, jobDoneChan)

	require.NotNil(t, clientResult)
	require.Equal(t, client, clientResult)
}

func Test_Client_JobDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{dbPool: dbPool}
	}

	t.Run("DeletesANonRunningJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, insertRes.Job.State)

		jobAfter, err := client.JobDelete(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.NotNil(t, jobAfter)
		require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)

		_, err = client.JobGet(ctx, insertRes.Job.ID)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DoesNotDeleteARunningJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var (
			doneCh    = make(chan struct{})
			startedCh = make(chan int64)
		)
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			close(startedCh)
			<-doneCh
			return nil
		}))

		require.NoError(t, client.Start(ctx))
		t.Cleanup(func() { require.NoError(t, client.Stop(ctx)) })
		t.Cleanup(func() { close(doneCh) }) // must close before stopping client

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		// Wait for the job to start:
		riversharedtest.WaitOrTimeout(t, startedCh)

		jobAfter, err := client.JobDelete(ctx, insertRes.Job.ID)
		require.ErrorIs(t, err, rivertype.ErrJobRunning)
		require.Nil(t, jobAfter)

		jobFromGet, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, jobFromGet.State)
	})

	t.Run("TxVariantAlsoDeletesANonRunningJob", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, insertRes.Job.State)

		var jobAfter *rivertype.JobRow

		err = pgx.BeginFunc(ctx, bundle.dbPool, func(tx pgx.Tx) error {
			var err error
			jobAfter, err = client.JobDeleteTx(ctx, tx, insertRes.Job.ID)
			return err
		})
		require.NoError(t, err)
		require.NotNil(t, jobAfter)
		require.Equal(t, insertRes.Job.ID, jobAfter.ID)
		require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)

		jobFromGet, err := client.JobGet(ctx, insertRes.Job.ID)
		require.ErrorIs(t, ErrNotFound, err)
		require.Nil(t, jobFromGet)
	})

	t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		jobAfter, err := client.JobDelete(ctx, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, jobAfter)
	})
}

func Test_Client_JobDeleteTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		exec   riverdriver.Executor
		execTx riverdriver.ExecutorTx
		schema string
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			dbPool: dbPool,
			exec:   driver.GetExecutor(),
			execTx: driver.UnwrapExecutor(tx),
			schema: schema,
			tx:     tx,
		}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		deletedJob, err := client.JobDeleteTx(ctx, bundle.tx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, job1.ID, deletedJob.ID)

		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)
		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)

		// Both jobs present because other transaction doesn't see the deletion.
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.NoError(t, err)
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
	})
}

func Test_Client_JobDeleteMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			exec:   client.driver.GetExecutor(),
			schema: schema,
		}
	}

	t.Run("FiltersByID", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().IDs(job1.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)
		_, err = client.JobGet(ctx, job2.ID)
		require.NoError(t, err)
		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().IDs(job2.ID, job3.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		// job4 should still be present
		_, err = client.JobGet(ctx, job4.ID)
		require.NoError(t, err)
	})

	t.Run("FiltersByIDAndPriorityAndKind", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("special_kind"), Priority: ptrutil.Ptr(1)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("special_kind"), Priority: ptrutil.Ptr(2)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("other_kind"), Priority: ptrutil.Ptr(1)})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().IDs(job1.ID, job2.ID, job3.ID).Priorities(1, 2).Kinds("special_kind"))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)
	})

	t.Run("FiltersByPriority", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(1)})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(2)})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(3)})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().Priorities(1))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job2.ID)
		require.NoError(t, err)
		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().Priorities(2, 3))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByKind", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1"), Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1"), Schema: bundle.schema})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_2"), Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().Kinds("test_kind_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().Kinds("test_kind_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByQueue", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1"), Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1"), Schema: bundle.schema})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_2"), Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().Queues("queue_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().Queues("queue_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByState", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), Schema: bundle.schema})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), Schema: bundle.schema})
			job4 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStatePending), Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().States(rivertype.JobStateAvailable))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job3.ID)
		require.NoError(t, err)
		_, err = client.JobGet(ctx, job4.ID)
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().States(rivertype.JobStateCompleted))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job4.ID)
		require.NoError(t, err)

		// All by default:
		deleteRes, err = client.JobDeleteMany(ctx, NewJobDeleteManyParams().UnsafeAll())
		require.NoError(t, err)
		require.Equal(t, []int64{job4.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("DeletesAllWithUnsafeAll", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			now  = time.Now().UTC()
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: &now})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), ScheduledAt: ptrutil.Ptr(now.Add(-2 * time.Second))})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().UnsafeAll())
		require.NoError(t, err)
		// sort order defaults to ID
		require.Equal(t, []int64{job1.ID, job2.ID, job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("EmptyFiltersError", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		_, err := client.JobDeleteMany(ctx, nil)
		require.EqualError(t, err, "delete with no filters not allowed to prevent accidental deletion of all jobs; either specify a predicate (e.g. JobDeleteManyParams.IDs, JobDeleteManyParams.Kinds, ...) or call JobDeleteManyParams.All")
	})

	t.Run("IgnoresRunningJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		)

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().IDs(job1.ID, job2.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = client.JobGet(ctx, job2.ID)
		require.NoError(t, err)
	})

	t.Run("WithCancelledContext", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		deleteRes, err := client.JobDeleteMany(ctx, NewJobDeleteManyParams().States(rivertype.JobStateRunning))
		require.ErrorIs(t, context.Canceled, err)
		require.Nil(t, deleteRes)
	})
}

func Test_Client_JobDeleteManyTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		exec   riverdriver.Executor
		execTx riverdriver.ExecutorTx
		schema string
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			dbPool: dbPool,
			exec:   driver.GetExecutor(),
			execTx: driver.UnwrapExecutor(tx),
			schema: schema,
			tx:     tx,
		}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		deleteRes, err := client.JobDeleteManyTx(ctx, bundle.tx, NewJobDeleteManyParams().IDs(job1.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)
		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID, Schema: bundle.schema})
		require.NoError(t, err)

		deleteRes, err = client.JobDeleteManyTx(ctx, bundle.tx, NewJobDeleteManyParams().IDs(job2.ID, job3.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(deleteRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)
		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)
		_, err = bundle.execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID, Schema: bundle.schema})
		require.ErrorIs(t, rivertype.ErrNotFound, err)

		// Jobs present because other transaction doesn't see the deletion.
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.NoError(t, err)
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
	})
}

func Test_Client_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			dbPool: dbPool,
			schema: schema,
		}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)
		jobRow := insertRes.Job
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, rivercommon.MaxAttemptsDefault, jobRow.MaxAttempts)
		require.JSONEq(t, "{}", string(jobRow.Metadata))
		require.Equal(t, (&noOpArgs{}).Kind(), jobRow.Kind)
		require.Equal(t, PriorityDefault, jobRow.Priority)
		require.Equal(t, QueueDefault, jobRow.Queue)
		require.Equal(t, []string{}, jobRow.Tags)
	})

	t.Run("ProducerFetchLimiterCalled", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPoolClone(ctx, t)
			driver = NewDriverWithoutListenNotify(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client, err := NewClient(driver, config)
		require.NoError(t, err)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		client.producersByQueueName[QueueDefault].testSignals.JobFetchTriggered.WaitOrTimeout()
	})

	// Not called for drivers that support a listener.
	t.Run("ProducerFetchLimiterNotCalled", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		client.producersByQueueName[QueueDefault].testSignals.JobFetchTriggered.RequireEmpty()
	})

	t.Run("WithInsertOpts", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{
			MaxAttempts: 17,
			Metadata:    []byte(`{"foo": "bar"}`),
			Priority:    3,
			Queue:       "custom",
			Tags:        []string{"custom"},
		})
		jobRow := insertRes.Job
		require.NoError(t, err)
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, 17, jobRow.MaxAttempts)
		require.Equal(t, (&noOpArgs{}).Kind(), jobRow.Kind)
		require.JSONEq(t, `{"foo": "bar"}`, string(jobRow.Metadata))
		require.WithinDuration(t, time.Now(), jobRow.ScheduledAt, 2*time.Second)
		require.Equal(t, 3, jobRow.Priority)
		require.Equal(t, "custom", jobRow.Queue)
		require.Equal(t, []string{"custom"}, jobRow.Tags)
	})

	t.Run("WithInsertOptsScheduledAtZeroTime", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Time{},
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), insertRes.Job.ScheduledAt, 2*time.Second)
	})

	t.Run("OnlyTriggersInsertNotificationForAvailableJobs", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		_, bundle := setup(t)

		config := newTestConfig(t, bundle.schema)
		config.FetchCooldown = 5 * time.Second
		config.FetchPollInterval = 5 * time.Second

		client := newTestClient(t, bundle.dbPool, config)

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		_, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{Queue: "a", ScheduledAt: time.Now().Add(1 * time.Hour)})
		require.NoError(t, err)

		// Queue `a` should be "due" to be triggered because it wasn't triggered above.
		require.True(t, client.insertNotifyLimiter.ShouldTrigger("a"))

		_, err = client.Insert(ctx, noOpArgs{}, &InsertOpts{Queue: "b"})
		require.NoError(t, err)

		// Queue `b` should *not* be "due" to be triggered because it was triggered above.
		require.False(t, client.insertNotifyLimiter.ShouldTrigger("b"))

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("WithUniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		job1, err := client.Insert(ctx, noOpArgs{Name: "foo"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.NotNil(t, job1)

		// Dupe, same args:
		job2, err := client.Insert(ctx, noOpArgs{Name: "foo"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.Equal(t, job1.Job.ID, job2.Job.ID)

		// Not a dupe, different args
		job3, err := client.Insert(ctx, noOpArgs{Name: "bar"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.NotEqual(t, job1.Job.ID, job3.Job.ID)
	})

	t.Run("ErrorsOnInvalidQueueName", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		_, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{Queue: "invalid*queue"})
		require.ErrorContains(t, err, "queue name is invalid")
	})

	t.Run("ErrorsOnDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, _ = setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)

		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		require.ErrorIs(t, err, errNoDriverDBPool)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		_, err := client.Insert(ctx, &unregisteredJobArgs{}, nil)
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.Workers = nil

		_, err := client.Insert(ctx, &unregisteredJobArgs{}, nil)
		require.NoError(t, err)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.SkipUnknownJobCheck = true

		_, err := client.Insert(ctx, &unregisteredJobArgs{}, nil)
		require.NoError(t, err)
	})
}

func Test_Client_InsertTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			schema: schema,
			tx:     tx,
		}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.InsertTx(ctx, bundle.tx, &noOpArgs{}, nil)
		require.NoError(t, err)
		jobRow := insertRes.Job
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, rivercommon.MaxAttemptsDefault, jobRow.MaxAttempts)
		require.Equal(t, (&noOpArgs{}).Kind(), jobRow.Kind)
		require.Equal(t, PriorityDefault, jobRow.Priority)
		require.Equal(t, QueueDefault, jobRow.Queue)
		require.Equal(t, []string{}, jobRow.Tags)

		// Job is not visible outside of the transaction.
		_, err = client.JobGet(ctx, jobRow.ID)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("WithInsertOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.InsertTx(ctx, bundle.tx, &noOpArgs{}, &InsertOpts{
			MaxAttempts: 17,
			Priority:    3,
			Queue:       "custom",
			Tags:        []string{"custom"},
		})
		jobRow := insertRes.Job
		require.NoError(t, err)
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, 17, jobRow.MaxAttempts)
		require.Equal(t, (&noOpArgs{}).Kind(), jobRow.Kind)
		require.Equal(t, 3, jobRow.Priority)
		require.Equal(t, "custom", jobRow.Queue)
		require.Equal(t, []string{"custom"}, jobRow.Tags)
	})

	// A client's allowed to send nil to their driver so they can, for example,
	// easily use test transactions in their test suite.
	t.Run("WithDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		_, err = client.InsertTx(ctx, bundle.tx, &noOpArgs{}, nil)
		require.NoError(t, err)
	})

	t.Run("WithUniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1, err := client.InsertTx(ctx, bundle.tx, noOpArgs{Name: "foo"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.NotNil(t, job1)

		// Dupe, same args:
		job2, err := client.InsertTx(ctx, bundle.tx, noOpArgs{Name: "foo"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.Equal(t, job1.Job.ID, job2.Job.ID)

		// Not a dupe, different args
		job3, err := client.InsertTx(ctx, bundle.tx, noOpArgs{Name: "bar"}, &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}})
		require.NoError(t, err)
		require.NotEqual(t, job1.Job.ID, job3.Job.ID)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		_, err := client.InsertTx(ctx, bundle.tx, &unregisteredJobArgs{}, nil)
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.Workers = nil

		_, err := client.InsertTx(ctx, bundle.tx, &unregisteredJobArgs{}, nil)
		require.NoError(t, err)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.SkipUnknownJobCheck = true

		_, err := client.InsertTx(ctx, bundle.tx, &unregisteredJobArgs{}, nil)
		require.NoError(t, err)
	})
}

func Test_Client_InsertManyFast(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			dbPool: dbPool,
			schema: schema,
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})

		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())
	})

	t.Run("TriggersImmediateWork", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		_, bundle := setup(t)

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)

		config := newTestConfig(t, bundle.schema)
		config.FetchCooldown = 20 * time.Millisecond
		config.FetchPollInterval = 20 * time.Second // essentially disable polling
		config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 2}, "another_queue": {MaxWorkers: 1}}

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		doneCh := make(chan struct{})
		close(doneCh) // don't need to block any jobs from completing
		startedCh := make(chan int64)
		AddWorker(config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

		client := newTestClient(t, bundle.dbPool, config)

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: JobArgs{}},
			{Args: JobArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Wait for the client to be ready by waiting for a job to be executed:
		riversharedtest.WaitOrTimeoutN(t, startedCh, 2)

		// Now that we've run one job, we shouldn't take longer than the cooldown to
		// fetch another after insertion. LISTEN/NOTIFY should ensure we find out
		// about the inserted job much faster than the poll interval.
		//
		// Note: we specifically use a different queue to ensure that the notify
		// limiter is immediately to fire on this queue.
		count, err = client.InsertManyFast(ctx, []InsertManyParams{
			{Args: JobArgs{}, InsertOpts: &InsertOpts{Queue: "another_queue"}},
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		select {
		case <-startedCh:
		// As long as this is meaningfully shorter than the poll interval, we can be
		// sure the re-fetch came from listen/notify.
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for another_queue job to start")
		}

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("DoesNotTriggerInsertNotificationForNonAvailableJob", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		_, bundle := setup(t)

		config := newTestConfig(t, bundle.schema)
		config.FetchCooldown = 5 * time.Second
		config.FetchPollInterval = 5 * time.Second

		client := newTestClient(t, bundle.dbPool, config)

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "a", ScheduledAt: time.Now().Add(1 * time.Hour)}},
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "b"}},
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)

		// Queue `a` should be "due" to be triggered because it wasn't triggered above.
		require.True(t, client.insertNotifyLimiter.ShouldTrigger("a"))
		// Queue `b` should *not* be "due" to be triggered because it was triggered above.
		require.False(t, client.insertNotifyLimiter.ShouldTrigger("b"))

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("WithInsertOptsScheduledAtZeroTime", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		now := time.Now().UTC()
		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{ScheduledAt: time.Time{}}},
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})

		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(noOpArgs{}).Kind())
		jobRow := jobs[0]
		require.WithinDuration(t, now, jobRow.ScheduledAt, 5*time.Second)
	})

	t.Run("ErrorsOnInvalidQueueName", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{Queue: "invalid*queue"}},
		})
		require.ErrorContains(t, err, "queue name is invalid")
		require.Equal(t, 0, count)
	})

	t.Run("ErrorsOnDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, _ = setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.ErrorIs(t, err, errNoDriverDBPool)
		require.Equal(t, 0, count)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Equal(t, 0, count)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Equal(t, 0, count)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.Workers = nil

		_, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.SkipUnknownJobCheck = true

		_, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("ErrorsOnInsertOptsWithoutRequiredUniqueStates", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertManyFast(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{
				ByArgs: true,
				// Attempt a custom state list that isn't supported in v3 unique jobs:
				ByState: []rivertype.JobState{rivertype.JobStateAvailable},
			}}},
		})
		require.EqualError(t, err, "UniqueOpts.ByState must contain all required states, missing: pending, running, scheduled")
		require.Equal(t, 0, count)
	})
}

func Test_Client_InsertManyFastTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			schema: schema,
			tx:     tx,
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, 2, count)

		jobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})

		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())

		require.NoError(t, bundle.tx.Commit(ctx))

		// Ensure the jobs are visible outside the transaction:
		jobs, err = client.driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())
	})

	t.Run("SetsScheduledAtToNowByDefault", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		_, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{{noOpArgs{}, nil}})
		require.NoError(t, err)

		insertedJobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)
		require.Equal(t, rivertype.JobStateAvailable, insertedJobs[0].State)
		require.WithinDuration(t, time.Now(), insertedJobs[0].ScheduledAt, 2*time.Second)
	})

	t.Run("SupportsScheduledJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{{noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Minute)}}})
		require.NoError(t, err)
		require.Equal(t, 1, count)

		insertedJobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)
		require.Equal(t, rivertype.JobStateScheduled, insertedJobs[0].State)
		require.WithinDuration(t, time.Now().Add(time.Minute), insertedJobs[0].ScheduledAt, 2*time.Second)
	})

	// A client's allowed to send nil to their driver so they can, for example,
	// easily use test transactions in their test suite.
	t.Run("WithDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Equal(t, 0, count)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Equal(t, 0, count)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.Workers = nil

		_, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.SkipUnknownJobCheck = true

		_, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("ErrorsOnInsertOptsWithV1UniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyFastTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{
				ByArgs: true,
				// force the v1 unique path with a custom state list that isn't supported in v3:
				ByState: []rivertype.JobState{rivertype.JobStateAvailable},
			}}},
		})
		require.EqualError(t, err, "UniqueOpts.ByState must contain all required states, missing: pending, running, scheduled")
		require.Equal(t, 0, count)
	})
}

func Test_Client_InsertMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			dbPool: dbPool,
			schema: schema,
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		now := time.Now().UTC()

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{Name: "Foo"}, InsertOpts: &InsertOpts{Metadata: []byte(`{"a": "b"}`), Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{ScheduledAt: now.Add(time.Minute)}},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)

		require.False(t, results[0].UniqueSkippedAsDuplicate)
		require.Equal(t, 0, results[0].Job.Attempt)
		require.Nil(t, results[0].Job.AttemptedAt)
		require.WithinDuration(t, now, results[0].Job.CreatedAt, 2*time.Second)
		require.Empty(t, results[0].Job.AttemptedBy)
		require.Positive(t, results[0].Job.ID)
		require.JSONEq(t, `{"name": "Foo"}`, string(results[0].Job.EncodedArgs))
		require.Empty(t, results[0].Job.Errors)
		require.Nil(t, results[0].Job.FinalizedAt)
		require.Equal(t, "noOp", results[0].Job.Kind)
		require.Equal(t, 25, results[0].Job.MaxAttempts)
		require.JSONEq(t, `{"a": "b"}`, string(results[0].Job.Metadata))
		require.Equal(t, 2, results[0].Job.Priority)
		require.Equal(t, "foo", results[0].Job.Queue)
		require.WithinDuration(t, now, results[0].Job.ScheduledAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateAvailable, results[0].Job.State)
		require.Empty(t, results[0].Job.Tags)
		require.Empty(t, results[0].Job.UniqueKey)

		require.False(t, results[1].UniqueSkippedAsDuplicate)
		require.Equal(t, 0, results[1].Job.Attempt)
		require.Nil(t, results[1].Job.AttemptedAt)
		require.WithinDuration(t, now, results[1].Job.CreatedAt, 2*time.Second)
		require.Empty(t, results[1].Job.AttemptedBy)
		require.Positive(t, results[1].Job.ID)
		require.JSONEq(t, `{"name": ""}`, string(results[1].Job.EncodedArgs))
		require.Empty(t, results[1].Job.Errors)
		require.Nil(t, results[1].Job.FinalizedAt)
		require.Equal(t, "noOp", results[1].Job.Kind)
		require.Equal(t, 25, results[1].Job.MaxAttempts)
		require.JSONEq(t, `{}`, string(results[1].Job.Metadata))
		require.Equal(t, 1, results[1].Job.Priority)
		require.Equal(t, "default", results[1].Job.Queue)
		require.WithinDuration(t, now.Add(time.Minute), results[1].Job.ScheduledAt, time.Millisecond)
		require.Equal(t, rivertype.JobStateScheduled, results[1].Job.State)
		require.Empty(t, results[1].Job.Tags)
		require.Empty(t, results[1].Job.UniqueKey)

		require.NotEqual(t, results[0].Job.ID, results[1].Job.ID)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())
	})

	t.Run("TriggersImmediateWork", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		_, bundle := setup(t)

		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		t.Cleanup(cancel)

		config := newTestConfig(t, bundle.schema)
		config.FetchCooldown = 20 * time.Millisecond
		config.FetchPollInterval = 20 * time.Second // essentially disable polling
		config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 2}, "another_queue": {MaxWorkers: 1}}

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		doneCh := make(chan struct{})
		close(doneCh) // don't need to block any jobs from completing
		startedCh := make(chan int64)
		AddWorker(config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

		client := newTestClient(t, bundle.dbPool, config)

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: JobArgs{}},
			{Args: JobArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Wait for the client to be ready by waiting for a job to be executed:
		riversharedtest.WaitOrTimeoutN(t, startedCh, 2)

		// Now that we've run one job, we shouldn't take longer than the cooldown to
		// fetch another after insertion. LISTEN/NOTIFY should ensure we find out
		// about the inserted job much faster than the poll interval.
		//
		// Note: we specifically use a different queue to ensure that the notify
		// limiter is immediately to fire on this queue.
		results, err = client.InsertMany(ctx, []InsertManyParams{
			{Args: JobArgs{}, InsertOpts: &InsertOpts{Queue: "another_queue"}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)

		select {
		case <-startedCh:
		// As long as this is meaningfully shorter than the poll interval, we can be
		// sure the re-fetch came from listen/notify.
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for another_queue job to start")
		}

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("DoesNotTriggerInsertNotificationForNonAvailableJob", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()

		_, bundle := setup(t)

		config := newTestConfig(t, bundle.schema)
		config.FetchCooldown = 5 * time.Second
		config.FetchPollInterval = 5 * time.Second

		client := newTestClient(t, bundle.dbPool, config)

		startClient(ctx, t, client)
		riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "a", ScheduledAt: time.Now().Add(1 * time.Hour)}},
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "b"}},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Queue `a` should be "due" to be triggered because it wasn't triggered above.
		require.True(t, client.insertNotifyLimiter.ShouldTrigger("a"))
		// Queue `b` should *not* be "due" to be triggered because it was triggered above.
		require.False(t, client.insertNotifyLimiter.ShouldTrigger("b"))

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("WithInsertOptsScheduledAtZeroTime", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{ScheduledAt: time.Time{}}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(noOpArgs{}).Kind())
		jobRow := jobs[0]
		require.WithinDuration(t, time.Now(), jobRow.ScheduledAt, 2*time.Second)
	})

	t.Run("ErrorsOnInvalidQueueName", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{Queue: "invalid*queue"}},
		})
		require.ErrorContains(t, err, "queue name is invalid")
		require.Nil(t, results)
	})

	t.Run("ErrorsOnDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, _ = setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.ErrorIs(t, err, errNoDriverDBPool)
		require.Nil(t, results)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		results, err := client.InsertMany(ctx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Nil(t, results)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Nil(t, results)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.Workers = nil

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.SkipUnknownJobCheck = true

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("ErrorsOnInsertOptsWithV1UniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		results, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{
				ByArgs: true,
				// force the v1 unique path with a custom state list that isn't supported in v3:
				ByState: []rivertype.JobState{rivertype.JobStateAvailable},
			}}},
		})
		require.EqualError(t, err, "UniqueOpts.ByState must contain all required states, missing: pending, running, scheduled")
		require.Empty(t, results)
	})
}

func Test_Client_InsertManyTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			schema: schema,
			tx:     tx,
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{Name: "Foo"}, InsertOpts: &InsertOpts{Metadata: []byte(`{"a": "b"}`), Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{ScheduledAt: now.Add(time.Minute)}},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)

		require.False(t, results[0].UniqueSkippedAsDuplicate)
		require.Equal(t, 0, results[0].Job.Attempt)
		require.Nil(t, results[0].Job.AttemptedAt)
		require.WithinDuration(t, now, results[0].Job.CreatedAt, 2*time.Second)
		require.Empty(t, results[0].Job.AttemptedBy)
		require.Positive(t, results[0].Job.ID)
		require.JSONEq(t, `{"name": "Foo"}`, string(results[0].Job.EncodedArgs))
		require.Empty(t, results[0].Job.Errors)
		require.Nil(t, results[0].Job.FinalizedAt)
		require.Equal(t, "noOp", results[0].Job.Kind)
		require.Equal(t, 25, results[0].Job.MaxAttempts)
		require.JSONEq(t, `{"a": "b"}`, string(results[0].Job.Metadata))
		require.Equal(t, 2, results[0].Job.Priority)
		require.Equal(t, "foo", results[0].Job.Queue)
		require.WithinDuration(t, now, results[0].Job.ScheduledAt, 2*time.Second)
		require.Equal(t, rivertype.JobStateAvailable, results[0].Job.State)
		require.Empty(t, results[0].Job.Tags)
		require.Empty(t, results[0].Job.UniqueKey)

		require.False(t, results[1].UniqueSkippedAsDuplicate)
		require.Equal(t, 0, results[1].Job.Attempt)
		require.Nil(t, results[1].Job.AttemptedAt)
		require.WithinDuration(t, now, results[1].Job.CreatedAt, 2*time.Second)
		require.Empty(t, results[1].Job.AttemptedBy)
		require.Positive(t, results[1].Job.ID)
		require.JSONEq(t, `{"name": ""}`, string(results[1].Job.EncodedArgs))
		require.Empty(t, results[1].Job.Errors)
		require.Nil(t, results[1].Job.FinalizedAt)
		require.Equal(t, "noOp", results[1].Job.Kind)
		require.Equal(t, 25, results[1].Job.MaxAttempts)
		require.JSONEq(t, `{}`, string(results[1].Job.Metadata))
		require.Equal(t, 1, results[1].Job.Priority)
		require.Equal(t, "default", results[1].Job.Queue)
		require.WithinDuration(t, now.Add(time.Minute), results[1].Job.ScheduledAt, time.Millisecond)
		require.Equal(t, rivertype.JobStateScheduled, results[1].Job.State)
		require.Empty(t, results[1].Job.Tags)
		require.Empty(t, results[1].Job.UniqueKey)

		require.NotEqual(t, results[0].Job.ID, results[1].Job.ID)

		jobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())
	})

	t.Run("SetsScheduledAtToNowByDefault", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{noOpArgs{}, nil}})
		require.NoError(t, err)
		require.Len(t, results, 1)

		require.Equal(t, rivertype.JobStateAvailable, results[0].Job.State)
		require.WithinDuration(t, time.Now(), results[0].Job.ScheduledAt, 2*time.Second)

		insertedJobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(noOpArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, insertedJobs, 1)
		require.Equal(t, rivertype.JobStateAvailable, insertedJobs[0].State)
		require.WithinDuration(t, time.Now(), insertedJobs[0].ScheduledAt, 2*time.Second)
	})

	t.Run("SupportsScheduledJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Minute)}}})
		require.NoError(t, err)
		require.Len(t, results, 1)

		require.Equal(t, rivertype.JobStateScheduled, results[0].Job.State)
		require.WithinDuration(t, time.Now().Add(time.Minute), results[0].Job.ScheduledAt, 2*time.Second)
	})

	// A client's allowed to send nil to their driver so they can, for example,
	// easily use test transactions in their test suite.
	t.Run("WithDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
			Schema: bundle.schema,
		})
		require.NoError(t, err)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("WithJobInsertMiddleware", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		config := newTestConfig(t, bundle.schema)
		config.Queues = nil

		insertCalled := false
		var innerResults []*rivertype.JobInsertResult

		middleware := &overridableJobMiddleware{
			insertManyFunc: func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
				insertCalled = true
				var err error
				for _, params := range manyParams {
					params.Metadata, err = sjson.SetBytes(params.Metadata, "middleware", "called")
					require.NoError(t, err)
				}

				results, err := doInner(ctx)
				require.NoError(t, err)
				innerResults = results
				return results, nil
			},
		}

		config.JobInsertMiddleware = []rivertype.JobInsertMiddleware{middleware}
		driver := riverpgxv5.New(nil)
		client, err := NewClient(driver, config)
		require.NoError(t, err)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{Args: noOpArgs{}}})
		require.NoError(t, err)
		require.Len(t, results, 1)

		require.True(t, insertCalled)
		require.Len(t, innerResults, 1)
		require.Len(t, results, 1)
		require.Equal(t, innerResults[0].Job.ID, results[0].Job.ID)
		require.JSONEq(t, `{"middleware": "called"}`, string(results[0].Job.Metadata))
	})

	t.Run("MiddlewareArchetypeInitialized", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t)

		config := newTestConfig(t, bundle.schema)
		config.Queues = nil

		type MiddlewareWithBaseService struct {
			baseservice.BaseService
			JobInsertMiddlewareFunc
		}

		var (
			middleware       = &MiddlewareWithBaseService{}
			middlewareCalled bool
		)
		middleware.JobInsertMiddlewareFunc = func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
			middlewareCalled = true
			require.NotEmpty(t, middleware.Name) // if name is non-empty, it means the base service was initialized properly
			return doInner(ctx)
		}

		config.Middleware = []rivertype.Middleware{middleware}
		driver := riverpgxv5.New(nil)
		client, err := NewClient(driver, config)
		require.NoError(t, err)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{Args: noOpArgs{}}})
		require.NoError(t, err)
		require.Len(t, results, 1)

		require.True(t, middlewareCalled)
	})

	t.Run("WithUniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{Args: noOpArgs{Name: "foo"}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}}}})
		require.NoError(t, err)
		require.Len(t, results, 1)
		job1 := results[0]

		// Dupe, same args:
		results, err = client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{Args: noOpArgs{Name: "foo"}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}}}})
		require.NoError(t, err)
		job2 := results[0]
		require.Equal(t, job1.Job.ID, job2.Job.ID)

		// Not a dupe, different args
		results, err = client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{Args: noOpArgs{Name: "bar"}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}}}})
		require.NoError(t, err)
		job3 := results[0]
		require.NotEqual(t, job1.Job.ID, job3.Job.ID)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Nil(t, results)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Nil(t, results)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.Workers = nil

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("AllowsUnknownJobKindWithSkipUnknownJobCheck", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.SkipUnknownJobCheck = true

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
		require.Len(t, results, 1)
	})

	t.Run("ErrorsOnInsertOptsWithV1UniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		results, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{
				ByArgs: true,
				// force the v1 unique path with a custom state list that isn't supported in v3:
				ByState: []rivertype.JobState{rivertype.JobStateAvailable},
			}}},
		})
		require.EqualError(t, err, "UniqueOpts.ByState must contain all required states, missing: pending, running, scheduled")
		require.Empty(t, results)
	})
}

func Test_Client_JobGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{}
	}

	t.Run("FetchesAnExistingJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		job, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, insertRes.Job.ID, job.ID)
		require.Equal(t, insertRes.Job.State, job.State)
	})

	t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		job, err := client.JobGet(ctx, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, job)
	})
}

func Test_Client_JobList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			exec:   client.driver.GetExecutor(),
			schema: schema,
		}
	}

	t.Run("FiltersByID", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().IDs(job1.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().IDs(job2.ID, job3.ID))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByIDAndPriorityAndKind", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("special_kind"), Priority: ptrutil.Ptr(1)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("special_kind"), Priority: ptrutil.Ptr(2)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Kind: ptrutil.Ptr("other_kind"), Priority: ptrutil.Ptr(1)})

		listRes, err := client.JobList(ctx, NewJobListParams().IDs(job1.ID, job2.ID, job3.ID).Priorities(1, 2).Kinds("special_kind"))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByPriority", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(1)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(2)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Priority: ptrutil.Ptr(3)})

		listRes, err := client.JobList(ctx, NewJobListParams().Priorities(1))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().Priorities(2, 3))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByKind", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1"), Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1"), Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_2"), Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().Kinds("test_kind_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().Kinds("test_kind_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByQueue", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1"), Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1"), Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_2"), Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().Queues("queue_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().Queues("queue_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByState", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), Schema: bundle.schema})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStatePending), Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().States(rivertype.JobStateAvailable))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().States(rivertype.JobStateRunning))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		// All by default:
		listRes, err = client.JobList(ctx, NewJobListParams())
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID, job3.ID, job4.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("DefaultsToOrderingByID", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderDesc))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("OrderByTimeSortsAvailableRetryableAndScheduledJobsByScheduledAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()

		states := []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		}
		for _, state := range states {
			job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(state), ScheduledAt: &now, Schema: bundle.schema})
			job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(state), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second)), Schema: bundle.schema})

			listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(state))
			require.NoError(t, err)
			require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			listRes, err = client.JobList(ctx, NewJobListParams().States(state).OrderBy(JobListOrderByTime, SortOrderDesc))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		}
	})

	t.Run("OrderByTimeSortsCancelledCompletedAndDiscardedJobsByFinalizedAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()

		states := []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		}
		for _, state := range states {
			job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(state), FinalizedAt: ptrutil.Ptr(now.Add(-10 * time.Second))})
			job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(state), FinalizedAt: ptrutil.Ptr(now.Add(-15 * time.Second))})

			listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(state))
			require.NoError(t, err)
			require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			listRes, err = client.JobList(ctx, NewJobListParams().States(state).OrderBy(JobListOrderByTime, SortOrderDesc))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		}
	})

	t.Run("OrderByTimeSortsRunningJobsByAttemptedAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: &now})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

		listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(rivertype.JobStateRunning))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().States(rivertype.JobStateRunning).OrderBy(JobListOrderByTime, SortOrderDesc))
		require.NoError(t, err)
		// Sort order was explicitly reversed:
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("WithNilParamsFiltersToAllStatesByDefault", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: &now})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), ScheduledAt: ptrutil.Ptr(now.Add(-2 * time.Second))})

		listRes, err := client.JobList(ctx, nil)
		require.NoError(t, err)
		// sort order defaults to ID
		require.Equal(t, []int64{job1.ID, job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("PaginatesWithAfter_JobListOrderByID", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().After(JobListCursorFromJob(job1)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByID, listRes.LastCursor.sortField)
		require.Equal(t, job3.ID, listRes.LastCursor.id)

		// No more results
		listRes, err = client.JobList(ctx, NewJobListParams().After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Nil(t, listRes.LastCursor)

		// Descending
		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByID, SortOrderDesc).After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByID, listRes.LastCursor.sortField)
		require.Equal(t, job1.ID, listRes.LastCursor.id)
	})

	t.Run("PaginatesWithAfter_JobListOrderByScheduledAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, ScheduledAt: &now})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Second))})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, ScheduledAt: ptrutil.Ptr(now.Add(2 * time.Second))})

		listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByScheduledAt, SortOrderAsc).After(JobListCursorFromJob(job1)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByScheduledAt, listRes.LastCursor.sortField)
		require.Equal(t, job3.ID, listRes.LastCursor.id)

		// No more results
		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByScheduledAt, SortOrderAsc).After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Nil(t, listRes.LastCursor)

		// Descending
		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByScheduledAt, SortOrderDesc).After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByScheduledAt, listRes.LastCursor.sortField)
		require.Equal(t, job1.ID, listRes.LastCursor.id)
	})

	t.Run("PaginatesWithAfter_JobListOrderByTime", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: &now})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second)), AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), ScheduledAt: ptrutil.Ptr(now.Add(-6 * time.Second)), AttemptedAt: &now})
		job5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), ScheduledAt: ptrutil.Ptr(now.Add(-7 * time.Second)), FinalizedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job6 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), ScheduledAt: ptrutil.Ptr(now.Add(-7 * time.Second)), FinalizedAt: &now})

		listRes, err := client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(rivertype.JobStateAvailable).After(JobListCursorFromJob(job1)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByTime, listRes.LastCursor.sortField)
		require.Equal(t, job2.ID, listRes.LastCursor.id)

		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(rivertype.JobStateRunning).After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{job4.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByTime, listRes.LastCursor.sortField)
		require.Equal(t, job4.ID, listRes.LastCursor.id)

		listRes, err = client.JobList(ctx, NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc).States(rivertype.JobStateCompleted).After(JobListCursorFromJob(job5)))
		require.NoError(t, err)
		require.Equal(t, []int64{job6.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		require.Equal(t, JobListOrderByTime, listRes.LastCursor.sortField)
		require.Equal(t, job6.ID, listRes.LastCursor.id)
	})

	t.Run("MetadataOnly", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"foo": "bar"}`), Schema: bundle.schema})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})

		listRes, err := client.JobList(ctx, NewJobListParams().Metadata(`{"foo": "bar"}`))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		listRes, err = client.JobList(ctx, NewJobListParams().Metadata(`{"baz": "value"}`).OrderBy(JobListOrderByTime, SortOrderDesc))
		require.NoError(t, err)
		// Sort order was explicitly reversed:
		require.Equal(t, []int64{job3.ID, job2.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("ArbitraryWhereRawSQL", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"foo": "bar"}`), Schema: bundle.schema})
			_    = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})
			_    = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})
		)

		listRes, err := client.JobList(ctx, NewJobListParams().Where(`jsonb_path_query_first(metadata, '$.foo') = '"bar"'::jsonb`))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("ArbitraryWhereNamedParams", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"foo": "bar"}`), Schema: bundle.schema})
			_    = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})
			_    = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`), Schema: bundle.schema})
		)

		listRes, err := client.JobList(ctx, NewJobListParams().Where("jsonb_path_query_first(metadata, @json_query) = @json_val", NamedArgs{
			"json_query": "$.foo",
			"json_val":   `"bar"`,
		}))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("ArbitraryWhereMultipleNamedParams", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job3 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			_    = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		listRes, err := client.JobList(ctx, NewJobListParams().Where("id IN (@id1, @id2, @id3)",
			NamedArgs{"id1": job1.ID},
			NamedArgs{"id2": job2.ID},
			NamedArgs{"id3": job3.ID},
		))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID, job2.ID, job3.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("ArbitraryWhereMultipleClauses", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				MaxAttempts: ptrutil.Ptr(27),
				Queue:       ptrutil.Ptr("custom_queue"),
				Schema:      bundle.schema,
				State:       ptrutil.Ptr(rivertype.JobStateDiscarded),
			})
			_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		listRes, err := client.JobList(ctx, NewJobListParams().
			Where("kind = @kind", NamedArgs{"kind": job.Kind}).
			Where("max_attempts = @max_attempts", NamedArgs{"max_attempts": job.MaxAttempts}).
			Where("queue = @queue", NamedArgs{"queue": job.Queue}),
		)
		require.NoError(t, err)
		require.Equal(t, []int64{job.ID}, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("WithCancelledContext", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		listRes, err := client.JobList(ctx, NewJobListParams().States(rivertype.JobStateRunning))
		require.ErrorIs(t, context.Canceled, err)
		require.Nil(t, listRes)
	})
}

func Test_Client_JobRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{dbPool: dbPool}
	}

	t.Run("UpdatesAJobScheduledInTheFutureToBeImmediatelyAvailable", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, insertRes.Job.State)

		job, err := client.JobRetry(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.NotNil(t, job)

		require.Equal(t, rivertype.JobStateAvailable, job.State)
		require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 5*time.Second)
	})

	t.Run("TxVariantAlsoUpdatesJobToAvailable", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, insertRes.Job.State)

		var jobAfter *rivertype.JobRow

		err = pgx.BeginFunc(ctx, bundle.dbPool, func(tx pgx.Tx) error {
			var err error
			jobAfter, err = client.JobRetryTx(ctx, tx, insertRes.Job.ID)
			return err
		})
		require.NoError(t, err)
		require.NotNil(t, jobAfter)

		require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
		require.WithinDuration(t, time.Now().UTC(), jobAfter.ScheduledAt, 5*time.Second)
	})

	t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		job, err := client.JobRetry(ctx, 0)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, job)
	})
}

func Test_Client_JobUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool *pgxpool.Pool
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{dbPool: dbPool}
	}

	t.Run("AllParams", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		job, err := client.JobUpdate(ctx, insertRes.Job.ID, &JobUpdateParams{
			Output: "my job output",
		})
		require.NoError(t, err)
		require.Equal(t, `"my job output"`, string(job.Output()))

		updatedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, `"my job output"`, string(updatedJob.Output()))
	})

	t.Run("NoParams", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		_, err = client.JobUpdate(ctx, insertRes.Job.ID, nil)
		require.NoError(t, err)
	})

	t.Run("OutputFromContext", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		ctx := context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, map[string]any{})
		require.NoError(t, RecordOutput(ctx, "my job output from context"))

		job, err := client.JobUpdate(ctx, insertRes.Job.ID, &JobUpdateParams{})
		require.NoError(t, err)
		require.Equal(t, `"my job output from context"`, string(job.Output()))

		updatedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, `"my job output from context"`, string(updatedJob.Output()))
	})

	t.Run("ParamOutputTakesPrecedenceOverContextOutput", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		ctx := context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, map[string]any{})
		require.NoError(t, RecordOutput(ctx, "my job output from context"))

		job, err := client.JobUpdate(ctx, insertRes.Job.ID, &JobUpdateParams{
			Output: "my job output from params",
		})
		require.NoError(t, err)
		require.Equal(t, `"my job output from params"`, string(job.Output()))

		updatedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, `"my job output from params"`, string(updatedJob.Output()))
	})

	t.Run("ParamOutputTooLarge", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		_, err = client.JobUpdate(ctx, insertRes.Job.ID, &JobUpdateParams{
			Output: strings.Repeat("x", maxOutputSizeBytes+1),
		})
		require.ErrorContains(t, err, "output is too large")
	})
}

func Test_Client_JobUpdateTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		executorTx riverdriver.ExecutorTx
		tx         pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			dbPool:     dbPool,
			executorTx: client.driver.UnwrapExecutor(tx),
			tx:         tx,
		}
	}

	t.Run("AllParams", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		job, err := client.JobUpdateTx(ctx, bundle.tx, insertRes.Job.ID, &JobUpdateParams{
			Output: "my job output",
		})
		require.NoError(t, err)
		require.Equal(t, `"my job output"`, string(job.Output()))

		updatedJob, err := client.JobGetTx(ctx, bundle.tx, job.ID)
		require.NoError(t, err)
		require.Equal(t, `"my job output"`, string(updatedJob.Output()))

		// Outside of transaction shows original
		updatedJob, err = client.JobGet(ctx, job.ID)
		require.NoError(t, err)
		require.Empty(t, string(updatedJob.Output()))
	})

	t.Run("NoParams", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{})
		require.NoError(t, err)

		_, err = client.JobUpdateTx(ctx, bundle.tx, insertRes.Job.ID, nil)
		require.NoError(t, err)
	})
}

func Test_Client_ErrorHandler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema        string
		subscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		client := runNewTestClient(ctx, t, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{
			schema:        client.config.Schema,
			subscribeChan: subscribeChan,
		}
	}

	t.Run("ErrorHandler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		handlerErr := errors.New("job error")
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return handlerErr
		}))

		var errorHandlerCalled bool
		config.ErrorHandler = &testErrorHandler{
			HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
				require.Equal(t, handlerErr, err)
				errorHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		_, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)

		require.True(t, errorHandlerCalled)
	})

	t.Run("ErrorHandler_UnknownJobKind", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		var errorHandlerCalled bool
		config.ErrorHandler = &testErrorHandler{
			HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
				var unknownJobKindErr *UnknownJobKindError
				require.ErrorAs(t, err, &unknownJobKindErr)
				require.Equal(t, UnknownJobKindError{Kind: "RandomWorkerNameThatIsNeverRegistered"}, *unknownJobKindErr)
				errorHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		// Bypass the normal Insert function because that will error on an
		// unknown job.
		insertParams, err := insertParamsFromConfigArgsAndOptions(&client.baseService.Archetype, config, unregisteredJobArgs{}, nil)
		require.NoError(t, err)
		_, err = client.driver.GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   []*riverdriver.JobInsertFastParams{(*riverdriver.JobInsertFastParams)(insertParams)},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)

		require.True(t, errorHandlerCalled)
	})

	t.Run("PanicHandler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			panic("panic val")
		}))

		var panicHandlerCalled bool
		config.ErrorHandler = &testErrorHandler{
			HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult {
				require.Equal(t, "panic val", panicVal)
				panicHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		_, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)

		require.True(t, panicHandlerCalled)
	})
}

func Test_Client_Maintenance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		exec   riverdriver.Executor
		schema string
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)
		config.Schema = schema

		client := newTestClient(t, dbPool, config)
		client.testSignals.Init(t)

		return client, &testBundle{
			exec:   client.driver.GetExecutor(),
			schema: schema,
		}
	}

	// Starts the client, then waits for it to be elected leader and for the
	// queue maintainer to start.
	startAndWaitForQueueMaintainer := func(ctx context.Context, t *testing.T, client *Client[pgx.Tx]) {
		t.Helper()

		startClient(ctx, t, client)
		client.testSignals.electedLeader.WaitOrTimeout()
		riversharedtest.WaitOrTimeout(t, client.queueMaintainer.Started())
	}

	t.Run("JobCleanerCleans", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.CancelledJobRetentionPeriod = 1 * time.Hour
		config.CompletedJobRetentionPeriod = 1 * time.Hour
		config.DiscardedJobRetentionPeriod = 1 * time.Hour

		client, bundle := setup(t, config)

		deleteHorizon := time.Now().Add(-config.CompletedJobRetentionPeriod)

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the cleaner could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		ineligibleJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		ineligibleJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		jobBeyondHorizon1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})

		// Will not be deleted.
		jobWithinHorizon1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})

		startAndWaitForQueueMaintainer(ctx, t, client)

		jc := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
		jc.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = client.JobGet(ctx, ineligibleJob1.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, ineligibleJob2.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, ineligibleJob3.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there

		_, err = client.JobGet(ctx, jobBeyondHorizon1.ID)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = client.JobGet(ctx, jobBeyondHorizon2.ID)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = client.JobGet(ctx, jobBeyondHorizon3.ID)
		require.ErrorIs(t, err, ErrNotFound)

		_, err = client.JobGet(ctx, jobWithinHorizon1.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, jobWithinHorizon2.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, jobWithinHorizon3.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
	})

	t.Run("JobCleanerDoesNotCleanWithMinusOneRetention", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.CancelledJobRetentionPeriod = -1
		config.CompletedJobRetentionPeriod = -1
		config.DiscardedJobRetentionPeriod = -1

		client, bundle := setup(t, config)

		// Normal long retention period.
		deleteHorizon := time.Now().Add(-riversharedmaintenance.DiscardedJobRetentionPeriodDefault)

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the cleaner could run its initial
		// pass before our insertion is complete.
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})

		startAndWaitForQueueMaintainer(ctx, t, client)

		jc := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
		jc.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = client.JobGet(ctx, job1.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, job2.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.JobGet(ctx, job3.ID)
		require.NotErrorIs(t, err, ErrNotFound) // still there
	})

	t.Run("JobRescuer", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.RetryPolicy = &retrypolicytest.RetryPolicySlow{} // make sure jobs aren't worked before we can assert on job
		config.RescueStuckJobsAfter = 5 * time.Minute

		client, bundle := setup(t, config)

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the rescuer could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(-time.Minute))})

		// large attempt number ensures these don't immediately start executing again:
		jobStuckToRetry1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(20), AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobStuckToRetry2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(20), AttemptedAt: ptrutil.Ptr(now.Add(-30 * time.Minute))})
		jobStuckToDiscard := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
			Attempt:     ptrutil.Ptr(20),
			AttemptedAt: ptrutil.Ptr(now.Add(-5*time.Minute - time.Second)),
			MaxAttempts: ptrutil.Ptr(1),
			Schema:      bundle.schema,
		})

		// Will not be rescued.
		jobNotYetStuck1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-4 * time.Minute))})
		jobNotYetStuck2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobNotYetStuck3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-10 * time.Second))})

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.JobRescuer](client.queueMaintainer)
		svc.TestSignals.FetchedBatch.WaitOrTimeout()
		svc.TestSignals.UpdatedBatch.WaitOrTimeout()

		requireJobHasState := func(jobID int64, state rivertype.JobState) *rivertype.JobRow {
			t.Helper()
			job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: jobID, Schema: bundle.schema})
			require.NoError(t, err)
			require.Equal(t, state, job.State)
			return job
		}

		// unchanged
		requireJobHasState(ineligibleJob1.ID, ineligibleJob1.State)
		requireJobHasState(ineligibleJob2.ID, ineligibleJob2.State)
		requireJobHasState(ineligibleJob3.ID, ineligibleJob3.State)

		// Jobs to retry should be retryable:
		updatedJobStuckToRetry1 := requireJobHasState(jobStuckToRetry1.ID, rivertype.JobStateRetryable)
		require.Greater(t, updatedJobStuckToRetry1.ScheduledAt, now.Add(10*time.Minute)) // make sure `scheduled_at` is a good margin in the future so it's not at risk of immediate retry (which could cause intermittent test issues)
		updatedJobStuckToRetry2 := requireJobHasState(jobStuckToRetry2.ID, rivertype.JobStateRetryable)
		require.Greater(t, updatedJobStuckToRetry2.ScheduledAt, now.Add(10*time.Minute))

		// This one should be discarded because it's already at MaxAttempts:
		requireJobHasState(jobStuckToDiscard.ID, rivertype.JobStateDiscarded)

		// not eligible for rescue, not stuck long enough yet:
		requireJobHasState(jobNotYetStuck1.ID, jobNotYetStuck1.State)
		requireJobHasState(jobNotYetStuck2.ID, jobNotYetStuck2.State)
		requireJobHasState(jobNotYetStuck3.ID, jobNotYetStuck3.State)
	})

	t.Run("JobScheduler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.Queues = map[string]QueueConfig{"another_queue": {MaxWorkers: 1}} // don't work jobs on the default queue we're using in this test

		client, bundle := setup(t, config)

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the scheduler could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		ineligibleJob2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateRunning)})
		ineligibleJob3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})

		jobInPast1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobInPast2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobInPast3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

		// Will not be scheduled.
		jobInFuture1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Hour))})
		jobInFuture2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Minute))})
		jobInFuture3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(now.Add(10 * time.Second))})

		startAndWaitForQueueMaintainer(ctx, t, client)

		scheduler := maintenance.GetService[*maintenance.JobScheduler](client.queueMaintainer)
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobHasState := func(jobID int64, state rivertype.JobState) {
			t.Helper()
			job, err := client.JobGet(ctx, jobID)
			require.NoError(t, err)
			require.Equal(t, state, job.State)
		}

		// unchanged
		requireJobHasState(ineligibleJob1.ID, ineligibleJob1.State)
		requireJobHasState(ineligibleJob2.ID, ineligibleJob2.State)
		requireJobHasState(ineligibleJob3.ID, ineligibleJob3.State)

		// Jobs with past timestamps should be now be made available:
		requireJobHasState(jobInPast1.ID, rivertype.JobStateAvailable)
		requireJobHasState(jobInPast2.ID, rivertype.JobStateAvailable)
		requireJobHasState(jobInPast3.ID, rivertype.JobStateAvailable)

		// not scheduled, still in future
		requireJobHasState(jobInFuture1.ID, jobInFuture1.State)
		requireJobHasState(jobInFuture2.ID, jobInFuture2.State)
		requireJobHasState(jobInFuture3.ID, jobInFuture3.State)
	})

	t.Run("PeriodicJobEnqueuerStartHook", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		var periodicJobsStartHookCalled bool
		config.Hooks = []rivertype.Hook{
			HookPeriodicJobsStartFunc(func(ctx context.Context, params *rivertype.HookPeriodicJobsStartParams) error {
				periodicJobsStartHookCalled = true

				client := ClientFromContext[pgx.Tx](ctx)

				client.PeriodicJobs().Clear()

				client.PeriodicJobs().Add(
					NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
						return periodicJobArgs{}, nil
					}, &PeriodicJobOpts{ID: "new_periodic_job", RunOnStart: true}),
				)

				return nil
			}),
		}

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{ID: "old_periodic_job", RunOnStart: true}),
		}

		client, _ := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		require.True(t, periodicJobsStartHookCalled)

		// Use the return value of these to determine that the OnStart callback
		// went through successfully. (True if the job was removed and false
		// otherwise.)
		require.False(t, svc.RemoveByID("old_periodic_job"))
		require.True(t, svc.RemoveByID("new_periodic_job"))
	})

	t.Run("PeriodicJobEnqueuerWithInsertOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		}

		client, bundle := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(periodicJobArgs{}).Kind())
	})

	t.Run("PeriodicJobEnqueuerNoInsertOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, nil),
		}

		client, bundle := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		// No jobs yet because the RunOnStart option was not specified.
		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Empty(t, jobs)
	})

	t.Run("PeriodicJobConstructorReturningNil", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				// Returning nil from the constructor function should not insert a new
				// job and should be handled cleanly
				return nil, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		}

		client, bundle := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.SkippedJob.WaitOrTimeout()

		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Empty(t, jobs, "Expected to find zero jobs of kind: "+(periodicJobArgs{}).Kind())
	})

	t.Run("PeriodicJobEnqueuerAddDynamically", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)

		client := newTestClient(t, dbPool, config)
		client.testSignals.Init(t)
		startClient(ctx, t, client)

		exec := client.driver.GetExecutor()

		client.testSignals.electedLeader.WaitOrTimeout()

		client.PeriodicJobs().Add(
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		// We get a queued job because RunOnStart was specified.
		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1)
	})

	t.Run("PeriodicJobEnqueuerRemoveDynamically", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)

		client := newTestClient(t, dbPool, config)
		client.testSignals.Init(t)
		exec := client.driver.GetExecutor()

		handle := client.PeriodicJobs().Add(
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		)

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		client.PeriodicJobs().Remove(handle)

		type OtherPeriodicArgs struct {
			testutil.JobArgsReflectKind[OtherPeriodicArgs]
		}

		client.PeriodicJobs().Add(
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return OtherPeriodicArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		)

		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		// One of each because the first periodic job was inserted on the first
		// go around due to RunOnStart, but then subsequently removed. The next
		// periodic job was inserted also due to RunOnStart, but only after the
		// first was removed.
		{
			jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{(periodicJobArgs{}).Kind()},
				Schema: client.config.Schema,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
		}
		{
			jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
				Kind:   []string{(OtherPeriodicArgs{}).Kind()},
				Schema: client.config.Schema,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
		}
	})

	t.Run("PeriodicJobEnqueuerRemoveByID", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)

		client := newTestClient(t, dbPool, config)
		client.testSignals.Init(t)
		exec := client.driver.GetExecutor()

		_ = client.PeriodicJobs().AddMany([]*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{ID: "my_periodic_job_1", RunOnStart: true}),
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{ID: "my_periodic_job_2", RunOnStart: true}),
		})

		require.True(t, client.PeriodicJobs().RemoveByID("my_periodic_job_1"))
		require.False(t, client.PeriodicJobs().RemoveByID("does_not_exist"))

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		// Only one periodic job added because one was removed before starting the client.
		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1)
	})

	t.Run("PeriodicJobEnqueuerRemoveManyByID", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)

		client := newTestClient(t, dbPool, config)
		client.testSignals.Init(t)
		exec := client.driver.GetExecutor()

		_ = client.PeriodicJobs().AddMany([]*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{ID: "my_periodic_job_1", RunOnStart: true}),
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{ID: "my_periodic_job_2", RunOnStart: true}),
		})

		client.PeriodicJobs().RemoveManyByID([]string{"my_periodic_job_1", "does_not_exist"})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		// Only one periodic job added because one was removed before starting the client.
		jobs, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1)
	})

	t.Run("PeriodicJobsPanicIfClientNotConfiguredToExecuteJobs", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.Queues = nil
		config.Workers = nil

		client := newTestClient(t, nil, config)

		require.PanicsWithValue(t, "client Queues and Workers must be configured to modify periodic jobs (otherwise, they'll have no effect because a client not configured to work jobs can't be started)", func() {
			client.PeriodicJobs()
		})
	})

	t.Run("PeriodicJobEnqueuerUsesMiddleware", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		}
		config.JobInsertMiddleware = []rivertype.JobInsertMiddleware{&overridableJobMiddleware{
			insertManyFunc: func(ctx context.Context, manyParams []*rivertype.JobInsertParams, doInner func(ctx context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
				for _, job := range manyParams {
					job.EncodedArgs = []byte(`{"from": "middleware"}`)
				}
				return doInner(ctx)
			},
		}}

		client, bundle := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		jobs, err := bundle.exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
			Kind:   []string{(periodicJobArgs{}).Kind()},
			Schema: client.config.Schema,
		})
		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(periodicJobArgs{}).Kind())
	})

	t.Run("QueueCleaner", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)
		client.testSignals.Init(t)

		exec := client.driver.GetExecutor()

		deleteHorizon := time.Now().Add(-maintenance.QueueRetentionPeriodDefault)

		// Take care to insert queues before starting the client because otherwise
		// there's a race condition where the cleaner could run its initial
		// pass before our insertion is complete.
		queueBeyondHorizon1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		queueBeyondHorizon2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		queueBeyondHorizon3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})

		// Will not be deleted.
		queueWithinHorizon1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		queueWithinHorizon2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		queueWithinHorizon3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Schema: schema, UpdatedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
		qc := maintenance.GetService[*maintenance.QueueCleaner](client.queueMaintainer)
		qc.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = client.QueueGet(ctx, queueBeyondHorizon1.Name)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = client.QueueGet(ctx, queueBeyondHorizon2.Name)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = client.QueueGet(ctx, queueBeyondHorizon3.Name)
		require.ErrorIs(t, err, ErrNotFound)

		_, err = client.QueueGet(ctx, queueWithinHorizon1.Name)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.QueueGet(ctx, queueWithinHorizon2.Name)
		require.NotErrorIs(t, err, ErrNotFound) // still there
		_, err = client.QueueGet(ctx, queueWithinHorizon3.Name)
		require.NotErrorIs(t, err, ErrNotFound) // still there
	})

	t.Run("Reindexer", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")
		config.ReindexerSchedule = &runOnceSchedule{}

		client, _ := setup(t, config)

		startAndWaitForQueueMaintainer(ctx, t, client)

		svc := maintenance.GetService[*maintenance.Reindexer](client.queueMaintainer)
		// There are two indexes to reindex by default:
		svc.TestSignals.Reindexed.WaitOrTimeout()
		svc.TestSignals.Reindexed.WaitOrTimeout()
	})
}

type runOnceSchedule struct {
	ran atomic.Bool
}

func (s *runOnceSchedule) Next(time.Time) time.Time {
	if !s.ran.Swap(true) {
		return time.Now()
	}
	// Return the maximum future time so that the schedule doesn't run again.
	return time.Unix(1<<63-1, 0)
}

func Test_Client_QueueGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			schema: schema,
		}
	}

	t.Run("FetchesAnExistingQueue", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{Schema: bundle.schema})

		queueRes, err := client.QueueGet(ctx, queue.Name)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), queueRes.CreatedAt, 2*time.Second)
		require.WithinDuration(t, queue.CreatedAt, queueRes.CreatedAt, time.Millisecond)
		require.Equal(t, []byte("{}"), queueRes.Metadata)
		require.Equal(t, queue.Name, queueRes.Name)
		require.Nil(t, queueRes.PausedAt)
	})

	t.Run("ReturnsErrNotFoundIfQueueDoesNotExist", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		queueRes, err := client.QueueGet(ctx, "a_queue_that_does_not_exist")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, queueRes)
	})
}

func Test_Client_QueueGetTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		executorTx riverdriver.ExecutorTx
		schema     string
		tx         pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			executorTx: client.driver.UnwrapExecutor(tx),
			schema:     schema,
			tx:         tx,
		}
	}

	t.Run("FetchesAnExistingQueue", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, bundle.executorTx, &testfactory.QueueOpts{Schema: bundle.schema})

		queueRes, err := client.QueueGetTx(ctx, bundle.tx, queue.Name)
		require.NoError(t, err)
		require.Equal(t, queue.Name, queueRes.Name)

		// Not visible outside of transaction.
		_, err = client.QueueGet(ctx, queue.Name)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("ReturnsErrNotFoundIfQueueDoesNotExist", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		queueRes, err := client.QueueGet(ctx, "a_queue_that_does_not_exist")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
		require.Nil(t, queueRes)
	})
}

func Test_Client_QueueList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			schema: schema,
		}
	}

	t.Run("ListsAndPaginatesQueues", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		requireQueuesEqual := func(t *testing.T, target, actual *rivertype.Queue) {
			t.Helper()
			require.WithinDuration(t, target.CreatedAt, actual.CreatedAt, time.Millisecond)
			require.Equal(t, target.Metadata, actual.Metadata)
			require.Equal(t, target.Name, actual.Name)
			if target.PausedAt == nil {
				require.Nil(t, actual.PausedAt)
			} else {
				require.NotNil(t, actual.PausedAt)
				require.WithinDuration(t, *target.PausedAt, *actual.PausedAt, time.Millisecond)
			}
		}

		listRes, err := client.QueueList(ctx, NewQueueListParams().First(2))
		require.NoError(t, err)
		require.Empty(t, listRes.Queues)

		// Make queue1, pause it, refetch:
		queue1 := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`), Schema: bundle.schema})
		require.NoError(t, client.QueuePause(ctx, queue1.Name, nil))
		queue1, err = client.QueueGet(ctx, queue1.Name)
		require.NoError(t, err)

		queue2 := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{Schema: bundle.schema})
		queue3 := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{Schema: bundle.schema})

		listRes, err = client.QueueList(ctx, NewQueueListParams().First(2))
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 2)
		requireQueuesEqual(t, queue1, listRes.Queues[0])
		requireQueuesEqual(t, queue2, listRes.Queues[1])

		listRes, err = client.QueueList(ctx, NewQueueListParams().First(3))
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 3)
		requireQueuesEqual(t, queue3, listRes.Queues[2])

		listRes, err = client.QueueList(ctx, NewQueueListParams().First(10))
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 3)
	})
}

func Test_Client_QueueListTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		executorTx riverdriver.ExecutorTx
		schema     string
		tx         pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			executorTx: client.driver.UnwrapExecutor(tx),
			schema:     schema,
			tx:         tx,
		}
	}

	t.Run("ListsQueues", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		listRes, err := client.QueueListTx(ctx, bundle.tx, NewQueueListParams())
		require.NoError(t, err)
		require.Empty(t, listRes.Queues)

		queue := testfactory.Queue(ctx, t, bundle.executorTx, &testfactory.QueueOpts{Schema: bundle.schema})

		listRes, err = client.QueueListTx(ctx, bundle.tx, NewQueueListParams())
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 1)
		require.Equal(t, queue.Name, listRes.Queues[0].Name)

		// Not visible outside of transaction.
		listRes, err = client.QueueList(ctx, NewQueueListParams())
		require.NoError(t, err)
		require.Empty(t, listRes.Queues)
	})
}

func Test_Client_QueueUpdate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		schema string
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		return client, &testBundle{
			schema: schema,
		}
	}

	t.Run("UpdatesQueueMetadata", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)
		startClient(ctx, t, client)

		type metadataUpdatePayload struct {
			Action   string          `json:"action"`
			Metadata json.RawMessage `json:"metadata"`
			Queue    string          `json:"queue"`
		}
		type notification struct {
			topic   notifier.NotificationTopic
			payload metadataUpdatePayload
		}
		notifyCh := make(chan notification, 10)

		handleNotification := func(topic notifier.NotificationTopic, payload string) {
			t.Logf("received notification: %s, %q", topic, payload)
			notif := notification{topic: topic}
			require.NoError(t, json.Unmarshal([]byte(payload), &notif.payload))
			notifyCh <- notif
		}

		sub, err := client.notifier.Listen(ctx, notifier.NotificationTopicControl, handleNotification)
		require.NoError(t, err)
		t.Cleanup(func() { sub.Unlisten(ctx) })

		queue := testfactory.Queue(ctx, t, client.driver.GetExecutor(), &testfactory.QueueOpts{Schema: bundle.schema})
		require.Equal(t, []byte(`{}`), queue.Metadata)

		queue, err = client.QueueUpdate(ctx, queue.Name, &QueueUpdateParams{
			Metadata: []byte(`{"foo":"bar"}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(queue.Metadata))

		notif := riversharedtest.WaitOrTimeout(t, notifyCh)
		require.Equal(t, notifier.NotificationTopicControl, notif.topic)
		require.Equal(t, metadataUpdatePayload{
			Action:   "metadata_changed",
			Metadata: []byte(`{"foo":"bar"}`),
			Queue:    queue.Name,
		}, notif.payload)

		queue, err = client.QueueUpdate(ctx, queue.Name, &QueueUpdateParams{
			Metadata: []byte(`{"foo":"baz"}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"baz"}`, string(queue.Metadata))

		notif = riversharedtest.WaitOrTimeout(t, notifyCh)
		require.Equal(t, notifier.NotificationTopicControl, notif.topic)
		require.Equal(t, metadataUpdatePayload{
			Action:   "metadata_changed",
			Metadata: []byte(`{"foo":"baz"}`),
			Queue:    queue.Name,
		}, notif.payload)

		queue, err = client.QueueUpdate(ctx, queue.Name, &QueueUpdateParams{
			Metadata: nil,
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"baz"}`, string(queue.Metadata))

		select {
		case notif = <-notifyCh:
			t.Fatalf("expected no notification, got: %+v", notif)
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("ProducerControlEventSent", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPoolClone(ctx, t)
			driver = NewDriverWithoutListenNotify(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client, err := NewClient(driver, config)
		require.NoError(t, err)
		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		_, err = client.QueueUpdate(ctx, QueueDefault, &QueueUpdateParams{
			Metadata: []byte(`{"foo":"baz"}`),
		})
		require.NoError(t, err)

		controlEvent := client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.WaitOrTimeout()
		require.NotNil(t, controlEvent)
		require.Equal(t, controlActionMetadataChanged, controlEvent.Action)
	})

	t.Run("ProducerControlEventNotSent", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.producersByQueueName[QueueDefault].testSignals.Init(t)

		startClient(ctx, t, client)

		_, err := client.QueueUpdate(ctx, QueueDefault, &QueueUpdateParams{
			Metadata: []byte(`{"foo":"baz"}`),
		})
		require.NoError(t, err)

		client.producersByQueueName[QueueDefault].testSignals.QueueControlEventTriggered.RequireEmpty()
	})
}

func Test_Client_QueueUpdateTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		executorTx riverdriver.ExecutorTx
		schema     string
		tx         pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			executorTx: client.driver.UnwrapExecutor(tx),
			schema:     schema,
			tx:         tx,
		}
	}

	t.Run("UpdatesQueueMetadata", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, bundle.executorTx, &testfactory.QueueOpts{Schema: bundle.schema})
		require.Equal(t, []byte(`{}`), queue.Metadata)

		queue, err := client.QueueUpdateTx(ctx, bundle.tx, queue.Name, &QueueUpdateParams{
			Metadata: []byte(`{"foo":"bar"}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(queue.Metadata))

		queue, err = client.QueueUpdateTx(ctx, bundle.tx, queue.Name, &QueueUpdateParams{
			Metadata: nil,
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(queue.Metadata))

		queue, err = client.QueueUpdateTx(ctx, bundle.tx, queue.Name, &QueueUpdateParams{
			Metadata: []byte(`{}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(queue.Metadata))
	})
}

func Test_Client_RetryPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("RetryUntilDiscarded", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		// The default policy would work too, but this takes some variability
		// out of it to make comparisons easier.
		config.RetryPolicy = &retrypolicytest.RetryPolicyNoJitter{}

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return errors.New("job error")
		}))

		client := newTestClient(t, dbPool, config)

		now := client.baseService.Time.StubNowUTC(time.Now().UTC())
		t.Logf("Now: %s", now)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		originalJobs := make([]*rivertype.JobRow, rivercommon.MaxAttemptsDefault)
		for i := range originalJobs {
			insertRes, err := client.Insert(ctx, JobArgs{}, nil)
			require.NoError(t, err)

			// regression protection to ensure we're testing the right number of jobs:
			require.Equal(t, rivercommon.MaxAttemptsDefault, insertRes.Job.MaxAttempts)

			updatedJob, err := client.driver.GetExecutor().JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				ID:                  insertRes.Job.ID,
				AttemptedAtDoUpdate: true,
				AttemptedAt:         &now, // we want a value here, but it'll be overwritten as jobs are locked by the producer
				AttemptDoUpdate:     true,
				Attempt:             i, // starts at i, but will be i + 1 by the time it's being worked
				Schema:              schema,

				// Need to find a cleaner way around this, but state is required
				// because sqlc can't encode an empty string to the
				// corresponding enum. This value is not actually used because
				// StateDoUpdate was not supplied.
				State: rivertype.JobStateAvailable,
			})
			require.NoError(t, err)

			originalJobs[i] = updatedJob
		}

		startClient(ctx, t, client)

		// Wait for the expected number of jobs to be finished.
		for i := range originalJobs {
			t.Logf("Waiting on job %d", i)
			_ = riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		finishedJobs, err := client.driver.GetExecutor().JobGetByIDMany(ctx, &riverdriver.JobGetByIDManyParams{
			ID:     sliceutil.Map(originalJobs, func(m *rivertype.JobRow) int64 { return m.ID }),
			Schema: schema,
		})
		require.NoError(t, err)

		// Jobs aren't guaranteed to come back out of the queue in the same
		// order that we inserted them, so make sure to compare using a lookup
		// map.
		finishedJobsByID := sliceutil.KeyBy(finishedJobs,
			func(m *rivertype.JobRow) (int64, *rivertype.JobRow) { return m.ID, m })

		for i, originalJob := range originalJobs {
			// This loop will check all jobs that were to be rescheduled, but
			// not the final job which is discarded.
			if i >= len(originalJobs)-1 {
				continue
			}

			finishedJob := finishedJobsByID[originalJob.ID]

			// We need to advance the original job's attempt number to represent
			// how it would've looked after being run through the queue.
			originalJob.Attempt += 1

			expectedNextScheduledAt := client.config.RetryPolicy.NextRetry(originalJob)

			t.Logf("Attempt number %d scheduled %v from original `attempted_at`",
				originalJob.Attempt, finishedJob.ScheduledAt.Sub(*originalJob.AttemptedAt))
			t.Logf("    Original attempt at:   %v", originalJob.AttemptedAt)
			t.Logf("    New scheduled at:      %v", finishedJob.ScheduledAt)
			t.Logf("    Expected scheduled at: %v", expectedNextScheduledAt)

			require.WithinDuration(t, expectedNextScheduledAt, finishedJob.ScheduledAt, time.Microsecond)

			require.Equal(t, rivertype.JobStateRetryable, finishedJob.State)
		}

		// One last discarded job.
		{
			originalJob := originalJobs[len(originalJobs)-1]
			finishedJob := finishedJobsByID[originalJob.ID]

			originalJob.Attempt += 1

			t.Logf("Attempt number %d discarded", originalJob.Attempt)

			require.WithinDuration(t, now, *finishedJob.FinalizedAt, time.Microsecond)
			require.Equal(t, rivertype.JobStateDiscarded, finishedJob.State)
		}
	})
}

func Test_Client_Subscribe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]

		Name string `json:"name"`
	}

	keyEventsByName := func(events []*Event) map[string]*Event {
		return sliceutil.KeyBy(events, func(event *Event) (string, *Event) {
			var args JobArgs
			require.NoError(t, json.Unmarshal(event.Job.EncodedArgs, &args))
			return args.Name, event
		})
	}

	requireInsert := func(ctx context.Context, client *Client[pgx.Tx], jobName string) *rivertype.JobRow {
		insertRes, err := client.Insert(ctx, JobArgs{Name: jobName}, nil)
		require.NoError(t, err)
		return insertRes.Job
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		// Fail/succeed jobs based on their name so we can get a mix of both to
		// verify.
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		jobCompleted1 := requireInsert(ctx, client, "completed1")
		jobCompleted2 := requireInsert(ctx, client, "completed2")
		jobFailed1 := requireInsert(ctx, client, "failed1")
		jobFailed2 := requireInsert(ctx, client, "failed2")

		expectedJobs := []*rivertype.JobRow{
			jobCompleted1,
			jobCompleted2,
			jobFailed1,
			jobFailed2,
		}

		startClient(ctx, t, client)

		events := make([]*Event, len(expectedJobs))

		for i := range expectedJobs {
			events[i] = riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		{
			eventCompleted1 := eventsByName["completed1"]
			require.Equal(t, EventKindJobCompleted, eventCompleted1.Kind)
			require.Equal(t, jobCompleted1.ID, eventCompleted1.Job.ID)
			require.Equal(t, rivertype.JobStateCompleted, eventCompleted1.Job.State)
		}

		{
			eventCompleted2 := eventsByName["completed2"]
			require.Equal(t, EventKindJobCompleted, eventCompleted2.Kind)
			require.Equal(t, jobCompleted2.ID, eventCompleted2.Job.ID)
			require.Equal(t, rivertype.JobStateCompleted, eventCompleted2.Job.State)
		}

		{
			eventFailed1 := eventsByName["failed1"]
			require.Equal(t, EventKindJobFailed, eventFailed1.Kind)
			require.Equal(t, jobFailed1.ID, eventFailed1.Job.ID)
			require.Equal(t, rivertype.JobStateRetryable, eventFailed1.Job.State)
		}

		{
			eventFailed2 := eventsByName["failed2"]
			require.Equal(t, EventKindJobFailed, eventFailed2.Kind)
			require.Equal(t, jobFailed2.ID, eventFailed2.Job.ID)
			require.Equal(t, rivertype.JobStateRetryable, eventFailed2.Job.State)
		}
	})

	t.Run("CompletedOnly", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		t.Cleanup(cancel)

		jobCompleted := requireInsert(ctx, client, "completed1")
		requireInsert(ctx, client, "failed1")

		expectedJobs := []*rivertype.JobRow{
			jobCompleted,
		}

		startClient(ctx, t, client)

		events := make([]*Event, len(expectedJobs))

		for i := range expectedJobs {
			events[i] = riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		eventCompleted := eventsByName["completed1"]
		require.Equal(t, EventKindJobCompleted, eventCompleted.Kind)
		require.Equal(t, jobCompleted.ID, eventCompleted.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, eventCompleted.Job.State)

		_, ok := eventsByName["failed1"] // filtered out
		require.False(t, ok)
	})

	t.Run("FailedOnly", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobFailed)
		t.Cleanup(cancel)

		requireInsert(ctx, client, "completed1")
		jobFailed := requireInsert(ctx, client, "failed1")

		expectedJobs := []*rivertype.JobRow{
			jobFailed,
		}

		startClient(ctx, t, client)

		events := make([]*Event, len(expectedJobs))

		for i := range expectedJobs {
			events[i] = riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		_, ok := eventsByName["completed1"] // filtered out
		require.False(t, ok)

		eventFailed := eventsByName["failed1"]
		require.Equal(t, EventKindJobFailed, eventFailed.Kind)
		require.Equal(t, jobFailed.ID, eventFailed.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, eventFailed.Job.State)
	})

	t.Run("PanicOnUnknownKind", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		require.PanicsWithError(t, "unknown event kind: does_not_exist", func() {
			_, _ = client.Subscribe(EventKind("does_not_exist"))
		})
	})

	t.Run("SubscriptionCancellation", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		cancel()

		// Drops through immediately because the channel is closed.
		riversharedtest.WaitOrTimeout(t, subscribeChan)

		require.Empty(t, client.subscriptionManager.subscriptions)
	})

	// Just make sure this doesn't fail on a nil pointer exception.
	t.Run("SubscribeOnClientWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)
		config.Queues = nil

		client := newTestClient(t, dbPool, config)

		require.PanicsWithValue(t, "created a subscription on a client that will never work jobs (Queues not configured)", func() {
			_, _ = client.Subscribe(EventKindJobCompleted)
		})
	})
}

// SubscribeConfig uses all the same code as Subscribe, so these are just a
// minimal set of new tests to make sure that the function also works when used
// independently.
func Test_Client_SubscribeConfig(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			Name string `json:"name"`
		}

		// Fail/succeed jobs based on their name so we can get a mix of both to
		// verify.
		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		}))

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.SubscribeConfig(&SubscribeConfig{
			Kinds: []EventKind{EventKindJobCompleted, EventKindJobFailed},
		})
		t.Cleanup(cancel)

		keyEventsByName := func(events []*Event) map[string]*Event {
			return sliceutil.KeyBy(events, func(event *Event) (string, *Event) {
				var args JobArgs
				require.NoError(t, json.Unmarshal(event.Job.EncodedArgs, &args))
				return args.Name, event
			})
		}

		requireInsert := func(ctx context.Context, client *Client[pgx.Tx], jobName string) *rivertype.JobRow {
			insertRes, err := client.Insert(ctx, JobArgs{Name: jobName}, nil)
			require.NoError(t, err)
			return insertRes.Job
		}

		jobCompleted1 := requireInsert(ctx, client, "completed1")
		jobCompleted2 := requireInsert(ctx, client, "completed2")
		jobFailed1 := requireInsert(ctx, client, "failed1")
		jobFailed2 := requireInsert(ctx, client, "failed2")

		expectedJobs := []*rivertype.JobRow{
			jobCompleted1,
			jobCompleted2,
			jobFailed1,
			jobFailed2,
		}

		startClient(ctx, t, client)

		events := make([]*Event, len(expectedJobs))

		for i := range expectedJobs {
			events[i] = riversharedtest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		{
			eventCompleted1 := eventsByName["completed1"]
			require.Equal(t, EventKindJobCompleted, eventCompleted1.Kind)
			require.Equal(t, jobCompleted1.ID, eventCompleted1.Job.ID)
			require.Equal(t, rivertype.JobStateCompleted, eventCompleted1.Job.State)
		}

		{
			eventCompleted2 := eventsByName["completed2"]
			require.Equal(t, EventKindJobCompleted, eventCompleted2.Kind)
			require.Equal(t, jobCompleted2.ID, eventCompleted2.Job.ID)
			require.Equal(t, rivertype.JobStateCompleted, eventCompleted2.Job.State)
		}

		{
			eventFailed1 := eventsByName["failed1"]
			require.Equal(t, EventKindJobFailed, eventFailed1.Kind)
			require.Equal(t, jobFailed1.ID, eventFailed1.Job.ID)
			require.Equal(t, rivertype.JobStateRetryable, eventFailed1.Job.State)
		}

		{
			eventFailed2 := eventsByName["failed2"]
			require.Equal(t, EventKindJobFailed, eventFailed2.Kind)
			require.Equal(t, jobFailed2.ID, eventFailed2.Job.ID)
			require.Equal(t, rivertype.JobStateRetryable, eventFailed2.Job.State)
		}
	})

	t.Run("EventsDropWithNoListeners", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		// A first channel that we'll use to make sure all the expected jobs are
		// finished.
		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		t.Cleanup(cancel)

		// Artificially lowered subscribe channel size so we don't have to try
		// and process thousands of jobs.
		const (
			subscribeChanSize = 100
			numJobsToInsert   = subscribeChanSize + 1
		)

		// Another channel with no listeners. Despite no listeners, it shouldn't
		// block or gum up the client's progress in any way.
		subscribeChan2, cancel := client.SubscribeConfig(&SubscribeConfig{
			ChanSize: subscribeChanSize,
			Kinds:    []EventKind{EventKindJobCompleted},
		})
		t.Cleanup(cancel)

		var (
			insertParams = make([]*riverdriver.JobInsertFastParams, numJobsToInsert)
			kind         = (&JobArgs{}).Kind()
		)
		for i := range numJobsToInsert {
			insertParams[i] = &riverdriver.JobInsertFastParams{
				Args:        &JobArgs{},
				EncodedArgs: []byte(`{}`),
				Kind:        kind,
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
				State:       rivertype.JobStateAvailable,
			}
		}

		_, err := client.driver.GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: schema,
		})
		require.NoError(t, err)

		// Need to start waiting on events before running the client or the
		// channel could overflow before we start listening.
		var wg sync.WaitGroup
		wg.Go(func() {
			_ = riversharedtest.WaitOrTimeoutN(t, subscribeChan, numJobsToInsert)
		})

		startClient(ctx, t, client)

		wg.Wait()

		// Filled to maximum.
		require.Len(t, subscribeChan2, subscribeChanSize)
	})

	t.Run("PanicOnChanSizeLessThanZero", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client := newTestClient(t, dbPool, config)

		require.PanicsWithValue(t, "SubscribeConfig.ChanSize must be greater or equal to 1", func() {
			_, _ = client.SubscribeConfig(&SubscribeConfig{
				ChanSize: -1,
			})
		})
	})

	t.Run("PanicOnUnknownKind", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)

		client := newTestClient(t, dbPool, config)

		require.PanicsWithError(t, "unknown event kind: does_not_exist", func() {
			_, _ = client.SubscribeConfig(&SubscribeConfig{
				Kinds: []EventKind{EventKind("does_not_exist")},
			})
		})
	})
}

func Test_Client_InsertTriggersImmediateWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		config = newTestConfig(t, schema)
	)
	config.FetchCooldown = 20 * time.Millisecond
	config.FetchPollInterval = 20 * time.Second // essentially disable polling
	config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}, "another_queue": {MaxWorkers: 1}}

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	doneCh := make(chan struct{})
	close(doneCh) // don't need to block any jobs from completing
	startedCh := make(chan int64)
	AddWorker(config.Workers, makeAwaitWorker[JobArgs](startedCh, doneCh))

	client := newTestClient(t, dbPool, config)

	startClient(ctx, t, client)
	riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())

	insertRes, err := client.Insert(ctx, JobArgs{}, nil)
	require.NoError(t, err)

	// Wait for the client to be ready by waiting for a job to be executed:
	select {
	case jobID := <-startedCh:
		require.Equal(t, insertRes.Job.ID, jobID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for warmup job to start")
	}

	// Now that we've run one job, we shouldn't take longer than the cooldown to
	// fetch another after insertion. LISTEN/NOTIFY should ensure we find out
	// about the inserted job much faster than the poll interval.
	//
	// Note: we specifically use a different queue to ensure that the notify
	// limiter is immediately to fire on this queue.
	insertRes2, err := client.Insert(ctx, JobArgs{}, &InsertOpts{Queue: "another_queue"})
	require.NoError(t, err)

	select {
	case jobID := <-startedCh:
		require.Equal(t, insertRes2.Job.ID, jobID)
	// As long as this is meaningfully shorter than the poll interval, we can be
	// sure the re-fetch came from listen/notify.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for 2nd job to start")
	}

	require.NoError(t, client.Stop(ctx))
}

func Test_Client_InsertNotificationsAreDeduplicatedAndDebounced(t *testing.T) {
	t.Parallel()

	// Keep limiter time deterministic so debounce checks don't depend on CI
	// jitter. Hold repeated `queue1` inserts inside `FetchCooldown` and assert no
	// extra notification, then advance time past cooldown and assert it notifies
	// again. `queue2`/`queue3` confirm debounce state is per queue.

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		config = newTestConfig(t, schema)
	)
	config.FetchPollInterval = 20 * time.Second // essentially disable polling
	config.FetchCooldown = time.Second
	config.schedulerInterval = 20 * time.Second // quiet scheduler
	config.Queues = map[string]QueueConfig{"queue1": {MaxWorkers: 1}, "queue2": {MaxWorkers: 1}, "queue3": {MaxWorkers: 1}}

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
		return nil
	}))

	client := newTestClient(t, dbPool, config)

	startClient(ctx, t, client)
	riversharedtest.WaitOrTimeout(t, client.baseStartStop.Started())
	// Anchor all limiter checks to a known base time so debounce behavior is
	// independent from scheduler jitter or machine load.
	now := client.baseService.Time.StubNowUTC(time.Now().UTC())

	type insertPayload struct {
		Queue string `json:"queue"`
	}
	type notification struct {
		topic   notifier.NotificationTopic
		payload insertPayload
	}
	notifyCh := make(chan notification, 10)
	handleNotification := func(topic notifier.NotificationTopic, payload string) {
		config.Logger.InfoContext(ctx, "received notification", slog.String("topic", string(topic)), slog.String("payload", payload))
		notif := notification{topic: topic}
		require.NoError(t, json.Unmarshal([]byte(payload), &notif.payload))
		notifyCh <- notif
	}
	sub, err := client.notifier.Listen(ctx, notifier.NotificationTopicInsert, handleNotification)
	require.NoError(t, err)
	t.Cleanup(func() { sub.Unlisten(ctx) })

	expectImmediateNotification := func(t *testing.T, queue string) {
		t.Helper()
		config.Logger.InfoContext(ctx, "inserting "+queue+" job")
		_, err = client.Insert(ctx, JobArgs{}, &InsertOpts{Queue: queue})
		require.NoError(t, err)
		notif := riversharedtest.WaitOrTimeout(t, notifyCh)
		require.Equal(t, notifier.NotificationTopicInsert, notif.topic)
		require.Equal(t, queue, notif.payload.Queue)
	}

	// Immediate first fire on queue1:
	expectImmediateNotification(t, "queue1")
	// Keep time fixed inside the cooldown window before issuing repeated queue1
	// inserts. This guarantees that all of these inserts are ineligible for a
	// second notification regardless of wall-clock runtime.
	client.baseService.Time.StubNowUTC(now.Add(500 * time.Millisecond))

	for range 5 {
		config.Logger.InfoContext(ctx, "inserting queue1 job")
		_, err = client.Insert(ctx, JobArgs{}, &InsertOpts{Queue: "queue1"})
		require.NoError(t, err)
	}
	// No second `queue1` notification should arrive while still in cooldown.
	select {
	case notification := <-notifyCh:
		t.Fatalf("received insert notification when it should have been debounced %+v", notification)
	case <-time.After(100 * time.Millisecond):
	}

	// First notifications on other queues are independent from queue1's debounce
	// state and should still fire immediately.
	expectImmediateNotification(t, "queue2") // Immediate first fire on queue2
	expectImmediateNotification(t, "queue3") // Immediate first fire on queue3

	// `ShouldTrigger` uses a strict `Before` check; move just past the boundary.
	client.baseService.Time.StubNowUTC(now.Add(config.FetchCooldown + time.Nanosecond))

	// Now queue1 should immediately notify again.
	expectImmediateNotification(t, "queue1")
}

func Test_Client_JobCompletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool        *pgxpool.Pool
		schema        string
		subscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)
		config.Schema = schema

		client := newTestClient(t, dbPool, config)
		startClient(ctx, t, client)

		subscribeChan, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{
			dbPool:        dbPool,
			schema:        schema,
			subscribeChan: subscribeChan,
		}
	}

	t.Run("JobThatReturnsNilIsCompleted", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return nil
		}))

		client, bundle := setup(t, config)

		now := client.baseService.Time.StubNowUTC(time.Now().UTC())

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(t, now, *reloadedJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobThatIsAlreadyCompletedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()

		client, bundle := setup(t, newTestConfig(t, ""))

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			_, err := client.driver.GetExecutor().JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				ID:                  job.ID,
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				Schema:              bundle.schema,
				StateDoUpdate:       true,
				State:               rivertype.JobStateCompleted,
			})
			require.NoError(t, err)
			return nil
		}))

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(t, now, *reloadedJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobThatReturnsErrIsRetryable", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return errors.New("oops")
		}))
		config.RetryPolicy = &retrypolicytest.RetryPolicyNoJitter{}

		retryPolicy := &retrypolicytest.RetryPolicySlow{} // make sure job isn't set back to available/running by the time we check it below
		config.RetryPolicy = retryPolicy

		client, bundle := setup(t, config)

		now := client.baseService.Time.StubNowUTC(time.Now().UTC())

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateRetryable, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateRetryable, reloadedJob.State)
		require.WithinDuration(t, now.Add(retryPolicy.Interval()), reloadedJob.ScheduledAt, time.Microsecond)
		require.Nil(t, reloadedJob.FinalizedAt)
	})

	t.Run("JobThatReturnsJobCancelErrorIsImmediatelyCancelled", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, "")

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobCancel(errors.New("oops"))
		}))

		client, bundle := setup(t, config)

		now := client.baseService.Time.StubNowUTC(time.Now().UTC())

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateCancelled, reloadedJob.State)
		require.NotNil(t, reloadedJob.FinalizedAt)
		require.WithinDuration(t, now, *reloadedJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobThatIsAlreadyDiscardedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		now := time.Now().UTC()

		client, bundle := setup(t, newTestConfig(t, ""))

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			_, err := client.driver.GetExecutor().JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				ID:                  job.ID,
				ErrorsDoUpdate:      true,
				Errors:              [][]byte{[]byte("{\"error\": \"oops\"}")},
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				Schema:              bundle.schema,
				StateDoUpdate:       true,
				State:               rivertype.JobStateDiscarded,
			})
			require.NoError(t, err)
			return errors.New("oops")
		}))

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateDiscarded, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateDiscarded, reloadedJob.State)
		require.NotNil(t, reloadedJob.FinalizedAt)
	})

	t.Run("JobThatIsCompletedManuallyIsNotTouchedByCompleter", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t, newTestConfig(t, ""))

		now := client.baseService.Time.StubNowUTC(time.Now().UTC())

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		var updatedJob *Job[JobArgs]
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.dbPool.Begin(ctx)
			require.NoError(t, err)

			updatedJob, err = JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
			require.NoError(t, err)

			return tx.Commit(ctx)
		}))

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, rivertype.JobStateCompleted, event.Job.State)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
		require.NotNil(t, updatedJob)
		require.NotNil(t, event.Job.FinalizedAt)
		require.NotNil(t, updatedJob.FinalizedAt)

		// Make sure the FinalizedAt is approximately ~now:
		require.WithinDuration(t, now, *updatedJob.FinalizedAt, time.Microsecond)

		// Make sure we're getting the same timestamp back from the event and the
		// updated job inside the txn:
		require.WithinDuration(t, *updatedJob.FinalizedAt, *event.Job.FinalizedAt, time.Microsecond)

		reloadedJob, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)

		require.Equal(t, rivertype.JobStateCompleted, reloadedJob.State)
		require.Equal(t, updatedJob.FinalizedAt, reloadedJob.FinalizedAt)
	})
}

type unregisteredJobArgs struct{}

func (unregisteredJobArgs) Kind() string { return "RandomWorkerNameThatIsNeverRegistered" }

func Test_Client_UnknownJobKindErrorsTheJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)

	doneCh := make(chan struct{})
	close(doneCh) // don't need to block any jobs from completing

	config := newTestConfig(t, "")
	client := runNewTestClient(ctx, t, config)

	subscribeChan, cancel := client.Subscribe(EventKindJobFailed)
	t.Cleanup(cancel)

	insertParams, err := insertParamsFromConfigArgsAndOptions(&client.baseService.Archetype, config, unregisteredJobArgs{}, nil)
	require.NoError(t, err)
	insertedResults, err := client.driver.GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
		Jobs:   []*riverdriver.JobInsertFastParams{(*riverdriver.JobInsertFastParams)(insertParams)},
		Schema: client.config.Schema,
	})
	require.NoError(t, err)

	insertedResult := insertedResults[0]

	event := riversharedtest.WaitOrTimeout(t, subscribeChan)
	require.Equal(t, insertedResult.Job.ID, event.Job.ID)
	require.Equal(t, "RandomWorkerNameThatIsNeverRegistered", insertedResult.Job.Kind)
	require.Len(t, event.Job.Errors, 1)
	require.Equal(t, (&UnknownJobKindError{Kind: "RandomWorkerNameThatIsNeverRegistered"}).Error(), event.Job.Errors[0].Error)
	require.Equal(t, rivertype.JobStateRetryable, event.Job.State)
	// Ensure that ScheduledAt was updated with next run time:
	require.True(t, event.Job.ScheduledAt.After(insertedResult.Job.ScheduledAt))
	// It's the 1st attempt that failed. Attempt won't be incremented again until
	// the job gets fetched a 2nd time.
	require.Equal(t, 1, event.Job.Attempt)

	require.NoError(t, client.Stop(ctx))
}

func Test_Client_Start_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("NoQueueConfiguration", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)
		config.Queues = nil
		config.Workers = nil

		client := newTestClient(t, dbPool, config)
		err := client.Start(ctx)
		require.EqualError(t, err, "client Queues and Workers must be configured for a client to start working")
	})

	t.Run("NoRegisteredWorkers", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)
		config.Workers = NewWorkers() // initialized, but empty

		client := newTestClient(t, dbPool, config)
		err := client.Start(ctx)
		require.EqualError(t, err, "at least one Worker must be added to the Workers bundle")
	})

	t.Run("DatabaseError", func(t *testing.T) {
		t.Parallel()

		dbConfig := riversharedtest.DBPool(ctx, t).Config().Copy()
		dbConfig.ConnConfig.Database = "does-not-exist-and-dont-create-it"

		dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
		require.NoError(t, err)

		config := newTestConfig(t, "")

		client := newTestClient(t, dbPool, config)

		err = client.Start(ctx)
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.InvalidCatalogName, pgErr.Code)
	})
}

func Test_NewClient_BaseServiceName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		config = newTestConfig(t, schema)
		client = newTestClient(t, dbPool, config)
	)

	// Ensure we get the clean name "Client" instead of the fully qualified name
	// with generic type param:
	require.Equal(t, "Client", client.baseService.Name)
}

func Test_NewClient_ClientIDWrittenToJobAttemptedByWhenFetched(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	config := newTestConfig(t, "")

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	doneCh := make(chan struct{})
	startedCh := make(chan *Job[JobArgs])
	AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
		startedCh <- job
		<-doneCh
		return nil
	}))

	client := runNewTestClient(ctx, t, config)
	t.Cleanup(func() { close(doneCh) })

	// enqueue job:
	insertCtx, insertCancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(insertCancel)
	insertRes, err := client.Insert(insertCtx, JobArgs{}, nil)
	require.NoError(t, err)
	require.Nil(t, insertRes.Job.AttemptedAt)
	require.Empty(t, insertRes.Job.AttemptedBy)

	var startedJob *Job[JobArgs]
	select {
	case startedJob = <-startedCh:
		require.Equal(t, []string{client.ID()}, startedJob.AttemptedBy)
		require.NotNil(t, startedJob.AttemptedAt)
		require.WithinDuration(t, time.Now().UTC(), *startedJob.AttemptedAt, 2*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}
}

func Test_NewClient_Defaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	client, err := NewClient(driver, &Config{
		Queues:  map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		Schema:  schema,
		Workers: workers,
	})
	require.NoError(t, err)

	require.Zero(t, client.config.AdvisoryLockPrefix)

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, riversharedmaintenance.CancelledJobRetentionPeriodDefault, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, riversharedmaintenance.CompletedJobRetentionPeriodDefault, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, riversharedmaintenance.DiscardedJobRetentionPeriodDefault, jobCleaner.Config.DiscardedJobRetentionPeriod)
	require.Equal(t, riversharedmaintenance.JobCleanerTimeoutDefault, jobCleaner.Config.Timeout)
	require.False(t, jobCleaner.StaggerStartupIsDisabled())

	enqueuer := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
	require.Zero(t, enqueuer.Config.AdvisoryLockPrefix)
	require.False(t, enqueuer.StaggerStartupIsDisabled())

	reindexer := maintenance.GetService[*maintenance.Reindexer](client.queueMaintainer)
	require.Contains(t, reindexer.Config.IndexNames, "river_job_args_index")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_kind")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_metadata_index")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_pkey")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_prioritized_fetching_index")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_state_and_finalized_at_index")
	require.Contains(t, reindexer.Config.IndexNames, "river_job_unique_idx")
	now := time.Now().UTC()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	require.Equal(t, nextMidnight, reindexer.Config.ScheduleFunc(now))

	require.Nil(t, client.config.ErrorHandler)
	require.Equal(t, FetchCooldownDefault, client.config.FetchCooldown)
	require.Equal(t, FetchPollIntervalDefault, client.config.FetchPollInterval)
	require.Equal(t, JobTimeoutDefault, client.config.JobTimeout)
	require.Nil(t, client.config.Hooks)
	require.NotZero(t, client.baseService.Logger)
	require.Equal(t, MaxAttemptsDefault, client.config.MaxAttempts)
	require.Equal(t, maintenance.ReindexerTimeoutDefault, client.config.ReindexerTimeout)
	require.IsType(t, &DefaultClientRetryPolicy{}, client.config.RetryPolicy)
	require.False(t, client.config.SkipUnknownJobCheck)
	require.IsType(t, nil, client.config.Test.Time)
	require.IsType(t, &baseservice.UnStubbableTimeGenerator{}, client.baseService.Time)
}

func Test_NewClient_Overrides(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)

	errorHandler := &testErrorHandler{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	retryPolicy := &DefaultClientRetryPolicy{}

	type noOpHook struct {
		HookDefaults
	}

	type noOpInsertMiddleware struct {
		JobInsertMiddlewareDefaults
	}

	type noOpWorkerMiddleware struct {
		WorkerMiddlewareDefaults
	}

	client, err := NewClient(driver, &Config{
		AdvisoryLockPrefix:          123_456,
		CancelledJobRetentionPeriod: 1 * time.Hour,
		CompletedJobRetentionPeriod: 2 * time.Hour,
		DiscardedJobRetentionPeriod: 3 * time.Hour,
		ErrorHandler:                errorHandler,
		FetchCooldown:               123 * time.Millisecond,
		FetchPollInterval:           124 * time.Millisecond,
		Hooks:                       []rivertype.Hook{&noOpHook{}},
		JobInsertMiddleware:         []rivertype.JobInsertMiddleware{&noOpInsertMiddleware{}},
		JobTimeout:                  125 * time.Millisecond,
		Logger:                      logger,
		MaxAttempts:                 5,
		Queues:                      map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		ReindexerSchedule:           &periodicIntervalSchedule{interval: time.Hour},
		ReindexerTimeout:            125 * time.Millisecond,
		RetryPolicy:                 retryPolicy,
		Schema:                      schema,
		SkipUnknownJobCheck:         true,
		TestOnly:                    true, // disables staggered start in maintenance services
		Workers:                     workers,
		WorkerMiddleware:            []rivertype.WorkerMiddleware{&noOpWorkerMiddleware{}},
	})
	require.NoError(t, err)

	require.Equal(t, int32(123_456), client.config.AdvisoryLockPrefix)

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, 1*time.Hour, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, 2*time.Hour, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, 3*time.Hour, jobCleaner.Config.DiscardedJobRetentionPeriod)
	require.True(t, jobCleaner.StaggerStartupIsDisabled())

	enqueuer := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
	require.Equal(t, int32(123_456), enqueuer.Config.AdvisoryLockPrefix)
	require.True(t, enqueuer.StaggerStartupIsDisabled())

	reindexer := maintenance.GetService[*maintenance.Reindexer](client.queueMaintainer)
	now := time.Now().UTC()
	require.Equal(t, now.Add(time.Hour), reindexer.Config.ScheduleFunc(now))

	require.Equal(t, errorHandler, client.config.ErrorHandler)
	require.Equal(t, 123*time.Millisecond, client.config.FetchCooldown)
	require.Equal(t, 124*time.Millisecond, client.config.FetchPollInterval)
	require.Len(t, client.config.JobInsertMiddleware, 1)
	require.Equal(t, 125*time.Millisecond, client.config.JobTimeout)
	require.Equal(t, []rivertype.Hook{&noOpHook{}}, client.config.Hooks)
	require.Equal(t, logger, client.baseService.Logger)
	require.Equal(t, 5, client.config.MaxAttempts)
	require.Equal(t, 125*time.Millisecond, client.config.ReindexerTimeout)
	require.Equal(t, retryPolicy, client.config.RetryPolicy)
	require.Equal(t, schema, client.config.Schema)
	require.True(t, client.config.SkipUnknownJobCheck)
	require.Len(t, client.config.WorkerMiddleware, 1)
}

func Test_NewClient_MissingParameters(t *testing.T) {
	t.Parallel()

	t.Run("ErrorOnNilDriver", func(t *testing.T) {
		t.Parallel()

		_, err := NewClient[pgx.Tx](nil, &Config{})
		require.ErrorIs(t, err, errMissingDriver)
	})

	t.Run("ErrorOnNilConfig", func(t *testing.T) {
		t.Parallel()

		_, err := NewClient(riverpgxv5.New(nil), nil)
		require.ErrorIs(t, err, errMissingConfig)
	})

	t.Run("ErrorOnDriverWithNoDatabasePoolAndQueues", func(t *testing.T) {
		t.Parallel()

		_, err := NewClient(riverpgxv5.New(nil), newTestConfig(t, ""))
		require.ErrorIs(t, err, errMissingDatabasePoolWithQueues)
	})
}

func Test_NewClient_Validations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		configFunc     func(*Config)
		wantErr        error
		validateResult func(*testing.T, *Client[pgx.Tx])
	}{
		{
			name:       "CompletedJobRetentionPeriod cannot be less than zero",
			configFunc: func(config *Config) { config.CompletedJobRetentionPeriod = -1 * time.Second },
			wantErr:    errors.New("CompletedJobRetentionPeriod cannot be less than zero"),
		},
		{
			name:       "FetchCooldown cannot be less than FetchCooldownMin",
			configFunc: func(config *Config) { config.FetchCooldown = time.Millisecond - 1 },
			wantErr:    errors.New("FetchCooldown must be at least 1ms"),
		},
		{
			name:       "FetchCooldown cannot be negative",
			configFunc: func(config *Config) { config.FetchCooldown = -1 },
			wantErr:    errors.New("FetchCooldown must be at least 1ms"),
		},
		{
			name:       "FetchCooldown defaults to FetchCooldownDefault",
			configFunc: func(config *Config) { config.FetchCooldown = 0 },
			wantErr:    nil,
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, FetchCooldownDefault, client.config.FetchCooldown)
			},
		},
		{
			name: "FetchCooldown cannot be less than FetchPollInterval",
			configFunc: func(config *Config) {
				config.FetchCooldown = 20 * time.Millisecond
				config.FetchPollInterval = 19 * time.Millisecond
			},
			wantErr: fmt.Errorf("FetchPollInterval cannot be shorter than FetchCooldown (%s)", 20*time.Millisecond),
		},
		{
			name:       "FetchPollInterval cannot be less than MinFetchPollInterval",
			configFunc: func(config *Config) { config.FetchPollInterval = time.Millisecond - 1 },
			wantErr:    errors.New("FetchPollInterval must be at least 1ms"),
		},
		{
			name:       "FetchPollInterval cannot be negative",
			configFunc: func(config *Config) { config.FetchPollInterval = -1 },
			wantErr:    errors.New("FetchPollInterval must be at least 1ms"),
		},
		{
			name:       "FetchPollInterval defaults to DefaultFetchPollInterval",
			configFunc: func(config *Config) { config.FetchPollInterval = 0 },
			wantErr:    nil,
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, FetchPollIntervalDefault, client.config.FetchPollInterval)
			},
		},
		{
			name: "ID cannot be longer than 100 characters",
			// configFunc: func(config *Config) { config.ID = strings.Repeat("a", 101) },
			configFunc: func(config *Config) {
				config.ID = strings.Repeat("a", 101)
			},
			wantErr: errors.New("ID cannot be longer than 100 characters"),
		},
		{
			name: "JobTimeout can be -1 (infinite)",
			configFunc: func(config *Config) {
				config.JobTimeout = -1
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, time.Duration(-1), client.config.JobTimeout)
			},
		},
		{
			name: "JobTimeout cannot be less than -1",
			configFunc: func(config *Config) {
				config.JobTimeout = -2
			},
			wantErr: errors.New("JobTimeout cannot be negative, except for -1 (infinite)"),
		},
		{
			name: "JobTimeout of zero applies DefaultJobTimeout",
			configFunc: func(config *Config) {
				config.JobTimeout = 0
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				// A client config value of zero gets interpreted as the default timeout:
				require.Equal(t, JobTimeoutDefault, client.config.JobTimeout)
			},
		},
		{
			name: "JobTimeout can be a large positive value",
			configFunc: func(config *Config) {
				config.JobTimeout = 7 * 24 * time.Hour
			},
		},
		{
			name: "MaxAttempts cannot be less than zero",
			configFunc: func(config *Config) {
				config.MaxAttempts = -1
			},
			wantErr: errors.New("MaxAttempts cannot be less than zero"),
		},
		{
			name: "MaxAttempts of zero applies DefaultMaxAttempts",
			configFunc: func(config *Config) {
				config.MaxAttempts = 0
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				// A client config value of zero gets interpreted as the default max attempts:
				require.Equal(t, MaxAttemptsDefault, client.config.MaxAttempts)
			},
		},
		{
			name: "Middleware can be configured independently",
			configFunc: func(config *Config) {
				config.Middleware = []rivertype.Middleware{&overridableJobMiddleware{}}
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindJobInsert), 1)
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindWorker), 1)
			},
		},
		{
			name: "JobInsertMiddleware and WorkMiddleware may be configured together with separate middlewares",
			configFunc: func(config *Config) {
				config.JobInsertMiddleware = []rivertype.JobInsertMiddleware{&overridableJobMiddleware{}}
				config.WorkerMiddleware = []rivertype.WorkerMiddleware{&overridableJobMiddleware{}}
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindJobInsert), 2)
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindWorker), 2)
			},
		},
		{
			name: "JobInsertMiddleware and WorkMiddleware may be configured together with same middleware",
			configFunc: func(config *Config) {
				middleware := &overridableJobMiddleware{}
				config.JobInsertMiddleware = []rivertype.JobInsertMiddleware{middleware}
				config.WorkerMiddleware = []rivertype.WorkerMiddleware{middleware}
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindJobInsert), 1)
				require.Len(t, client.middlewareLookupGlobal.ByMiddlewareKind(middlewarelookup.MiddlewareKindWorker), 1)
			},
		},
		{
			name: "Middleware not allowed with JobInsertMiddleware",
			configFunc: func(config *Config) {
				config.JobInsertMiddleware = []rivertype.JobInsertMiddleware{&overridableJobMiddleware{}}
				config.Middleware = []rivertype.Middleware{&overridableJobMiddleware{}}
			},
			wantErr: errors.New("only one of the pair JobInsertMiddleware/WorkerMiddleware or Middleware may be provided (Middleware is recommended, and may contain both job insert and worker middleware)"),
		},
		{
			name: "Middleware not allowed with WorkerMiddleware",
			configFunc: func(config *Config) {
				config.Middleware = []rivertype.Middleware{&overridableJobMiddleware{}}
				config.WorkerMiddleware = []rivertype.WorkerMiddleware{&overridableJobMiddleware{}}
			},
			wantErr: errors.New("only one of the pair JobInsertMiddleware/WorkerMiddleware or Middleware may be provided (Middleware is recommended, and may contain both job insert and worker middleware)"),
		},
		{
			name: "ReindexerTimeout can be -1 (infinite)",
			configFunc: func(config *Config) {
				config.ReindexerTimeout = -1
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, time.Duration(-1), client.config.ReindexerTimeout)
			},
		},
		{
			name: "ReindexerTimeout cannot be less than -1",
			configFunc: func(config *Config) {
				config.ReindexerTimeout = -2
			},
			wantErr: errors.New("ReindexerTimeout cannot be negative, except for -1 (infinite)"),
		},
		{
			name: "ReindexerTimeout of zero applies maintenance.DefaultReindexerTimeout",
			configFunc: func(config *Config) {
				config.ReindexerTimeout = 0
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, maintenance.ReindexerTimeoutDefault, client.config.ReindexerTimeout)
			},
		},
		{
			name: "ReindexerTimeout can be a large positive value",
			configFunc: func(config *Config) {
				config.ReindexerTimeout = 7 * 24 * time.Hour
			},
		},
		{
			name: "RescueStuckJobsAfter may be overridden",
			configFunc: func(config *Config) {
				config.RescueStuckJobsAfter = 23 * time.Hour
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, 23*time.Hour, client.config.RescueStuckJobsAfter)
			},
		},
		{
			name: "RescueStuckJobsAfter must be larger than JobTimeout",
			configFunc: func(config *Config) {
				config.JobTimeout = 7 * time.Hour
				config.RescueStuckJobsAfter = 6 * time.Hour
			},
			wantErr: errors.New("RescueStuckJobsAfter cannot be less than JobTimeout"),
		},
		{
			name: "RescueStuckJobsAfter increased automatically on a high JobTimeout when not set explicitly",
			configFunc: func(config *Config) {
				config.JobTimeout = 23 * time.Hour
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, 23*time.Hour+maintenance.JobRescuerRescueAfterDefault, client.config.RescueStuckJobsAfter)
			},
		},
		{
			name: "Schema length must be less than or equal to 46 characters",
			configFunc: func(config *Config) {
				config.Schema = strings.Repeat("a", 47)
			},
			wantErr: errors.New("Schema length must be less than or equal to 46 characters"),
		},
		{
			name: "Schema cannot contain invalid characters",
			configFunc: func(config *Config) {
				config.Schema = "invalid-schema@name"
			},
			wantErr: errors.New("Schema name can only contain letters, numbers, and underscores, and must start with a letter or underscore"),
		},
		{
			name: "Queues can be nil when Workers is also nil",
			configFunc: func(config *Config) {
				config.Queues = nil
				config.Workers = nil
			},
		},
		{
			name: "Queues can be nil when Workers is not nil",
			configFunc: func(config *Config) {
				config.Queues = nil
			},
		},
		{
			name:       "Queues can be empty",
			configFunc: func(config *Config) { config.Queues = make(map[string]QueueConfig) },
		},
		{
			name: "Queues FetchCooldown can be overridden",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{QueueDefault: {FetchCooldown: 9 * time.Millisecond, MaxWorkers: 1}}
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, 9*time.Millisecond, client.producersByQueueName[QueueDefault].config.FetchCooldown)
			},
		},
		{
			name: "Queues FetchCooldown can't be greater than Client FetchPollInterval",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{QueueDefault: {FetchCooldown: 10 * time.Millisecond, MaxWorkers: 1}}
				config.FetchPollInterval = 9 * time.Millisecond
			},
			wantErr: fmt.Errorf("FetchPollInterval cannot be shorter than FetchCooldown (%s)", FetchCooldownDefault),
		},
		{
			name: "Queues FetchPollInterval can be overridden",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{QueueDefault: {FetchPollInterval: 9 * time.Second, MaxWorkers: 1}}
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, 9*time.Second, client.producersByQueueName[QueueDefault].config.FetchPollInterval)
			},
		},
		{
			name: "Queues MaxWorkers can't be negative",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: -1}}
			},
			wantErr: errors.New("invalid number of workers for queue \"default\": -1"),
		},
		{
			name: "Queues can't have limits larger than MaxQueueNumWorkers",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: QueueNumWorkersMax + 1}}
			},
			wantErr: fmt.Errorf("invalid number of workers for queue \"default\": %d", QueueNumWorkersMax+1),
		},
		{
			name: "Queues queue names can't be empty",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"": {MaxWorkers: 1}}
			},
			wantErr: errors.New("queue name cannot be empty"),
		},
		{
			name: "Queues queue names can't be too long",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{strings.Repeat("a", 65): {MaxWorkers: 1}}
			},
			wantErr: errors.New("queue name cannot be longer than 64 characters"),
		},
		{
			name: "Queues queue names can't have asterisks",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"no*hyphens": {MaxWorkers: 1}}
			},
			wantErr: errors.New("queue name is invalid, expected letters and numbers separated by underscores or hyphens: \"no*hyphens\""),
		},
		{
			name: "Queues queue names can be letters and numbers joined by underscores",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"some_awesome_3rd_queue_namezzz": {MaxWorkers: 1}}
			},
		},
		{
			name: "Queues queue names can be letters and numbers joined by hyphens",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"some-awesome-3rd-queue-namezzz": {MaxWorkers: 1}}
			},
		},
		{
			name: "Workers can be nil",
			configFunc: func(config *Config) {
				config.Queues = nil
				config.Workers = nil
			},
		},
		{
			name: "Workers can be empty", // but notably, not allowed to be empty if started
			configFunc: func(config *Config) {
				config.Workers = NewWorkers()
			},
		},
		{
			name: "Workers cannot be empty if Queues is set",
			configFunc: func(config *Config) {
				config.Workers = nil
			},
			wantErr: errors.New("Workers must be set if Queues is set"),
		},
		{
			name: "Job kinds must be valid",
			configFunc: func(config *Config) {
				AddWorker(config.Workers, &invalidKindWorker{})
			},
			wantErr: fmt.Errorf("job kind %q should match regex %s", "this kind is invalid", rivercommon.UserSpecifiedIDOrKindRE.String()),
		},
		{
			name: "Job kind validation skipped with SkipJobKindValidation",
			configFunc: func(config *Config) {
				// this run will emit a warning; make sure it's collated under
				// this test as opposed to going to stdout/stderr
				config.Logger = riversharedtest.Logger(t)
				config.SkipJobKindValidation = true
				AddWorker(config.Workers, &invalidKindWorker{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			var (
				dbPool = riversharedtest.DBPool(ctx, t)
				driver = riverpgxv5.New(dbPool)
				schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			)

			workers := NewWorkers()
			AddWorker(workers, &noOpWorker{})

			config := &Config{
				Queues:  map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
				Schema:  schema,
				Workers: workers,
			}
			tt.configFunc(config)

			client, err := NewClient(driver, config)
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr.Error())
				return
			}
			require.NoError(t, err)

			if tt.validateResult != nil {
				tt.validateResult(t, client)
			}
		})
	}
}

type timeoutTestArgs struct {
	TimeoutValue time.Duration `json:"timeout_value"`
}

func (timeoutTestArgs) Kind() string { return "timeoutTest" }

type testWorkerDeadline struct {
	deadline time.Time
	ok       bool
}

type timeoutTestWorker struct {
	WorkerDefaults[timeoutTestArgs]

	doneCh chan testWorkerDeadline
}

func (w *timeoutTestWorker) Timeout(job *Job[timeoutTestArgs]) time.Duration {
	return job.Args.TimeoutValue
}

func (w *timeoutTestWorker) Work(ctx context.Context, job *Job[timeoutTestArgs]) error {
	deadline, ok := ctx.Deadline()
	w.doneCh <- testWorkerDeadline{deadline: deadline, ok: ok}
	return nil
}

func TestClient_JobTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		jobArgTimeout    time.Duration
		clientJobTimeout time.Duration
		wantDuration     time.Duration
	}{
		{
			name:             "ClientJobTimeoutIsUsedIfJobArgTimeoutIsZero",
			jobArgTimeout:    0,
			clientJobTimeout: time.Hour,
			wantDuration:     time.Hour,
		},
		{
			name:             "JobArgTimeoutTakesPrecedenceIfBothAreSet",
			jobArgTimeout:    2 * time.Hour,
			clientJobTimeout: time.Hour,
			wantDuration:     2 * time.Hour,
		},
		{
			name:             "DefaultJobTimeoutIsUsedIfBothAreZero",
			jobArgTimeout:    0,
			clientJobTimeout: 0,
			wantDuration:     JobTimeoutDefault,
		},
		{
			name:             "NoJobTimeoutIfClientIsNegativeOneAndJobArgIsZero",
			jobArgTimeout:    0,
			clientJobTimeout: -1,
			wantDuration:     0, // infinite
		},
		{
			name:             "NoJobTimeoutIfJobArgIsNegativeOne",
			jobArgTimeout:    -1,
			clientJobTimeout: time.Hour,
			wantDuration:     0, // infinite
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			testWorker := &timeoutTestWorker{doneCh: make(chan testWorkerDeadline)}

			workers := NewWorkers()
			AddWorker(workers, testWorker)

			config := newTestConfig(t, "")
			config.JobTimeout = tt.clientJobTimeout
			config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}}
			config.Workers = workers

			client := runNewTestClient(ctx, t, config)
			_, err := client.Insert(ctx, timeoutTestArgs{TimeoutValue: tt.jobArgTimeout}, nil)
			require.NoError(t, err)

			result := riversharedtest.WaitOrTimeout(t, testWorker.doneCh)
			if tt.wantDuration == 0 {
				require.False(t, result.ok, "expected no deadline")
				return
			}
			require.True(t, result.ok, "expected a deadline, but none was set")
			require.WithinDuration(t, time.Now().Add(tt.wantDuration), result.deadline, 2*time.Second)
		})
	}
}

type JobArgsStaticKind struct {
	kind string
}

func (a JobArgsStaticKind) Kind() string {
	return a.kind
}

type JobArgsReflectKind[TKind any] struct{}

func (a JobArgsReflectKind[TKind]) Kind() string {
	return reflect.TypeFor[JobArgsReflectKind[TKind]]().Name()
}

func TestInsertParamsFromJobArgsAndOptions(t *testing.T) {
	t.Parallel()

	archetype := riversharedtest.BaseServiceArchetype(t)
	config := newTestConfig(t, "")

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, nil)
		require.NoError(t, err)
		require.JSONEq(t, `{"name":""}`, string(insertParams.EncodedArgs))
		require.Equal(t, (noOpArgs{}).Kind(), insertParams.Kind)
		require.Equal(t, config.MaxAttempts, insertParams.MaxAttempts)
		require.Equal(t, rivercommon.PriorityDefault, insertParams.Priority)
		require.Equal(t, QueueDefault, insertParams.Queue)
		require.Nil(t, insertParams.ScheduledAt)
		require.Equal(t, []string{}, insertParams.Tags)
		require.Empty(t, insertParams.UniqueKey)
		require.Zero(t, insertParams.UniqueStates)
	})

	t.Run("ConfigOverrides", func(t *testing.T) {
		t.Parallel()

		overrideConfig := &Config{
			MaxAttempts: 34,
		}

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, overrideConfig, noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, overrideConfig.MaxAttempts, insertParams.MaxAttempts)
	})

	t.Run("InsertOptsOverrides", func(t *testing.T) {
		t.Parallel()

		opts := &InsertOpts{
			MaxAttempts: 42,
			Priority:    2,
			Queue:       "other",
			ScheduledAt: time.Now().Add(time.Hour),
			Tags:        []string{"tag1", "tag2"},
		}
		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.Equal(t, opts.ScheduledAt, *insertParams.ScheduledAt)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("WorkerInsertOptsOverrides", func(t *testing.T) {
		t.Parallel()

		nearFuture := time.Now().Add(5 * time.Minute)

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, &customInsertOptsJobArgs{
			ScheduledAt: nearFuture,
		}, nil)
		require.NoError(t, err)
		// All these come from overrides in customInsertOptsJobArgs's definition:
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.NotNil(t, insertParams.ScheduledAt)
		require.Equal(t, nearFuture, *insertParams.ScheduledAt)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("WorkerInsertOptsScheduledAtNotRespectedIfZero", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, &customInsertOptsJobArgs{
			ScheduledAt: time.Time{},
		}, nil)
		require.NoError(t, err)
		require.Nil(t, insertParams.ScheduledAt)
	})

	t.Run("TagFormatValidated", func(t *testing.T) {
		t.Parallel()

		{
			_, err := insertParamsFromConfigArgsAndOptions(archetype, config, &customInsertOptsJobArgs{}, &InsertOpts{
				Tags: []string{strings.Repeat("h", 256)},
			})
			require.EqualError(t, err, "tags should be a maximum of 255 characters long")
		}

		{
			_, err := insertParamsFromConfigArgsAndOptions(archetype, config, &customInsertOptsJobArgs{}, &InsertOpts{
				Tags: []string{"tag,with,comma"},
			})
			require.EqualError(t, err, "tags should match regex "+tagRE.String())
		}
	})

	t.Run("UniqueOptsDefaultStates", func(t *testing.T) {
		t.Parallel()

		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Time.StubNowUTC(time.Now().UTC())

		uniqueOpts := UniqueOpts{
			ByArgs:      true,
			ByPeriod:    10 * time.Second,
			ByQueue:     true,
			ExcludeKind: true,
		}

		params, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, &InsertOpts{UniqueOpts: uniqueOpts})
		require.NoError(t, err)
		internalUniqueOpts := &dbunique.UniqueOpts{
			ByArgs:      true,
			ByPeriod:    10 * time.Second,
			ByQueue:     true,
			ByState:     []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted, rivertype.JobStatePending, rivertype.JobStateRunning, rivertype.JobStateRetryable, rivertype.JobStateScheduled},
			ExcludeKind: true,
		}

		expectedKey, err := dbunique.UniqueKey(archetype.Time, internalUniqueOpts, params)
		require.NoError(t, err)

		require.Equal(t, expectedKey, params.UniqueKey)
		require.Equal(t, internalUniqueOpts.StateBitmask(), params.UniqueStates)
	})

	t.Run("UniqueOptsCustomStates", func(t *testing.T) {
		t.Parallel()

		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Time.StubNowUTC(time.Now().UTC())

		states := []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateRunning,
			rivertype.JobStateScheduled,
		}

		uniqueOpts := UniqueOpts{
			ByPeriod: 10 * time.Second,
			ByQueue:  true,
			ByState:  states,
		}

		params, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, &InsertOpts{UniqueOpts: uniqueOpts})
		require.NoError(t, err)
		internalUniqueOpts := &dbunique.UniqueOpts{
			ByPeriod: 10 * time.Second,
			ByQueue:  true,
			ByState:  states,
		}

		expectedKey, err := dbunique.UniqueKey(archetype.Time, internalUniqueOpts, params)
		require.NoError(t, err)

		require.Equal(t, expectedKey, params.UniqueKey)
		require.Equal(t, internalUniqueOpts.StateBitmask(), params.UniqueStates)
	})

	t.Run("UniqueOptsWithPartialArgs", func(t *testing.T) {
		t.Parallel()

		uniqueOpts := UniqueOpts{ByArgs: true}

		type PartialArgs struct {
			JobArgsStaticKind

			Included bool `json:"included" river:"unique"`
			Excluded bool `json:"excluded"`
		}

		args := PartialArgs{
			JobArgsStaticKind: JobArgsStaticKind{kind: "partialArgs"},
			Included:          true,
			Excluded:          true,
		}

		params, err := insertParamsFromConfigArgsAndOptions(archetype, config, args, &InsertOpts{UniqueOpts: uniqueOpts})
		require.NoError(t, err)
		internalUniqueOpts := &dbunique.UniqueOpts{ByArgs: true}

		expectedKey, err := dbunique.UniqueKey(archetype.Time, internalUniqueOpts, params)
		require.NoError(t, err)
		require.Equal(t, expectedKey, params.UniqueKey)
		require.Equal(t, internalUniqueOpts.StateBitmask(), params.UniqueStates)

		argsWithExcludedFalse := PartialArgs{
			JobArgsStaticKind: JobArgsStaticKind{kind: "partialArgs"},
			Included:          true,
			Excluded:          false,
		}

		params2, err := insertParamsFromConfigArgsAndOptions(archetype, config, argsWithExcludedFalse, &InsertOpts{UniqueOpts: uniqueOpts})
		require.NoError(t, err)
		internalUniqueOpts2 := &dbunique.UniqueOpts{ByArgs: true}

		expectedKey2, err := dbunique.UniqueKey(archetype.Time, internalUniqueOpts2, params2)
		require.NoError(t, err)
		require.Equal(t, expectedKey2, params2.UniqueKey)
		require.Equal(t, internalUniqueOpts2.StateBitmask(), params.UniqueStates)
		require.Equal(t, params.UniqueKey, params2.UniqueKey, "unique keys should be identical because included args are the same, even though others differ")
	})

	t.Run("PriorityMinimum1", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, &InsertOpts{Priority: -1})
		require.ErrorContains(t, err, "priority must be between 1 and 4")
		require.Nil(t, insertParams)
	})

	t.Run("PriorityMaximum4", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, noOpArgs{}, &InsertOpts{Priority: 5})
		require.ErrorContains(t, err, "priority must be between 1 and 4")
		require.Nil(t, insertParams)
	})

	t.Run("NonEmptyArgs", func(t *testing.T) {
		t.Parallel()

		args := timeoutTestArgs{TimeoutValue: time.Hour}
		insertParams, err := insertParamsFromConfigArgsAndOptions(archetype, config, args, nil)
		require.NoError(t, err)
		require.Equal(t, `{"timeout_value":3600000000000}`, string(insertParams.EncodedArgs))
	})

	t.Run("UniqueOptsAreValidated", func(t *testing.T) {
		t.Parallel()

		// Ensure that unique opts are validated. No need to be exhaustive here
		// since we already have tests elsewhere for that. Just make sure validation
		// is running.
		insertParams, err := insertParamsFromConfigArgsAndOptions(
			archetype,
			config,
			noOpArgs{},
			&InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: 1 * time.Millisecond}},
		)
		require.EqualError(t, err, "UniqueOpts.ByPeriod should not be less than 1 second")
		require.Nil(t, insertParams)
	})
}

func TestID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("IsGeneratedWhenNotSpecifiedInConfig", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		require.NotEmpty(t, client.ID())
	})

	t.Run("IsGeneratedWhenNotSpecifiedInConfig", func(t *testing.T) {
		t.Parallel()

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
		)
		config.ID = "my-client-id"

		client := newTestClient(t, dbPool, config)
		require.Equal(t, "my-client-id", client.ID())
	})
}

type customInsertOptsJobArgs struct {
	ScheduledAt time.Time `json:"scheduled_at"`
}

func (w *customInsertOptsJobArgs) Kind() string { return "customInsertOpts" }

func (w *customInsertOptsJobArgs) InsertOpts() InsertOpts {
	return InsertOpts{
		MaxAttempts: 42,
		Priority:    2,
		Queue:       "other",
		ScheduledAt: w.ScheduledAt,
		Tags:        []string{"tag1", "tag2"},
	}
}

func (w *customInsertOptsJobArgs) Work(context.Context, *Job[noOpArgs]) error { return nil }

func TestInsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var (
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	config := &Config{
		Queues:  map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		Schema:  schema,
		Workers: workers,
	}

	client, err := NewClient(riverpgxv5.New(dbPool), config)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	requireEqualArgs := func(t *testing.T, expectedArgs *noOpArgs, actualPayload []byte) {
		t.Helper()
		var actualArgs noOpArgs
		if err := json.Unmarshal(actualPayload, &actualArgs); err != nil {
			t.Fatal(err)
		}
		require.Equal(t, *expectedArgs, actualArgs)
	}

	tests := []struct {
		name   string
		args   noOpArgs
		opts   *InsertOpts
		assert func(t *testing.T, args *noOpArgs, opts *InsertOpts, insertedJob *rivertype.JobRow)
	}{
		{
			name: "all options specified",
			args: noOpArgs{Name: "testJob"},
			opts: &InsertOpts{
				Queue:    "other",
				Priority: 2,
				// TODO: comprehensive timezone testing
				ScheduledAt: now.Add(time.Hour).In(time.FixedZone("UTC-5", -5*60*60)),
				Tags:        []string{"tag1", "tag2"},
			},
			assert: func(t *testing.T, args *noOpArgs, opts *InsertOpts, insertedJob *rivertype.JobRow) {
				t.Helper()

				require := require.New(t)
				// specified by inputs:
				requireEqualArgs(t, args, insertedJob.EncodedArgs)
				require.Equal("other", insertedJob.Queue)
				require.Equal(2, insertedJob.Priority)
				// Postgres timestamptz only stores microsecond precision so we need to
				// assert approximate equality:
				require.WithinDuration(opts.ScheduledAt.UTC(), insertedJob.ScheduledAt, time.Microsecond)
				require.Equal([]string{"tag1", "tag2"}, insertedJob.Tags)
				// derived state:
				require.Equal(rivertype.JobStateScheduled, insertedJob.State)
				require.Equal("noOp", insertedJob.Kind)
				// default state:
				// require.Equal([]byte("{}"), insertedJob.metadata)
			},
		},
		{
			name: "all defaults",
			args: noOpArgs{Name: "testJob"},
			opts: nil,
			assert: func(t *testing.T, args *noOpArgs, opts *InsertOpts, insertedJob *rivertype.JobRow) {
				t.Helper()

				require := require.New(t)
				// specified by inputs:
				requireEqualArgs(t, args, insertedJob.EncodedArgs)
				// derived state:
				require.Equal(rivertype.JobStateAvailable, insertedJob.State)
				require.Equal("noOp", insertedJob.Kind)
				// default state:
				require.Equal(QueueDefault, insertedJob.Queue)
				require.Equal(1, insertedJob.Priority)
				// Default comes from database now(), and we can't know the exact value:
				require.WithinDuration(time.Now(), insertedJob.ScheduledAt, 2*time.Second)
				require.Equal([]string{}, insertedJob.Tags)
				// require.Equal([]byte("{}"), insertedJob.metadata)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)
			if tt.assert == nil {
				t.Fatalf("test %q did not specify an assert function", tt.name)
			}

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			insertRes, err := client.Insert(ctx, tt.args, tt.opts)
			require.NoError(err)
			tt.assert(t, &tt.args, tt.opts, insertRes.Job)

			// Also test InsertTx:
			tx, err := dbPool.Begin(ctx)
			require.NoError(err)
			defer tx.Rollback(ctx)

			insertedJob2, err := client.InsertTx(ctx, tx, tt.args, tt.opts)
			require.NoError(err)
			tt.assert(t, &tt.args, tt.opts, insertedJob2.Job)
		})
	}
}

func TestUniqueOpts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		workers := NewWorkers()
		AddWorker(workers, &noOpWorker{})

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			config = newTestConfig(t, schema)
			client = newTestClient(t, dbPool, config)
		)

		// Tests that use ByPeriod below can be sensitive to intermittency if
		// the tests run at say 23:59:59.998, then it's possible to accidentally
		// cross a period threshold, even if very unlikely. So here, seed mostly
		// the current time, but make sure it's nicened up a little to be
		// roughly in the middle of the hour and well clear of any period
		// boundaries.
		client.baseService.Time.StubNowUTC(
			time.Now().Truncate(1 * time.Hour).Add(37*time.Minute + 23*time.Second + 123*time.Millisecond).UTC(),
		)

		return client, &testBundle{}
	}

	t.Run("DeduplicatesJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		uniqueOpts := UniqueOpts{
			ByPeriod: 24 * time.Hour,
		}

		insertRes0, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)
		require.False(t, insertRes0.UniqueSkippedAsDuplicate)

		insertRes1, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)
		require.True(t, insertRes1.UniqueSkippedAsDuplicate)

		// Expect the same job to come back.
		require.Equal(t, insertRes0.Job.ID, insertRes1.Job.ID)
	})

	t.Run("UniqueByCustomStates", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		uniqueOpts := UniqueOpts{
			ByPeriod: 24 * time.Hour,
			ByState:  rivertype.JobStates(),
			ByQueue:  true,
		}

		insertRes0, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		insertRes1, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		// Expect the same job to come back because we deduplicate from the original.
		require.Equal(t, insertRes0.Job.ID, insertRes1.Job.ID)

		insertRes2, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			// Use another queue so the job can be inserted:
			Queue:      "other",
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		// This job however is _not_ the same because it's inserted as
		// `scheduled` which is outside the unique constraints.
		require.NotEqual(t, insertRes0.Job.ID, insertRes2.Job.ID)
	})

	t.Run("ErrorsWithUniqueV1CustomStates", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		uniqueOpts := UniqueOpts{
			ByPeriod: 24 * time.Hour,
			ByState:  []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted},
		}

		insertRes, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.EqualError(t, err, "UniqueOpts.ByState must contain all required states, missing: pending, running, scheduled")
		require.Nil(t, insertRes)
	})
}

func TestDefaultClientID(t *testing.T) {
	t.Parallel()

	host, _ := os.Hostname()
	require.NotEmpty(t, host)

	startedAt := time.Date(2024, time.March, 7, 4, 39, 12, 123456789, time.UTC)

	require.Equal(t, strings.ReplaceAll(host, ".", "_")+"_2024_03_07T04_39_12_123456", defaultClientID(startedAt))
}

func TestDefaultClientIDWithHost(t *testing.T) {
	t.Parallel()

	host, _ := os.Hostname()
	require.NotEmpty(t, host)

	startedAt := time.Date(2024, time.March, 7, 4, 39, 12, 123456789, time.UTC)

	require.Equal(t, "example_com_2024_03_07T04_39_12_123456", defaultClientIDWithHost(startedAt,
		"example.com"))
	require.Equal(t, "this_is_a_degenerately_long_host_name_that_will_be_truncated_2024_03_07T04_39_12_123456", defaultClientIDWithHost(startedAt,
		"this.is.a.degenerately.long.host.name.that.will.be.truncated.so.were.not.storing.massive.strings.to.the.database.com"))

	// Test strings right around the boundary to make sure we don't have some off-by-one slice error.
	require.Equal(t, strings.Repeat("a", 59)+"_2024_03_07T04_39_12_123456", defaultClientIDWithHost(startedAt, strings.Repeat("a", 59)))
	require.Equal(t, strings.Repeat("a", 60)+"_2024_03_07T04_39_12_123456", defaultClientIDWithHost(startedAt, strings.Repeat("a", 60)))
	require.Equal(t, strings.Repeat("a", 60)+"_2024_03_07T04_39_12_123456", defaultClientIDWithHost(startedAt, strings.Repeat("a", 61)))
}

// DriverPollOnly simulates a driver without a listener. An example of this is
// Postgres through `riverdatabasesql`, which is Postgres (so it can notify),
// but where `database/sql` provides no listener mechanism. We could use the
// real `riverdatabasesql`, but avoid doing so here so we don't have to bring
// that into the top level package as a dependency.
type DriverPollOnly struct {
	riverpgxv5.Driver
}

func NewDriverPollOnly(dbPool *pgxpool.Pool) *DriverPollOnly {
	return &DriverPollOnly{
		Driver: *riverpgxv5.New(dbPool),
	}
}

func (d *DriverPollOnly) SupportsListener() bool { return false }

// DriverWithoutListenNotify simulates a driver without any listen/notify
// support at all. An example of this is SQLite through `riversqlite` where
// SQLite doesn't support listen/notify or anything like it. We could use the
// real `riversqlite`, but avoid doing so here so we don't have to bring that
// into the top level package as a dependency.
type DriverWithoutListenNotify struct {
	riverpgxv5.Driver
}

func NewDriverWithoutListenNotify(dbPool *pgxpool.Pool) *DriverWithoutListenNotify {
	return &DriverWithoutListenNotify{
		Driver: *riverpgxv5.New(dbPool),
	}
}

func (d *DriverWithoutListenNotify) SupportsListener() bool     { return false }
func (d *DriverWithoutListenNotify) SupportsListenNotify() bool { return false }

type JobArgsWithHooksFunc func() []rivertype.Hook

func (JobArgsWithHooksFunc) Kind() string { return "job_args_with_hooks" }

func (f JobArgsWithHooksFunc) Hooks() []rivertype.Hook {
	return f()
}

func (JobArgsWithHooksFunc) MarshalJSON() ([]byte, error) { return []byte("{}"), nil }

func (JobArgsWithHooksFunc) UnmarshalJSON([]byte) error { return nil }
