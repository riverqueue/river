package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/testfactory"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func waitForClientHealthy(ctx context.Context, t *testing.T, statusUpdateCh <-chan componentstatus.ClientSnapshot) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		select {
		case status := <-statusUpdateCh:
			t.Logf("Client status: elector=%d notifier=%d producers=%+v", status.Elector, status.Notifier, status.Producers)
			if status.Healthy() {
				return
			}
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal("exceeded deadline waiting for client to become ready")
			}
			return
		}
	}
}

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

type callbackFunc func(context.Context, *Job[callbackArgs]) error

func makeAwaitCallback(startedCh chan<- int64, doneCh chan struct{}) callbackFunc {
	return func(ctx context.Context, job *Job[callbackArgs]) error {
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
	}
}

type callbackArgs struct {
	Name string `json:"name"`
}

func (callbackArgs) Kind() string { return "callback" }

type callbackWorker struct {
	WorkerDefaults[callbackArgs]
	fn callbackFunc
}

func (w *callbackWorker) Work(ctx context.Context, job *Job[callbackArgs]) error {
	return w.fn(ctx, job)
}

func newTestConfig(t *testing.T, callback callbackFunc) *Config {
	t.Helper()
	workers := NewWorkers()
	if callback != nil {
		AddWorker(workers, &callbackWorker{fn: callback})
	}
	AddWorker(workers, &noOpWorker{})

	return &Config{
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		Logger:            riverinternaltest.Logger(t),
		Queues:            map[string]QueueConfig{QueueDefault: {MaxWorkers: 50}},
		Workers:           workers,
		schedulerInterval: riverinternaltest.SchedulerShortInterval,
	}
}

func newTestClient(t *testing.T, dbPool *pgxpool.Pool, config *Config) *Client[pgx.Tx] {
	t.Helper()

	client, err := NewClient(riverpgxv5.New(dbPool), config)
	require.NoError(t, err)

	client.testSignals.Init()

	return client
}

func startClient(ctx context.Context, t *testing.T, client *Client[pgx.Tx]) {
	t.Helper()

	if err := client.Start(ctx); err != nil {
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, client.Stop(ctx))
	})
}

func runNewTestClient(ctx context.Context, t *testing.T, config *Config) *Client[pgx.Tx] {
	t.Helper()

	dbPool := riverinternaltest.TestDB(ctx, t)
	client := newTestClient(t, dbPool, config)
	startClient(ctx, t, client)
	return client
}

func Test_Client(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool        *pgxpool.Pool
		subscribeChan <-chan *Event
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		subscribeChan, _ := client.Subscribe(
			EventKindJobCancelled,
			EventKindJobCompleted,
			EventKindJobFailed,
			EventKindJobSnoozed,
		)

		return client, &testBundle{
			dbPool:        dbPool,
			subscribeChan: subscribeChan,
		}
	}

	t.Run("StartInsertAndWork", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		workedChan := make(chan struct{})

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			workedChan <- struct{}{}
			return nil
		}))

		startClient(ctx, t, client)

		_, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		riverinternaltest.WaitOrTimeout(t, workedChan)
	})

	t.Run("JobCancelErrorReturned", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobCancel(errors.New("a persisted internal error"))
		}))

		startClient(ctx, t, client)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		updatedJob, err := client.JobGet(ctx, insertedJob.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, updatedJob.State)
		require.WithinDuration(t, time.Now(), *updatedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobSnoozeErrorReturned", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobSnooze(15 * time.Minute)
		}))

		startClient(ctx, t, client)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, EventKindJobSnoozed, event.Kind)
		require.Equal(t, JobStateScheduled, event.Job.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), event.Job.ScheduledAt, 2*time.Second)

		updatedJob, err := client.JobGet(ctx, insertedJob.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, updatedJob.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), updatedJob.ScheduledAt, 2*time.Second)
	})

	// This helper is used to test cancelling a job both _in_ a transaction and
	// _outside of_ a transaction. The exact same test logic applies to each case,
	// the only difference is a different cancelFunc provided by the specific
	// subtest.
	cancelRunningJobTestHelper := func(t *testing.T, cancelFunc func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error)) { //nolint:thelper
		client, bundle := setup(t)

		jobStartedChan := make(chan int64)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			<-ctx.Done()
			return ctx.Err()
		}))

		statusUpdateCh := client.monitor.RegisterUpdates()
		startClient(ctx, t, client)
		waitForClientHealthy(ctx, t, statusUpdateCh)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riverinternaltest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertedJob.ID, startedJobID)

		// Cancel the job:
		updatedJob, err := cancelFunc(ctx, bundle.dbPool, client, insertedJob.ID)
		require.NoError(t, err)
		require.NotNil(t, updatedJob)
		// Job is still actively running at this point because the query wouldn't
		// modify that column for a running job:
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		event := riverinternaltest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		jobAfterCancel, err := client.JobGet(ctx, insertedJob.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, jobAfterCancel.State)
		require.WithinDuration(t, time.Now(), *jobAfterCancel.FinalizedAt, 2*time.Second)
	}

	t.Run("CancelRunningJob", func(t *testing.T) {
		t.Parallel()

		cancelRunningJobTestHelper(t, func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error) {
			return client.JobCancel(ctx, jobID)
		})
	})

	t.Run("CancelRunningJobInTx", func(t *testing.T) {
		t.Parallel()

		cancelRunningJobTestHelper(t, func(ctx context.Context, dbPool *pgxpool.Pool, client *Client[pgx.Tx], jobID int64) (*rivertype.JobRow, error) {
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

		jobStartedChan := make(chan int64)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			<-ctx.Done()
			return ctx.Err()
		}))

		startClient(ctx, t, client)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(5 * time.Minute)})
		require.NoError(t, err)

		// Cancel the job:
		updatedJob, err := client.JobCancel(ctx, insertedJob.ID)
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
			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{ID: 0})
			require.ErrorIs(t, err, ErrNotFound)
			require.Nil(t, jobAfter)
			return nil
		})
		require.NoError(t, err)
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

		client, err := NewClient(riverpgxv5.New(dbPool), newTestConfig(t, nil))
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

	t.Run("StopAndCancel", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)
		jobStartedChan := make(chan int64)
		jobDoneChan := make(chan struct{})

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			jobStartedChan <- job.ID
			<-ctx.Done()
			require.ErrorIs(t, context.Cause(ctx), rivercommon.ErrShutdown)
			close(jobDoneChan)
			return nil
		}))

		startClient(ctx, t, client)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riverinternaltest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertedJob.ID, startedJobID)

		select {
		case <-client.Stopped():
			t.Fatal("expected client to not be stopped yet")
		default:
		}

		require.NoError(t, client.StopAndCancel(ctx))

		riverinternaltest.WaitOrTimeout(t, client.Stopped())
	})
}

func Test_Client_Stop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Performs continual job insertion on a number of background goroutines.
	// Returns a `finish` function that should be deferred to stop insertion and
	// safely stop goroutines.
	doParallelContinualInsertion := func(ctx context.Context, t *testing.T, client *Client[pgx.Tx]) func() {
		t.Helper()

		ctx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}

					_, err := client.Insert(ctx, callbackArgs{}, nil)
					if errors.Is(err, context.Canceled) {
						return
					}
					require.NoError(t, err)

					// Sleep a brief time between inserts.
					client.baseService.CancellableSleepRandomBetween(ctx, 1*time.Microsecond, 10*time.Millisecond)
				}
			}()
		}

		return func() {
			cancel()
			wg.Wait()
		}
	}

	t.Run("not started", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)
		client := newTestClient(t, dbPool, newTestConfig(t, nil))

		err := client.Stop(ctx)
		require.Error(t, err)
		require.Equal(t, "client not started", err.Error())
	})

	t.Run("no jobs in progress", func(t *testing.T) {
		t.Parallel()
		client := runNewTestClient(ctx, t, newTestConfig(t, nil))

		// Should shut down quickly:
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		require.NoError(t, client.Stop(ctx))
	})

	t.Run("jobs in progress, completing promptly", func(t *testing.T) {
		t.Parallel()
		require := require.New(t)
		doneCh := make(chan struct{})
		startedCh := make(chan int64)

		client := runNewTestClient(ctx, t, newTestConfig(t, makeAwaitCallback(startedCh, doneCh)))

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		// enqueue job:
		insertedJob, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		var startedJobID int64
		select {
		case startedJobID = <-startedCh:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timed out waiting for job to start")
		}
		require.Equal(insertedJob.ID, startedJobID)

		// Should not shut down immediately, not until jobs are given the signal to
		// complete:
		go func() {
			<-time.After(50 * time.Millisecond)
			close(doneCh)
		}()

		require.NoError(client.Stop(ctx))
	})

	t.Run("jobs in progress, failing to complete before stop context", func(t *testing.T) {
		t.Parallel()

		jobDoneChan := make(chan struct{})
		jobStartedChan := make(chan int64)

		callbackFunc := func(ctx context.Context, job *Job[callbackArgs]) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobStartedChan <- job.ID:
			}

			select {
			case <-ctx.Done():
				require.FailNow(t, "Did not expect job to be cancelled")
			case <-jobDoneChan:
			}

			return nil
		}

		client := runNewTestClient(ctx, t, newTestConfig(t, callbackFunc))

		insertedJob, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riverinternaltest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertedJob.ID, startedJobID)

		go func() {
			<-time.After(100 * time.Millisecond)
			close(jobDoneChan)
		}()

		t.Logf("Shutting down client with timeout, but while jobs are still in progress")

		// Context should expire while jobs are still in progress:
		stopCtx, stopCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		t.Cleanup(stopCancel)

		err = client.Stop(stopCtx)
		require.Equal(t, context.DeadlineExceeded, err)

		select {
		case <-jobDoneChan:
			require.FailNow(t, "Expected Stop to return before job was done")
		default:
		}
	})

	t.Run("with continual insertion, no jobs are left running", func(t *testing.T) {
		t.Parallel()

		startedCh := make(chan int64)
		callbackFunc := func(ctx context.Context, job *Job[callbackArgs]) error {
			select {
			case startedCh <- job.ID:
			default:
			}
			return nil
		}

		config := newTestConfig(t, callbackFunc)
		client := runNewTestClient(ctx, t, config)

		finish := doParallelContinualInsertion(ctx, t, client)
		defer finish()

		// Wait for at least one job to start
		riverinternaltest.WaitOrTimeout(t, startedCh)

		require.NoError(t, client.Stop(ctx))

		runningJobs, err := client.JobList(ctx, NewJobListParams().State(rivertype.JobStateRunning))
		require.NoError(t, err)
		require.Empty(t, runningJobs, "expected no jobs to be left running")
	})

	t.Run("WithSubscriber", func(t *testing.T) {
		t.Parallel()

		callbackFunc := func(ctx context.Context, job *Job[callbackArgs]) error { return nil }

		client := runNewTestClient(ctx, t, newTestConfig(t, callbackFunc))

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		defer cancel()

		finish := doParallelContinualInsertion(ctx, t, client)
		defer finish()

		// Arbitrarily wait for 100 jobs to come through.
		for i := 0; i < 100; i++ {
			riverinternaltest.WaitOrTimeout(t, subscribeChan)
		}

		require.NoError(t, client.Stop(ctx))
	})
}

func Test_Client_StopAndCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("jobs in progress, only completing when context is canceled", func(t *testing.T) {
		t.Parallel()

		jobDoneChan := make(chan struct{})
		jobStartedChan := make(chan int64)

		callbackFunc := func(ctx context.Context, job *Job[callbackArgs]) error {
			defer close(jobDoneChan)

			// indicate the job has started, unless context is already done:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobStartedChan <- job.ID:
			}

			t.Logf("Job waiting for context cancellation")
			defer t.Logf("Job finished")

			select {
			case <-ctx.Done(): // don't stop running until context is canceled
			case <-time.After(10 * time.Second):
				require.FailNow(t, "Job should've been cancelled by now")
			}

			return nil
		}
		config := newTestConfig(t, callbackFunc)
		client := runNewTestClient(ctx, t, config)

		insertedJob, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(t, err)

		startedJobID := riverinternaltest.WaitOrTimeout(t, jobStartedChan)
		require.Equal(t, insertedJob.ID, startedJobID)

		t.Logf("Initiating hard stop, while jobs are still in progress")

		stopCtx, stopCancel := context.WithTimeout(ctx, time.Second)
		t.Cleanup(stopCancel)

		stopStartedAt := time.Now()

		err = client.StopAndCancel(stopCtx)
		require.NoError(t, err)

		t.Logf("Waiting on job to be done")

		select {
		case <-jobDoneChan:
		default:
			require.FailNow(t, "Expected job to be done before stop returns")
		}

		// Stop should be ~immediate:
		//
		// TODO: client stop seems to take a widely variable amount of time,
		// between 1ms and >50ms, due to the JobComplete query taking that long.
		// Investigate and solve that if we can, or consider reworking this test.
		require.WithinDuration(t, time.Now(), stopStartedAt, 200*time.Millisecond)
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

	require := require.New(t)
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
	config := newTestConfig(t, nil)
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
	require.NoError(err)

	var jobCtx context.Context
	select {
	case jobCtx = <-jobCtxCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}

	require.Equal("River", jobCtx.Value(customContextKey("BestGoPostgresQueue")), "job should persist the context value from the client context")
	jobDeadline, ok := jobCtx.Deadline()
	require.True(ok, "job should have a deadline")
	require.Equal(deadline, jobDeadline, "job should have the same deadline as the client context (shorter than the job's timeout)")
}

func Test_Client_ClientFromContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var clientResult *Client[pgx.Tx]
	jobDoneChan := make(chan struct{})
	config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
		clientResult = ClientFromContext[pgx.Tx](ctx)
		close(jobDoneChan)
		return nil
	})
	client := runNewTestClient(ctx, t, config)

	_, err := client.Insert(ctx, callbackArgs{}, nil)
	require.NoError(t, err)

	riverinternaltest.WaitOrTimeout(t, jobDoneChan)

	require.NotNil(t, clientResult)
	require.Equal(t, client, clientResult)
}

func Test_Client_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		return client, &testBundle{}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		jobRow, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, rivercommon.MaxAttemptsDefault, jobRow.MaxAttempts)
		require.JSONEq(t, "{}", string(jobRow.Metadata))
		require.Equal(t, (&noOpArgs{}).Kind(), jobRow.Kind)
		require.Equal(t, PriorityDefault, jobRow.Priority)
		require.Equal(t, QueueDefault, jobRow.Queue)
		require.Equal(t, []string{}, jobRow.Tags)
	})

	t.Run("WithInsertOpts", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		jobRow, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{
			MaxAttempts: 17,
			Metadata:    []byte(`{"foo": "bar"}`),
			Priority:    3,
			Queue:       "custom",
			Tags:        []string{"custom"},
		})
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

		jobRow, err := client.Insert(ctx, &noOpArgs{}, &InsertOpts{
			ScheduledAt: time.Time{},
		})
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), jobRow.ScheduledAt, 2*time.Second)
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
			Logger: riverinternaltest.Logger(t),
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
}

func Test_Client_InsertTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		tx pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			tx: tx,
		}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		jobRow, err := client.InsertTx(ctx, bundle.tx, &noOpArgs{}, nil)
		require.NoError(t, err)
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

		jobRow, err := client.InsertTx(ctx, bundle.tx, &noOpArgs{}, &InsertOpts{
			MaxAttempts: 17,
			Priority:    3,
			Queue:       "custom",
			Tags:        []string{"custom"},
		})
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
			Logger: riverinternaltest.Logger(t),
		})
		require.NoError(t, err)

		_, err = client.InsertTx(ctx, bundle.tx, &noOpArgs{}, nil)
		require.NoError(t, err)
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
}

func Test_Client_InsertMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		return client, &testBundle{}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), count)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, []string{(noOpArgs{}).Kind()})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind()) //nolint:goconst
	})

	t.Run("WithInsertOptsScheduledAtZeroTime", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{ScheduledAt: time.Time{}}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), count)

		jobs, err := client.driver.GetExecutor().JobGetByKindMany(ctx, []string{(noOpArgs{}).Kind()})
		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(noOpArgs{}).Kind())
		jobRow := jobs[0]
		require.WithinDuration(t, time.Now(), jobRow.ScheduledAt, 2*time.Second)
	})

	t.Run("ErrorsOnInvalidQueueName", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: &noOpArgs{}, InsertOpts: &InsertOpts{Queue: "invalid*queue"}},
		})
		require.ErrorContains(t, err, "queue name is invalid")
		require.Equal(t, int64(0), count)
	})

	t.Run("ErrorsOnDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, _ = setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riverinternaltest.Logger(t),
		})
		require.NoError(t, err)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.ErrorIs(t, err, errNoDriverDBPool)
		require.Equal(t, int64(0), count)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Equal(t, int64(0), count)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Equal(t, int64(0), count)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		client.config.Workers = nil

		_, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("ErrorsOnInsertOptsUniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}}},
		})
		require.EqualError(t, err, "UniqueOpts are not supported for batch inserts")
		require.Equal(t, int64(0), count)
	})
}

func Test_Client_InsertManyTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		tx pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		tx, err := dbPool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			tx: tx,
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), count)

		jobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, []string{(noOpArgs{}).Kind()})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())

		require.NoError(t, bundle.tx.Commit(ctx))

		// Ensure the jobs are visible outside the transaction:
		jobs, err = client.driver.GetExecutor().JobGetByKindMany(ctx, []string{(noOpArgs{}).Kind()})
		require.NoError(t, err)
		require.Len(t, jobs, 2, "Expected to find exactly two jobs of kind: "+(noOpArgs{}).Kind())
	})

	t.Run("SupportsScheduledJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		startClient(ctx, t, client)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{{noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Minute)}}})
		require.NoError(t, err)
		require.Equal(t, int64(1), count)

		insertedJobs, err := client.driver.UnwrapExecutor(bundle.tx).JobGetByKindMany(ctx, []string{(noOpArgs{}).Kind()})
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
			Logger: riverinternaltest.Logger(t),
		})
		require.NoError(t, err)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	t.Run("ErrorsWithZeroJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{})
		require.EqualError(t, err, "no jobs to insert")
		require.Equal(t, int64(0), count)
	})

	t.Run("ErrorsOnUnknownJobKindWithWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		var unknownJobKindErr *UnknownJobKindError
		require.ErrorAs(t, err, &unknownJobKindErr)
		require.Equal(t, (&unregisteredJobArgs{}).Kind(), unknownJobKindErr.Kind)
		require.Equal(t, int64(0), count)
	})

	t.Run("AllowsUnknownJobKindWithoutWorkers", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		client.config.Workers = nil

		_, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: unregisteredJobArgs{}},
		})
		require.NoError(t, err)
	})

	t.Run("ErrorsOnInsertOptsUniqueOpts", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertManyTx(ctx, bundle.tx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{UniqueOpts: UniqueOpts{ByArgs: true}}},
		})
		require.EqualError(t, err, "UniqueOpts are not supported for batch inserts")
		require.Equal(t, int64(0), count)
	})
}

func Test_Client_JobGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		return client, &testBundle{}
	}

	t.Run("FetchesAnExistingJob", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		newJob, err := client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)

		job, err := client.JobGet(ctx, newJob.ID)
		require.NoError(t, err)
		require.NotNil(t, job)

		require.Equal(t, newJob.ID, job.ID)
		require.Equal(t, newJob.State, job.State)
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
		exec riverdriver.Executor
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		return client, &testBundle{
			exec: client.driver.GetExecutor(),
		}
	}

	t.Run("FiltersByKind", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1")})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_1")})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("test_kind_2")})

		jobs, err := client.JobList(ctx, NewJobListParams().Kinds("test_kind_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().Kinds("test_kind_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByQueue", func(t *testing.T) { //nolint:dupl
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1")})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_1")})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Queue: ptrutil.Ptr("queue_2")})

		jobs, err := client.JobList(ctx, NewJobListParams().Queues("queue_1"))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().Queues("queue_2"))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("FiltersByState", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		jobs, err := client.JobList(ctx, NewJobListParams().State(JobStateAvailable))
		require.NoError(t, err)
		// jobs ordered by ScheduledAt ASC by default
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().State(JobStateRunning))
		require.NoError(t, err)
		require.Equal(t, []int64{job3.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("SortsAvailableRetryableAndScheduledJobsByScheduledAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()

		states := map[rivertype.JobState]rivertype.JobState{
			JobStateAvailable: rivertype.JobStateAvailable,
			JobStateRetryable: rivertype.JobStateRetryable,
			JobStateScheduled: rivertype.JobStateScheduled,
		}
		for state, dbState := range states {
			job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(dbState), ScheduledAt: &now})
			job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(dbState), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

			jobs, err := client.JobList(ctx, NewJobListParams().State(state))
			require.NoError(t, err)
			require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			jobs, err = client.JobList(ctx, NewJobListParams().State(state).OrderBy(JobListOrderByTime, SortOrderDesc))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		}
	})

	t.Run("SortsCancelledCompletedAndDiscardedJobsByFinalizedAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()

		states := map[rivertype.JobState]rivertype.JobState{
			JobStateCancelled: rivertype.JobStateCancelled,
			JobStateCompleted: rivertype.JobStateCompleted,
			JobStateDiscarded: rivertype.JobStateDiscarded,
		}
		for state, dbState := range states {
			job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(dbState), FinalizedAt: ptrutil.Ptr(now.Add(-10 * time.Second))})
			job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(dbState), FinalizedAt: ptrutil.Ptr(now.Add(-15 * time.Second))})

			jobs, err := client.JobList(ctx, NewJobListParams().State(state))
			require.NoError(t, err)
			require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

			jobs, err = client.JobList(ctx, NewJobListParams().State(state).OrderBy(JobListOrderByTime, SortOrderDesc))
			require.NoError(t, err)
			require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
		}
	})

	t.Run("SortsRunningJobsByAttemptedAt", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: &now})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

		jobs, err := client.JobList(ctx, NewJobListParams().State(JobStateRunning))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().State(JobStateRunning).OrderBy(JobListOrderByTime, SortOrderDesc))
		require.NoError(t, err)
		// Sort order was explicitly reversed:
		require.Equal(t, []int64{job1.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("WithNilParamsFiltersToAvailableByDefault", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: &now})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		jobs, err := client.JobList(ctx, nil)
		require.NoError(t, err)
		// sort order is switched by ScheduledAt values:
		require.Equal(t, []int64{job2.ID, job1.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("PaginatesWithAfter", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		now := time.Now().UTC()
		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable), ScheduledAt: &now})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job4 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: &now})
		job5 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(-5 * time.Second))})
		job6 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: &now})

		jobs, err := client.JobList(ctx, NewJobListParams().After(JobListCursorFromJob(job1)))
		require.NoError(t, err)
		require.Equal(t, []int64{job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().State(rivertype.JobStateRunning).After(JobListCursorFromJob(job3)))
		require.NoError(t, err)
		require.Equal(t, []int64{job4.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().State(rivertype.JobStateCompleted).After(JobListCursorFromJob(job5)))
		require.NoError(t, err)
		require.Equal(t, []int64{job6.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("MetadataOnly", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job1 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"foo": "bar"}`)})
		job2 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`)})
		job3 := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Metadata: []byte(`{"baz": "value"}`)})

		jobs, err := client.JobList(ctx, NewJobListParams().State("").Metadata(`{"foo": "bar"}`))
		require.NoError(t, err)
		require.Equal(t, []int64{job1.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))

		jobs, err = client.JobList(ctx, NewJobListParams().State("").Metadata(`{"baz": "value"}`).OrderBy(JobListOrderByTime, SortOrderDesc))
		require.NoError(t, err)
		// Sort order was explicitly reversed:
		require.Equal(t, []int64{job3.ID, job2.ID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
	})

	t.Run("WithCancelledContext", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel() // cancel immediately

		jobs, err := client.JobList(ctx, NewJobListParams().State(JobStateRunning))
		require.ErrorIs(t, context.Canceled, err)
		require.Empty(t, jobs)
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

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)

		return client, &testBundle{dbPool: dbPool}
	}

	t.Run("UpdatesAJobScheduledInTheFutureToBeImmediatelyAvailable", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		newJob, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, newJob.State)

		job, err := client.JobRetry(ctx, newJob.ID)
		require.NoError(t, err)
		require.NotNil(t, job)

		require.Equal(t, rivertype.JobStateAvailable, job.State)
		require.WithinDuration(t, time.Now().UTC(), job.ScheduledAt, 5*time.Second)
	})

	t.Run("TxVariantAlsoUpdatesJobToAvailable", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		newJob, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateScheduled, newJob.State)

		var jobAfter *rivertype.JobRow

		err = pgx.BeginFunc(ctx, bundle.dbPool, func(tx pgx.Tx) error {
			var err error
			jobAfter, err = client.JobRetryTx(ctx, tx, newJob.ID)
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

func Test_Client_ErrorHandler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		SubscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		client := runNewTestClient(ctx, t, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{SubscribeChan: subscribeChan}
	}

	requireInsert := func(ctx context.Context, client *Client[pgx.Tx]) *rivertype.JobRow {
		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(t, err)
		return job
	}

	t.Run("ErrorHandler", func(t *testing.T) {
		t.Parallel()

		handlerErr := errors.New("job error")
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return handlerErr
		})

		var errorHandlerCalled bool
		config.ErrorHandler = &testErrorHandler{
			HandleErrorFunc: func(ctx context.Context, job *rivertype.JobRow, err error) *ErrorHandlerResult {
				require.Equal(t, handlerErr, err)
				errorHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		requireInsert(ctx, client)
		riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)

		require.True(t, errorHandlerCalled)
	})

	t.Run("ErrorHandler_UnknownJobKind", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)

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
		insertParams, _, err := insertParamsFromArgsAndOptions(unregisteredJobArgs{}, nil)
		require.NoError(t, err)
		_, err = client.driver.GetExecutor().JobInsertFast(ctx, insertParams)
		require.NoError(t, err)

		riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)

		require.True(t, errorHandlerCalled)
	})

	t.Run("PanicHandler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			panic("panic val")
		})

		var panicHandlerCalled bool
		config.ErrorHandler = &testErrorHandler{
			HandlePanicFunc: func(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult {
				require.Equal(t, "panic val", panicVal)
				panicHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		requireInsert(ctx, client)
		riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)

		require.True(t, panicHandlerCalled)
	})
}

func Test_Client_Maintenance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("JobCleaner", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, nil)
		config.CancelledJobRetentionPeriod = 1 * time.Hour
		config.CompletedJobRetentionPeriod = 1 * time.Hour
		config.DiscardedJobRetentionPeriod = 1 * time.Hour
		config.disableSleep = true

		client := newTestClient(t, dbPool, config)
		exec := client.driver.GetExecutor()

		deleteHorizon := time.Now().Add(-config.CompletedJobRetentionPeriod)

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the cleaner could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		ineligibleJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		ineligibleJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled)})

		jobBeyondHorizon1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})

		// Will not be deleted.
		jobWithinHorizon1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
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

	t.Run("JobRescuer", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, nil)
		config.RescueStuckJobsAfter = 5 * time.Minute
		config.disableSleep = true

		client := newTestClient(t, dbPool, config)
		exec := client.driver.GetExecutor()

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the rescuer could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRetryable), ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(-time.Minute))})

		// large attempt number ensures these don't immediately start executing again:
		jobStuckToRetry1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(20), AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobStuckToRetry2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRunning), Attempt: ptrutil.Ptr(20), AttemptedAt: ptrutil.Ptr(now.Add(-30 * time.Minute))})
		jobStuckToDiscard := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			State:       ptrutil.Ptr(rivertype.JobStateRunning),
			Attempt:     ptrutil.Ptr(20),
			AttemptedAt: ptrutil.Ptr(now.Add(-5*time.Minute - time.Second)),
			MaxAttempts: ptrutil.Ptr(1),
		})

		// Will not be rescued.
		jobNotYetStuck1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-4 * time.Minute))})
		jobNotYetStuck2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobNotYetStuck3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: ptrutil.Ptr("noOp"), State: ptrutil.Ptr(rivertype.JobStateRunning), AttemptedAt: ptrutil.Ptr(now.Add(-10 * time.Second))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.JobRescuer](client.queueMaintainer)
		svc.TestSignals.FetchedBatch.WaitOrTimeout()
		svc.TestSignals.UpdatedBatch.WaitOrTimeout()

		requireJobHasState := func(jobID int64, state rivertype.JobState) {
			t.Helper()
			job, err := exec.JobGetByID(ctx, jobID)
			require.NoError(t, err)
			require.Equal(t, state, job.State)
		}

		// unchanged
		requireJobHasState(ineligibleJob1.ID, ineligibleJob1.State)
		requireJobHasState(ineligibleJob2.ID, ineligibleJob2.State)
		requireJobHasState(ineligibleJob3.ID, ineligibleJob3.State)

		// Jobs to retry should be retryable:
		requireJobHasState(jobStuckToRetry1.ID, rivertype.JobStateRetryable)
		requireJobHasState(jobStuckToRetry2.ID, rivertype.JobStateRetryable)

		// This one should be discarded because it's already at MaxAttempts:
		requireJobHasState(jobStuckToDiscard.ID, rivertype.JobStateDiscarded)

		// not eligible for rescue, not stuck long enough yet:
		requireJobHasState(jobNotYetStuck1.ID, jobNotYetStuck1.State)
		requireJobHasState(jobNotYetStuck2.ID, jobNotYetStuck2.State)
		requireJobHasState(jobNotYetStuck3.ID, jobNotYetStuck3.State)
	})

	t.Run("JobScheduler", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, nil)
		config.Queues = map[string]QueueConfig{"another_queue": {MaxWorkers: 1}} // don't work jobs on the default queue we're using in this test
		config.disableSleep = true

		client := newTestClient(t, dbPool, config)
		exec := client.driver.GetExecutor()

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the scheduler could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateAvailable)})
		ineligibleJob2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		ineligibleJob3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})

		jobInPast1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobInPast2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobInPast3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateScheduled), ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

		// Will not be scheduled.
		jobInFuture1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCancelled), FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Hour))})
		jobInFuture2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateCompleted), FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Minute))})
		jobInFuture3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateDiscarded), FinalizedAt: ptrutil.Ptr(now.Add(10 * time.Second))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
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

	t.Run("PeriodicJobEnqueuerWithOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		config.disableSleep = true

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		}

		client := runNewTestClient(ctx, t, config)
		exec := client.driver.GetExecutor()

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		jobs, err := exec.JobGetByKindMany(ctx, []string{(periodicJobArgs{}).Kind()})
		require.NoError(t, err)
		require.Len(t, jobs, 1, "Expected to find exactly one job of kind: "+(periodicJobArgs{}).Kind())
	})

	t.Run("PeriodicJobEnqueuerNoOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		config.disableSleep = true

		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, nil),
		}

		client := runNewTestClient(ctx, t, config)
		exec := client.driver.GetExecutor()

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		// No jobs yet because the RunOnStart option was not specified.
		jobs, err := exec.JobGetByKindMany(ctx, []string{(periodicJobArgs{}).Kind()})
		require.NoError(t, err)
		require.Empty(t, jobs)
	})

	t.Run("Reindexer", func(t *testing.T) {
		t.Parallel()
		t.Skip("Reindexer is disabled for further development")

		config := newTestConfig(t, nil)
		config.ReindexerSchedule = cron.Every(time.Second)
		config.disableSleep = true

		client := runNewTestClient(ctx, t, config)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.Reindexer](client.queueMaintainer)
		// There are two indexes to reindex by default:
		svc.TestSignals.Reindexed.WaitOrTimeout()
		svc.TestSignals.Reindexed.WaitOrTimeout()
	})
}

func Test_Client_RetryPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	requireInsert := func(ctx context.Context, client *Client[pgx.Tx]) *rivertype.JobRow {
		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(t, err)
		return job
	}

	t.Run("RetryUntilDiscarded", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return errors.New("job error")
		})

		// The default policy would work too, but this takes some variability
		// out of it to make comparisons easier.
		config.RetryPolicy = &retryPolicyNoJitter{}

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		originalJobs := make([]*rivertype.JobRow, rivercommon.MaxAttemptsDefault)
		for i := 0; i < len(originalJobs); i++ {
			job := requireInsert(ctx, client)
			// regression protection to ensure we're testing the right number of jobs:
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)

			updatedJob, err := client.driver.GetExecutor().JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  job.ID,
				AttemptedAtDoUpdate: true,
				AttemptedAt:         ptrutil.Ptr(time.Now().UTC()),
				AttemptDoUpdate:     true,
				Attempt:             i, // starts at i, but will be i + 1 by the time it's being worked

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
		for i := 0; i < len(originalJobs); i++ {
			t.Logf("Waiting on job %d", i)
			_ = riverinternaltest.WaitOrTimeout(t, subscribeChan)
		}

		finishedJobs, err := client.driver.GetExecutor().JobGetByIDMany(ctx,
			sliceutil.Map(originalJobs, func(m *rivertype.JobRow) int64 { return m.ID }))
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

			// TODO(brandur): This tolerance could be reduced if we could inject
			// time.Now into adapter which may happen with baseservice
			require.WithinDuration(t, expectedNextScheduledAt, finishedJob.ScheduledAt, 2*time.Second)

			require.Equal(t, rivertype.JobStateRetryable, finishedJob.State)
		}

		// One last discarded job.
		{
			originalJob := originalJobs[len(originalJobs)-1]
			finishedJob := finishedJobsByID[originalJob.ID]

			originalJob.Attempt += 1

			t.Logf("Attempt number %d discarded", originalJob.Attempt)

			// TODO(brandur): See note on tolerance above.
			require.WithinDuration(t, time.Now(), *finishedJob.FinalizedAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateDiscarded, finishedJob.State)
		}
	})
}

func Test_Client_Subscribe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	keyEventsByName := func(events []*Event) map[string]*Event {
		return sliceutil.KeyBy(events, func(event *Event) (string, *Event) {
			var args callbackArgs
			require.NoError(t, json.Unmarshal(event.Job.EncodedArgs, &args))
			return args.Name, event
		})
	}

	requireInsert := func(ctx context.Context, client *Client[pgx.Tx], jobName string) *rivertype.JobRow {
		job, err := client.Insert(ctx, callbackArgs{Name: jobName}, nil)
		require.NoError(t, err)
		return job
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		// Fail/succeed jobs based on their name so we can get a mix of both to
		// verify.
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		})

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

		for i := 0; i < len(expectedJobs); i++ {
			events[i] = riverinternaltest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		{
			eventCompleted1 := eventsByName["completed1"]
			require.Equal(t, EventKindJobCompleted, eventCompleted1.Kind)
			require.Equal(t, jobCompleted1.ID, eventCompleted1.Job.ID)
			require.Equal(t, JobStateCompleted, eventCompleted1.Job.State)
		}

		{
			eventCompleted2 := eventsByName["completed2"]
			require.Equal(t, EventKindJobCompleted, eventCompleted2.Kind)
			require.Equal(t, jobCompleted2.ID, eventCompleted2.Job.ID)
			require.Equal(t, JobStateCompleted, eventCompleted2.Job.State)
		}

		{
			eventFailed1 := eventsByName["failed1"]
			require.Equal(t, EventKindJobFailed, eventFailed1.Kind)
			require.Equal(t, jobFailed1.ID, eventFailed1.Job.ID)
			require.Equal(t, JobStateRetryable, eventFailed1.Job.State)
		}

		{
			eventFailed2 := eventsByName["failed2"]
			require.Equal(t, EventKindJobFailed, eventFailed2.Kind)
			require.Equal(t, jobFailed2.ID, eventFailed2.Job.ID)
			require.Equal(t, JobStateRetryable, eventFailed2.Job.State)
		}
	})

	t.Run("CompletedOnly", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		})

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

		for i := 0; i < len(expectedJobs); i++ {
			events[i] = riverinternaltest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		eventCompleted := eventsByName["completed1"]
		require.Equal(t, EventKindJobCompleted, eventCompleted.Kind)
		require.Equal(t, jobCompleted.ID, eventCompleted.Job.ID)
		require.Equal(t, JobStateCompleted, eventCompleted.Job.State)

		_, ok := eventsByName["failed1"] // filtered out
		require.False(t, ok)
	})

	t.Run("FailedOnly", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			if strings.HasPrefix(job.Args.Name, "failed") {
				return errors.New("job error")
			}
			return nil
		})

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

		for i := 0; i < len(expectedJobs); i++ {
			events[i] = riverinternaltest.WaitOrTimeout(t, subscribeChan)
		}

		eventsByName := keyEventsByName(events)

		_, ok := eventsByName["completed1"] // filtered out
		require.False(t, ok)

		eventFailed := eventsByName["failed1"]
		require.Equal(t, EventKindJobFailed, eventFailed.Kind)
		require.Equal(t, jobFailed.ID, eventFailed.Job.ID)
		require.Equal(t, JobStateRetryable, eventFailed.Job.State)
	})

	t.Run("EventsDropWithNoListeners", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(t, dbPool, config)

		// A first channel that we'll use to make sure all the expected jobs are
		// finished.
		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		t.Cleanup(cancel)

		// Another channel with no listeners. Despite no listeners, it shouldn't
		// block or gum up the client's progress in any way.
		_, cancel = client.Subscribe(EventKindJobCompleted)
		t.Cleanup(cancel)

		// Insert more jobs than the maximum channel size. We'll be pulling from
		// one channel but not the other.
		for i := 0; i < subscribeChanSize+1; i++ {
			_ = requireInsert(ctx, client, fmt.Sprintf("job %d", i))
		}

		// Need to start waiting on events before running the client or the
		// channel could overflow before we start listening.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = riverinternaltest.WaitOrTimeoutN(t, subscribeChan, subscribeChanSize+1)
		}()

		startClient(ctx, t, client)

		wg.Wait()
	})

	t.Run("PanicOnUnknownKind", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(t, dbPool, config)

		require.PanicsWithError(t, "unknown event kind: does_not_exist", func() {
			_, _ = client.Subscribe(EventKind("does_not_exist"))
		})
	})

	t.Run("SubscriptionCancellation", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(t, dbPool, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted)
		cancel()

		// Drops through immediately because the channel is closed.
		riverinternaltest.WaitOrTimeout(t, subscribeChan)

		require.Empty(t, client.subscriptions)
	})
}

func Test_Client_InsertTriggersImmediateWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require := require.New(t)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)

	doneCh := make(chan struct{})
	close(doneCh) // don't need to block any jobs from completing
	startedCh := make(chan int64)

	dbPool := riverinternaltest.TestDB(ctx, t)

	config := newTestConfig(t, makeAwaitCallback(startedCh, doneCh))
	config.FetchCooldown = 20 * time.Millisecond
	config.FetchPollInterval = 20 * time.Second // essentially disable polling
	config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 2}}

	client := newTestClient(t, dbPool, config)
	statusUpdateCh := client.monitor.RegisterUpdates()

	startClient(ctx, t, client)
	waitForClientHealthy(ctx, t, statusUpdateCh)

	insertedJob, err := client.Insert(ctx, callbackArgs{}, nil)
	require.NoError(err)

	// Wait for the client to be ready by waiting for a job to be executed:
	select {
	case jobID := <-startedCh:
		require.Equal(insertedJob.ID, jobID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for warmup job to start")
	}

	// Now that we've run one job, we shouldn't take longer than the cooldown to
	// fetch another after insertion. LISTEN/NOTIFY should ensure we find out
	// about the inserted job much faster than the poll interval.
	insertedJob2, err := client.Insert(ctx, callbackArgs{}, nil)
	require.NoError(err)

	select {
	case jobID := <-startedCh:
		require.Equal(insertedJob2.ID, jobID)
	// As long as this is meaningfully shorter than the poll interval, we can be
	// sure the re-fetch came from listen/notify.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for 2nd job to start")
	}

	require.NoError(client.Stop(ctx))
}

func Test_Client_JobCompletion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		DBPool        *pgxpool.Pool
		SubscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		client := newTestClient(t, dbPool, config)
		startClient(ctx, t, client)

		subscribeChan, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{
			DBPool:        dbPool,
			SubscribeChan: subscribeChan,
		}
	}

	t.Run("JobThatReturnsNilIsCompleted", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return nil
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCompleted, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(time.Now(), *reloadedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobThatIsAlreadyCompletedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		var exec riverdriver.Executor
		now := time.Now().UTC()
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			_, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  job.ID,
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				StateDoUpdate:       true,
				State:               rivertype.JobStateCompleted,
			})
			require.NoError(err)
			return nil
		})

		client, bundle := setup(t, config)
		exec = client.driver.GetExecutor()

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCompleted, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(now, *reloadedJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobThatReturnsErrIsRetryable", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return errors.New("oops")
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateRetryable, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateRetryable, reloadedJob.State)
		require.WithinDuration(time.Now(), reloadedJob.ScheduledAt, 2*time.Second)
		require.Nil(reloadedJob.FinalizedAt)
	})

	t.Run("JobThatReturnsJobCancelErrorIsImmediatelyCancelled", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, job *Job[callbackArgs]) error {
			return JobCancel(errors.New("oops"))
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCancelled, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateCancelled, reloadedJob.State)
		require.NotNil(reloadedJob.FinalizedAt)
		require.WithinDuration(time.Now(), *reloadedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobThatIsAlreadyDiscardedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		now := time.Now().UTC()

		client, bundle := setup(t, newTestConfig(t, nil))

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			_, err := client.driver.GetExecutor().JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:                  job.ID,
				ErrorsDoUpdate:      true,
				Errors:              [][]byte{[]byte("{\"error\": \"oops\"}")},
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				StateDoUpdate:       true,
				State:               rivertype.JobStateDiscarded,
			})
			require.NoError(err)
			return errors.New("oops")
		}))

		job, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateDiscarded, event.Job.State)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateDiscarded, reloadedJob.State)
		require.NotNil(reloadedJob.FinalizedAt)
	})

	t.Run("JobThatIsCompletedManuallyIsNotTouchedByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		now := time.Now().UTC()

		client, bundle := setup(t, newTestConfig(t, nil))

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		var updatedJob *Job[JobArgs]
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			tx, err := bundle.DBPool.Begin(ctx)
			require.NoError(err)

			updatedJob, err = JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
			require.NoError(err)

			return tx.Commit(ctx)
		}))

		job, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCompleted, event.Job.State)
		require.Equal(JobStateCompleted, updatedJob.State)
		require.NotNil(updatedJob)
		require.NotNil(event.Job.FinalizedAt)
		require.NotNil(updatedJob.FinalizedAt)

		// Make sure the FinalizedAt is approximately ~now:
		require.WithinDuration(now, *updatedJob.FinalizedAt, time.Second)

		// Make sure we're getting the same timestamp back from the event and the
		// updated job inside the txn:
		require.WithinDuration(*updatedJob.FinalizedAt, *event.Job.FinalizedAt, time.Microsecond)

		reloadedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(err)

		require.Equal(rivertype.JobStateCompleted, reloadedJob.State)
		require.Equal(updatedJob.FinalizedAt, reloadedJob.FinalizedAt)
	})
}

type unregisteredJobArgs struct{}

func (unregisteredJobArgs) Kind() string { return "RandomWorkerNameThatIsNeverRegistered" }

func Test_Client_UnknownJobKindErrorsTheJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	require := require.New(t)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)

	doneCh := make(chan struct{})
	close(doneCh) // don't need to block any jobs from completing

	config := newTestConfig(t, nil)
	client := runNewTestClient(ctx, t, config)

	subscribeChan, cancel := client.Subscribe(EventKindJobFailed)
	t.Cleanup(cancel)

	insertParams, _, err := insertParamsFromArgsAndOptions(unregisteredJobArgs{}, nil)
	require.NoError(err)
	insertedJob, err := client.driver.GetExecutor().JobInsertFast(ctx, insertParams)
	require.NoError(err)

	event := riverinternaltest.WaitOrTimeout(t, subscribeChan)
	require.Equal(insertedJob.ID, event.Job.ID)
	require.Equal("RandomWorkerNameThatIsNeverRegistered", insertedJob.Kind)
	require.Len(event.Job.Errors, 1)
	require.Equal((&UnknownJobKindError{Kind: "RandomWorkerNameThatIsNeverRegistered"}).Error(), event.Job.Errors[0].Error)
	require.Equal(JobStateRetryable, event.Job.State)
	// Ensure that ScheduledAt was updated with next run time:
	require.True(event.Job.ScheduledAt.After(insertedJob.ScheduledAt))
	// It's the 1st attempt that failed. Attempt won't be incremented again until
	// the job gets fetched a 2nd time.
	require.Equal(1, event.Job.Attempt)

	require.NoError(client.Stop(ctx))
}

func Test_Client_Start_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("NoQueueConfiguration", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, nil)
		config.Queues = nil
		config.Workers = nil

		client := newTestClient(t, dbPool, config)
		err := client.Start(ctx)
		require.EqualError(t, err, "client Queues and Workers must be configured for a client to start working")
	})

	t.Run("NoRegisteredWorkers", func(t *testing.T) {
		t.Parallel()

		dbPool := riverinternaltest.TestDB(ctx, t)

		config := newTestConfig(t, nil)
		config.Workers = NewWorkers() // initialized, but empty

		client := newTestClient(t, dbPool, config)
		err := client.Start(ctx)
		require.EqualError(t, err, "at least one Worker must be added to the Workers bundle")
	})

	t.Run("DatabaseError", func(t *testing.T) {
		t.Parallel()

		dbConfig := riverinternaltest.DatabaseConfig("does-not-exist-and-dont-create-it")

		dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
		require.NoError(t, err)

		config := newTestConfig(t, nil)

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
	dbPool := riverinternaltest.TestDB(ctx, t)
	client := newTestClient(t, dbPool, newTestConfig(t, nil))
	// Ensure we get the clean name "Client" instead of the fully qualified name
	// with generic type param:
	require.Equal(t, "Client", client.baseService.Name)
}

func Test_NewClient_ClientIDWrittenToJobAttemptedByWhenFetched(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()
	doneCh := make(chan struct{})
	startedCh := make(chan *Job[callbackArgs])

	callback := func(ctx context.Context, job *Job[callbackArgs]) error {
		startedCh <- job
		<-doneCh
		return nil
	}

	client := runNewTestClient(ctx, t, newTestConfig(t, callback))
	t.Cleanup(func() { close(doneCh) })

	// enqueue job:
	insertCtx, insertCancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(insertCancel)
	insertedJob, err := client.Insert(insertCtx, callbackArgs{}, nil)
	require.NoError(err)
	require.Nil(insertedJob.AttemptedAt)
	require.Empty(insertedJob.AttemptedBy)

	var startedJob *Job[callbackArgs]
	select {
	case startedJob = <-startedCh:
		require.Equal([]string{client.ID()}, startedJob.AttemptedBy)
		require.NotNil(startedJob.AttemptedAt)
		require.WithinDuration(time.Now().UTC(), *startedJob.AttemptedAt, 2*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for job to start")
	}
}

func Test_NewClient_Defaults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	client, err := NewClient(riverpgxv5.New(dbPool), &Config{
		Queues:  map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		Workers: workers,
	})
	require.NoError(t, err)

	require.Zero(t, client.uniqueInserter.AdvisoryLockPrefix)

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, maintenance.CancelledJobRetentionPeriodDefault, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, maintenance.CompletedJobRetentionPeriodDefault, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, maintenance.DiscardedJobRetentionPeriodDefault, jobCleaner.Config.DiscardedJobRetentionPeriod)

	enqueuer := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
	require.Zero(t, enqueuer.Config.AdvisoryLockPrefix)

	require.Nil(t, client.config.ErrorHandler)
	require.Equal(t, FetchCooldownDefault, client.config.FetchCooldown)
	require.Equal(t, FetchPollIntervalDefault, client.config.FetchPollInterval)
	require.Equal(t, JobTimeoutDefault, client.config.JobTimeout)
	require.NotZero(t, client.baseService.Logger)
	require.IsType(t, &DefaultClientRetryPolicy{}, client.config.RetryPolicy)
	require.False(t, client.baseService.DisableSleep)
	require.False(t, client.config.disableSleep)
}

func Test_NewClient_Overrides(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	dbPool := riverinternaltest.TestDB(ctx, t)
	errorHandler := &testErrorHandler{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	retryPolicy := &DefaultClientRetryPolicy{}

	client, err := NewClient(riverpgxv5.New(dbPool), &Config{
		AdvisoryLockPrefix:          123_456,
		CancelledJobRetentionPeriod: 1 * time.Hour,
		CompletedJobRetentionPeriod: 2 * time.Hour,
		DiscardedJobRetentionPeriod: 3 * time.Hour,
		ErrorHandler:                errorHandler,
		FetchCooldown:               123 * time.Millisecond,
		FetchPollInterval:           124 * time.Millisecond,
		JobTimeout:                  125 * time.Millisecond,
		Logger:                      logger,
		Queues:                      map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		RetryPolicy:                 retryPolicy,
		Workers:                     workers,
		disableSleep:                true,
	})
	require.NoError(t, err)

	require.Equal(t, int32(123_456), client.uniqueInserter.AdvisoryLockPrefix)

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, 1*time.Hour, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, 2*time.Hour, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, 3*time.Hour, jobCleaner.Config.DiscardedJobRetentionPeriod)

	enqueuer := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
	require.Equal(t, int32(123_456), enqueuer.Config.AdvisoryLockPrefix)

	require.Equal(t, errorHandler, client.config.ErrorHandler)
	require.Equal(t, 123*time.Millisecond, client.config.FetchCooldown)
	require.Equal(t, 124*time.Millisecond, client.config.FetchPollInterval)
	require.Equal(t, 125*time.Millisecond, client.config.JobTimeout)
	require.Equal(t, logger, client.baseService.Logger)
	require.Equal(t, retryPolicy, client.config.RetryPolicy)
	require.True(t, client.baseService.DisableSleep)
	require.True(t, client.config.disableSleep)
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

		_, err := NewClient[pgx.Tx](riverpgxv5.New(nil), nil)
		require.ErrorIs(t, err, errMissingConfig)
	})

	t.Run("ErrorOnDriverWithNoDatabasePoolAndQueues", func(t *testing.T) {
		t.Parallel()

		_, err := NewClient[pgx.Tx](riverpgxv5.New(nil), newTestConfig(t, nil))
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
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			require := require.New(t)
			dbPool := riverinternaltest.TestDB(ctx, t)

			workers := NewWorkers()
			AddWorker(workers, &noOpWorker{})

			config := &Config{
				Queues:  map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
				Workers: workers,
			}
			tt.configFunc(config)

			client, err := NewClient(riverpgxv5.New(dbPool), config)
			if tt.wantErr != nil {
				require.Error(err)
				require.ErrorContains(err, tt.wantErr.Error())
				return
			}
			require.NoError(err)

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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require := require.New(t)
			ctx := context.Background()

			testWorker := &timeoutTestWorker{doneCh: make(chan testWorkerDeadline)}

			workers := NewWorkers()
			AddWorker(workers, testWorker)

			config := newTestConfig(t, nil)
			config.JobTimeout = tt.clientJobTimeout
			config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}}
			config.Workers = workers

			client := runNewTestClient(ctx, t, config)
			_, err := client.Insert(ctx, timeoutTestArgs{TimeoutValue: tt.jobArgTimeout}, nil)
			require.NoError(err)

			result := riverinternaltest.WaitOrTimeout(t, testWorker.doneCh)
			if tt.wantDuration == 0 {
				require.False(result.ok, "expected no deadline")
				return
			}
			require.True(result.ok, "expected a deadline, but none was set")
			require.WithinDuration(time.Now().Add(tt.wantDuration), result.deadline, 2*time.Second)
		})
	}
}

func TestInsertParamsFromJobArgsAndOptions(t *testing.T) {
	t.Parallel()

	t.Run("Defaults", func(t *testing.T) {
		t.Parallel()

		insertParams, uniqueOpts, err := insertParamsFromArgsAndOptions(noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, `{"name":""}`, string(insertParams.EncodedArgs))
		require.Equal(t, (noOpArgs{}).Kind(), insertParams.Kind)
		require.Equal(t, rivercommon.MaxAttemptsDefault, insertParams.MaxAttempts)
		require.Equal(t, rivercommon.PriorityDefault, insertParams.Priority)
		require.Equal(t, QueueDefault, insertParams.Queue)
		require.Nil(t, insertParams.ScheduledAt)
		require.Equal(t, []string{}, insertParams.Tags)

		require.True(t, uniqueOpts.IsEmpty())
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
		insertParams, _, err := insertParamsFromArgsAndOptions(noOpArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.Equal(t, opts.ScheduledAt, *insertParams.ScheduledAt)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("WorkerInsertOptsOverrides", func(t *testing.T) {
		t.Parallel()

		insertParams, _, err := insertParamsFromArgsAndOptions(&customInsertOptsJobArgs{}, nil)
		require.NoError(t, err)
		// All these come from overrides in customInsertOptsJobArgs's definition:
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("UniqueOpts", func(t *testing.T) {
		t.Parallel()

		uniqueOpts := UniqueOpts{
			ByArgs:   true,
			ByPeriod: 10 * time.Second,
			ByQueue:  true,
			ByState:  []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted},
		}

		_, internalUniqueOpts, err := insertParamsFromArgsAndOptions(noOpArgs{}, &InsertOpts{UniqueOpts: uniqueOpts})
		require.NoError(t, err)
		require.Equal(t, uniqueOpts.ByArgs, internalUniqueOpts.ByArgs)
		require.Equal(t, uniqueOpts.ByPeriod, internalUniqueOpts.ByPeriod)
		require.Equal(t, uniqueOpts.ByQueue, internalUniqueOpts.ByQueue)
		require.Equal(t, uniqueOpts.ByState, internalUniqueOpts.ByState)
	})

	t.Run("PriorityIsLimitedTo4", func(t *testing.T) {
		t.Parallel()

		insertParams, _, err := insertParamsFromArgsAndOptions(noOpArgs{}, &InsertOpts{Priority: 5})
		require.ErrorContains(t, err, "priority must be between 1 and 4")
		require.Nil(t, insertParams)
	})

	t.Run("NonEmptyArgs", func(t *testing.T) {
		t.Parallel()

		args := timeoutTestArgs{TimeoutValue: time.Hour}
		insertParams, _, err := insertParamsFromArgsAndOptions(args, nil)
		require.NoError(t, err)
		require.Equal(t, `{"timeout_value":3600000000000}`, string(insertParams.EncodedArgs))
	})

	t.Run("UniqueOptsAreValidated", func(t *testing.T) {
		t.Parallel()

		// Ensure that unique opts are validated. No need to be exhaustive here
		// since we already have tests elsewhere for that. Just make sure validation
		// is running.
		insertParams, _, err := insertParamsFromArgsAndOptions(
			noOpArgs{},
			&InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: 1 * time.Millisecond}},
		)
		require.EqualError(t, err, "JobUniqueOpts.ByPeriod should not be less than 1 second")
		require.Nil(t, insertParams)
	})
}

func TestID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("IsGeneratedWhenNotSpecifiedInConfig", func(t *testing.T) {
		t.Parallel()
		dbPool := riverinternaltest.TestDB(ctx, t)
		client := newTestClient(t, dbPool, newTestConfig(t, nil))
		require.NotEmpty(t, client.ID())
	})

	t.Run("IsGeneratedWhenNotSpecifiedInConfig", func(t *testing.T) {
		t.Parallel()
		config := newTestConfig(t, nil)
		config.ID = "my-client-id"
		dbPool := riverinternaltest.TestDB(ctx, t)
		client := newTestClient(t, dbPool, config)
		require.Equal(t, "my-client-id", client.ID())
	})
}

type customInsertOptsJobArgs struct{}

func (w *customInsertOptsJobArgs) Kind() string { return "customInsertOpts" }

func (w *customInsertOptsJobArgs) InsertOpts() InsertOpts {
	return InsertOpts{
		MaxAttempts: 42,
		Priority:    2,
		Queue:       "other",
		Tags:        []string{"tag1", "tag2"},
	}
}

func (w *customInsertOptsJobArgs) Work(context.Context, *Job[noOpArgs]) error { return nil }

func TestInsert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)
	workers := NewWorkers()
	AddWorker(workers, &noOpWorker{})

	config := &Config{
		FetchCooldown: 2 * time.Millisecond,
		Queues:        map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}},
		Workers:       workers,
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
				Priority: 2, // TODO: enforce a range on priority
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
				require.Equal(JobStateScheduled, insertedJob.State)
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
				require.Equal(JobStateAvailable, insertedJob.State)
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require := require.New(t)
			if tt.assert == nil {
				t.Fatalf("test %q did not specify an assert function", tt.name)
			}

			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			insertedJob, err := client.Insert(ctx, tt.args, tt.opts)
			require.NoError(err)
			tt.assert(t, &tt.args, tt.opts, insertedJob)

			// Also test InsertTx:
			tx, err := dbPool.Begin(ctx)
			require.NoError(err)
			defer tx.Rollback(ctx)

			insertedJob2, err := client.InsertTx(ctx, tx, tt.args, tt.opts)
			require.NoError(err)
			tt.assert(t, &tt.args, tt.opts, insertedJob2)
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

		dbPool := riverinternaltest.TestDB(ctx, t)

		client := newTestClient(t, dbPool, newTestConfig(t, nil))

		return client, &testBundle{}
	}

	t.Run("DeduplicatesJobs", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		uniqueOpts := UniqueOpts{
			ByPeriod: 24 * time.Hour,
		}

		job0, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		job1, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		// Expect the same job to come back.
		require.Equal(t, job0.ID, job1.ID)
	})

	t.Run("UniqueByState", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		uniqueOpts := UniqueOpts{
			ByPeriod: 24 * time.Hour,
			ByState:  []rivertype.JobState{JobStateAvailable, JobStateCompleted},
		}

		job0, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		job1, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			UniqueOpts: uniqueOpts,
		})
		require.NoError(t, err)

		// Expect the same job to come back because the original is either still
		// `available` or `completed`, both which we deduplicate off of.
		require.Equal(t, job0.ID, job1.ID)

		job2, err := client.Insert(ctx, noOpArgs{}, &InsertOpts{
			// Use a scheduled time so the job's inserted in state `scheduled`
			// instead of `available`.
			ScheduledAt: time.Now().Add(1 * time.Hour),
			UniqueOpts:  uniqueOpts,
		})
		require.NoError(t, err)

		// This job however is _not_ the same because it's inserted as
		// `scheduled` which is outside the unique constraints.
		require.NotEqual(t, job0.ID, job2.ID)
	})
}
