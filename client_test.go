package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
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
	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/rivertest"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/internal/util/valutil"
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

func (w *noOpWorker) Work(ctx context.Context, j *Job[noOpArgs]) error { return nil }

type periodicJobArgs struct{}

func (periodicJobArgs) Kind() string { return "periodic_job" }

type periodicJobWorker struct {
	WorkerDefaults[periodicJobArgs]
}

func (w *periodicJobWorker) Work(ctx context.Context, j *Job[periodicJobArgs]) error {
	return nil
}

type callbackFunc func(context.Context, *Job[callbackArgs]) error

func makeAwaitCallback(startedCh chan<- int64, doneCh chan struct{}) callbackFunc {
	return func(ctx context.Context, j *Job[callbackArgs]) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case startedCh <- j.ID:
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

func (w *callbackWorker) Work(ctx context.Context, j *Job[callbackArgs]) error {
	return w.fn(ctx, j)
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
		disableSleep:      true,
	}
}

func newTestClient(ctx context.Context, t *testing.T, config *Config) *Client[pgx.Tx] {
	t.Helper()

	dbPool := riverinternaltest.TestDB(ctx, t)

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
	client := newTestClient(ctx, t, config)
	startClient(ctx, t, client)
	return client
}

func Test_Client(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		queries       *dbsqlc.Queries
		subscribeChan <-chan *Event
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		subscribeChan, _ := client.Subscribe(
			EventKindJobCancelled,
			EventKindJobCompleted,
			EventKindJobFailed,
			EventKindJobSnoozed,
		)

		return client, &testBundle{
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

		rivertest.WaitOrTimeout(t, workedChan)
	})

	t.Run("JobCancel", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type JobArgs struct {
			JobArgsReflectKind[JobArgs]
		}

		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return JobCancel(fmt.Errorf("a persisted internal error"))
		}))

		startClient(ctx, t, client)

		insertedJob, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := rivertest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, EventKindJobCancelled, event.Kind)
		require.Equal(t, JobStateCancelled, event.Job.State)
		require.WithinDuration(t, time.Now(), *event.Job.FinalizedAt, 2*time.Second)

		updatedJob, err := bundle.queries.JobGetByID(ctx, client.driver.GetDBPool(), insertedJob.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateCancelled, updatedJob.State)
		require.WithinDuration(t, time.Now(), *updatedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobSnooze", func(t *testing.T) {
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

		event := rivertest.WaitOrTimeout(t, bundle.subscribeChan)
		require.Equal(t, EventKindJobSnoozed, event.Kind)
		require.Equal(t, JobStateScheduled, event.Job.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), event.Job.ScheduledAt, 2*time.Second)

		updatedJob, err := bundle.queries.JobGetByID(ctx, client.driver.GetDBPool(), insertedJob.ID)
		require.NoError(t, err)
		require.Equal(t, dbsqlc.JobStateScheduled, updatedJob.State)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), updatedJob.ScheduledAt, 2*time.Second)
	})

	t.Run("AlternateSchema", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		// Reconfigure the pool with an alternate schema, initialize a new pool
		dbPoolConfig := client.driver.GetDBPool().Config() // a copy of the original config
		dbPoolConfig.ConnConfig.RuntimeParams["search_path"] = "alternate_schema"

		dbPool, err := pgxpool.NewWithConfig(ctx, dbPoolConfig)
		require.NoError(t, err)
		t.Cleanup(dbPool.Close)

		client, err = NewClient(riverpgxv5.New(dbPool), newTestConfig(t, nil))
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

		callbackFunc := func(ctx context.Context, j *Job[callbackArgs]) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobStartedChan <- j.ID:
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
		default:
			require.FailNow(t, "Expected job to be done before stop returns")
		}
	})

	t.Run("with continual insertion, no jobs are left running", func(t *testing.T) {
		t.Parallel()

		startedCh := make(chan int64)
		callbackFunc := func(ctx context.Context, j *Job[callbackArgs]) error {
			select {
			case startedCh <- j.ID:
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

		count, err := (&dbsqlc.Queries{}).JobCountRunning(ctx, client.driver.GetDBPool())
		require.NoError(t, err)
		require.Equal(t, int64(0), count, "expected no jobs to be left running")
	})

	t.Run("WithSubscriber", func(t *testing.T) {
		t.Parallel()

		callbackFunc := func(ctx context.Context, j *Job[callbackArgs]) error { return nil }

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

		callbackFunc := func(ctx context.Context, j *Job[callbackArgs]) error {
			defer close(jobDoneChan)

			// indicate the job has started, unless context is already done:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case jobStartedChan <- j.ID:
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

		stopStart := time.Now()

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
		require.WithinDuration(t, time.Now(), stopStart, 100*time.Millisecond)
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

func (w *callbackWorkerWithCustomTimeout) Work(ctx context.Context, j *Job[callbackWithCustomTimeoutArgs]) error {
	return w.fn(ctx, j)
}

func (w *callbackWorkerWithCustomTimeout) Timeout(j *Job[callbackWithCustomTimeoutArgs]) time.Duration {
	return j.Args.TimeoutValue
}

func Test_Client_JobContextInheritsFromProvidedContext(t *testing.T) {
	t.Parallel()

	deadline := time.Now().Add(2 * time.Minute)

	require := require.New(t)
	jobCtxCh := make(chan context.Context)
	doneCh := make(chan struct{})
	close(doneCh)

	callbackFunc := func(ctx context.Context, j *Job[callbackWithCustomTimeoutArgs]) error {
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

func Test_Client_Insert(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		return client, &testBundle{}
	}

	t.Run("Succeeds", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		jobRow, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, 0, jobRow.Attempt)
		require.Equal(t, rivercommon.MaxAttemptsDefault, jobRow.MaxAttempts)
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

	t.Run("ErrorsOnDriverWithoutPool", func(t *testing.T) {
		t.Parallel()

		_, _ = setup(t)

		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riverinternaltest.Logger(t),
		})
		require.NoError(t, err)

		_, err = client.Insert(ctx, &noOpArgs{}, nil)
		require.ErrorIs(t, err, errInsertNoDriverDBPool)
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
		queries *dbsqlc.Queries
		tx      pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		tx, err := client.driver.GetDBPool().Begin(ctx)
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
		_, err = bundle.queries.JobGetByID(ctx, client.driver.GetDBPool(), jobRow.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
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

	type testBundle struct {
		queries *dbsqlc.Queries
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		return client, &testBundle{
			queries: dbsqlc.New(),
		}
	}

	t.Run("SucceedsWithMultipleJobs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		count, err := client.InsertMany(ctx, []InsertManyParams{
			{Args: noOpArgs{}, InsertOpts: &InsertOpts{Queue: "foo", Priority: 2}},
			{Args: noOpArgs{}},
		})
		require.NoError(t, err)
		require.Equal(t, int64(2), count)

		jobs, err := bundle.queries.JobGetByKind(ctx, client.driver.GetDBPool(), (noOpArgs{}).Kind())
		require.NoError(t, err)
		require.Len(t, jobs, 2, fmt.Sprintf("Expected to find exactly two jobs of kind: %s", (noOpArgs{}).Kind()))
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
		require.ErrorIs(t, err, errInsertNoDriverDBPool)
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
		queries *dbsqlc.Queries
		tx      pgx.Tx
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		tx, err := client.driver.GetDBPool().Begin(ctx)
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

		jobs, err := bundle.queries.JobGetByKind(ctx, bundle.tx, (noOpArgs{}).Kind())
		require.NoError(t, err)
		require.Len(t, jobs, 2, fmt.Sprintf("Expected to find exactly two jobs of kind: %s", (noOpArgs{}).Kind()))

		require.NoError(t, bundle.tx.Commit(ctx))

		// Ensure the jobs are visible outside the transaction:
		jobs, err = bundle.queries.JobGetByKind(ctx, client.driver.GetDBPool(), (noOpArgs{}).Kind())
		require.NoError(t, err)
		require.Len(t, jobs, 2, fmt.Sprintf("Expected to find exactly two jobs of kind: %s", (noOpArgs{}).Kind()))
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

		handlerErr := fmt.Errorf("job error")
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
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
				require.Equal(t, *unknownJobKindErr, UnknownJobKindError{Kind: "RandomWorkerNameThatIsNeverRegistered"})
				errorHandlerCalled = true
				return &ErrorHandlerResult{}
			},
		}

		client, bundle := setup(t, config)

		// Bypass the normal Insert function because that will error on an
		// unknown job.
		insertParams, err := insertParamsFromArgsAndOptions(unregisteredJobArgs{}, nil)
		require.NoError(t, err)
		_, err = client.adapter.JobInsert(ctx, insertParams)
		require.NoError(t, err)

		riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)

		require.True(t, errorHandlerCalled)
	})

	t.Run("PanicHandler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
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

	var (
		ctx     = context.Background()
		queries = dbsqlc.New()
	)

	type insertJobParams struct {
		Attempt     int16
		AttemptedAt *time.Time
		FinalizedAt *time.Time
		Kind        string
		MaxAttempts int16
		ScheduledAt *time.Time
		State       dbsqlc.JobState
	}

	insertJob := func(ctx context.Context, dbtx dbsqlc.DBTX, params insertJobParams) *dbsqlc.RiverJob {
		// This is a lot of boilerplate to get a realistic job into the database
		// with the number of errors that corresponds to its attempt count. Without
		// that, the rescued/errored jobs can retry immediately with no backoff and
		// cause flakiness as they quickly get picked back up again.
		errorCount := int(params.Attempt - 1)
		if params.Attempt == 0 {
			errorCount = int(params.Attempt)
		}

		errorsBytes := make([][]byte, errorCount)
		for i := 0; i < errorCount; i++ {
			var err error
			errorsBytes[i], err = json.Marshal(rivertype.AttemptError{
				At:      time.Now(),
				Attempt: i + 1,
				Error:   "mocked error",
				Trace:   "none",
			})
			require.NoError(t, err)
		}

		job, err := queries.JobInsert(ctx, dbtx, dbsqlc.JobInsertParams{
			Attempt:     valutil.FirstNonZero(params.Attempt, int16(1)),
			AttemptedAt: params.AttemptedAt,
			Errors:      errorsBytes,
			FinalizedAt: params.FinalizedAt,
			Kind:        valutil.FirstNonZero(params.Kind, "test_kind"),
			MaxAttempts: valutil.FirstNonZero(params.MaxAttempts, int16(rivercommon.MaxAttemptsDefault)),
			Priority:    int16(rivercommon.PriorityDefault),
			Queue:       QueueDefault,
			ScheduledAt: params.ScheduledAt,
			State:       params.State,
		})
		require.NoError(t, err)
		return job
	}

	t.Run("JobCleaner", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		config.CancelledJobRetentionPeriod = 1 * time.Hour
		config.CompletedJobRetentionPeriod = 1 * time.Hour
		config.DiscardedJobRetentionPeriod = 1 * time.Hour

		client := newTestClient(ctx, t, config)

		deleteHorizon := time.Now().Add(-config.CompletedJobRetentionPeriod)

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the cleaner could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateAvailable})
		ineligibleJob2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateRunning})
		ineligibleJob3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateScheduled})

		jobBeyondHorizon1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})
		jobBeyondHorizon3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(-1 * time.Hour))})

		// Will not be deleted.
		jobWithinHorizon1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})
		jobWithinHorizon3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(deleteHorizon.Add(1 * time.Hour))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
		jc := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
		jc.TestSignals.DeletedBatch.WaitOrTimeout()

		var err error
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), ineligibleJob1.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), ineligibleJob2.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), ineligibleJob3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there

		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobBeyondHorizon1.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobBeyondHorizon2.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobBeyondHorizon3.ID)
		require.ErrorIs(t, err, pgx.ErrNoRows)

		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobWithinHorizon1.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobWithinHorizon2.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
		_, err = queries.JobGetByID(ctx, client.driver.GetDBPool(), jobWithinHorizon3.ID)
		require.NotErrorIs(t, err, pgx.ErrNoRows) // still there
	})

	t.Run("PeriodicJobEnqueuerWithOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, &PeriodicJobOpts{RunOnStart: true}),
		}

		client := runNewTestClient(ctx, t, config)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.InsertedJobs.WaitOrTimeout()

		jobs, err := queries.JobGetByKind(ctx, client.driver.GetDBPool(), (periodicJobArgs{}).Kind())
		require.NoError(t, err)
		require.Len(t, jobs, 1, fmt.Sprintf("Expected to find exactly one job of kind: %s", (periodicJobArgs{}).Kind()))
	})

	t.Run("PeriodicJobEnqueuerNoOpts", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		worker := &periodicJobWorker{}
		AddWorker(config.Workers, worker)
		config.PeriodicJobs = []*PeriodicJob{
			NewPeriodicJob(cron.Every(15*time.Minute), func() (JobArgs, *InsertOpts) {
				return periodicJobArgs{}, nil
			}, nil),
		}

		client := runNewTestClient(ctx, t, config)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.PeriodicJobEnqueuer](client.queueMaintainer)
		svc.TestSignals.EnteredLoop.WaitOrTimeout()

		// No jobs yet because the RunOnStart option was not specified.
		jobs, err := queries.JobGetByKind(ctx, client.driver.GetDBPool(), (periodicJobArgs{}).Kind())
		require.NoError(t, err)
		require.Len(t, jobs, 0)
	})

	t.Run("Reindexer", func(t *testing.T) {
		t.Parallel()
		t.Skip("Reindexer is disabled for further development")

		config := newTestConfig(t, nil)
		config.ReindexerSchedule = cron.Every(time.Second)

		client := runNewTestClient(ctx, t, config)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.Reindexer](client.queueMaintainer)
		// There are two indexes to reindex by default:
		svc.TestSignals.Reindexed.WaitOrTimeout()
		svc.TestSignals.Reindexed.WaitOrTimeout()
	})

	t.Run("Rescuer", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		config.RescueStuckJobsAfter = 5 * time.Minute

		client := newTestClient(ctx, t, config)

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the rescuer could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRetryable, ScheduledAt: ptrutil.Ptr(now.Add(time.Minute))})
		ineligibleJob3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(now.Add(-time.Minute))})

		// large attempt number ensures these don't immediately start executing again:
		jobStuckToRetry1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRunning, Attempt: 20, AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobStuckToRetry2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRunning, Attempt: 20, AttemptedAt: ptrutil.Ptr(now.Add(-30 * time.Minute))})
		jobStuckToDiscard := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{
			State:       dbsqlc.JobStateRunning,
			Attempt:     20,
			AttemptedAt: ptrutil.Ptr(now.Add(-5*time.Minute - time.Second)),
			MaxAttempts: 1,
		})

		// Will not be rescued.
		jobNotYetStuck1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(now.Add(-4 * time.Minute))})
		jobNotYetStuck2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobNotYetStuck3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{Kind: "noOp", State: dbsqlc.JobStateRunning, AttemptedAt: ptrutil.Ptr(now.Add(-10 * time.Second))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
		svc := maintenance.GetService[*maintenance.Rescuer](client.queueMaintainer)
		svc.TestSignals.FetchedBatch.WaitOrTimeout()
		svc.TestSignals.UpdatedBatch.WaitOrTimeout()

		requireJobHasState := func(jobID int64, state dbsqlc.JobState) {
			t.Helper()
			job, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), jobID)
			require.NoError(t, err)
			require.Equal(t, state, job.State)
		}

		// unchanged
		requireJobHasState(ineligibleJob1.ID, ineligibleJob1.State)
		requireJobHasState(ineligibleJob2.ID, ineligibleJob2.State)
		requireJobHasState(ineligibleJob3.ID, ineligibleJob3.State)

		// Jobs to retry should be retryable:
		requireJobHasState(jobStuckToRetry1.ID, dbsqlc.JobStateRetryable)
		requireJobHasState(jobStuckToRetry2.ID, dbsqlc.JobStateRetryable)

		// This one should be discarded because it's already at MaxAttempts:
		requireJobHasState(jobStuckToDiscard.ID, dbsqlc.JobStateDiscarded)

		// not eligible for rescue, not stuck long enough yet:
		requireJobHasState(jobNotYetStuck1.ID, jobNotYetStuck1.State)
		requireJobHasState(jobNotYetStuck2.ID, jobNotYetStuck2.State)
		requireJobHasState(jobNotYetStuck3.ID, jobNotYetStuck3.State)
	})

	t.Run("Scheduler", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, nil)
		config.Queues = map[string]QueueConfig{"another_queue": {MaxWorkers: 1}} // don't work jobs on the default queue we're using in this test

		client := newTestClient(ctx, t, config)

		now := time.Now()

		// Take care to insert jobs before starting the client because otherwise
		// there's a race condition where the scheduler could run its initial
		// pass before our insertion is complete.
		ineligibleJob1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateAvailable})
		ineligibleJob2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateRunning})
		ineligibleJob3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCompleted})

		jobInPast1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour))})
		jobInPast2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Minute))})
		jobInPast3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateScheduled, ScheduledAt: ptrutil.Ptr(now.Add(-5 * time.Second))})

		// Will not be scheduled.
		jobInFuture1 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCancelled, FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Hour))})
		jobInFuture2 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateCompleted, FinalizedAt: ptrutil.Ptr(now.Add(1 * time.Minute))})
		jobInFuture3 := insertJob(ctx, client.driver.GetDBPool(), insertJobParams{State: dbsqlc.JobStateDiscarded, FinalizedAt: ptrutil.Ptr(now.Add(10 * time.Second))})

		startClient(ctx, t, client)

		client.testSignals.electedLeader.WaitOrTimeout()
		scheduler := maintenance.GetService[*maintenance.Scheduler](client.queueMaintainer)
		scheduler.TestSignals.ScheduledBatch.WaitOrTimeout()

		requireJobHasState := func(jobID int64, state dbsqlc.JobState) {
			t.Helper()
			job, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), jobID)
			require.NoError(t, err)
			require.Equal(t, state, job.State)
		}

		// unchanged
		requireJobHasState(ineligibleJob1.ID, ineligibleJob1.State)
		requireJobHasState(ineligibleJob2.ID, ineligibleJob2.State)
		requireJobHasState(ineligibleJob3.ID, ineligibleJob3.State)

		// Jobs with past timestamps should be now be made available:
		requireJobHasState(jobInPast1.ID, dbsqlc.JobStateAvailable)
		requireJobHasState(jobInPast2.ID, dbsqlc.JobStateAvailable)
		requireJobHasState(jobInPast3.ID, dbsqlc.JobStateAvailable)

		// not scheduled, still in future
		requireJobHasState(jobInFuture1.ID, jobInFuture1.State)
		requireJobHasState(jobInFuture2.ID, jobInFuture2.State)
		requireJobHasState(jobInFuture3.ID, jobInFuture3.State)
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

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return fmt.Errorf("job error")
		})

		// The default policy would work too, but this takes some variability
		// out of it to make comparisons easier.
		config.RetryPolicy = &retryPolicyNoJitter{}

		client := newTestClient(ctx, t, config)
		queries := dbsqlc.New()

		subscribeChan, cancel := client.Subscribe(EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		originalJobs := make([]*dbsqlc.RiverJob, rivercommon.MaxAttemptsDefault)
		for i := 0; i < len(originalJobs); i++ {
			job := requireInsert(ctx, client)
			// regression protection to ensure we're testing the right number of jobs:
			require.Equal(t, rivercommon.MaxAttemptsDefault, job.MaxAttempts)

			updatedJob, err := queries.JobUpdate(ctx, client.driver.GetDBPool(), dbsqlc.JobUpdateParams{
				ID:                  job.ID,
				AttemptedAtDoUpdate: true,
				AttemptedAt:         ptrutil.Ptr(time.Now().UTC()),
				AttemptDoUpdate:     true,
				Attempt:             int16(i), // starts at i, but will be i + 1 by the time it's being worked

				// Need to find a cleaner way around this, but state is required
				// because sqlc can't encode an empty string to the
				// corresponding enum. This value is not actually used because
				// StateDoUpdate was not supplied.
				State: dbsqlc.JobStateAvailable,
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

		finishedJobs, err := queries.JobGetByIDMany(ctx, client.driver.GetDBPool(),
			sliceutil.Map(originalJobs, func(m *dbsqlc.RiverJob) int64 { return m.ID }))
		require.NoError(t, err)

		// Jobs aren't guaranteed to come back out of the queue in the same
		// order that we inserted them, so make sure to compare using a lookup
		// map.
		finishedJobsByID := sliceutil.KeyBy(finishedJobs,
			func(m *dbsqlc.RiverJob) (int64, *dbsqlc.RiverJob) { return m.ID, m })

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

			expectedNextScheduledAt := client.config.RetryPolicy.NextRetry(dbsqlc.JobRowFromInternal(originalJob))

			t.Logf("Attempt number %d scheduled %v from original `attempted_at`",
				originalJob.Attempt, finishedJob.ScheduledAt.Sub(*originalJob.AttemptedAt))
			t.Logf("    Original attempt at:   %v", originalJob.AttemptedAt)
			t.Logf("    New scheduled at:      %v", finishedJob.ScheduledAt)
			t.Logf("    Expected scheduled at: %v", expectedNextScheduledAt)

			// TODO(brandur): This tolerance could be reduced if we could inject
			// time.Now into adapter which may happen with baseservice
			require.WithinDuration(t, expectedNextScheduledAt, finishedJob.ScheduledAt, 2*time.Second)

			require.Equal(t, dbsqlc.JobStateRetryable, finishedJob.State)
		}

		// One last discarded job.
		{
			originalJob := originalJobs[len(originalJobs)-1]
			finishedJob := finishedJobsByID[originalJob.ID]

			originalJob.Attempt += 1

			t.Logf("Attempt number %d discarded", originalJob.Attempt)

			// TODO(brandur): See note on tolerance above.
			require.WithinDuration(t, time.Now(), *finishedJob.FinalizedAt, 2*time.Second)
			require.Equal(t, dbsqlc.JobStateDiscarded, finishedJob.State)
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

		// Fail/succeed jobs based on their name so we can get a mix of both to
		// verify.
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			if strings.HasPrefix(j.Args.Name, "failed") {
				return fmt.Errorf("job error")
			}
			return nil
		})

		client := newTestClient(ctx, t, config)

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

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			if strings.HasPrefix(j.Args.Name, "failed") {
				return fmt.Errorf("job error")
			}
			return nil
		})

		client := newTestClient(ctx, t, config)

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

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			if strings.HasPrefix(j.Args.Name, "failed") {
				return fmt.Errorf("job error")
			}
			return nil
		})

		client := newTestClient(ctx, t, config)

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

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(ctx, t, config)

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

		// for i := 0; i < subscribeChanSize+1; i++ {
		// 	x := riverinternaltest.WaitOrTimeout(t, subscribeChan)
		// 	fmt.Printf("--- received: %+v\n\n", string(x.Job.EncodedArgs))
		// }
	})

	t.Run("PanicOnUnknownKind", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(ctx, t, config)

		require.PanicsWithError(t, "unknown event kind: does_not_exist", func() {
			_, _ = client.Subscribe(EventKind("does_not_exist"))
		})
	})

	t.Run("SubscriptionCancellation", func(t *testing.T) {
		t.Parallel()

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return nil
		})

		client := newTestClient(ctx, t, config)

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
	config := newTestConfig(t, makeAwaitCallback(startedCh, doneCh))
	config.FetchCooldown = 20 * time.Millisecond
	config.FetchPollInterval = 20 * time.Second // essentially disable polling
	config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 2}}

	client := newTestClient(ctx, t, config)
	statusUpdateCh := client.monitor.RegisterUpdates()

	insertedJob, err := client.Insert(ctx, callbackArgs{}, nil)
	require.NoError(err)

	startClient(ctx, t, client)

	// Wait for the client to be ready by waiting for a job to be executed:
	select {
	case jobID := <-startedCh:
		require.Equal(insertedJob.ID, jobID)
	case <-ctx.Done():
		t.Fatal("timed out waiting for warmup job to start")
	}
	waitForClientHealthy(ctx, t, statusUpdateCh)

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
		SubscribeChan <-chan *Event
	}

	setup := func(t *testing.T, config *Config) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		client := runNewTestClient(ctx, t, config)

		subscribeChan, cancel := client.Subscribe(EventKindJobCancelled, EventKindJobCompleted, EventKindJobFailed)
		t.Cleanup(cancel)

		return client, &testBundle{SubscribeChan: subscribeChan}
	}

	queries := dbsqlc.New()

	t.Run("JobThatReturnsNilIsCompleted", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return nil
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCompleted, event.Job.State)

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(time.Now(), *reloadedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobThatIsAlreadyCompletedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		var dbPool *pgxpool.Pool
		now := time.Now().UTC()
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			_, err := queries.JobSetCompleted(ctx, dbPool, dbsqlc.JobSetCompletedParams{ID: j.ID, FinalizedAt: now})
			require.NoError(err)
			return nil
		})

		client, bundle := setup(t, config)
		dbPool = client.driver.GetDBPool()

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCompleted, event.Job.State)

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateCompleted, reloadedJob.State)
		require.WithinDuration(now, *reloadedJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobThatReturnsErrIsRetryable", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return errors.New("oops")
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateRetryable, event.Job.State)

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateRetryable, reloadedJob.State)
		require.WithinDuration(time.Now(), reloadedJob.ScheduledAt, 2*time.Second)
		require.Nil(reloadedJob.FinalizedAt)
	})

	t.Run("JobThatReturnsJobCancelErrorIsImmediatelyCancelled", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			return JobCancel(errors.New("oops"))
		})

		client, bundle := setup(t, config)

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateCancelled, event.Job.State)

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateCancelled, reloadedJob.State)
		require.NotNil(reloadedJob.FinalizedAt)
		require.WithinDuration(time.Now(), *reloadedJob.FinalizedAt, 2*time.Second)
	})

	t.Run("JobThatIsAlreadyDiscardedIsNotAlteredByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		var dbPool *pgxpool.Pool
		now := time.Now().UTC()
		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			_, err := queries.JobSetDiscarded(ctx, dbPool, dbsqlc.JobSetDiscardedParams{
				ID:          j.ID,
				Error:       []byte("{\"error\": \"oops\"}"),
				FinalizedAt: now,
			})
			require.NoError(err)
			return errors.New("oops")
		})

		client, bundle := setup(t, config)
		dbPool = client.driver.GetDBPool()

		job, err := client.Insert(ctx, callbackArgs{}, nil)
		require.NoError(err)

		event := riverinternaltest.WaitOrTimeout(t, bundle.SubscribeChan)
		require.Equal(job.ID, event.Job.ID)
		require.Equal(JobStateDiscarded, event.Job.State)

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateDiscarded, reloadedJob.State)
		require.NotNil(reloadedJob.FinalizedAt)
	})

	t.Run("JobThatIsCompletedManuallyIsNotTouchedByCompleter", func(t *testing.T) {
		t.Parallel()

		require := require.New(t)
		var dbPool *pgxpool.Pool
		now := time.Now().UTC()
		var updatedJob *Job[callbackArgs]

		config := newTestConfig(t, func(ctx context.Context, j *Job[callbackArgs]) error {
			tx, err := dbPool.Begin(ctx)
			require.NoError(err)

			updatedJob, err = JobCompleteTx[*riverpgxv5.Driver](ctx, tx, j)
			require.NoError(err)

			return tx.Commit(ctx)
		})

		client, bundle := setup(t, config)
		dbPool = client.driver.GetDBPool()

		job, err := client.Insert(ctx, callbackArgs{}, nil)
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

		reloadedJob, err := queries.JobGetByID(ctx, client.driver.GetDBPool(), job.ID)
		require.NoError(err)

		require.Equal(dbsqlc.JobStateCompleted, reloadedJob.State)
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

	insertParams, err := insertParamsFromArgsAndOptions(unregisteredJobArgs{}, nil)
	require.NoError(err)
	insertRes, err := client.adapter.JobInsert(ctx, insertParams)
	require.NoError(err)

	event := riverinternaltest.WaitOrTimeout(t, subscribeChan)
	require.Equal(insertRes.Job.ID, event.Job.ID)
	require.Equal(insertRes.Job.Kind, "RandomWorkerNameThatIsNeverRegistered")
	require.Len(event.Job.Errors, 1)
	require.Equal((&UnknownJobKindError{Kind: "RandomWorkerNameThatIsNeverRegistered"}).Error(), event.Job.Errors[0].Error)
	require.Equal(JobStateRetryable, event.Job.State)
	// Ensure that ScheduledAt was updated with next run time:
	require.True(event.Job.ScheduledAt.After(insertRes.Job.ScheduledAt))
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

		config := newTestConfig(t, nil)
		config.Queues = nil
		config.Workers = nil

		client := newTestClient(ctx, t, config)
		err := client.Start(ctx)
		require.EqualError(t, err, "client Queues and Workers must be configured for a client to start working")
	})

	t.Run("DatabaseError", func(t *testing.T) {
		t.Parallel()

		dbConfig := riverinternaltest.DatabaseConfig("does-not-exist-and-dont-create-it")

		dbPool, err := pgxpool.NewWithConfig(ctx, dbConfig)
		require.NoError(t, err)

		config := newTestConfig(t, nil)

		client := newTestClient(ctx, t, config)
		client.driver = riverpgxv5.New(dbPool)

		err = client.Start(ctx)
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.InvalidCatalogName, pgErr.Code)
	})
}

func Test_NewClient_ClientIDWrittenToJobAttemptedByWhenFetched(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	ctx := context.Background()
	doneCh := make(chan struct{})
	startedCh := make(chan *Job[callbackArgs])

	callback := func(ctx context.Context, j *Job[callbackArgs]) error {
		startedCh <- j
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
		require.Equal([]string{client.id}, startedJob.AttemptedBy)
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

	require.Zero(t, client.adapter.(*dbadapter.StandardAdapter).Config.AdvisoryLockPrefix) //nolint:forcetypeassert

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, maintenance.CancelledJobRetentionPeriodDefault, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, maintenance.CompletedJobRetentionPeriodDefault, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, maintenance.DiscardedJobRetentionPeriodDefault, jobCleaner.Config.DiscardedJobRetentionPeriod)

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

	require.Equal(t, int32(123_456), client.adapter.(*dbadapter.StandardAdapter).Config.AdvisoryLockPrefix) //nolint:forcetypeassert

	jobCleaner := maintenance.GetService[*maintenance.JobCleaner](client.queueMaintainer)
	require.Equal(t, 1*time.Hour, jobCleaner.Config.CancelledJobRetentionPeriod)
	require.Equal(t, 2*time.Hour, jobCleaner.Config.CompletedJobRetentionPeriod)
	require.Equal(t, 3*time.Hour, jobCleaner.Config.DiscardedJobRetentionPeriod)

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
			wantErr: fmt.Errorf("RescueStuckJobsAfter cannot be less than JobTimeout"),
		},
		{
			name: "RescueStuckJobsAfter increased automatically on a high JobTimeout when not set explicitly",
			configFunc: func(config *Config) {
				config.JobTimeout = 23 * time.Hour
			},
			validateResult: func(t *testing.T, client *Client[pgx.Tx]) { //nolint:thelper
				require.Equal(t, 23*time.Hour+maintenance.RescueAfterDefault, client.config.RescueStuckJobsAfter)
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
			wantErr: fmt.Errorf("invalid number of workers for queue \"default\": -1"),
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
			name: "Queues queue names can't have hyphens",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"no-hyphens": {MaxWorkers: 1}}
			},
			wantErr: fmt.Errorf("queue name is invalid, see documentation: \"no-hyphens\""),
		},
		{
			name: "Queues queue names can be letters and numbers joined by underscores",
			configFunc: func(config *Config) {
				config.Queues = map[string]QueueConfig{"some_awesome_3rd_queue_namezzz": {MaxWorkers: 1}}
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
			name: "Workers cannot be empty if Queues is set",
			configFunc: func(config *Config) {
				config.Workers = nil
			},
			wantErr: errors.New("Workers must be set if Queues is set"),
		},
		{
			name: "Workers must contain at least one worker",
			configFunc: func(config *Config) {
				config.Workers = NewWorkers()
			},
			wantErr: errors.New("at least one Worker must be added to the Workers bundle"),
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
				require.NotNil(err)
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

func (w *timeoutTestWorker) Timeout(j *Job[timeoutTestArgs]) time.Duration {
	return j.Args.TimeoutValue
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

		insertParams, err := insertParamsFromArgsAndOptions(noOpArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, `{"name":""}`, string(insertParams.EncodedArgs))
		require.Equal(t, (noOpArgs{}).Kind(), insertParams.Kind)
		require.Equal(t, rivercommon.MaxAttemptsDefault, insertParams.MaxAttempts)
		require.Equal(t, rivercommon.PriorityDefault, insertParams.Priority)
		require.Equal(t, QueueDefault, insertParams.Queue)
		require.Equal(t, time.Time{}, insertParams.ScheduledAt)
		require.Equal(t, []string(nil), insertParams.Tags)
		require.False(t, insertParams.Unique)
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
		insertParams, err := insertParamsFromArgsAndOptions(noOpArgs{}, opts)
		require.NoError(t, err)
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.Equal(t, opts.ScheduledAt, insertParams.ScheduledAt)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("WorkerInsertOptsOverrides", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromArgsAndOptions(&customInsertOptsJobArgs{}, nil)
		require.NoError(t, err)
		// All these come from overrides in customInsertOptsJobArgs's definition:
		require.Equal(t, 42, insertParams.MaxAttempts)
		require.Equal(t, 2, insertParams.Priority)
		require.Equal(t, "other", insertParams.Queue)
		require.Equal(t, []string{"tag1", "tag2"}, insertParams.Tags)
	})

	t.Run("PriorityIsLimitedTo4", func(t *testing.T) {
		t.Parallel()

		insertParams, err := insertParamsFromArgsAndOptions(noOpArgs{}, &InsertOpts{Priority: 5})
		require.ErrorContains(t, err, "priority must be between 1 and 4")
		require.Nil(t, insertParams)
	})

	t.Run("NonEmptyArgs", func(t *testing.T) {
		t.Parallel()

		args := timeoutTestArgs{TimeoutValue: time.Hour}
		insertParams, err := insertParamsFromArgsAndOptions(args, nil)
		require.NoError(t, err)
		require.Equal(t, `{"timeout_value":3600000000000}`, string(insertParams.EncodedArgs))
	})

	t.Run("UniqueOptsAreValidated", func(t *testing.T) {
		t.Parallel()

		// Ensure that unique opts are validated. No need to be exhaustive here
		// since we already have tests elsewhere for that. Just make sure validation
		// is running.
		insertParams, err := insertParamsFromArgsAndOptions(
			noOpArgs{},
			&InsertOpts{UniqueOpts: UniqueOpts{ByPeriod: 1 * time.Millisecond}},
		)
		require.EqualError(t, err, "JobUniqueOpts.ByPeriod should not be less than 1 second")
		require.Nil(t, insertParams)
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

		client := newTestClient(ctx, t, newTestConfig(t, nil))

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
