package riverdrivertest

import (
	"context"
	"database/sql"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/urlutil"
	"github.com/riverqueue/river/rivertype"
)

func TestClientWithDriverRiverDatabaseSQLLibPQ(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(t, err)

	stdPool := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
	)
}

func TestClientWithDriverRiverDatabaseSQLPgx(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
	)
}

func TestClientWithDriverRiverPgxV5(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		dbPool = riversharedtest.DBPool(ctx, t)
		driver = riverpgxv5.New(dbPool)
	)

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[pgx.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
	)
}

func TestClientWithDriverRiverLibSQL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			var (
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, &riverdbtest.TestSchemaOpts{
					ProcurePool: func(ctx context.Context, schema string) (any, string) {
						return riversharedtest.DBPoolLibSQL(ctx, t, schema), "" // could also be `main` instead of empty string
					},
				})
			)
			return driver, schema
		},
	)
}

func TestClientWithDriverRiverSQLiteModernC(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			var (
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, &riverdbtest.TestSchemaOpts{
					ProcurePool: func(ctx context.Context, schema string) (any, string) {
						return riversharedtest.DBPoolSQLite(ctx, t, schema), "" // could also be `main` instead of empty string
					},
				})
			)
			return driver, schema
		},
	)
}

type noOpArgs struct {
	Name string `json:"name"`
}

func (noOpArgs) Kind() string { return "noOp" }

type noOpWorker struct {
	river.WorkerDefaults[noOpArgs]
}

func (w *noOpWorker) Work(ctx context.Context, job *river.Job[noOpArgs]) error { return nil }

// Try to keep this helper close to the one found in the top-level package so we
// can copy/paste between them reasonably easily.
func newTestConfig(t *testing.T, schema string) *river.Config {
	t.Helper()

	workers := river.NewWorkers()
	river.AddWorker(workers, &noOpWorker{})

	return &river.Config{
		FetchCooldown:     20 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
		Logger:            riversharedtest.Logger(t),
		MaxAttempts:       river.MaxAttemptsDefault,
		Queues:            map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 50}},
		Schema:            schema,
		Test: river.TestConfig{
			Time: &riversharedtest.TimeStub{},
		},
		TestOnly: true, // disables staggered start in maintenance services
		Workers:  workers,
	}
}

// Try to keep this helper close to the one found in the top-level package so we
// can copy/paste between them reasonably easily.
func startClient[TTx any](ctx context.Context, t *testing.T, client *river.Client[TTx]) {
	t.Helper()

	require.NoError(t, client.Start(ctx))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		require.NoError(t, client.Stop(ctx))
	})
}

// Try to keep this helper close to the one found in the top-level package so we
// can copy/paste between them reasonably easily.
func subscribe[TTx any](t *testing.T, client *river.Client[TTx]) <-chan *river.Event {
	t.Helper()

	subscribeChan, cancel := client.Subscribe(
		river.EventKindJobCancelled,
		river.EventKindJobCompleted,
		river.EventKindJobFailed,
		river.EventKindJobSnoozed,
		river.EventKindQueuePaused,
		river.EventKindQueueResumed,
	)
	t.Cleanup(cancel)
	return subscribeChan
}

// ExerciseClient exercises a client using a generic driver using a minimal set
// of test cases to verify that the driver works end to end.
func ExerciseClient[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T) (riverdriver.Driver[TTx], string),
) {
	t.Helper()

	type testBundle struct {
		config *river.Config
		driver riverdriver.Driver[TTx]
		exec   riverdriver.Executor
		schema string
	}

	// Alternate setup returning only client Config rather than a full Client.
	setupConfig := func(t *testing.T) (*river.Config, *testBundle) {
		t.Helper()

		var (
			driver, schema = driverWithSchema(ctx, t)
			config         = newTestConfig(t, schema)
		)

		return config, &testBundle{
			config: config,
			driver: driver,
			exec:   driver.GetExecutor(),
			schema: schema,
		}
	}

	setup := func(t *testing.T) (*river.Client[TTx], *testBundle) {
		t.Helper()

		config, bundle := setupConfig(t)

		client, err := river.NewClient(bundle.driver, config)
		require.NoError(t, err)

		return client, bundle
	}

	beginTx := func(ctx context.Context, t *testing.T, bundle *testBundle) (TTx, riverdriver.ExecutorTx) {
		t.Helper()

		execTx, err := bundle.driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)

		// Ignore error on cleanup so we can roll back early in tests where desirable.
		t.Cleanup(func() { _ = execTx.Rollback(ctx) })

		return bundle.driver.UnwrapTx(execTx), execTx
	}

	t.Run("StartInsertAndWork", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[JobArgs]) error {
			return nil
		}))

		subscribeChan := subscribe(t, client)

		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, river.EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes.Job.ID, event.Job.ID)
		require.Equal(t, insertRes.Job.Kind, event.Job.Kind)
	})

	t.Run("JobGet", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		fetchedJob, err := client.JobGet(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.ID, fetchedJob.ID)
	})

	t.Run("JobGetTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{Schema: bundle.schema})

		fetchedJob, err := client.JobGetTx(ctx, tx, job.ID)
		require.NoError(t, err)
		require.Equal(t, job.ID, fetchedJob.ID)
	})

	t.Run("JobList", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		listRes, err := client.JobList(ctx, river.NewJobListParams())
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 2)
		require.Equal(t, job1.ID, listRes.Jobs[0].ID)
		require.Equal(t, job2.ID, listRes.Jobs[1].ID)
	})

	t.Run("JobListAllArgs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobList(ctx,
			river.NewJobListParams().
				IDs(job.ID).
				Kinds(job.Kind).
				Priorities(int16(min(job.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job.Queue).
				States(job.State),
		)
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListMetadata", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Metadata: []byte(`{"foo":"bar","bar":"baz"}`),
			Schema:   bundle.schema,
		})

		listRes, err := client.JobList(ctx, river.NewJobListParams().Metadata(`{"foo":"bar"}`))
		if bundle.driver.DatabaseName() == databaseNameSQLite {
			t.Logf("Ignoring unsupported JobListResult.Metadata on SQLite")
			require.EqualError(t, err, "JobListParams.Metadata is not supported on SQLite")
			return
		}
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		var (
			job1 = testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{Schema: bundle.schema})
		)

		listRes, err := client.JobListTx(ctx, tx, river.NewJobListParams())
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 2)
		require.Equal(t, job1.ID, listRes.Jobs[0].ID)
		require.Equal(t, job2.ID, listRes.Jobs[1].ID)
	})

	t.Run("JobListTxAllArgs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobListTx(ctx, tx,
			river.NewJobListParams().
				IDs(job.ID).
				Kinds(job.Kind).
				Priorities(int16(min(job.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job.Queue).
				States(job.State),
		)
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListTxMetadata", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{
			Metadata: []byte(`{"foo":"bar","bar":"baz"}`),
			Schema:   bundle.schema,
		})

		listRes, err := client.JobListTx(ctx, tx, river.NewJobListParams().Metadata(`{"foo":"bar"}`))
		if bundle.driver.DatabaseName() == databaseNameSQLite {
			t.Logf("Ignoring unsupported JobListTxResult.Metadata on SQLite")
			require.EqualError(t, err, "JobListParams.Metadata is not supported on SQLite")
			return
		}
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListTxWhere", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{
			Metadata: []byte(`{"foo":"bar","bar":"baz"}`),
			Schema:   bundle.schema,
		})

		listParams := river.NewJobListParams()

		if bundle.driver.DatabaseName() == databaseNameSQLite {
			listParams = listParams.Where("metadata ->> @json_path = @json_val", river.NamedArgs{"json_path": "$.foo", "json_val": "bar"})
		} else {
			// "bar" is quoted in this branch because `jsonb_path_query_first` needs to be compared to a JSON value
			listParams = listParams.Where("jsonb_path_query_first(metadata, @json_path) = @json_val", river.NamedArgs{"json_path": "$.foo", "json_val": `"bar"`})
		}

		listRes, err := client.JobListTx(ctx, tx, listParams)
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("QueueGet", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Schema: bundle.schema})

		fetchedQueue, err := client.QueueGet(ctx, queue.Name)
		require.NoError(t, err)
		require.Equal(t, queue.Name, fetchedQueue.Name)
	})

	t.Run("QueueGetTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		queue := testfactory.Queue(ctx, t, execTx, &testfactory.QueueOpts{Schema: bundle.schema})

		fetchedQueue, err := client.QueueGetTx(ctx, tx, queue.Name)
		require.NoError(t, err)
		require.Equal(t, queue.Name, fetchedQueue.Name)
	})

	t.Run("QueueList", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			queue1 = testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Schema: bundle.schema})
			queue2 = testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Schema: bundle.schema})
		)

		listRes, err := client.QueueList(ctx, river.NewQueueListParams())
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 2)
		require.Equal(t, queue1.Name, listRes.Queues[0].Name)
		require.Equal(t, queue2.Name, listRes.Queues[1].Name)
	})

	t.Run("QueueListTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		tx, execTx := beginTx(ctx, t, bundle)

		var (
			queue1 = testfactory.Queue(ctx, t, execTx, &testfactory.QueueOpts{Schema: bundle.schema})
			queue2 = testfactory.Queue(ctx, t, execTx, &testfactory.QueueOpts{Schema: bundle.schema})
		)

		listRes, err := client.QueueListTx(ctx, tx, river.NewQueueListParams())
		require.NoError(t, err)
		require.Len(t, listRes.Queues, 2)
		require.Equal(t, queue1.Name, listRes.Queues[0].Name)
		require.Equal(t, queue2.Name, listRes.Queues[1].Name)
	})

	t.Run("QueuePauseAndResume", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes1, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, river.EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes1.Job.ID, event.Job.ID)

		require.NoError(t, client.QueuePause(ctx, river.QueueDefault, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &river.Event{Kind: river.EventKindQueuePaused, Queue: &rivertype.Queue{Name: river.QueueDefault}}, event)

		insertRes2, err := client.Insert(ctx, &noOpArgs{}, nil)
		require.NoError(t, err)

		// Re-fetch the job to make sure it's paused and hasn't been changed to
		// `running`. This is potentially a little racy in that it could show a
		// false negative, but the alternative is test intermittency with sleeps.
		job2, err := client.JobGet(ctx, insertRes2.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, job2.State)

		// Also check that the subscription channel is fully empty (no job
		// completions, no queue resumes).
		select {
		case event := <-subscribeChan:
			require.Nil(t, event, "Expected to find nothing in subscription channel, but found: %+v", event)
		default:
		}

		require.NoError(t, client.QueueResume(ctx, river.QueueDefault, nil))
		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, &river.Event{Kind: river.EventKindQueueResumed, Queue: &rivertype.Queue{Name: river.QueueDefault}}, event)

		event = riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, river.EventKindJobCompleted, event.Kind)
		require.Equal(t, insertRes2.Job.ID, event.Job.ID)
	})

	t.Run("QueueUpdate", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Schema: bundle.schema})

		updatedQueue, err := client.QueueUpdate(ctx, queue.Name, &river.QueueUpdateParams{
			Metadata: []byte(`{"foo":"bar"}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(updatedQueue.Metadata))
		require.Equal(t, queue.Name, updatedQueue.Name)
	})

	t.Run("QueueUpdateTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		queue := testfactory.Queue(ctx, t, bundle.exec, &testfactory.QueueOpts{Schema: bundle.schema})

		tx, execTx := beginTx(ctx, t, bundle)

		updatedQueue, err := client.QueueUpdateTx(ctx, tx, queue.Name, &river.QueueUpdateParams{
			Metadata: []byte(`{"foo":"bar"}`),
		})
		require.NoError(t, err)
		require.JSONEq(t, `{"foo":"bar"}`, string(updatedQueue.Metadata))
		require.Equal(t, queue.Name, updatedQueue.Name)

		require.NoError(t, execTx.Rollback(ctx))

		fetchedQueue, err := client.QueueGet(ctx, queue.Name)
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(fetchedQueue.Metadata))
	})
}
