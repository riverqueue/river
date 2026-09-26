package riverdrivertest

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
	_ "turso.tech/database/tursogo"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
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

func TestClientWithDriverRiverDatabaseSQLPgxWithPgxListener(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.NewWithPgxListener(stdPool, dbPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
	)
}

func TestClientWithDriverRiverDatabaseSQLPgxWithPgxListenerJobCompleteTx(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.NewWithPgxListener(stdPool, dbPool)
		schema  = riverdbtest.TestSchema(ctx, t, driver, nil)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	var jobCompleted testsignal.TestSignal[int64]
	jobCompleted.Init(t)

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	config := newTestConfig(t, schema)
	config.FetchPollInterval = time.Minute
	river.AddWorker(config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[JobArgs]) error {
		tx, err := stdPool.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		updatedJob, err := river.JobCompleteTx[*riverdatabasesql.Driver](ctx, tx, job)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		jobCompleted.Signal(updatedJob.ID)
		return nil
	}))

	client, err := river.NewClient(driver, config)
	require.NoError(t, err)
	startClient(ctx, t, client)

	insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
	require.NoError(t, err)
	require.Equal(t, insertRes.Job.ID, jobCompleted.WaitOrTimeout())

	completedJob, err := client.JobGet(ctx, insertRes.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
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

func TestClientWithDriverRiverTurso(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	ExerciseClient(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			var (
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, &riverdbtest.TestSchemaOpts{
					ProcurePool: func(ctx context.Context, schema string) (any, string) {
						return riversharedtest.DBPoolTurso(ctx, t, schema), "" // could also be `main` instead of empty string
					},
				})
			)
			return driver, schema
		},
	)
}

// customJSONArgs are job args encoded as an arbitrary JSON object, like args
// with a custom MarshalJSON implementation.
type customJSONArgs struct {
	values map[string]string
}

func (customJSONArgs) Kind() string { return "customJSON" }

func (a customJSONArgs) MarshalJSON() ([]byte, error) { return json.Marshal(a.values) }

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
		river.EventKindJobInterrupted,
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

	t.Run("CancelRunningJobWithListener", func(t *testing.T) {
		t.Parallel()

		config, bundle := setupConfig(t)
		if bundle.driver.DatabaseName() != riverdriver.DatabaseNamePostgres || !bundle.driver.SupportsListener() {
			t.Skip("requires a Postgres listener")
		}
		config.FetchPollInterval = time.Minute

		client, err := river.NewClient(bundle.driver, config)
		require.NoError(t, err)

		var jobStarted testsignal.TestSignal[int64]
		jobStarted.Init(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[JobArgs]) error {
			jobStarted.Signal(job.ID)
			<-ctx.Done()
			return ctx.Err()
		}))

		subscribeChan := subscribe(t, client)
		startClient(ctx, t, client)

		insertRes, err := client.Insert(ctx, &JobArgs{}, nil)
		require.NoError(t, err)
		require.Equal(t, insertRes.Job.ID, jobStarted.WaitOrTimeout())

		updatedJob, err := client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		event := riversharedtest.WaitOrTimeout(t, subscribeChan)
		require.Equal(t, river.EventKindJobCancelled, event.Kind)
		require.Equal(t, rivertype.JobStateCancelled, event.Job.State)
	})

	// Keys containing gjson/sjson path syntax (and the empty key) are distinct
	// keys when unique by all args, so args differing in their values aren't
	// duplicates.
	t.Run("InsertUniqueByArgsAllArgsWithPathSyntaxKeys", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[customJSONArgs]) error {
			return nil
		}))

		insert := func(t *testing.T, values map[string]string) *rivertype.JobInsertResult {
			t.Helper()

			insertRes, err := client.Insert(ctx, customJSONArgs{values: values}, &river.InsertOpts{
				UniqueOpts: river.UniqueOpts{ByArgs: true},
			})
			require.NoError(t, err)
			return insertRes
		}

		var (
			keys       = []string{"", "!x", ":x", "[x", "alice@example.com", "file.name", "x*?", "x#", "x|", `x\y`, "{x"}
			baseValues = map[string]string{"x": "x"}
		)
		for _, key := range keys {
			baseValues[key] = "value"
		}

		insertRes0 := insert(t, baseValues)
		require.False(t, insertRes0.UniqueSkippedAsDuplicate)

		insertRes1 := insert(t, maps.Clone(baseValues))
		require.True(t, insertRes1.UniqueSkippedAsDuplicate)
		require.Equal(t, insertRes0.Job.ID, insertRes1.Job.ID)

		for _, key := range keys {
			values := maps.Clone(baseValues)
			values[key] = "other"

			insertRes := insert(t, values)
			require.False(t, insertRes.UniqueSkippedAsDuplicate, "key: %q", key)
		}
	})

	// Unique fields whose JSON keys contain gjson/sjson path syntax, or whose
	// `json` tag has no name, are part of the unique key.
	t.Run("InsertUniqueByArgsUniqueFieldsWithPathSyntaxKeys", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type User struct {
			ID string `json:"id" river:"unique"`
		}

		//nolint:tagliatelle // non-snake keys are intentional
		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]

			Bang      string `json:"!bang"             river:"unique"`
			Colon     string `json:":colon"            river:"unique"`
			Email     string `json:"alice@example.com" river:"unique"`
			Other     string `json:"other"`
			Recipient string `json:",omitempty"        river:"unique"`
			User      User   `json:"user"`
			UserID    string `json:"user.id"           river:"unique"`
			Wildcard  string `json:"wild*?"            river:"unique"`
		}

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[JobArgs]) error {
			return nil
		}))

		insert := func(t *testing.T, args *JobArgs) *rivertype.JobInsertResult {
			t.Helper()

			insertRes, err := client.Insert(ctx, args, &river.InsertOpts{
				UniqueOpts: river.UniqueOpts{ByArgs: true},
			})
			require.NoError(t, err)
			return insertRes
		}

		baseArgs := JobArgs{
			Bang:      "bang",
			Colon:     "colon",
			Email:     "email",
			Other:     "other",
			Recipient: "recipient",
			User:      User{ID: "nested"},
			UserID:    "u1",
			Wildcard:  "wildcard",
		}

		insertRes0 := insert(t, &baseArgs)
		require.False(t, insertRes0.UniqueSkippedAsDuplicate)

		// A change to a field that isn't unique is still a duplicate.
		args := baseArgs
		args.Other = "changed"
		insertRes1 := insert(t, &args)
		require.True(t, insertRes1.UniqueSkippedAsDuplicate)
		require.Equal(t, insertRes0.Job.ID, insertRes1.Job.ID)

		for _, modify := range []func(args *JobArgs){
			func(args *JobArgs) { args.Bang = "changed" },
			func(args *JobArgs) { args.Colon = "changed" },
			func(args *JobArgs) { args.Email = "changed" },
			func(args *JobArgs) { args.Recipient = "changed" },
			func(args *JobArgs) { args.User.ID = "changed" },
			func(args *JobArgs) { args.UserID = "u2" },
			func(args *JobArgs) { args.Wildcard = "changed" },
		} {
			args := baseArgs
			modify(&args)

			insertRes := insert(t, &args)
			require.False(t, insertRes.UniqueSkippedAsDuplicate, "args: %+v", args)
		}
	})

	t.Run("InsertUniqueByPeriod", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		type JobArgs struct {
			testutil.JobArgsReflectKind[JobArgs]
		}

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[JobArgs]) error {
			return nil
		}))

		var (
			chicago = time.FixedZone("CDT", -5*60*60)

			// Far enough in the future that the test can't cross a period
			// boundary while it runs.
			scheduledAt = time.Now().UTC().Add(72 * time.Hour).Truncate(24 * time.Hour).Add(9 * time.Hour)
			uniqueOpts  = river.UniqueOpts{ByPeriod: 24 * time.Hour}
		)

		insertRes0, err := client.Insert(ctx, &JobArgs{}, &river.InsertOpts{
			ScheduledAt: scheduledAt.In(chicago),
			UniqueOpts:  uniqueOpts,
		})
		require.NoError(t, err)
		require.False(t, insertRes0.UniqueSkippedAsDuplicate)

		// Same UTC period expressed in UTC is a duplicate.
		insertRes1, err := client.Insert(ctx, &JobArgs{}, &river.InsertOpts{
			ScheduledAt: scheduledAt.Add(10 * time.Hour),
			UniqueOpts:  uniqueOpts,
		})
		require.NoError(t, err)
		require.True(t, insertRes1.UniqueSkippedAsDuplicate)
		require.Equal(t, insertRes0.Job.ID, insertRes1.Job.ID)

		// The next period is not.
		insertRes2, err := client.Insert(ctx, &JobArgs{}, &river.InsertOpts{
			ScheduledAt: scheduledAt.Add(24 * time.Hour),
			UniqueOpts:  uniqueOpts,
		})
		require.NoError(t, err)
		require.False(t, insertRes2.UniqueSkippedAsDuplicate)
	})

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		deletedJob, err := client.JobDelete(ctx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, job1.ID, deletedJob.ID)

		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
	})

	t.Run("JobDeleteTx", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		tx, execTx := beginTx(ctx, t, bundle)

		deletedJob, err := client.JobDeleteTx(ctx, tx, job1.ID)
		require.NoError(t, err)
		require.Equal(t, job1.ID, deletedJob.ID)

		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)

		// SQLite can't support multiple concurrent transactions, so skip this extra check there.
		if bundle.driver.DatabaseName() != riverdriver.DatabaseNameSQLite {
			_, otherExecTx := beginTx(ctx, t, bundle)

			// Both jobs present because other transaction doesn't see the deletion.
			_, err = otherExecTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
			require.NoError(t, err)
			_, err = otherExecTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
			require.NoError(t, err)
		}
	})

	t.Run("JobDeleteManyUnsafeAll", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx, river.NewJobDeleteManyParams().UnsafeAll())
		require.NoError(t, err)
		require.Len(t, deleteRes.Jobs, 2)
		require.Equal(t, job1.ID, deleteRes.Jobs[0].ID)
		require.Equal(t, job2.ID, deleteRes.Jobs[1].ID)

		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("JobDeleteManyAllArgs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		deleteRes, err := client.JobDeleteMany(ctx,
			river.NewJobDeleteManyParams().
				IDs(job1.ID).
				Kinds(job1.Kind).
				Priorities(int16(min(job1.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job1.Queue).
				States(job1.State),
		)
		require.NoError(t, err)
		require.Len(t, deleteRes.Jobs, 1)
		require.Equal(t, job1.ID, deleteRes.Jobs[0].ID)

		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
	})

	t.Run("JobDeleteManyTxUnsafeAll", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		tx, execTx := beginTx(ctx, t, bundle)

		deleteRes, err := client.JobDeleteManyTx(ctx, tx, river.NewJobDeleteManyParams().UnsafeAll())
		require.NoError(t, err)
		require.Len(t, deleteRes.Jobs, 2)
		require.Equal(t, job1.ID, deleteRes.Jobs[0].ID)
		require.Equal(t, job2.ID, deleteRes.Jobs[1].ID)

		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)

		// SQLite can't support multiple concurrent transactions, so skip this extra check there.
		if bundle.driver.DatabaseName() != riverdriver.DatabaseNameSQLite {
			_, otherExecTx := beginTx(ctx, t, bundle)

			// Jobs present because other transaction doesn't see the deletions.
			_, err = otherExecTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
			require.NoError(t, err)
			_, err = otherExecTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
			require.NoError(t, err)
		}
	})

	t.Run("JobDeleteManyTxAllArgs", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		var (
			job1 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
			job2 = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
		)

		tx, execTx := beginTx(ctx, t, bundle)

		deleteRes, err := client.JobDeleteManyTx(ctx, tx,
			river.NewJobDeleteManyParams().
				IDs(job1.ID).
				Kinds(job1.Kind).
				Priorities(int16(min(job1.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job1.Queue).
				States(job1.State),
		)
		require.NoError(t, err)
		require.Len(t, deleteRes.Jobs, 1)
		require.Equal(t, job1.ID, deleteRes.Jobs[0].ID)

		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: bundle.schema})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		_, err = execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID, Schema: bundle.schema})
		require.NoError(t, err)
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

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Schema: bundle.schema,
			Tags:   []string{"all-args-tag", "all-args-secondary"},
		})

		listRes, err := client.JobList(ctx,
			river.NewJobListParams().
				IDs(job.ID).
				Kinds(job.Kind).
				Priorities(int16(min(job.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job.Queue).
				States(job.State).
				TagsAll("all-args-tag").
				TagsAny("all-args-secondary"),
		)
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListCustomStateConditions", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			args          river.NamedArgs
			name          string
			sql           string
			wantAvailable bool
			wantCompleted bool
		}{
			{nil, "ContradictoryFinalizedAt", "finalized_at IS NULL", false, false},
			{river.NamedArgs{"other_state": "available"}, "ContradictoryState", "state = @other_state", false, false},
			{river.NamedArgs{"other_state": "completed"}, "GroupedOr", "(state = @other_state OR finalized_at IS NULL)", false, true},
			{nil, "UngroupedOr", "false OR finalized_at IS NULL", true, false},
			{river.NamedArgs{"other_state": "completed"}, "UngroupedOrWithNamedArgument", "state = @other_state OR finalized_at IS NULL", true, true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				client, bundle := setup(t)
				available := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})
				completed := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
					FinalizedAt: new(time.Now().UTC().Truncate(time.Second)), Schema: bundle.schema, State: new(rivertype.JobStateCompleted),
				})
				params := river.NewJobListParams().States(rivertype.JobStateCompleted).
					OrderBy(river.JobListOrderByTime, river.SortOrderDesc).Where(tt.sql, tt.args)
				result, err := client.JobList(ctx, params)
				require.NoError(t, err)
				gotIDs := make([]int64, 0, len(result.Jobs))
				var wantIDs []int64
				for _, job := range result.Jobs {
					gotIDs = append(gotIDs, job.ID)
				}
				if tt.wantAvailable {
					wantIDs = append(wantIDs, available.ID)
				}
				if tt.wantCompleted {
					wantIDs = append(wantIDs, completed.ID)
				}
				require.ElementsMatch(t, wantIDs, gotIDs)
			})
		}
	})

	t.Run("JobListCustomStatePagination", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)
		if bundle.driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			t.Skip("uses PostgreSQL array and JSON containment syntax")
		}
		now := time.Now().UTC().Truncate(time.Second)
		wantIDs := make([]int64, 0, 3)
		for range 3 {
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
				FinalizedAt: &now, Metadata: []byte(`{"selected":true}`), Schema: bundle.schema, State: new(rivertype.JobStateCompleted),
			})
			wantIDs = append(wantIDs, job.ID)
		}
		slices.Reverse(wantIDs)
		params := river.NewJobListParams().States(rivertype.JobStateCompleted).
			OrderBy(river.JobListOrderByTime, river.SortOrderDesc).First(1).
			Where("(state = ANY(@state) OR finalized_at IS NULL)").
			Where("id > @minimum_id", river.NamedArgs{"minimum_id": 0}).Metadata(`{"selected":true}`)
		var gotIDs []int64
		pageParams := params
		for page := range 4 {
			result, err := client.JobList(ctx, pageParams)
			require.NoError(t, err)
			if page == 3 {
				require.Empty(t, result.Jobs)
				break
			}
			require.Len(t, result.Jobs, 1)
			gotIDs = append(gotIDs, result.Jobs[0].ID)
			pageParams = params.After(result.LastCursor)
		}
		require.Equal(t, wantIDs, gotIDs)
	})

	t.Run("JobListFinalized", func(t *testing.T) {
		t.Parallel()

		type testBundle struct {
			exec   riverdriver.Executor
			jobs   map[rivertype.JobState][]*rivertype.JobRow
			now    time.Time
			schema string
		}

		setup := func(t *testing.T) (*river.Client[TTx], *testBundle) {
			t.Helper()

			client, bundle := setup(t)
			now := time.Date(2026, 9, 9, 12, 0, 0, 123000000, time.UTC)
			jobs := make(map[rivertype.JobState][]*rivertype.JobRow)

			// IDs and timestamps deliberately disagree. Each timestamp has three
			// jobs, so a two-job page ends partway through a group of equal times.
			for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
				for _, offset := range []time.Duration{time.Second, 0, time.Second, 0, time.Second, 0} {
					job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
						FinalizedAt: new(now.Add(offset)),
						Kind:        new("selected"),
						Priority:    new(2),
						Queue:       new("selected"),
						Schema:      bundle.schema,
						State:       new(state),
						Tags:        []string{"alpha", "beta"},
					})
					jobs[state] = append(jobs[state], job)
				}
			}

			return client, &testBundle{
				exec:   bundle.exec,
				jobs:   jobs,
				now:    now,
				schema: bundle.schema,
			}
		}

		t.Run("Filters", func(t *testing.T) {
			t.Parallel()

			for _, tt := range []struct {
				includeID bool
				name      string
				optsFunc  func(*testfactory.JobOpts)
			}{
				{false, "IDs", func(opts *testfactory.JobOpts) {}},
				{true, "Kinds", func(opts *testfactory.JobOpts) { opts.Kind = new("other") }},
				{true, "Priorities", func(opts *testfactory.JobOpts) { opts.Priority = new(3) }},
				{true, "Queues", func(opts *testfactory.JobOpts) { opts.Queue = new("other") }},
				{true, "States", func(opts *testfactory.JobOpts) { opts.State = new(rivertype.JobStateDiscarded) }},
				{true, "TagsAll", func(opts *testfactory.JobOpts) { opts.Tags = []string{"beta"} }},
				{true, "TagsAny", func(opts *testfactory.JobOpts) { opts.Tags = []string{"alpha"} }},
			} {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					client, bundle := setup(t)

					// All filters match the completed jobs. The extra job fails only
					// the filter named by this case, so no other filter can hide it.
					opts := &testfactory.JobOpts{
						FinalizedAt: &bundle.now,
						Kind:        new("selected"),
						Priority:    new(2),
						Queue:       new("selected"),
						Schema:      bundle.schema,
						State:       new(rivertype.JobStateCompleted),
						Tags:        []string{"alpha", "beta"},
					}
					tt.optsFunc(opts)
					excludedJob := testfactory.Job(ctx, t, bundle.exec, opts)
					wantIDs := sliceutil.Map(bundle.jobs[rivertype.JobStateCompleted], func(job *rivertype.JobRow) int64 { return job.ID })
					filterIDs := slices.Clone(wantIDs)
					if tt.includeID {
						filterIDs = append(filterIDs, excludedJob.ID)
					}

					listRes, err := client.JobList(ctx, river.NewJobListParams().
						IDs(filterIDs...).Kinds("selected").Priorities(2).Queues("selected").
						States(rivertype.JobStateCompleted).TagsAll("alpha").TagsAny("beta", "gamma").
						OrderBy(river.JobListOrderByTime, river.SortOrderDesc))
					require.NoError(t, err)
					require.ElementsMatch(t, wantIDs, sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
				})
			}
		})

		t.Run("Ordering", func(t *testing.T) {
			t.Parallel()

			for _, tt := range []struct {
				name   string
				params *river.JobListParams
				want   []int
			}{
				{"FinalizedAtAsc", river.NewJobListParams().OrderBy(river.JobListOrderByFinalizedAt, river.SortOrderAsc), []int{1, 3}},
				{"FinalizedAtDesc", river.NewJobListParams().OrderBy(river.JobListOrderByFinalizedAt, river.SortOrderDesc), []int{4, 2}},
				{"TimeAsc", river.NewJobListParams().OrderBy(river.JobListOrderByTime, river.SortOrderAsc), []int{1, 3}},
				{"TimeDesc", river.NewJobListParams().OrderBy(river.JobListOrderByTime, river.SortOrderDesc), []int{4, 2}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					client, bundle := setup(t)

					for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
						jobs := bundle.jobs[state]
						listRes, err := client.JobList(ctx, tt.params.States(state).First(2))
						require.NoError(t, err)
						require.Equal(t, []int64{jobs[tt.want[0]].ID, jobs[tt.want[1]].ID},
							sliceutil.Map(listRes.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }), "state: %s", state)
					}
				})
			}
		})

		t.Run("Pagination", func(t *testing.T) {
			t.Parallel()

			for _, tt := range []struct {
				name   string
				params *river.JobListParams
				want   []int
			}{
				{"FinalizedAtAsc", river.NewJobListParams().OrderBy(river.JobListOrderByFinalizedAt, river.SortOrderAsc), []int{1, 3, 5, 0, 2, 4}},
				{"FinalizedAtDesc", river.NewJobListParams().OrderBy(river.JobListOrderByFinalizedAt, river.SortOrderDesc), []int{4, 2, 0, 5, 3, 1}},
				{"TimeAsc", river.NewJobListParams().OrderBy(river.JobListOrderByTime, river.SortOrderAsc), []int{1, 3, 5, 0, 2, 4}},
				{"TimeDesc", river.NewJobListParams().OrderBy(river.JobListOrderByTime, river.SortOrderDesc), []int{4, 2, 0, 5, 3, 1}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					client, bundle := setup(t)

					for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
						params := tt.params.States(state).First(2)
						jobs := bundle.jobs[state]
						wantIDs := sliceutil.Map(tt.want, func(index int) int64 { return jobs[index].ID })

						firstPage, err := client.JobList(ctx, params)
						require.NoError(t, err)
						require.Equal(t, wantIDs[:2], sliceutil.Map(firstPage.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }), "state: %s", state)

						// Resume in the middle of a timestamp group with a serialized cursor.
						encoded, err := firstPage.LastCursor.MarshalText()
						require.NoError(t, err)
						var cursor river.JobListCursor
						require.NoError(t, cursor.UnmarshalText(encoded))
						secondPage, err := client.JobList(ctx, params.After(&cursor))
						require.NoError(t, err)
						require.Equal(t, wantIDs[2:4], sliceutil.Map(secondPage.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }), "state: %s", state)

						// The next boundary splits the other timestamp group. Use a job-derived cursor.
						thirdPage, err := client.JobList(ctx, params.After(river.JobListCursorFromJob(secondPage.Jobs[1])))
						require.NoError(t, err)
						require.Equal(t, wantIDs[4:], sliceutil.Map(thirdPage.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }), "state: %s", state)

						emptyPage, err := client.JobList(ctx, params.After(thirdPage.LastCursor))
						require.NoError(t, err)
						require.Empty(t, emptyPage.Jobs)
					}
				})
			}
		})
	})

	t.Run("JobListMetadata", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			Metadata: []byte(`{"foo":"bar","bar":"baz"}`),
			Schema:   bundle.schema,
		})

		listRes, err := client.JobList(ctx, river.NewJobListParams().Metadata(`{"foo":"bar"}`))
		if bundle.driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			t.Logf("Ignoring unsupported JobListResult.Metadata on SQLite")
			require.EqualError(t, err, "JobListParams.Metadata is not supported on SQLite")
			return
		}
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, job.ID, listRes.Jobs[0].ID)
	})

	t.Run("JobListScheduledPagination", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name  string
			order river.SortOrder
		}{
			{"Ascending", river.SortOrderAsc},
			{"Descending", river.SortOrderDesc},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				client, bundle := setup(t)

				// The time comparison must recognize equality so the ID cursor
				// can advance through available jobs sharing a scheduled time.
				now := time.Date(2026, 9, 9, 12, 0, 0, 123000000, time.UTC)
				opts := &testfactory.JobOpts{Kind: new("selected"), ScheduledAt: &now, Schema: bundle.schema}
				job1 := testfactory.Job(ctx, t, bundle.exec, opts)
				job2 := testfactory.Job(ctx, t, bundle.exec, opts)
				wantIDs := []int64{job1.ID, job2.ID}
				if tt.order == river.SortOrderDesc {
					slices.Reverse(wantIDs)
				}
				params := river.NewJobListParams().States(rivertype.JobStateAvailable).
					OrderBy(river.JobListOrderByScheduledAt, tt.order).First(1).
					Where("kind = @kind_name", river.NamedArgs{"kind_name": "selected"})

				firstPage, err := client.JobList(ctx, params)
				require.NoError(t, err)
				require.Equal(t, wantIDs[:1], sliceutil.Map(firstPage.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
				secondPage, err := client.JobList(ctx, params.After(firstPage.LastCursor))
				require.NoError(t, err)
				require.Equal(t, wantIDs[1:], sliceutil.Map(secondPage.Jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
				emptyPage, err := client.JobList(ctx, params.After(secondPage.LastCursor))
				require.NoError(t, err)
				require.Empty(t, emptyPage.Jobs)
			})
		}
	})

	t.Run("JobListStateFilters", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name           string
			params         *river.JobListParams
			wantJobIndexes []int
		}{
			{"Default", river.NewJobListParams(), []int{0, 1, 2, 3}},
			{"ExplicitEmpty", river.NewJobListParams().States(), []int{0, 1, 2, 3}},
			{"FinalizedDefaults", river.NewJobListParams().OrderBy(river.JobListOrderByFinalizedAt, river.SortOrderDesc), []int{1, 2, 3}},
			{"Mixed", river.NewJobListParams().States(rivertype.JobStateCompleted, rivertype.JobStateAvailable).OrderBy(river.JobListOrderByTime, river.SortOrderDesc), []int{0, 2}},
			{"NonFinalized", river.NewJobListParams().States(rivertype.JobStateAvailable).OrderBy(river.JobListOrderByTime, river.SortOrderDesc), []int{0}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				client, bundle := setup(t)

				now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
				allIDs := make([]int64, 0, 4)
				for _, state := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded} {
					opts := &testfactory.JobOpts{Schema: bundle.schema, State: new(state)}
					if state != rivertype.JobStateAvailable {
						opts.FinalizedAt = &now
					}
					job := testfactory.Job(ctx, t, bundle.exec, opts)
					allIDs = append(allIDs, job.ID)
				}
				wantIDs := make([]int64, 0, len(tt.wantJobIndexes))
				for _, index := range tt.wantJobIndexes {
					wantIDs = append(wantIDs, allIDs[index])
				}

				result, err := client.JobList(ctx, tt.params)
				require.NoError(t, err)
				gotIDs := make([]int64, 0, len(result.Jobs))
				for _, job := range result.Jobs {
					gotIDs = append(gotIDs, job.ID)
				}
				require.ElementsMatch(t, wantIDs, gotIDs)
			})
		}
	})

	t.Run("JobListTags", func(t *testing.T) {
		t.Parallel()

		client, bundle := setup(t)

		jobAlphaBeta := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Tags: []string{"alpha", "beta", "shared"}})
		jobAlpha := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Tags: []string{"alpha"}})
		jobBeta := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Tags: []string{"beta"}})
		jobUpperAlpha := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Tags: []string{"ALPHA"}})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema, Tags: []string{"gamma"}})
		_ = testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Schema: bundle.schema})

		listRes, err := client.JobList(ctx, river.NewJobListParams().TagsAny("alpha", "beta"))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 3)
		require.Equal(t, jobAlphaBeta.ID, listRes.Jobs[0].ID)
		require.Equal(t, jobAlpha.ID, listRes.Jobs[1].ID)
		require.Equal(t, jobBeta.ID, listRes.Jobs[2].ID)

		listRes, err = client.JobList(ctx, river.NewJobListParams().TagsAll("alpha", "beta"))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, jobAlphaBeta.ID, listRes.Jobs[0].ID)

		listRes, err = client.JobList(ctx, river.NewJobListParams().TagsAll("shared").TagsAny("alpha", "gamma"))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, jobAlphaBeta.ID, listRes.Jobs[0].ID)

		listRes, err = client.JobList(ctx, river.NewJobListParams().TagsAny("ALPHA"))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, jobUpperAlpha.ID, listRes.Jobs[0].ID)

		params := river.NewJobListParams().TagsAny("alpha", "beta").First(1)
		listRes, err = client.JobList(ctx, params)
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, jobAlphaBeta.ID, listRes.Jobs[0].ID)

		listRes, err = client.JobList(ctx, params.After(listRes.LastCursor))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 1)
		require.Equal(t, jobAlpha.ID, listRes.Jobs[0].ID)

		listRes, err = client.JobList(ctx, river.NewJobListParams().TagsAny("alpha").TagsAny())
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, 6)
	})

	t.Run("JobListTimeArguments", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name          string
			valueFunc     func(time.Time) any
			wantFinalized bool
		}{
			{"NilTimePointer", func(time.Time) any { return (*time.Time)(nil) }, false},
			{"Time", func(value time.Time) any { return value }, true},
			{"TimePointer", func(value time.Time) any { return &value }, true},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				_, bundle := setup(t)

				now := time.Date(2026, 9, 9, 12, 0, 0, 123000000, time.UTC)
				available := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{Kind: new("selected"), Schema: bundle.schema})
				completed := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
					FinalizedAt: &now, Kind: new("selected"), Schema: bundle.schema, State: new(rivertype.JobStateCompleted),
				})
				wantID := available.ID
				if tt.wantFinalized {
					wantID = completed.ID
				}

				// Match the same instant in another zone, including milliseconds.
				// Use the driver directly to verify it leaves reusable arguments intact.
				arg := tt.valueFunc(now.In(time.FixedZone("test", -7*60*60)))
				params := &riverdriver.JobListParams{
					Max:           100,
					NamedArgs:     map[string]any{"kind": "selected", "time": arg},
					OrderByClause: "id ASC",
					Schema:        bundle.schema,
					WhereClause:   "kind = @kind AND (finalized_at = @time OR (finalized_at IS NULL AND @time IS NULL))",
				}
				for range 2 {
					jobs, err := bundle.exec.JobList(ctx, params)
					require.NoError(t, err)
					require.Equal(t, []int64{wantID}, sliceutil.Map(jobs, func(job *rivertype.JobRow) int64 { return job.ID }))
				}
				require.Equal(t, map[string]any{"kind": "selected", "time": arg}, params.NamedArgs)
			})
		}
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

		job := testfactory.Job(ctx, t, execTx, &testfactory.JobOpts{
			Schema: bundle.schema,
			Tags:   []string{"all-args-tag", "all-args-secondary"},
		})

		listRes, err := client.JobListTx(ctx, tx,
			river.NewJobListParams().
				IDs(job.ID).
				Kinds(job.Kind).
				Priorities(int16(min(job.Priority, math.MaxInt16))). //nolint:gosec
				Queues(job.Queue).
				States(job.State).
				TagsAll("all-args-tag").
				TagsAny("all-args-secondary"),
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
		if bundle.driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
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

		if bundle.driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
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

	t.Run("LeaderElectionDisabled", func(t *testing.T) {
		t.Parallel()

		for _, testCase := range []struct {
			name     string
			pollOnly bool
		}{
			{name: "Default"},
			{name: "PollOnly", pollOnly: true},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				config, bundle := setupConfig(t)
				config.LeaderElectionDisabled = true
				config.PollOnly = testCase.pollOnly

				client, err := river.NewClient(bundle.driver, config)
				require.NoError(t, err)

				// Exercise restart as well as initial startup, including shutdown
				// without a queue maintainer or a leadership lease to resign.
				for range 2 {
					subscribeChan := subscribe(t, client)
					startClient(ctx, t, client)

					insertRes, err := client.Insert(ctx, noOpArgs{}, nil)
					require.NoError(t, err)
					event := riversharedtest.WaitOrTimeout(t, subscribeChan)
					require.Equal(t, river.EventKindJobCompleted, event.Kind)
					require.Equal(t, insertRes.Job.ID, event.Job.ID)

					_, err = bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{Schema: bundle.schema})
					require.ErrorIs(t, err, rivertype.ErrNotFound)

					stopCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
					err = client.Stop(stopCtx)
					cancelFunc()
					require.NoError(t, err)
				}
			})
		}
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
