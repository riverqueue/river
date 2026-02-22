package riverdrivertest_test

import (
	"context"
	"database/sql"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverdrivertest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/urlutil"
	"github.com/riverqueue/river/rivertype"
)

func TestDriverRiverDatabaseSQLLibPQ(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(t, err)

	stdPool := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			tx, schema := riverdbtest.TestTx(ctx, t, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)

			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverDatabaseSQLPgx(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			tx, schema := riverdbtest.TestTx(ctx, t, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(t, err)

			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverPgxV5(t *testing.T) {
	t.Parallel()

	// Default/primary pgx path with prepared statement caching.
	t.Run("DefaultMode", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeCacheStatement)
	})

	// PgBouncer transaction-pooling compatibility path (simple protocol).
	t.Run("SimpleProtocol", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeSimpleProtocol)
	})

	// Text-parameter execution path without prepared statement caching.
	t.Run("ExecMode", func(t *testing.T) {
		t.Parallel()

		exerciseDriverRiverPgxV5WithMode(t, pgx.QueryExecModeExec)
	})
}

func exerciseDriverRiverPgxV5WithMode(t *testing.T, mode pgx.QueryExecMode) {
	t.Helper()

	var (
		ctx    = context.Background()
		dbPool = dbPoolWithExecMode(ctx, t, mode)
		driver = riverpgxv5.New(dbPool)
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[pgx.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, opts)
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[pgx.Tx]) {
			t.Helper()

			tx, _ := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)
			return driver.UnwrapExecutor(tx), driver
		})
}

func dbPoolWithExecMode(ctx context.Context, t *testing.T, mode pgx.QueryExecMode) *pgxpool.Pool {
	t.Helper()

	config := riversharedtest.DBPool(ctx, t).Config()
	config.ConnConfig.DefaultQueryExecMode = mode

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(dbPool.Close)

	return dbPool
}

func TestDriverRiverLiteLibSQL(t *testing.T) { //nolint:dupl
	t.Parallel()

	var (
		ctx         = context.Background()
		procurePool = func(ctx context.Context, schema string) (any, string) {
			return riversharedtest.DBPoolLibSQL(ctx, t, schema), "" // could also be `main` instead of empty string
		}
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			if opts == nil {
				opts = &riverdbtest.TestSchemaOpts{}
			}
			opts.ProcurePool = procurePool

			var (
				// Driver will have its pool set by TestSchema.
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, opts)
			)
			return driver, schema
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			// Driver will have its pool set by TestSchema.
			driver := riversqlite.New(nil)

			tx, _ := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
				// Unfortunately, the normal test transaction schema sharing has
				// to be disabled for SQLite. When enabled, there's too much
				// contention on the shared test databases and operations fail
				// with `database is locked (5) (SQLITE_BUSY)`, which is a
				// common concurrency error in SQLite whose recommended
				// remediation is a backoff and retry. I tried various
				// techniques like journal_mode=WAL, but it didn't seem to help
				// enough. SQLite databases are just local files anyway, and
				// test transactions can still reuse schemas freed by other
				// tests through TestSchema, so this should be okay performance
				// wise.
				DisableSchemaSharing: true,

				ProcurePool: procurePool,
			})
			return driver.UnwrapExecutor(tx), driver
		})
}

func TestDriverRiverSQLiteModernC(t *testing.T) { //nolint:dupl
	t.Parallel()

	var (
		ctx         = context.Background()
		procurePool = func(ctx context.Context, schema string) (any, string) {
			return riversharedtest.DBPoolSQLite(ctx, t, schema), "" // could also be `main` instead of empty string
		}
	)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			if opts == nil {
				opts = &riverdbtest.TestSchemaOpts{}
			}
			opts.ProcurePool = procurePool

			var (
				// Driver will have its pool set by TestSchema.
				driver = riversqlite.New(nil)
				schema = riverdbtest.TestSchema(ctx, t, driver, opts)
			)
			return driver, schema
		},
		func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[*sql.Tx]) {
			t.Helper()

			// Driver will have its pool set by TestSchema.
			driver := riversqlite.New(nil)

			tx, _ := riverdbtest.TestTx(ctx, t, driver, &riverdbtest.TestTxOpts{
				// Unfortunately, the normal test transaction schema sharing has
				// to be disabled for SQLite. When enabled, there's too much
				// contention on the shared test databases and operations fail
				// with `database is locked (5) (SQLITE_BUSY)`, which is a
				// common concurrency error in SQLite whose recommended
				// remediation is a backoff and retry. I tried various
				// techniques like journal_mode=WAL, but it didn't seem to help
				// enough. SQLite databases are just local files anyway, and
				// test transactions can still reuse schemas freed by other
				// tests through TestSchema, so this should be okay performance
				// wise.
				DisableSchemaSharing: true,

				ProcurePool: procurePool,
			})
			return driver.UnwrapExecutor(tx), driver
		})
}

func BenchmarkDriverRiverDatabaseSQLLibPQ(b *testing.B) {
	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(b, err)

	stdPool := sql.OpenDB(connector)
	b.Cleanup(func() { require.NoError(b, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[*sql.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()

			tx, schema := riverdbtest.TestTx(ctx, b, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(b, err)

			return driver.UnwrapExecutor(tx)
		},
	)
}

func BenchmarkDriverRiverDatabaseSQLPgx(b *testing.B) {
	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, b)
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
		schema  = riverdbtest.TestSchema(ctx, b, driver, nil)
	)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[*sql.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()

			tx, schema := riverdbtest.TestTx(ctx, b, driver, nil)

			// The same thing as the built-in riverdbtest.TestTxPgx does.
			_, err := tx.ExecContext(ctx, "SET search_path TO '"+schema+"'")
			require.NoError(b, err)

			return driver.UnwrapExecutor(tx)
		},
	)
}

func BenchmarkDriverRiverPgxV5(b *testing.B) {
	var (
		ctx    = context.Background()
		dbPool = riversharedtest.DBPool(ctx, b)
		driver = riverpgxv5.New(dbPool)
		schema = riverdbtest.TestSchema(ctx, b, driver, nil)
	)

	riverdrivertest.Benchmark(ctx, b,
		func(ctx context.Context, b *testing.B) (riverdriver.Driver[pgx.Tx], string) {
			b.Helper()

			return driver, schema
		},
		func(ctx context.Context, b *testing.B) riverdriver.Executor {
			b.Helper()
			return riverpgxv5.New(nil).UnwrapExecutor(riverdbtest.TestTxPgx(ctx, b))
		},
	)
}

func BenchmarkDriverRiverPgxV5_Executor(b *testing.B) {
	const (
		clientID = "test-client-id"
		timeout  = 5 * time.Minute
	)

	ctx := context.Background()

	type testBundle struct {
		schema string
	}

	setupPool := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		var (
			dbPool = riversharedtest.DBPool(ctx, b)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, b, driver, nil)
		)

		b.ResetTimer()

		return driver.GetExecutor(), &testBundle{
			schema: schema,
		}
	}

	setupTx := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		driver := riverpgxv5.New(nil)
		tx := riverdbtest.TestTxPgx(ctx, b)

		b.ResetTimer()

		return driver.UnwrapExecutor(tx), &testBundle{}
	}

	makeInsertParams := func(bundle *testBundle) *riverdriver.JobInsertFastManyParams {
		return &riverdriver.JobInsertFastManyParams{
			Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`),
				Kind:        "fake_job",
				MaxAttempts: river.MaxAttemptsDefault,
				Metadata:    []byte(`{}`),
				Priority:    river.PriorityDefault,
				Queue:       river.QueueDefault,
				ScheduledAt: nil,
				State:       rivertype.JobStateAvailable,
			}},
			Schema: bundle.schema,
		}
	}

	b.Run("JobInsert_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupTx(b)

		for range b.N {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobInsert_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupPool(b)

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})

	b.Run("JobGetAvailable_100_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupTx(b)

		for range b.N * 100 {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for range b.N {
			if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:  clientID,
				MaxToLock: 100,
				Queue:     river.QueueDefault,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobGetAvailable_100_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, bundle := setupPool(b)

		for range b.N * 100 * runtime.NumCPU() {
			if _, err := exec.JobInsertFastMany(ctx, makeInsertParams(bundle)); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
					ClientID:  clientID,
					MaxToLock: 100,
					Queue:     river.QueueDefault,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkDriverRiverPgxV5Insert(b *testing.B) {
	ctx := context.Background()

	type testBundle struct {
		exec riverdriver.Executor
		tx   pgx.Tx
	}

	setup := func(b *testing.B) (*riverpgxv5.Driver, *testBundle) {
		b.Helper()

		var (
			driver = riverpgxv5.New(nil)
			tx     = riverdbtest.TestTxPgx(ctx, b)
		)

		bundle := &testBundle{
			exec: driver.UnwrapExecutor(tx),
			tx:   tx,
		}

		return driver, bundle
	}

	b.Run("InsertFastMany", func(b *testing.B) {
		_, bundle := setup(b)

		for range b.N {
			_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{{
					EncodedArgs: []byte(`{"encoded": "args"}`),
					Kind:        "test_kind",
					MaxAttempts: river.MaxAttemptsDefault,
					Priority:    river.PriorityDefault,
					Queue:       river.QueueDefault,
					State:       rivertype.JobStateAvailable,
				}},
			})
			require.NoError(b, err)
		}
	})

	b.Run("InsertFastMany_WithUnique", func(b *testing.B) {
		_, bundle := setup(b)

		for i := range b.N {
			_, err := bundle.exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
				Jobs: []*riverdriver.JobInsertFastParams{{
					EncodedArgs:  []byte(`{"encoded": "args"}`),
					Kind:         "test_kind",
					MaxAttempts:  river.MaxAttemptsDefault,
					Priority:     river.PriorityDefault,
					Queue:        river.QueueDefault,
					State:        rivertype.JobStateAvailable,
					UniqueKey:    []byte("test_unique_key_" + strconv.Itoa(i)),
					UniqueStates: 0xFB,
				}},
			})
			require.NoError(b, err)
		}
	})
}
