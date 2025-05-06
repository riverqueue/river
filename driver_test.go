package river_test

import (
	"context"
	"database/sql"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest/riverdrivertest"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/urlutil"
	"github.com/riverqueue/river/rivertype"
)

func TestDriverDatabaseSQLLibPQ(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	connector, err := pq.NewConnector(urlutil.DatabaseSQLCompatibleURL(riversharedtest.TestDatabaseURL()))
	require.NoError(t, err)

	stdPool := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	driver := riverdatabasesql.New(stdPool)

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
		func(ctx context.Context, t *testing.T) riverdriver.Executor {
			t.Helper()

			tx := riverdbtest.TestTx(ctx, t, driver, nil)

			// TODO(brandur): Set `search_path` path here when SQLite changes come in.

			return riverdatabasesql.New(nil).UnwrapExecutor(tx)
		})
}

func TestDriverDatabaseSQLPgx(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		dbPool  = riversharedtest.DBPool(ctx, t) // TODO
		stdPool = stdlib.OpenDBFromPool(dbPool)
		driver  = riverdatabasesql.New(stdPool)
	)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	// Make sure to use the same schema as Pgx test transactions use or else
	// operations without a schema included will clash with each other due to
	// different `river_state` types with the error "ERROR: cached plan must not
	// change result type (SQLSTATE 0A000)".
	//
	// Alternatively, we could switch dbPool to use a DBPoolClone so it's got a
	// different statement cache.
	var testTxSchema string
	tx := riverdbtest.TestTxPgx(ctx, t)
	require.NoError(t, tx.QueryRow(ctx, "SELECT current_schema()").Scan(&testTxSchema))

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[*sql.Tx], string) {
			t.Helper()

			return driver, riverdbtest.TestSchema(ctx, t, driver, nil)
		},
		func(ctx context.Context, t *testing.T) riverdriver.Executor {
			t.Helper()

			tx, err := stdPool.BeginTx(ctx, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			// The same thing as the built-in riversharedtest.TestTx does.
			_, err = tx.ExecContext(ctx, "SET search_path TO '"+testTxSchema+"'")
			require.NoError(t, err)

			return riverdatabasesql.New(nil).UnwrapExecutor(tx)
		})
}

func TestDriverRiverPgxV5(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T) (riverdriver.Driver[pgx.Tx], string) {
			t.Helper()

			var (
				dbPool = riversharedtest.DBPool(ctx, t)
				driver = riverpgxv5.New(dbPool)
				schema = riverdbtest.TestSchema(ctx, t, driver, nil)
			)

			return driver, schema
		},
		func(ctx context.Context, t *testing.T) riverdriver.Executor {
			t.Helper()

			tx := riverdbtest.TestTxPgx(ctx, t)
			return riverpgxv5.New(nil).UnwrapExecutor(tx)
		})
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
				MaxAttempts: rivercommon.MaxAttemptsDefault,
				Metadata:    []byte(`{}`),
				Priority:    rivercommon.PriorityDefault,
				Queue:       rivercommon.QueueDefault,
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
				ClientID: clientID,
				Max:      100,
				Queue:    rivercommon.QueueDefault,
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
					ClientID: clientID,
					Max:      100,
					Queue:    rivercommon.QueueDefault,
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
					MaxAttempts: rivercommon.MaxAttemptsDefault,
					Priority:    rivercommon.PriorityDefault,
					Queue:       rivercommon.QueueDefault,
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
					MaxAttempts:  rivercommon.MaxAttemptsDefault,
					Priority:     rivercommon.PriorityDefault,
					Queue:        rivercommon.QueueDefault,
					State:        rivertype.JobStateAvailable,
					UniqueKey:    []byte("test_unique_key_" + strconv.Itoa(i)),
					UniqueStates: 0xFB,
				}},
			})
			require.NoError(b, err)
		}
	})
}
