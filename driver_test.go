package river_test

import (
	"context"
	"database/sql"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/riverdrivertest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func TestDriverDatabaseSQL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dbPool := riverinternaltest.TestDB(ctx, t)

	stdPool := stdlib.OpenDBFromPool(dbPool)
	t.Cleanup(func() { require.NoError(t, stdPool.Close()) })

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T) riverdriver.Driver[*sql.Tx] {
			t.Helper()

			return riverdatabasesql.New(stdPool)
		},
		func(ctx context.Context, t *testing.T) riverdriver.Executor {
			t.Helper()

			tx, err := stdPool.BeginTx(ctx, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			return riverdatabasesql.New(nil).UnwrapExecutor(tx)
		})
}

func TestDriverRiverPgxV5(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverdrivertest.Exercise(ctx, t,
		func(ctx context.Context, t *testing.T) riverdriver.Driver[pgx.Tx] {
			t.Helper()

			dbPool := riverinternaltest.TestDB(ctx, t)
			return riverpgxv5.New(dbPool)
		},
		func(ctx context.Context, t *testing.T) riverdriver.Executor {
			t.Helper()

			tx := riverinternaltest.TestTx(ctx, t)
			return riverpgxv5.New(nil).UnwrapExecutor(tx)
		})
}

func BenchmarkDriverRiverPgxV5_Executor(b *testing.B) {
	const (
		clientID = "test-client-id"
		timeout  = 5 * time.Minute
	)

	ctx := context.Background()

	type testBundle struct{}

	setupPool := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		driver := riverpgxv5.New(riverinternaltest.TestDB(ctx, b))

		b.ResetTimer()

		return driver.GetExecutor(), &testBundle{}
	}

	setupTx := func(b *testing.B) (riverdriver.Executor, *testBundle) {
		b.Helper()

		driver := riverpgxv5.New(nil)
		tx := riverinternaltest.TestTx(ctx, b)

		b.ResetTimer()

		return driver.UnwrapExecutor(tx), &testBundle{}
	}

	makeInsertParams := func() *riverdriver.JobInsertFastParams {
		return &riverdriver.JobInsertFastParams{
			EncodedArgs: []byte(`{}`),
			Kind:        "fake_job",
			MaxAttempts: rivercommon.MaxAttemptsDefault,
			Metadata:    []byte(`{}`),
			Priority:    rivercommon.PriorityDefault,
			Queue:       rivercommon.QueueDefault,
			ScheduledAt: nil,
			State:       rivertype.JobStateAvailable,
		}
	}

	b.Run("JobInsert_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, _ := setupTx(b)

		for i := 0; i < b.N; i++ {
			if _, err := exec.JobInsertFast(ctx, makeInsertParams()); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobInsert_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, _ := setupPool(b)

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := exec.JobInsertFast(ctx, makeInsertParams()); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	})

	b.Run("JobGetAvailable_100_Sequential", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, _ := setupTx(b)

		for i := 0; i < b.N*100; i++ {
			if _, err := exec.JobInsertFast(ctx, makeInsertParams()); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				AttemptedBy: clientID,
				Max:         100,
				Queue:       rivercommon.QueueDefault,
			}); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JobGetAvailable_100_Parallel", func(b *testing.B) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		exec, _ := setupPool(b)

		for i := 0; i < b.N*100*runtime.NumCPU(); i++ {
			if _, err := exec.JobInsertFast(ctx, makeInsertParams()); err != nil {
				b.Fatal(err)
			}
		}

		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
					AttemptedBy: clientID,
					Max:         100,
					Queue:       rivercommon.QueueDefault,
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
