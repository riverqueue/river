package rivertest

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

type testArgsB struct{}

func (testArgsB) Kind() string { return "rivertest_work_test_b" }

func TestExecutor_Work(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		config *river.Config
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
		tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)

		workers := river.NewWorkers()
		config := &river.Config{
			ID:       "rivertest-executor",
			Schema:   schema,
			TestOnly: true,
			Workers:  workers,
		}

		return &testBundle{
			config: config,
			driver: driver,
			tx:     tx,
		}
	}

	t.Run("DispatchesByKind", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var (
			ranA bool
			ranB bool
		)

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			ranA = true
			return nil
		}))
		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[testArgsB]) error {
			ranB = true
			return nil
		}))

		executor := NewExecutorOpts(t, &ExecutorOpts[pgx.Tx]{
			Config: bundle.config,
			Driver: bundle.driver,
		})

		_, err := executor.Work(ctx, t, bundle.tx, testArgs{Value: "first"}, nil)
		require.NoError(t, err)

		_, err = executor.Work(ctx, t, bundle.tx, testArgsB{}, nil)
		require.NoError(t, err)

		require.True(t, ranA)
		require.True(t, ranB)
	})
}

func TestExecutor_AfterWork(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		config *river.Config
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
		tx, schema := riverdbtest.TestTxPgxDriver(ctx, t, driver, nil)

		workers := river.NewWorkers()
		config := &river.Config{
			ID:       "rivertest-executor-hooks",
			Schema:   schema,
			TestOnly: true,
			Workers:  workers,
		}

		return &testBundle{
			config: config,
			driver: driver,
			tx:     tx,
		}
	}

	t.Run("InvokesAfterWork", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		var afterWorkCalled bool

		river.AddWorker(bundle.config.Workers, river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		}))

		executor := NewExecutorOpts(t, &ExecutorOpts[pgx.Tx]{
			AfterWork: func(ctx context.Context, tx pgx.Tx, result *WorkResult) error {
				afterWorkCalled = true
				return nil
			},
			Config: bundle.config,
			Driver: bundle.driver,
		})

		_, err := executor.Work(ctx, t, bundle.tx, testArgs{Value: "test"}, nil)
		require.NoError(t, err)
		require.True(t, afterWorkCalled)
	})
}
