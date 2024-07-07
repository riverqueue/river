package rivertest_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/rivershared/util/slogutil"
)

type RequiredArgs struct {
	Message string `json:"message"`
}

func (RequiredArgs) Kind() string { return "required" }

type RequiredWorker struct {
	river.WorkerDefaults[RequiredArgs]
}

func (w *RequiredWorker) Work(ctx context.Context, job *river.Job[RequiredArgs]) error { return nil }

// Example_requireInserted demonstrates the use of the RequireInserted test
// assertion, which verifies that a single job was inserted.
func Example_requireInserted() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_testdb_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Required for the purpose of this test, but not necessary in real usage.
	if err := riverinternaltest.TruncateRiverTables(ctx, dbPool); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &RequiredWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger:  slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	_, err = riverClient.InsertTx(ctx, tx, &RequiredArgs{
		Message: "Hello.",
	}, nil)
	if err != nil {
		panic(err)
	}

	// Required for purposes of our example here, but in reality t will be the
	// *testing.T that comes from a test's argument.
	t := &testing.T{}

	job := rivertest.RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &RequiredArgs{}, nil)
	fmt.Printf("Test passed with message: %s\n", job.Args.Message)

	// Verify the same job again, and this time that it was inserted at the
	// default priority and default queue.
	_ = rivertest.RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &RequiredArgs{}, &rivertest.RequireInsertedOpts{
		Priority: 1,
		Queue:    river.QueueDefault,
	})

	// Insert and verify one on a pool instead of transaction.
	_, err = riverClient.Insert(ctx, &RequiredArgs{Message: "Hello from pool."}, nil)
	if err != nil {
		panic(err)
	}
	_ = rivertest.RequireInserted(ctx, t, riverpgxv5.New(dbPool), &RequiredArgs{}, nil)

	// Output:
	// Test passed with message: Hello.
}
