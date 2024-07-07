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
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertest"
)

type FirstRequiredArgs struct {
	Message string `json:"message"`
}

func (FirstRequiredArgs) Kind() string { return "first_required" }

type FirstRequiredWorker struct {
	river.WorkerDefaults[FirstRequiredArgs]
}

func (w *FirstRequiredWorker) Work(ctx context.Context, job *river.Job[FirstRequiredArgs]) error {
	return nil
}

type SecondRequiredArgs struct {
	Message string `json:"message"`
}

func (SecondRequiredArgs) Kind() string { return "second_required" }

type SecondRequiredWorker struct {
	river.WorkerDefaults[SecondRequiredArgs]
}

func (w *SecondRequiredWorker) Work(ctx context.Context, job *river.Job[SecondRequiredArgs]) error {
	return nil
}

// Example_requireManyInserted demonstrates the use of the RequireManyInserted test
// assertion, which requires that multiple jobs of the specified kinds were
// inserted.
func Example_requireManyInserted() {
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
	river.AddWorker(workers, &FirstRequiredWorker{})
	river.AddWorker(workers, &SecondRequiredWorker{})

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

	_, err = riverClient.InsertTx(ctx, tx, &FirstRequiredArgs{Message: "Hello from first."}, nil)
	if err != nil {
		panic(err)
	}

	_, err = riverClient.InsertTx(ctx, tx, &SecondRequiredArgs{Message: "Hello from second."}, nil)
	if err != nil {
		panic(err)
	}

	_, err = riverClient.InsertTx(ctx, tx, &FirstRequiredArgs{Message: "Hello from first (again)."}, nil)
	if err != nil {
		panic(err)
	}

	// Required for purposes of our example here, but in reality t will be the
	// *testing.T that comes from a test's argument.
	t := &testing.T{}

	jobs := rivertest.RequireManyInsertedTx[*riverpgxv5.Driver](ctx, t, tx, []rivertest.ExpectedJob{
		{Args: &FirstRequiredArgs{}},
		{Args: &SecondRequiredArgs{}},
		{Args: &FirstRequiredArgs{}},
	})
	for i, job := range jobs {
		fmt.Printf("Job %d args: %s\n", i, string(job.EncodedArgs))
	}

	// Verify again, and this time that the second job was inserted at the
	// default priority and default queue.
	_ = rivertest.RequireManyInsertedTx[*riverpgxv5.Driver](ctx, t, tx, []rivertest.ExpectedJob{
		{Args: &SecondRequiredArgs{}, Opts: &rivertest.RequireInsertedOpts{
			Priority: 1,
			Queue:    river.QueueDefault,
		}},
	})

	// Insert and verify one on a pool instead of transaction.
	_, err = riverClient.Insert(ctx, &FirstRequiredArgs{Message: "Hello from pool."}, nil)
	if err != nil {
		panic(err)
	}
	_ = rivertest.RequireManyInserted(ctx, t, riverpgxv5.New(dbPool), []rivertest.ExpectedJob{
		{Args: &FirstRequiredArgs{}},
	})

	// Output:
	// Job 0 args: {"message": "Hello from first."}
	// Job 1 args: {"message": "Hello from second."}
	// Job 2 args: {"message": "Hello from first (again)."}
}
