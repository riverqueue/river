package rivertest_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverschematest"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
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

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &FirstRequiredWorker{})
	river.AddWorker(workers, &SecondRequiredWorker{})

	schema := riverschematest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger:  slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Schema:  schema, // only necessary for the example test
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

	// This is needed because rivertest does not yet support an injected schema.
	if _, err := tx.Exec(ctx, "SET search_path TO "+schema); err != nil {
		panic(err)
	}

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

	// Due to some refactoring to make schemas injectable, we don't yet have a
	// way of injecting a schema at the pool level. The rivertest API will need
	// to be expanded to allow it.
	/*
		// Insert and verify one on a pool instead of transaction.
		_, err = riverClient.Insert(ctx, &FirstRequiredArgs{Message: "Hello from pool."}, nil)
		if err != nil {
			panic(err)
		}
		_ = rivertest.RequireManyInserted(ctx, t, riverpgxv5.New(dbPool), []rivertest.ExpectedJob{
			{Args: &FirstRequiredArgs{}},
		})
	*/

	// Output:
	// Job 0 args: {"message": "Hello from first."}
	// Job 1 args: {"message": "Hello from second."}
	// Job 2 args: {"message": "Hello from first (again)."}
}
