package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/rivershared/util/slogutil"
)

type CancellingArgs struct {
	ShouldCancel bool
}

func (args CancellingArgs) Kind() string { return "Cancelling" }

type CancellingWorker struct {
	river.WorkerDefaults[CancellingArgs]
}

func (w *CancellingWorker) Work(ctx context.Context, job *river.Job[CancellingArgs]) error {
	if job.Args.ShouldCancel {
		fmt.Println("cancelling job")
		return river.JobCancel(errors.New("this wrapped error message will be persisted to DB"))
	}
	return nil
}

// Example_jobCancel demonstrates how to permanently cancel a job from within
// Work using JobCancel.
func Example_jobCancel() { //nolint:dupl
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
	river.AddWorker(workers, &CancellingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	// Not strictly needed, but used to help this test wait until job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCancelled)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}
	if _, err = riverClient.Insert(ctx, CancellingArgs{ShouldCancel: true}, nil); err != nil {
		panic(err)
	}
	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// cancelling job
}
