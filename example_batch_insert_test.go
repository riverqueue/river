package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type BatchInsertArgs struct{}

func (BatchInsertArgs) Kind() string { return "batch_insert" }

// BatchInsertWorker is a job worker demonstrating use of custom
// job-specific insertion options.
type BatchInsertWorker struct {
	river.WorkerDefaults[BatchInsertArgs]
}

func (w *BatchInsertWorker) Work(ctx context.Context, job *river.Job[BatchInsertArgs]) error {
	fmt.Printf("Worked a job\n")
	return nil
}

// Example_batchInsert demonstrates how many jobs can be inserted for work as
// part of a single operation.
func Example_batchInsert() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_test_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Required for the purpose of this test, but not necessary in real usage.
	if err := riverinternaltest.TruncateRiverTables(ctx, dbPool); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &BatchInsertWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	results, err := riverClient.InsertMany(ctx, []river.InsertManyParams{
		{Args: BatchInsertArgs{}},
		{Args: BatchInsertArgs{}},
		{Args: BatchInsertArgs{}},
		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 4}},
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Inserted %d jobs\n", len(results))

	waitForNJobs(subscribeChan, 5)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Inserted 5 jobs
	// Worked a job
	// Worked a job
	// Worked a job
	// Worked a job
	// Worked a job
}
