package river_test

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
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

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &BatchInsertWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	}))
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

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

	// Start the client after inserting and printing so that the "Inserted"
	// message is guaranteed to appear before any "Worked" messages.
	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 5)

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
