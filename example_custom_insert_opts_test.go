package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/slogutil"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type AlwaysHighPriorityArgs struct{}

func (AlwaysHighPriorityArgs) Kind() string { return "always_high_priority" }

// InsertOpts returns custom insert options that every job of this type will
// inherit by default.
func (AlwaysHighPriorityArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		Queue: "high_priority",
	}
}

// AlwaysHighPriorityWorker is a job worker demonstrating use of custom
// job-specific insertion options.
type AlwaysHighPriorityWorker struct {
	river.WorkerDefaults[AlwaysHighPriorityArgs]
}

func (w *AlwaysHighPriorityWorker) Work(ctx context.Context, job *river.Job[AlwaysHighPriorityArgs]) error {
	fmt.Printf("Ran in queue: %s\n", job.Queue)
	return nil
}

type SometimesHighPriorityArgs struct{}

func (SometimesHighPriorityArgs) Kind() string { return "sometimes_high_priority" }

// SometimesHighPriorityWorker is a job worker that's made high-priority
// sometimes through the use of options at insertion time.
type SometimesHighPriorityWorker struct {
	river.WorkerDefaults[SometimesHighPriorityArgs]
}

func (w *SometimesHighPriorityWorker) Work(ctx context.Context, job *river.Job[SometimesHighPriorityArgs]) error {
	fmt.Printf("Ran in queue: %s\n", job.Queue)
	return nil
}

// Example_customInsertOpts demonstrates the use of a job with custom
// job-specific insertion options.
func Example_customInsertOpts() {
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
	river.AddWorker(workers, &AlwaysHighPriorityWorker{})
	river.AddWorker(workers, &SometimesHighPriorityWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
			"high_priority":    {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to make wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// This job always runs in the high-priority queue because its job-specific
	// options on the struct above dictate that it will.
	_, err = riverClient.Insert(ctx, AlwaysHighPriorityArgs{}, nil)
	if err != nil {
		panic(err)
	}

	// This job will run in the high-priority queue because of the options given
	// at insertion time.
	_, err = riverClient.Insert(ctx, SometimesHighPriorityArgs{}, &river.InsertOpts{
		Queue: "high_priority",
	})
	if err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 2)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Ran in queue: high_priority
	// Ran in queue: high_priority
}
