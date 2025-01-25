package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type SnoozingArgs struct {
	ShouldSnooze bool
}

func (args SnoozingArgs) Kind() string { return "Snoozing" }

type SnoozingWorker struct {
	river.WorkerDefaults[SnoozingArgs]
}

func (w *SnoozingWorker) Work(ctx context.Context, job *river.Job[SnoozingArgs]) error {
	if job.Args.ShouldSnooze {
		fmt.Println("snoozing job for 5 minutes")
		return river.JobSnooze(5 * time.Minute)
	}
	return nil
}

// Example_jobSnooze demonstrates how to snooze a job from within Work using
// JobSnooze. The job will be run again after 5 minutes and the snooze attempt
// will decrement the job's attempt count, ensuring that one can snooze as many
// times as desired without being impacted by the max attempts.
func Example_jobSnooze() { //nolint:dupl
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
	river.AddWorker(workers, &SnoozingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// The subscription bits are not needed in real usage, but are used to make
	// sure the test waits until the job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobSnoozed)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}
	if _, err = riverClient.Insert(ctx, SnoozingArgs{ShouldSnooze: true}, nil); err != nil {
		panic(err)
	}
	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// snoozing job for 5 minutes
}
