package example_job_snooze

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
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
func Example_jobSnooze() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &SnoozingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Schema:   riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil), // only necessary for the example test
		TestOnly: true,                                                                         // suitable only for use in tests; remove for live environments
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
	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// snoozing job for 5 minutes
}
