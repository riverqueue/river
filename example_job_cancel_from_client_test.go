package river_test

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type SleepingArgs struct{}

func (args SleepingArgs) Kind() string { return "SleepingWorker" }

type SleepingWorker struct {
	river.WorkerDefaults[CancellingArgs]
	jobChan chan int64
}

func (w *SleepingWorker) Work(ctx context.Context, job *river.Job[CancellingArgs]) error {
	w.jobChan <- job.ID
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		return errors.New("sleeping worker timed out")
	}
	return ctx.Err()
}

// Example_jobCancelFromClient demonstrates how to permanently cancel a job from
// any Client using JobCancel.
func Example_jobCancelFromClient() {
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

	jobChan := make(chan int64)

	workers := river.NewWorkers()
	river.AddWorker(workers, &SleepingWorker{jobChan: jobChan})

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

	// Not strictly needed, but used to help this test wait until job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCancelled)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}
	insertRes, err := riverClient.Insert(ctx, CancellingArgs{ShouldCancel: true}, nil)
	if err != nil {
		panic(err)
	}
	select {
	case <-jobChan:
	case <-time.After(2 * time.Second):
		panic("no jobChan signal received")
	}

	// There is presently no way to wait for the client to be 100% ready, so we
	// sleep for a bit to give it time to start up. This is only needed in this
	// example because we need the notifier to be ready for it to receive the
	// cancellation signal.
	time.Sleep(500 * time.Millisecond)

	if _, err = riverClient.JobCancel(ctx, insertRes.Job.ID); err != nil {
		panic(err)
	}
	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// jobexecutor.JobExecutor: job cancelled remotely
}
