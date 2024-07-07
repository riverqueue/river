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

type ReportingArgs struct{}

func (args ReportingArgs) Kind() string { return "Reporting" }

type ReportingWorker struct {
	river.WorkerDefaults[ReportingArgs]
	jobWorkedCh chan<- string
}

func (w *ReportingWorker) Work(ctx context.Context, job *river.Job[ReportingArgs]) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.jobWorkedCh <- job.Queue:
		return nil
	}
}

// Example_queuePause demonstrates how to pause queues to prevent them from
// working new jobs, and later resume them.
func Example_queuePause() {
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

	const (
		unreliableQueue = "unreliable_external_service"
		reliableQueue   = "reliable_jobs"
	)

	workers := river.NewWorkers()
	jobWorkedCh := make(chan string)
	river.AddWorker(workers, &ReportingWorker{jobWorkedCh: jobWorkedCh})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			unreliableQueue: {MaxWorkers: 10},
			reliableQueue:   {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a queue is paused or unpaused.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindQueuePaused, river.EventKindQueueResumed)
	defer subscribeCancel()

	fmt.Printf("Pausing %s queue\n", unreliableQueue)
	if err := riverClient.QueuePause(ctx, unreliableQueue, nil); err != nil {
		panic(err)
	}

	// Wait for queue to be paused:
	waitOrTimeout(subscribeChan)

	fmt.Println("Inserting one job each into unreliable and reliable queues")
	if _, err = riverClient.Insert(ctx, ReportingArgs{}, &river.InsertOpts{Queue: unreliableQueue}); err != nil {
		panic(err)
	}
	if _, err = riverClient.Insert(ctx, ReportingArgs{}, &river.InsertOpts{Queue: reliableQueue}); err != nil {
		panic(err)
	}
	// The unreliable queue is paused so its job should get worked yet, while
	// reliable queue is not paused so its job should get worked immediately:
	receivedQueue := waitOrTimeout(jobWorkedCh)
	fmt.Printf("Job worked on %s queue\n", receivedQueue)

	// Resume the unreliable queue so it can work the job:
	fmt.Printf("Resuming %s queue\n", unreliableQueue)
	if err := riverClient.QueueResume(ctx, unreliableQueue, nil); err != nil {
		panic(err)
	}
	receivedQueue = waitOrTimeout(jobWorkedCh)
	fmt.Printf("Job worked on %s queue\n", receivedQueue)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Pausing unreliable_external_service queue
	// Inserting one job each into unreliable and reliable queues
	// Job worked on reliable_jobs queue
	// Resuming unreliable_external_service queue
	// Job worked on unreliable_external_service queue
}

func waitOrTimeout[T any](ch <-chan T) T {
	select {
	case item := <-ch:
		return item
	case <-time.After(5 * time.Second):
		panic("WaitOrTimeout timed out after waiting 5s")
	}
}
