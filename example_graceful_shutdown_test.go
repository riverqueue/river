package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/util/slogutil"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

type WaitsForCancelOnlyArgs struct{}

func (WaitsForCancelOnlyArgs) Kind() string { return "waits_for_cancel_only" }

// WaitsForCancelOnlyWorker is a worker that will never finish jobs until its
// context is cancelled.
type WaitsForCancelOnlyWorker struct {
	river.WorkerDefaults[WaitsForCancelOnlyArgs]
	jobStarted chan struct{}
}

func (w *WaitsForCancelOnlyWorker) Work(ctx context.Context, job *river.Job[WaitsForCancelOnlyArgs]) error {
	fmt.Printf("Working job that doesn't finish until cancelled\n")
	close(w.jobStarted)
	<-ctx.Done()
	fmt.Printf("Job cancelled\n")
	return nil
}

// Example_gracefulShutdown demonstrates a realistic-looking stop loop for
// River. It listens for a SIGTERM (like might be received on a platform like
// Heroku to stop a process) and when receives, tries a soft stop that waits for
// work to finish. If it doesn't finish in time, a second SIGTERM will initiate
// a hard stop that cancels all jobs using context cancellation.
func Example_gracefulShutdown() {
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

	jobStarted := make(chan struct{})

	workers := river.NewWorkers()
	river.AddWorker(workers, &WaitsForCancelOnlyWorker{jobStarted: jobStarted})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.DefaultQueue: {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, WaitsForCancelOnlyArgs{}, nil)
	if err != nil {
		panic(err)
	}

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	riverClientStopped := make(chan struct{})

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)

	// This is meant to be a realistic-looking stop goroutine that might go in a
	// real program. It waits for SIGTERM and when received, tries to stop
	// gracefully by allowing a chance for jobs to finish. But if that isn't
	// working, a second SIGTERM will tell it to terminate with prejudice and
	// it'll issue a hard stop that cancels the context of all active jobs.
	go func() {
		defer close(riverClientStopped)

		<-sigterm
		fmt.Printf("Received SIGTERM; initiating soft stop (try to wait for jobs to finish)\n")

		softStopSucceeded := make(chan struct{})
		go func() {
			if err := riverClient.Stop(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					panic(err)
				}
			}
			close(softStopSucceeded)
		}()

		select {
		case <-sigterm:
			fmt.Printf("Received SIGTERM again; initiating hard stop (cancel everything)\n")
			if err := riverClient.StopAndCancel(ctx); err != nil {
				panic(err)
			}
		case <-softStopSucceeded:
			// Will never be reached in this example.
		}
	}()

	// Make sure our job starts being worked before doing anything else.
	<-jobStarted

	// Cheat a little by sending a SIGTERM manually for the purpose of this
	// example (normally this will be sent by user or supervisory process). The
	// first SIGTERM tries a soft stop in which jobs are givcen a chance to
	// finish up.
	sigterm <- syscall.SIGTERM

	// The soft stop will never work in this example because our job only
	// respects context cancellation, but wait a short amount of time to give it
	// a chance. After it elapses, send another SIGTERM to initiate a hard stop.
	select {
	case <-riverClientStopped:
		// Will never be reached in this example because our job will only ever
		// finish on context cancellation.
		fmt.Printf("Soft stop succeeded\n")

	case <-time.After(100 * time.Millisecond):
		sigterm <- syscall.SIGTERM
		<-riverClientStopped
	}

	// Output:
	// Working job that doesn't finish until cancelled
	// Received SIGTERM; initiating soft stop (try to wait for jobs to finish)
	// Received SIGTERM again; initiating hard stop (cancel everything)
	// Job cancelled
}
