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
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
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

	// In the event of cancellation, an error should be returned so that the job
	// goes back in the retry queue.
	return ctx.Err()
}

// Example_gracefulShutdown demonstrates a realistic-looking stop loop for
// River. It listens for SIGINT/SIGTERM (like might be received by a Ctrl+C
// locally or on a platform like Heroku to stop a process) and when received,
// tries a soft stop that waits for work to finish. If it doesn't finish in
// time, a second SIGINT/SIGTERM will initiate a hard stop that cancels all jobs
// using context cancellation. A third will give up on the stop procedure and
// exit uncleanly.
func Example_gracefulShutdown() {
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

	jobStarted := make(chan struct{})

	workers := river.NewWorkers()
	river.AddWorker(workers, &WaitsForCancelOnlyWorker{jobStarted: jobStarted})

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

	_, err = riverClient.Insert(ctx, WaitsForCancelOnlyArgs{}, nil)
	if err != nil {
		panic(err)
	}

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	sigintOrTerm := make(chan os.Signal, 1)
	signal.Notify(sigintOrTerm, syscall.SIGINT, syscall.SIGTERM)

	// This is meant to be a realistic-looking stop goroutine that might go in a
	// real program. It waits for SIGINT/SIGTERM and when received, tries to stop
	// gracefully by allowing a chance for jobs to finish. But if that isn't
	// working, a second SIGINT/SIGTERM will tell it to terminate with prejudice and
	// it'll issue a hard stop that cancels the context of all active jobs. In
	// case that doesn't work, a third SIGINT/SIGTERM ignores River's stop procedure
	// completely and exits uncleanly.
	go func() {
		<-sigintOrTerm
		fmt.Printf("Received SIGINT/SIGTERM; initiating soft stop (try to wait for jobs to finish)\n")

		softStopCtx, softStopCtxCancel := context.WithTimeout(ctx, 10*time.Second)
		defer softStopCtxCancel()

		go func() {
			select {
			case <-sigintOrTerm:
				fmt.Printf("Received SIGINT/SIGTERM again; initiating hard stop (cancel everything)\n")
				softStopCtxCancel()
			case <-softStopCtx.Done():
				fmt.Printf("Soft stop timeout; initiating hard stop (cancel everything)\n")
			}
		}()

		err := riverClient.Stop(softStopCtx)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			panic(err)
		}
		if err == nil {
			fmt.Printf("Soft stop succeeded\n")
			return
		}

		hardStopCtx, hardStopCtxCancel := context.WithTimeout(ctx, 10*time.Second)
		defer hardStopCtxCancel()

		// As long as all jobs respect context cancellation, StopAndCancel will
		// always work. However, in the case of a bug where a job blocks despite
		// being cancelled, it may be necessary to either ignore River's stop
		// result (what's shown here) or have a supervisor kill the process.
		err = riverClient.StopAndCancel(hardStopCtx)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			fmt.Printf("Hard stop timeout; ignoring stop procedure and exiting unsafely\n")
		} else if err != nil {
			panic(err)
		}

		// hard stop succeeded
	}()

	// Make sure our job starts being worked before doing anything else.
	<-jobStarted

	// Cheat a little by sending a SIGTERM manually for the purpose of this
	// example (normally this will be sent by user or supervisory process). The
	// first SIGTERM tries a soft stop in which jobs are given a chance to
	// finish up.
	sigintOrTerm <- syscall.SIGTERM

	// The soft stop will never work in this example because our job only
	// respects context cancellation, but wait a short amount of time to give it
	// a chance. After it elapses, send another SIGTERM to initiate a hard stop.
	select {
	case <-riverClient.Stopped():
		// Will never be reached in this example because our job will only ever
		// finish on context cancellation.
		fmt.Printf("Soft stop succeeded\n")

	case <-time.After(100 * time.Millisecond):
		sigintOrTerm <- syscall.SIGTERM
		<-riverClient.Stopped()
	}

	// Output:
	// Working job that doesn't finish until cancelled
	// Received SIGINT/SIGTERM; initiating soft stop (try to wait for jobs to finish)
	// Received SIGINT/SIGTERM again; initiating hard stop (cancel everything)
	// Job cancelled
	// jobexecutor.JobExecutor: Job errored; retrying
}
