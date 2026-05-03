package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
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

// Example_gracefulShutdown demonstrates graceful stop using SoftStopTimeout.
// When a SIGINT/SIGTERM arrives, the start context is cancelled, which
// initiates a soft stop. If running jobs don't finish within the configured
// SoftStopTimeout, their contexts are automatically cancelled (hard stop).
func Example_gracefulShutdown() {
	ctx := context.Background()

	jobStarted := make(chan struct{})

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &WaitsForCancelOnlyWorker{jobStarted: jobStarted})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError, ReplaceAttr: slogutil.NoLevelTimeJobID})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		SoftStopTimeout: 100 * time.Millisecond,
		Workers:         workers,
	}))
	if err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, WaitsForCancelOnlyArgs{}, nil)
	if err != nil {
		panic(err)
	}

	// Use signal.NotifyContext to cancel the start context on SIGINT/SIGTERM.
	// When the signal fires, the client initiates a soft stop. If running jobs
	// don't finish within SoftStopTimeout, their contexts are cancelled.
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := riverClient.Start(signalCtx); err != nil {
		panic(err)
	}

	// Make sure our job starts being worked before doing anything else.
	<-jobStarted

	// Cheat by sending ourselves a SIGTERM for the purpose of this example
	// (normally this is sent by user or supervisory process). The signal
	// cancels the start context, initiating a soft stop.
	selfProcess, _ := os.FindProcess(os.Getpid())
	_ = selfProcess.Signal(syscall.SIGTERM)

	// Wait for the first signal handler to consume the SIGTERM and cancel the
	// start context before tearing it down.
	<-signalCtx.Done()
	stop()

	// Initiated inside river.Client when signalCtx is cancelled.
	fmt.Printf("Received SIGINT/SIGTERM; initiating soft stop (waiting for cancelled jobs to finish)\n")

	<-riverClient.Stopped()

	// Output:
	// Working job that doesn't finish until cancelled
	// Received SIGINT/SIGTERM; initiating soft stop (waiting for cancelled jobs to finish)
	// Job cancelled
}
