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
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

// Example_gracefulShutdownStopCancel demonstrates graceful stop with explicit
// fallback to StopAndCancel. When a SIGINT/SIGTERM arrives, Stop initiates a
// graceful stop. If running jobs don't finish before the graceful stop context
// expires, StopAndCancel cancels their contexts. This example is intended to
// demonstrate advanced use of StopAndCancel. Generally, prefer the method shown
// in Example_gracefulShutdown over the one here.
func Example_gracefulShutdownStopAndCancel() {
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
		Workers: workers,
	}))
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

	// Use signal.NotifyContext to detect SIGINT/SIGTERM, but don't pass the
	// signal context to Start. Cancelling the Start context would cancel running
	// job contexts immediately, which is equivalent to StopAndCancel.
	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Make sure our job starts being worked before doing anything else.
	<-jobStarted

	// Cheat by sending ourselves a SIGTERM for the purpose of this example
	// (normally this is sent by user or supervisory process).
	selfProcess, _ := os.FindProcess(os.Getpid())
	_ = selfProcess.Signal(syscall.SIGTERM)

	// Wait for the first signal handler to consume the SIGTERM and cancel the
	// start context before tearing it down.
	<-signalCtx.Done()
	stop()

	fmt.Printf("Received SIGINT/SIGTERM; initiating soft stop (waiting for jobs to finish)\n")

	softStopCtx, softStopCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer softStopCancel()

	if err := riverClient.Stop(softStopCtx); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			panic(err)
		}

		fmt.Printf("Soft stop timeout; cancelling remaining jobs\n")

		if err := riverClient.StopAndCancel(ctx); err != nil {
			panic(err)
		}
	}

	// Output:
	// Working job that doesn't finish until cancelled
	// Received SIGINT/SIGTERM; initiating soft stop (waiting for jobs to finish)
	// Soft stop timeout; cancelling remaining jobs
	// Job cancelled
}
