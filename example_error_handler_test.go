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
	"github.com/riverqueue/river/rivertype"
)

type CustomErrorHandler struct{}

func (*CustomErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	fmt.Printf("Job errored with: %s\n", err)
	return nil
}

func (*CustomErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any) *river.ErrorHandlerResult {
	fmt.Printf("Job panicked with: %v\n", panicVal)

	// Either function can also set the job to be immediately cancelled.
	return &river.ErrorHandlerResult{SetCancelled: true}
}

type ErroringArgs struct {
	ShouldError bool
	ShouldPanic bool
}

func (ErroringArgs) Kind() string { return "erroring" }

// Here to make sure our jobs are never accidentally retried which would add
// additional output and fail the example.
func (ErroringArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{MaxAttempts: 1}
}

type ErroringWorker struct {
	river.WorkerDefaults[ErroringArgs]
}

func (w *ErroringWorker) Work(ctx context.Context, j *river.Job[ErroringArgs]) error {
	switch {
	case j.Args.ShouldError:
		return fmt.Errorf("this job errored")
	case j.Args.ShouldPanic:
		panic("this job panicked")
	}
	return nil
}

// Example_errorHandler demonstrates how to use the ErrorHandler interface for
// custom application telemetry.
func Example_errorHandler() {
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
	river.AddWorker(workers, &ErroringWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		ErrorHandler: &CustomErrorHandler{},
		Logger:       slog.New(&slogutil.SlogMessageOnlyHandler{Level: 9}), // Suppress logging so example output is cleaner (9 > slog.LevelError).
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		panic(err)
	}

	// Not strictly needed, but used to help this test wait until job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCancelled, river.EventKindJobFailed)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	if _, err = riverClient.Insert(ctx, ErroringArgs{ShouldError: true}, nil); err != nil {
		panic(err)
	}

	// Wait for the first job before inserting another to guarantee test output
	// is ordered correctly.
	waitForNJobs(subscribeChan, 1)

	if _, err = riverClient.Insert(ctx, ErroringArgs{ShouldPanic: true}, nil); err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Job errored with: this job errored
	// Job panicked with: this job panicked
}
