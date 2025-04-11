package riverlog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverlog"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertype"
)

type LoggingArgs struct{}

func (LoggingArgs) Kind() string { return "logging" }

type LoggingWorker struct {
	river.WorkerDefaults[LoggingArgs]
}

func (w *LoggingWorker) Work(ctx context.Context, job *river.Job[LoggingArgs]) error {
	riverlog.Logger(ctx).InfoContext(ctx, "Logged from worker")
	riverlog.Logger(ctx).InfoContext(ctx, "Another line logged from worker")
	return nil
}

// Example_middleware demonstrates the use of riverlog middleware to inject a
// logger into context that'll persist its output onto the job record.
func Example_middleware() {
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
	river.AddWorker(workers, &LoggingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(&slogutil.SlogMessageOnlyHandler{Level: slog.LevelWarn}),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Middleware: []rivertype.Middleware{
			riverlog.NewMiddleware(func(w io.Writer) slog.Handler {
				// We have to use a specialized handler without timestamps to
				// make test output reproducible, but in reality this would as
				// simple as something like:
				//
				// 	return slog.NewJSONHandler(w, nil)
				return &slogutil.SlogMessageOnlyHandler{Out: w}
			}, nil),
		},
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Out of example scope, but used to wait until a job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, LoggingArgs{}, nil)
	if err != nil {
		panic(err)
	}

	type metadataWithLog struct {
		RiverLog []struct {
			Log string `json:"log"`
		} `json:"river:log"`
	}

	// Wait for job to complete, extract log data out of metadata, and print it.
	for _, job := range waitForNJobs(subscribeChan, 1) {
		var metadataWithLog metadataWithLog
		if err := json.Unmarshal(job.Metadata, &metadataWithLog); err != nil {
			panic(err)
		}
		for _, logAttempt := range metadataWithLog.RiverLog {
			fmt.Print(logAttempt.Log)
		}
	}

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Logged from worker
	// Another line logged from worker
}
