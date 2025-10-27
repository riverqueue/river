package riverlog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverlog"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
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

// ExampleNewMiddleware demonstrates the use of riverlog middleware to inject a
// logger into context that'll persist its output onto the job record.
func ExampleNewMiddleware() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &LoggingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Middleware: []rivertype.Middleware{
			riverlog.NewMiddleware(func(w io.Writer) slog.Handler {
				// We have to use a specialized ReplacedAttr without level or
				// timestamps to make test output reproducible, but in reality
				// this would as simple as something like:
				//
				// 	return slog.NewJSONHandler(w, nil)
				return slog.NewTextHandler(w, &slog.HandlerOptions{ReplaceAttr: slogutil.NoLevelTime})
			}, nil),
		},
		Schema:   riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil), // only necessary for the example test
		TestOnly: true,                                                                         // suitable only for use in tests; remove for live environments
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

	// Wait for job to complete, extract log data out of metadata, and print it.
	for _, event := range riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1) {
		var metadataWithLog metadataWithLog
		if err := json.Unmarshal(event.Job.Metadata, &metadataWithLog); err != nil {
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
	// msg="Logged from worker"
	// msg="Another line logged from worker"
}

type metadataWithLog struct {
	RiverLog []struct {
		Log string `json:"log"`
	} `json:"river:log"`
}
