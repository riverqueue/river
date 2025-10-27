package riverlog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
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

// Callers should define their own context key to extract their a logger back
// out of work context.
type customContextKey struct{}

type CustomContextLoggingArgs struct{}

func (CustomContextLoggingArgs) Kind() string { return "logging" }

type CustomContextLoggingWorker struct {
	river.WorkerDefaults[CustomContextLoggingArgs]
}

func (w *CustomContextLoggingWorker) Work(ctx context.Context, job *river.Job[CustomContextLoggingArgs]) error {
	logger := ctx.Value(customContextKey{}).(*log.Logger) //nolint:forcetypeassert
	logger.Printf("Raw log from worker")
	return nil
}

// ExampleNewMiddlewareCustomContext demonstrates the use of riverlog middleware
// with an arbitrary new context function that can be used to inject any sort of
// logger into context.
func ExampleNewMiddlewareCustomContext() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &CustomContextLoggingWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Middleware: []rivertype.Middleware{
			riverlog.NewMiddlewareCustomContext(func(ctx context.Context, w io.Writer) context.Context {
				// For demonstration purposes we show the use of a built-in
				// non-slog logger, but this could be anything like Logrus or
				// Zap. Even the raw writer could be stored if so desired.
				logger := log.New(w, "", 0)
				return context.WithValue(ctx, customContextKey{}, logger)
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

	_, err = riverClient.Insert(ctx, CustomContextLoggingArgs{}, nil)
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
	// Raw log from worker
}
