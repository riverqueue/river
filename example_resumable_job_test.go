package river_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type ResumableArgs struct{}

func (ResumableArgs) Kind() string { return "resumable" }

type ResumableWorker struct {
	river.WorkerDefaults[ResumableArgs]
}

func (w *ResumableWorker) Work(ctx context.Context, job *river.Job[ResumableArgs]) error {
	river.ResumableStep(ctx, "step1", func(ctx context.Context) error {
		fmt.Println("Step 1")
		return nil
	})

	river.ResumableStep(ctx, "step2", func(ctx context.Context) error {
		fmt.Println("Step 2")
		return nil
	})

	river.ResumableStep(ctx, "step3", func(ctx context.Context) error {
		fmt.Println("Step 3")
		return nil
	})

	return nil
}

// Example_resumable demonstrates the use of a "resumable job", a job that has
// multiple steps, and which can be resumed after each one.
func Example_resumable() { //nolint:dupl
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ResumableWorker{})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
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

	if _, err = riverClient.Insert(ctx, ResumableArgs{}, nil); err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Step 1
	// Step 2
	// Step 3
}
