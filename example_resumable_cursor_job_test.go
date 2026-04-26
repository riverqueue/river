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

type ResumableCursorArgs struct {
	IDs []int `json:"ids"`
}

func (ResumableCursorArgs) Kind() string { return "resumable_cursor" }

type ResumableCursor struct {
	LastProcessedID int `json:"last_processed_id"`
}

type ResumableCursorWorker struct {
	river.WorkerDefaults[ResumableCursorArgs]
}

func (w *ResumableCursorWorker) Work(ctx context.Context, job *river.Job[ResumableCursorArgs]) error {
	river.ResumableStepCursor(ctx, "process_ids", func(ctx context.Context, cursor ResumableCursor) error {
		for _, id := range job.Args.IDs {
			if id <= cursor.LastProcessedID {
				continue
			}

			fmt.Printf("Processed %d\n", id)

			if err := river.ResumableSetCursor(ctx, ResumableCursor{LastProcessedID: id}); err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}

// Example_resumableCursor demonstrates the use of a resumable cursor step, a
// step that can store arbitrary JSON state to resume a partially completed loop.
func Example_resumableCursor() { //nolint:dupl
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ResumableCursorWorker{})

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

	if _, err = riverClient.Insert(ctx, ResumableCursorArgs{
		IDs: []int{1, 2, 3},
	}, nil); err != nil {
		panic(err)
	}

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Processed 1
	// Processed 2
	// Processed 3
}
