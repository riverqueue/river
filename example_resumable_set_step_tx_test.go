package river_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

type ResumableStepTxArgs struct{}

func (ResumableStepTxArgs) Kind() string { return "resumable_step_tx" }

// ResumableStepTxWorker persists resumable step progress transactionally before
// failing the job.
type ResumableStepTxWorker struct {
	river.WorkerDefaults[ResumableStepTxArgs]

	dbPool *pgxpool.Pool
}

func (w *ResumableStepTxWorker) Work(ctx context.Context, job *river.Job[ResumableStepTxArgs]) error {
	const durableStep = "durable_step"

	river.ResumableStep(ctx, durableStep, func(ctx context.Context) error {
		tx, err := w.dbPool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Perform some kind database work in a transaction.
		var result int
		if err := tx.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
			return err
		}

		// Then, record the step as completed in the same transaction.
		if _, err := river.ResumableSetStepTx[*riverpgxv5.Driver](ctx, tx, job, durableStep); err != nil {
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}

		return errors.New("simulated failure after persisting step")
	})

	return nil
}

// Example_resumableSetStepTx demonstrates how to transactionally persist a
// resumable step so it survives a failed attempt.
func Example_resumableSetStepTx() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &ResumableStepTxWorker{dbPool: dbPool})

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

	// Used only to help the example test wait for the failed attempt.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobFailed)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	insertRes, err := riverClient.Insert(ctx, ResumableStepTxArgs{}, nil)
	if err != nil {
		panic(err)
	}

	// Wait for the failed attempt so the persisted step can be inspected.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	jobAfter, err := riverClient.JobGet(ctx, insertRes.Job.ID)
	if err != nil {
		panic(err)
	}

	var metadata map[string]any
	if err := json.Unmarshal(jobAfter.Metadata, &metadata); err != nil {
		panic(err)
	}

	fmt.Printf("Persisted resumable step: %s\n", metadata[rivercommon.MetadataKeyResumableStep])

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Persisted resumable step: durable_step
}
