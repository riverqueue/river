package river_test

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
)

type TransactionalArgs struct{}

func (TransactionalArgs) Kind() string { return "transactional_worker" }

// TransactionalWorker is a job worker which runs an operation on the database
// and transactionally completes the current job.
//
// While this example is simplified, any operations could be performed within
// the transaction such as inserting additional jobs or manipulating other data.
type TransactionalWorker struct {
	river.WorkerDefaults[TransactionalArgs]
	dbPool *pgxpool.Pool
}

func (w *TransactionalWorker) Work(ctx context.Context, job *river.Job[TransactionalArgs]) error {
	tx, err := w.dbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var result int
	if err := tx.QueryRow(ctx, "SELECT 1").Scan(&result); err != nil {
		return err
	}

	// The function needs to know the type of the database driver in use by the
	// Client, but the other generic parameters can be inferred.
	jobAfter, err := river.JobCompleteTx[*riverpgxv5.Driver](ctx, tx, job)
	if err != nil {
		return err
	}
	fmt.Printf("Transitioned TransactionalWorker job from %q to %q\n", job.State, jobAfter.State)

	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// Example_completeJobWithinTx demonstrates how to transactionally complete
// a job alongside other database changes being made.
func Example_completeJobWithinTx() {
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
	river.AddWorker(workers, &TransactionalWorker{dbPool: dbPool})
	river.AddWorker(workers, &SortWorker{})

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

	// Not strictly needed, but used to help this test wait until job is worked.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCompleted)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	if _, err = riverClient.Insert(ctx, TransactionalArgs{}, nil); err != nil {
		panic(err)
	}

	waitForNJobs(subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Transitioned TransactionalWorker job from "running" to "completed"
}
