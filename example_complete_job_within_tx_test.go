package river_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

type TransactionalArgs struct{}

func (TransactionalArgs) Kind() string { return "transactional_worker" }

// transactionBeginner is satisfied by both *pgxpool.Pool and pgx.Tx: called
// on a pool it begins a new transaction, while called on an existing
// transaction it begins a savepoint within it.
type transactionBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// TransactionalWorker is a job worker which runs an operation on the database
// and transactionally completes the current job.
//
// While this example is simplified, any operations could be performed within
// the transaction such as inserting additional jobs or manipulating other data.
type TransactionalWorker struct {
	river.WorkerDefaults[TransactionalArgs]

	db transactionBeginner
}

func (w *TransactionalWorker) Work(ctx context.Context, job *river.Job[TransactionalArgs]) error {
	tx, err := w.db.Begin(ctx)
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
//
// See TestTransactionalWorker for a test of this worker using rivertest.Worker.
func Example_completeJobWithinTx() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	workers := river.NewWorkers()
	river.AddWorker(workers, &TransactionalWorker{db: dbPool})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	}))
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

	// Wait for jobs to complete. Only needed for purposes of the example test.
	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Transitioned TransactionalWorker job from "running" to "completed"
}

func TestTransactionalWorker(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	// The test fixture rolls back this outer transaction during cleanup.
	tx := riverdbtest.TestTxPgx(ctx, t)
	driver := riverpgxv5.New(nil)

	// Begin on the outer transaction opens a savepoint. It sees the job
	// inserted by the test, and the worker's Commit releases the savepoint.
	worker := &TransactionalWorker{db: tx}
	testWorker := rivertest.NewWorker(t, driver, &river.Config{}, river.WorkFunc(
		func(ctx context.Context, job *river.Job[TransactionalArgs]) error {
			if err := worker.Work(ctx, job); err != nil {
				return err
			}

			// Check the database before returning to River, which would
			// otherwise complete a job automatically on success.
			client := river.ClientFromContext[pgx.Tx](ctx)
			completedJob, err := client.JobGetTx(ctx, tx, job.ID)
			if err != nil {
				return err
			}
			if completedJob.State != rivertype.JobStateCompleted {
				return fmt.Errorf("job state after transactional completion: got %q, want %q",
					completedJob.State, rivertype.JobStateCompleted)
			}
			return nil
		},
	))

	result, err := testWorker.Work(ctx, t, tx, TransactionalArgs{}, nil)
	require.NoError(t, err)
	require.Equal(t, river.EventKindJobCompleted, result.EventKind)
	require.Equal(t, rivertype.JobStateCompleted, result.Job.State)
}
