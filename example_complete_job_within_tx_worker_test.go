package river_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

// TestTransactionalWorker runs the worker from Example_completeJobWithinTx
// against a test transaction. It's kept in a separate file because a test
// function in the example's file would stop go/doc from showing the whole
// file as the example. See https://pkg.go.dev/go/doc#Examples.
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
