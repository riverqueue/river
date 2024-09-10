package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobCompleteTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		JobArgsReflectKind[JobArgs]
	}

	type testBundle struct {
		exec riverdriver.Executor
		tx   pgx.Tx
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)

		return &testBundle{
			exec: riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:   tx,
		}
	}

	t.Run("CompletesJob", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: ptrutil.Ptr(rivertype.JobStateRunning),
		})

		completedJob, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.WithinDuration(t, time.Now(), *completedJob.FinalizedAt, 2*time.Second)

		updatedJob, err := bundle.exec.JobGetByID(ctx, job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, updatedJob.State)
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.EqualError(t, err, "job must be running")
	})
}
