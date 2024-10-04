package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
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
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(ctx context.Context, t *testing.T) (context.Context, *testBundle) {
		t.Helper()

		tx := riverinternaltest.TestTx(ctx, t)
		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)

		return ctx, &testBundle{
			client: client,
			exec:   riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:     tx,
		}
	}

	t.Run("CompletesJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t)

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

		ctx, bundle := setup(ctx, t)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.EqualError(t, err, "job must be running")
	})
}
