package river

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func TestJobUniqueOpts_isEmpty(t *testing.T) {
	t.Parallel()

	require.True(t, (&UniqueOpts{}).isEmpty())
	require.False(t, (&UniqueOpts{ByArgs: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByPeriod: 1 * time.Nanosecond}).isEmpty())
	require.False(t, (&UniqueOpts{ByQueue: true}).isEmpty())
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{JobStateAvailable}}).isEmpty())
}

func TestJobUniqueOpts_validate(t *testing.T) {
	t.Parallel()

	require.NoError(t, (&UniqueOpts{}).validate())
	require.NoError(t, (&UniqueOpts{
		ByArgs:   true,
		ByPeriod: 1 * time.Second,
		ByQueue:  true,
		ByState:  []rivertype.JobState{JobStateAvailable},
	}).validate())

	require.EqualError(t, (&UniqueOpts{ByPeriod: 1 * time.Millisecond}).validate(), "JobUniqueOpts.ByPeriod should not be less than 1 second")
	require.EqualError(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobState("invalid")}}).validate(), `JobUniqueOpts.ByState contains invalid state "invalid"`)
}

func TestJobFinalizedAtConstraint(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		queries *dbsqlc.Queries
		tx      pgx.Tx
	}

	capitalizeJobState := func(state dbsqlc.JobState) string {
		return cases.Title(language.English, cases.NoLower).String(string(state))
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		config := newTestConfig(t, nil)
		client := newTestClient(ctx, t, config)

		tx, err := client.driver.GetDBPool().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { tx.Rollback(ctx) })

		return client, &testBundle{
			tx: tx,
		}
	}

	for _, state := range []dbsqlc.JobState{
		dbsqlc.JobStateCancelled,
		dbsqlc.JobStateCompleted,
		dbsqlc.JobStateDiscarded,
	} {
		state := state // capture range variable

		t.Run(fmt.Sprintf("CannotSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
			t.Parallel()
			client, bundle := setup(t)

			insertedJob, err := client.Insert(ctx, noOpArgs{}, nil)
			require.NoError(t, err)

			// Try updating the job to the target state but without a finalized_at,
			// expect an error:
			_, err = bundle.queries.JobUpdate(ctx, bundle.tx, dbsqlc.JobUpdateParams{
				ID:            insertedJob.ID,
				State:         state,
				StateDoUpdate: true,
			})
			require.ErrorContains(t, err, "violates check constraint \"finalized_or_finalized_at_null\"")
		})

		t.Run(fmt.Sprintf("CanSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
			t.Parallel()
			client, bundle := setup(t)

			insertedJob, err := client.Insert(ctx, noOpArgs{}, nil)
			require.NoError(t, err)

			// Try updating the job to the target state but without a finalized_at,
			// expect an error:
			jobAfter, err := bundle.queries.JobUpdate(ctx, bundle.tx, dbsqlc.JobUpdateParams{
				ID:                  insertedJob.ID,
				FinalizedAt:         ptrutil.Ptr(time.Now()),
				FinalizedAtDoUpdate: true,
				State:               state,
				StateDoUpdate:       true,
			})
			require.NoError(t, err)
			require.Equal(t, state, jobAfter.State)
		})
	}

	for _, state := range []dbsqlc.JobState{
		dbsqlc.JobStateAvailable,
		dbsqlc.JobStateRetryable,
		dbsqlc.JobStateRunning,
		dbsqlc.JobStateScheduled,
	} {
		state := state // capture range variable

		t.Run(fmt.Sprintf("CanSetState%sWithoutFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
			t.Parallel()
			client, bundle := setup(t)

			insertedJob, err := client.Insert(ctx, noOpArgs{}, nil)
			require.NoError(t, err)

			// Try updating the job to the target state but without a finalized_at,
			// expect an error:
			jobAfter, err := bundle.queries.JobUpdate(ctx, bundle.tx, dbsqlc.JobUpdateParams{
				ID:            insertedJob.ID,
				State:         state,
				StateDoUpdate: true,
			})
			require.NoError(t, err)
			require.Equal(t, state, jobAfter.State)
		})

		t.Run(fmt.Sprintf("CannotSetState%sWithFinalizedAt", capitalizeJobState(state)), func(t *testing.T) {
			t.Parallel()
			client, bundle := setup(t)

			insertedJob, err := client.Insert(ctx, noOpArgs{}, nil)
			require.NoError(t, err)

			// Try updating the job to the target state but without a finalized_at,
			// expect an error:
			_, err = bundle.queries.JobUpdate(ctx, bundle.tx, dbsqlc.JobUpdateParams{
				ID:                  insertedJob.ID,
				FinalizedAt:         ptrutil.Ptr(time.Now()),
				FinalizedAtDoUpdate: true,
				State:               state,
				StateDoUpdate:       true,
			})
			require.ErrorContains(t, err, "violates check constraint \"finalized_or_finalized_at_null\"")
		})
	}
}
