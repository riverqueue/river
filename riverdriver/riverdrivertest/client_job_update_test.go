package riverdrivertest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

// exerciseClientJobUpdate exercises job state changes through the client,
// including concurrent calls that must return the latest committed state.
func exerciseClientJobUpdate[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T) (riverdriver.Driver[TTx], string),
) {
	t.Helper()

	type testBundle struct {
		client *river.Client[TTx]
		driver riverdriver.Driver[TTx]
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		driver, schema := driverWithSchema(ctx, t)
		client, err := river.NewClient(driver, newTestConfig(t, schema))
		require.NoError(t, err)

		return &testBundle{
			client: client,
			driver: driver,
		}
	}

	t.Run("JobCancelConcurrentRaceFreshReturn", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		// Keep the job scheduled while concurrent calls cancel it.
		insertRes, err := bundle.client.Insert(ctx, &noOpArgs{}, &river.InsertOpts{ScheduledAt: time.Now().Add(5 * time.Minute)})
		require.NoError(t, err)

		const cancelRounds = 20

		var firstFinalizedAt *time.Time

		for range cancelRounds {
			var (
				group sync.WaitGroup
				rows  [2]*rivertype.JobRow
				errs  [2]error
			)

			group.Go(func() {
				rows[0], errs[0] = bundle.client.JobCancel(ctx, insertRes.Job.ID)
			})
			group.Go(func() {
				rows[1], errs[1] = bundle.client.JobCancel(ctx, insertRes.Job.ID)
			})
			group.Wait()

			for i := range 2 {
				require.NoError(t, errs[i])
				require.Equal(t, rivertype.JobStateCancelled, rows[i].State)
			}

			require.Equal(t, *rows[0].FinalizedAt, *rows[1].FinalizedAt)

			if firstFinalizedAt == nil {
				firstFinalizedAt = rows[0].FinalizedAt
			}
			require.Equal(t, *firstFinalizedAt, *rows[0].FinalizedAt,
				"finalized_at must be written exactly once; later cancels must not re-stamp it")
		}

		finalRow, err := bundle.client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, finalRow.State)
		require.Equal(t, *firstFinalizedAt, *finalRow.FinalizedAt)
	})

	t.Run("JobRetryRaceLoserSeesWinnerCommit", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		if bundle.driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			t.Skip("the retry race requires PostgreSQL row locking and statement snapshots")
		}

		// Hold the winner's row lock until the loser is waiting on it, so the
		// fallback read must observe a commit newer than its statement snapshot.
		pool := riversharedtest.DBPool(ctx, t)

		insertRes, err := bundle.client.Insert(ctx, &noOpArgs{}, &river.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
		require.NoError(t, err)

		cancelledJob, err := bundle.client.JobCancel(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)

		execTx, err := bundle.driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = execTx.Rollback(ctx) })

		winnerRow, err := bundle.client.JobRetryTx(ctx, bundle.driver.UnwrapTx(execTx), insertRes.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateAvailable, winnerRow.State)
		require.Nil(t, winnerRow.FinalizedAt)

		loserExecTx, err := bundle.driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = loserExecTx.Rollback(ctx) })

		var loserPID int
		require.NoError(t, loserExecTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&loserPID))

		type retryResult struct {
			err error
			row *rivertype.JobRow
		}
		loserDone := make(chan retryResult, 1)
		retryCtx, cancelRetry := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
		defer cancelRetry()
		go func() {
			row, err := bundle.client.JobRetryTx(retryCtx, bundle.driver.UnwrapTx(loserExecTx), insertRes.Job.ID)
			loserDone <- retryResult{err: err, row: row}
		}()

		require.Eventually(t, func() bool {
			var waitEventType string
			err := pool.QueryRow(ctx,
				"SELECT COALESCE(wait_event_type, '') FROM pg_stat_activity WHERE pid = $1", loserPID).
				Scan(&waitEventType)
			return err == nil && waitEventType == "Lock"
		}, riversharedtest.WaitTimeout(), 10*time.Millisecond, "the loser of the race condition never entered a lock wait on the job row")

		commitCtx, cancelCommit := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
		defer cancelCommit()
		require.NoError(t, execTx.Commit(commitCtx))

		loser := riversharedtest.WaitOrTimeout(t, loserDone)
		require.NoError(t, loser.err)

		require.Equal(t, rivertype.JobStateAvailable, loser.row.State,
			"the loser of the race condition returned a stale pre-commit row; its fallback read must see the winner's commit")
		require.Nil(t, loser.row.FinalizedAt,
			"the loser of the race condition must observe the winner's finalization clear, not its own snapshot's")
	})
	t.Run("JobScheduleSkipsLockedJobs", func(t *testing.T) {
		t.Parallel()

		driver, schema := driverWithSchema(ctx, t)
		if driver.DatabaseName() != riverdriver.DatabaseNameMySQL {
			t.Skip("MySQL skips locked scheduler candidates; other drivers may wait for them")
		}
		exec := driver.GetExecutor()
		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			ScheduledAt: new(time.Now().Add(-time.Minute)), Schema: schema, State: new(rivertype.JobStateScheduled),
		})
		tx, err := exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		_, err = tx.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{ID: job.ID, Schema: schema})
		require.NoError(t, err)

		scheduleCtx, cancel := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
		defer cancel()
		rows, err := exec.JobSchedule(scheduleCtx, &riverdriver.JobScheduleParams{Max: 10, Schema: schema})
		require.NoError(t, err)
		require.Empty(t, rows)
		require.NoError(t, tx.Rollback(ctx))

		rows, err = exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{Max: 10, Schema: schema})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, job.ID, rows[0].Job.ID)
	})

	// A MySQL transaction can already have a REPEATABLE READ snapshot when it
	// invokes a state change. No-op updates must still return the current row.
	t.Run("JobUpdateWithExistingSnapshot", func(t *testing.T) {
		t.Parallel()

		for _, operation := range []string{"Cancel", "DeleteMany", "Retry", "SetStateIfRunning"} {
			t.Run(operation, func(t *testing.T) {
				t.Parallel()

				driver, schema := driverWithSchema(ctx, t)
				if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
					t.Skip("SQLite does not permit a second writer with an open read transaction")
				}
				exec := driver.GetExecutor()
				state := rivertype.JobStateScheduled
				switch operation {
				case "Retry":
					state = rivertype.JobStateCancelled
				case "SetStateIfRunning":
					state = rivertype.JobStateRunning
				}
				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema, State: &state})
				tx, err := exec.Begin(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback(ctx) })
				_, err = tx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
				require.NoError(t, err)

				var current *rivertype.JobRow
				switch operation {
				case "Cancel":
					params := &riverdriver.JobCancelParams{ID: job.ID, CancelAttemptedAt: time.Now(), Schema: schema}
					_, err = exec.JobCancel(ctx, params)
					require.NoError(t, err)
					current, err = tx.JobCancel(ctx, params)
					require.NoError(t, err)
					require.Equal(t, rivertype.JobStateCancelled, current.State)
				case "DeleteMany":
					_, err = exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
						ID: job.ID, Schema: schema, State: rivertype.JobStateRunning, StateDoUpdate: true,
					})
					require.NoError(t, err)
					rows, err := tx.JobDeleteMany(ctx, &riverdriver.JobDeleteManyParams{
						Max: 10, OrderByClause: "id", Schema: schema, WhereClause: "true",
					})
					require.NoError(t, err)
					require.Empty(t, rows, "jobs that are now running must not be deleted")
				case "Retry":
					params := &riverdriver.JobRetryParams{ID: job.ID, Schema: schema}
					_, err = exec.JobRetry(ctx, params)
					require.NoError(t, err)
					current, err = tx.JobRetry(ctx, params)
					require.NoError(t, err)
					require.Equal(t, rivertype.JobStateAvailable, current.State)
				case "SetStateIfRunning":
					_, err = exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
						ID: job.ID, FinalizedAt: new(time.Now()), FinalizedAtDoUpdate: true, Schema: schema,
						State: rivertype.JobStateCompleted, StateDoUpdate: true,
					})
					require.NoError(t, err)
					rows, err := tx.JobSetStateIfRunningMany(ctx, &riverdriver.JobSetStateIfRunningManyParams{
						ID: []int64{job.ID}, Attempt: []*int{nil}, ErrData: [][]byte{nil}, FinalizedAt: []*time.Time{new(time.Now())},
						MetadataDoMerge: []bool{false}, MetadataUpdates: [][]byte{nil}, ScheduledAt: []*time.Time{nil},
						Schema: schema, State: []rivertype.JobState{rivertype.JobStateCancelled},
					})
					require.NoError(t, err)
					require.Len(t, rows, 1)
					require.Equal(t, rivertype.JobStateCompleted, rows[0].State)
				}
			})
		}
	})
}
