package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// The loser of the race condition parks on the row lock (pg_stat_activity)
// while the winner's retry commits; RED without a locking fallback read.
func exerciseClientRetryRaceLoserSeesWinnerCommit[TTx any](ctx context.Context, t *testing.T, driver riverdriver.Driver[TTx], schema string) {
	t.Helper()

	if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
		t.Skip("the retry race needs PostgreSQL row locking; SQLite JobRetry is a single non-locking UPDATE so the stale-snapshot return cannot occur")
	}

	pool := riversharedtest.DBPool(ctx, t)
	config := newTestConfig(t, schema)

	client, err := river.NewClient(driver, config)
	require.NoError(t, err)

	insertRes, err := client.Insert(ctx, &noOpArgs{}, &river.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	cancelledJob, err := client.JobCancel(ctx, insertRes.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)

	execTx, err := driver.GetExecutor().Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = execTx.Rollback(ctx) })

	winnerRow, err := client.JobRetryTx(ctx, driver.UnwrapTx(execTx), insertRes.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateAvailable, winnerRow.State)
	require.Nil(t, winnerRow.FinalizedAt)

	loserExecTx, err := driver.GetExecutor().Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = loserExecTx.Rollback(ctx) })

	var loserPID int
	require.NoError(t, loserExecTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&loserPID))

	type retryResult struct {
		row *rivertype.JobRow
		err error
	}
	loserDone := make(chan retryResult, 1)
	retryCtx, cancelRetry := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
	defer cancelRetry()
	go func() {
		row, err := client.JobRetryTx(retryCtx, driver.UnwrapTx(loserExecTx), insertRes.Job.ID)
		loserDone <- retryResult{row, err}
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
}
