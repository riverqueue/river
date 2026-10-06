package harness

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestCancel(t *testing.T) {
	t.Parallel()

	// A job cancelled by one implementation between the other's claim of it
	// committing and its work starting must start its worker already
	// cancelled. The claimer holds its claim on a barrier, so the job is
	// running without an executor when the cancellation arrives, and the
	// claimer's stats show the worker started cancelled, so a cancellation
	// that only arrived after the claim was released fails.
	t.Run("ClaimTime", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, canceller, claimer *Adapter) {
			claimer.Start(t, protocol.StartParams{ClaimBarrier: "claim", ClientID: "claim-time-cancel", MaxWorkers: 1})
			// Remote cancellation arrives by notification, so the claimer
			// must be listening before it claims.
			env.DB.WaitListening(t, claimer)

			job := canceller.InsertJob(t, echo("claim-time cancellation", protocol.BehaviorCooperativeCancel))
			running := env.DB.WaitJob(t, job.ID, workWait, "running")
			require.Equal(t, []string{"claim-time-cancel"}, running.AttemptedBy)
			require.Equal(t, "running", canceller.Cancel(t, protocol.JobParams{ID: job.ID}).State, "cancelling a claimed job only requests cancellation")

			// Give the claimer time to receive the cancellation while it holds
			// the claim. SQLite listeners poll every 50 ms, and Postgres
			// delivers notifications at commit.
			time.Sleep(time.Second)
			claimer.Release(t, "claim")

			cancelled := env.DB.WaitJob(t, job.ID, workWait)
			require.Equal(t, "cancelled", cancelled.State)
			require.Equal(t, 1, cancelled.Attempt)
			require.Len(t, cancelled.Errors, 1)
			require.Equal(t, errorCancelledRemotely, cancelled.Errors[0].Error)
			require.Equal(t, 1, claimer.Stats(t).CancelledAtStart, "the claimer started the job without its cancellation")
		})
	})

	// A client that only polls finds another implementation's insert, and
	// notices the other's cancellation of its running job by polling.
	t.Run("PollOnly", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "poll-only", FetchPollIntervalMS: 100, MaxWorkers: 1, PollOnly: true})
			polled := controller.InsertJob(t, echo("poll-only fetch", protocol.BehaviorComplete))
			requireWorkedOnceBy(t, env.DB.WaitJob(t, polled.ID, workWait), "poll-only")

			job := controller.InsertJob(t, echo("poll-only cancel", protocol.BehaviorCooperativeCancel))
			env.DB.WaitJob(t, job.ID, workWait, "running")
			startedAt := time.Now()
			controller.Cancel(t, protocol.JobParams{ID: job.ID})
			cancelled := env.DB.WaitJob(t, job.ID, workWait)
			require.Equal(t, "cancelled", cancelled.State)
			require.Len(t, cancelled.Errors, 1)
			require.Equal(t, errorCancelledRemotely, cancelled.Errors[0].Error)
			require.Less(t, time.Since(startedAt), 6*time.Second)
		})
	})

	// A cancel and then a retry race between the implementations. The winner
	// holds the job's row lock in an open transaction until the loser's
	// request is observed waiting on it, so the loser's statement starts
	// before the winner commits. Its update then matches nothing, and it must
	// return the winner's committed row rather than the row as its statement
	// first saw it.
	t.Run("Race", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env, winner, loser *Adapter) {
			job := winner.InsertJob(t, withOpts(echo("cancel and retry race", protocol.BehaviorComplete),
				protocol.InsertOpts{ScheduledAt: new(time.Now().Add(time.Hour).UTC())}))
			for _, method := range []string{protocol.MethodCancel, protocol.MethodRetry} {
				winner.TxBegin(t, method)
				var won protocol.Job
				require.NoError(t, winner.Call(method, &protocol.JobParams{ID: job.ID, Tx: method}, &won))

				lostErr := make(chan error, 1)
				var lost protocol.Job
				go func() { lostErr <- loser.Call(method, &protocol.JobParams{ID: job.ID}, &lost) }()
				env.DB.WaitLockWait(t, loser)
				select {
				case err := <-lostErr:
					require.FailNowf(t, "returned early", "%s's %s returned while the winner's was uncommitted: %v", loser.Label, method, err)
				default:
				}
				winner.TxEnd(t, method, true)
				select {
				case err := <-lostErr:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					require.FailNowf(t, "still blocked", "%s's %s stayed blocked after the winner committed", loser.Label, method)
				}
				require.Equal(t, won, lost, "%s lost a %s race and must return the committed row", loser.Label, method)
				require.Equal(t, &won, env.DB.MustJob(t, job.ID))
			}
		})
	})

	// Cancelling a running job from the other implementation records the
	// request in its metadata and reaches the worker through a control
	// notification. The worker polls once a minute, so it can't learn of the
	// cancellation by polling, and the job is cancelled, not failed.
	t.Run("Remote", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "remote-cancel", FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 1})
			job := controller.InsertJob(t, echo("remote cancel", protocol.BehaviorCooperativeCancel))
			env.DB.WaitJob(t, job.ID, workWait, "running")

			startedAt := time.Now()
			requested := controller.Cancel(t, protocol.JobParams{ID: job.ID})
			require.Equal(t, "running", requested.State, "cancelling a running job only requests cancellation")
			cancelAttemptedAt, ok := requested.Metadata["cancel_attempted_at"].(string)
			require.True(t, ok, "cancel_attempted_at must be a time string: %v", requested.Metadata)
			require.Regexp(t, goTimeTextPattern, cancelAttemptedAt)

			cancelled := env.DB.WaitJob(t, job.ID, workWait)
			require.Less(t, time.Since(startedAt), 5*time.Second)
			require.Equal(t, "cancelled", cancelled.State)
			require.Equal(t, 1, cancelled.Attempt)
			require.NotNil(t, cancelled.FinalizedAt)
			require.Len(t, cancelled.Errors, 1)
			require.Equal(t, errorCancelledRemotely, cancelled.Errors[0].Error)
			require.Equal(t, cancelAttemptedAt, cancelled.Metadata["cancel_attempted_at"])

			stats := worker.WaitStats(t, "the job cancelled", func(stats *protocol.StatsResult) bool {
				return slices.Contains(stats.Events, "job_cancelled")
			})
			require.NotContains(t, stats.Events, "job_failed")
		})
	})
}
