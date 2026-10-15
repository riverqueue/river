package harness

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestLeadership(t *testing.T) {
	t.Parallel()

	// A client with leader election disabled, next to an eligible client of
	// the other implementation, rejects periodic jobs, works the periodic
	// job the eligible leader enqueues, runs no leader-only maintenance, and
	// never becomes leader, including after the eligible leader stops and
	// after it restarts. Where the implementation allows it, it elects on a
	// short interval, so one that still took part in elections would win.
	t.Run("ElectionDisabled", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, disabled, eligible *Adapter) {
			disabledParams := protocol.StartParams{ClientID: "election-disabled", LeaderElectionDisabled: true, MaxWorkers: 1, Tuning: fastTuning}
			rejected := disabledParams
			rejected.PeriodicRunOnStart = true
			RequireErrorCode(t, disabled.Call(protocol.MethodStart, &rejected, nil), protocol.CodeRejected)

			disabled.Start(t, disabledParams)
			requireWorkedByDisabled := func(step string) {
				marker := eligible.InsertJob(t, echo(step, protocol.BehaviorComplete))
				requireWorkedOnceBy(t, env.DB.WaitJob(t, marker.ID, workWait), "election-disabled")
				_, hasLeader := env.DB.Leader(t)
				require.False(t, hasLeader, "a client with leader election disabled became leader %s", step)
			}
			requireWorkedByDisabled("before an eligible client starts")

			// The eligible client works another queue, so only the disabled
			// client works the periodic job it enqueues into the default one.
			eligible.Start(t, protocol.StartParams{
				ClientID: "election-eligible", MaxWorkers: 1, PeriodicRunOnStart: true, Queues: []string{"election_eligible"}, Tuning: fastTuning,
			})
			require.Equal(t, "election-eligible", env.DB.WaitLeader(t, "").LeaderID)
			eligible.WaitStats(t, "the periodic enqueuer starting", func(stats *protocol.StatsResult) bool { return stats.PeriodicStarts == 1 })
			periodic := waitPeriodicJobs(t, env, protocol.PeriodicJobID, 1)[0]
			requireWorkedOnceBy(t, env.DB.WaitJob(t, periodic.ID, workWait), "election-disabled")
			require.Zero(t, disabled.Stats(t).PeriodicStarts, "a client with leader election disabled ran the periodic enqueuer")

			eligible.Stop(t, protocol.StopParams{})
			requireWorkedByDisabled("after the eligible leader stops")
			disabled.Stop(t, protocol.StopParams{})
			disabled.Start(t, disabledParams)
			requireWorkedByDisabled("after a restart")
			require.Zero(t, disabled.Stats(t).PeriodicStarts, "a client with leader election disabled ran the periodic enqueuer")
			require.Len(t, periodicJobs(t, env, protocol.PeriodicJobID), 1)
		})
	})

	// Leadership moves between the implementations through resignation
	// requests and graceful stops, both ways.
	t.Run("Failover", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			clientIDs := map[*Adapter]string{env.Reference: "reference-worker", env.Candidate: "candidate-worker"}
			start := func(adapter *Adapter) {
				adapter.Start(t, protocol.StartParams{ClientID: clientIDs[adapter], MaxWorkers: 2})
			}
			start(env.Reference)
			start(env.Candidate)

			first := env.DB.WaitLeader(t, "")
			env.Reference.RequestResign(t, protocol.RequestResignParams{})
			second := env.DB.WaitNewTerm(t, first.ElectedAt)
			env.Candidate.RequestResign(t, protocol.RequestResignParams{})
			third := env.DB.WaitNewTerm(t, second.ElectedAt)

			leader, follower := env.Reference, env.Candidate
			if third.LeaderID == clientIDs[env.Candidate] {
				leader, follower = follower, leader
			}
			require.Equal(t, clientIDs[leader], third.LeaderID)
			leader.Stop(t, protocol.StopParams{})
			require.Equal(t, clientIDs[follower], env.DB.WaitLeader(t, clientIDs[leader]).LeaderID)
			start(leader)
			follower.Stop(t, protocol.StopParams{})
			require.Equal(t, clientIDs[leader], env.DB.WaitLeader(t, clientIDs[follower]).LeaderID)
		})
	})

	// Resignation requested by one implementation, directly and in
	// transactions, makes the other's leader resign. A request in a
	// rolled-back transaction publishes nothing, and one in a committed
	// transaction publishes exactly once.
	t.Run("RequestResign", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, leader, requester *Adapter) {
			leader.Start(t, protocol.StartParams{ClientID: "resigning-leader", MaxWorkers: 1})
			initial := env.DB.WaitLeader(t, "")
			require.Equal(t, "resigning-leader", initial.LeaderID)

			requester.RequestResign(t, protocol.RequestResignParams{})
			afterDirect := env.DB.WaitNewTerm(t, initial.ElectedAt)

			notifications := env.DB.Listen(t)
			resignationRequests := func() int {
				requests := 0
				for _, notification := range notifications.Next(t) {
					var payload struct {
						Action string `json:"action"`
					}
					require.NoError(t, json.Unmarshal([]byte(notification.Payload), &payload))
					if notification.Topic == "river_leadership" && payload.Action == "request_resign" {
						requests++
					}
				}
				return requests
			}

			requester.TxBegin(t, "resign_rollback")
			requester.RequestResign(t, protocol.RequestResignParams{Tx: "resign_rollback"})
			requester.TxEnd(t, "resign_rollback", false)
			require.Zero(t, resignationRequests(), "a rolled-back resignation request published")
			current, ok := env.DB.Leader(t)
			require.True(t, ok)
			require.Equal(t, afterDirect.ElectedAt, current.ElectedAt)

			requester.TxBegin(t, "resign_commit")
			requester.RequestResign(t, protocol.RequestResignParams{Tx: "resign_commit"})
			requester.TxEnd(t, "resign_commit", true)
			require.Equal(t, 1, resignationRequests(), "a committed resignation request wasn't published once")
			env.DB.WaitNewTerm(t, afterDirect.ElectedAt)
		})
	})
}
