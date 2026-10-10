package harness

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// TestMultiEngine runs a fleet of three implementations: the reference, the
// candidate, and the peer RIVER_CONFORMANCE_PEER names. Scenarios between
// two non-reference implementations are the ordinary suite run with
// RIVER_CONFORMANCE_REFERENCE set to one of them.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestMultiEngine(t *testing.T) {
	t.Parallel()

	peer := RequirePeer(t)

	// engines returns the fleet's adapters by client ID.
	engines := func(t *testing.T, env *Env) map[string]*Adapter {
		t.Helper()

		return map[string]*Adapter{
			"reference-engine": env.Reference,
			"candidate-engine": env.Candidate,
			"peer-engine":      env.StartAdapter(t, peer),
		}
	}
	startAll := func(t *testing.T, fleet map[string]*Adapter) {
		t.Helper()

		for clientID, engine := range fleet {
			engine.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1})
		}
	}

	// Every engine claims one of three blocked jobs, one from each.
	t.Run("Competition", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			fleet := engines(t, env)
			startAll(t, fleet)
			ids := make([]int64, 0, len(fleet))
			for _, inserter := range fleet {
				ids = append(ids, inserter.InsertJob(t, withDuration(echo("multi-engine competition", protocol.BehaviorSleep), time.Second)).ID)
			}
			workers := map[string]bool{}
			for _, id := range ids {
				running := env.DB.WaitJob(t, id, workWait, "running", "completed")
				require.Len(t, running.AttemptedBy, 1)
				workers[running.AttemptedBy[0]] = true
			}
			require.ElementsMatch(t, slices.Collect(maps.Keys(fleet)), slices.Collect(maps.Keys(workers)), "every engine must claim one blocked job")
			for _, id := range ids {
				completed := env.DB.WaitJob(t, id, workWait)
				require.Equal(t, "completed", completed.State)
				require.Equal(t, 1, completed.Attempt)
			}
		})
	})

	// Insert notifications and cancellations pass directly between the two
	// non-reference engines, with the reference only observing. The worker
	// polls once a minute, so prompt work proves the notification path.
	t.Run("DirectedWork", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			fleet := engines(t, env)
			for _, pair := range [][2]string{{"candidate-engine", "peer-engine"}, {"peer-engine", "candidate-engine"}} {
				controller, worker := fleet[pair[0]], fleet[pair[1]]
				worker.Start(t, protocol.StartParams{ClientID: pair[1], FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 1})
				env.DB.WaitListening(t, worker)

				startedAt := time.Now()
				woken := controller.InsertJob(t, echo(pair[0]+" to "+pair[1], protocol.BehaviorComplete))
				requireWorkedOnceBy(t, env.DB.WaitJob(t, woken.ID, workWait), pair[1])
				require.Less(t, time.Since(startedAt), 5*time.Second, "%s didn't wake %s by notification", pair[0], pair[1])

				// Outlast insert notification throttling, so this insert
				// notifies too.
				time.Sleep(250 * time.Millisecond)
				cancelled := controller.InsertJob(t, echo(pair[0]+" cancels "+pair[1], protocol.BehaviorCooperativeCancel))
				env.DB.WaitJob(t, cancelled.ID, workWait, "running")
				controller.Cancel(t, protocol.JobParams{ID: cancelled.ID})
				finished := env.DB.WaitJob(t, cancelled.ID, workWait)
				require.Equal(t, "cancelled", finished.State)
				require.Len(t, finished.Errors, 1)
				require.Equal(t, errorCancelledRemotely, finished.Errors[0].Error)
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// Every engine's connections are terminated in turn, and afterwards jobs
	// from every engine are worked.
	t.Run("FaultRecovery", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			fleet := engines(t, env)
			startAll(t, fleet)
			for _, engine := range fleet {
				env.DB.WaitListening(t, engine)
				require.Positive(t, env.DB.TerminateConnections(t, engine, false))
				env.DB.WaitListening(t, engine)
			}
			for clientID, inserter := range fleet {
				inserted := inserter.InsertJob(t, echo("after faults from "+clientID, protocol.BehaviorComplete))
				completed := env.DB.WaitJob(t, inserted.ID, workWait)
				require.Equal(t, "completed", completed.State)
				require.Equal(t, 1, completed.Attempt)
			}
		})
	})

	// Leadership passes from engine to engine as each leader stops, and all
	// end up agreeing on the last.
	t.Run("LeaderFailover", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			fleet := engines(t, env)
			startAll(t, fleet)
			stopped := make([]string, 0, len(fleet)-1)
			leader := env.DB.WaitLeader(t, "").LeaderID
			for range len(fleet) - 1 {
				require.NotContains(t, stopped, leader, "a stopped engine is still the leader")
				fleet[leader].Stop(t, protocol.StopParams{})
				stopped = append(stopped, leader)
				leader = env.DB.WaitLeader(t, leader).LeaderID
			}
			require.NotContains(t, stopped, leader)
			for _, clientID := range stopped {
				fleet[clientID].Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1})
			}
			time.Sleep(time.Second)
			current, ok := env.DB.Leader(t)
			require.True(t, ok)
			require.Equal(t, leader, current.LeaderID)
		})
	})

	// A running fleet's connections stay bounded.
	t.Run("ResourceBound", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			fleet := engines(t, env)
			startAll(t, fleet)
			total := 0
			for clientID, engine := range fleet {
				count := env.DB.ConnectionCount(t, engine)
				require.LessOrEqual(t, count, connectionLimit, "%s connections grew without bound", clientID)
				total += count
			}
			require.LessOrEqual(t, total, connectionLimit*len(fleet), "the fleet holds %d connections", total)
		})
	})
}
