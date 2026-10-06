package harness

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// requireUnclaimed requires that a job is still available with none of its
// attempts used.
func requireUnclaimed(t *testing.T, env *Env, id int64) {
	t.Helper()

	job := env.DB.MustJob(t, id)
	require.Equal(t, "available", job.State, "job %d (%s)", id, job.Kind)
	require.Zero(t, job.Attempt, "job %d (%s)", id, job.Kind)
	require.Empty(t, job.AttemptedBy, "job %d (%s)", id, job.Kind)
	require.Empty(t, job.Errors, "job %d (%s)", id, job.Kind)
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestFleet(t *testing.T) {
	t.Parallel()

	// One implementation claims the other's jobs as River Go does, by
	// priority, then scheduled_at, then ID. Jobs whose ID, scheduled_at, and
	// priority orders all differ, including two with the same priority and
	// scheduled_at, become available together when the worker's scheduler
	// runs, and the worker works them one at a time. Each sleeps briefly, so
	// the attempts' times are distinct and record the order.
	t.Run("ClaimOrder", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			base := time.Now().UTC().Truncate(time.Millisecond)
			ids := map[string]int64{}
			// In insertion (ID) order.
			for _, job := range []struct {
				ago      time.Duration
				name     string
				priority int
			}{
				{ago: 30 * time.Second, name: "priority 1, latest", priority: 1},
				{ago: time.Minute, name: "priority 4", priority: 4},
				{ago: time.Minute, name: "priority 1, later", priority: 1},
				{ago: 3 * time.Minute, name: "priority 3, earliest", priority: 3},
				{ago: 2 * time.Minute, name: "priority 1, earliest, lower ID", priority: 1},
				{ago: 2 * time.Minute, name: "priority 1, earliest, higher ID", priority: 1},
			} {
				inserted := inserter.InsertJob(t, withDuration(withOpts(echo("claim order "+job.name, protocol.BehaviorSleep), protocol.InsertOpts{
					Priority: job.priority, ScheduledAt: new(base.Add(-job.ago)),
				}), 5*time.Millisecond))
				// Like Go, an explicit schedule inserts the job scheduled even
				// when it's due, and the leader's scheduler makes it available.
				require.Equal(t, "scheduled", inserted.State, job.name)
				ids[job.name] = inserted.ID
			}

			worker.Start(t, protocol.StartParams{ClientID: "claim-order", MaxWorkers: 1, Tuning: fastTuning})
			type claim struct {
				at   time.Time
				name string
			}
			claims := make([]claim, 0, len(ids))
			for name, id := range ids {
				worked := env.DB.WaitJob(t, id, maintenanceWait)
				requireWorkedOnceBy(t, worked, "claim-order")
				claims = append(claims, claim{at: *worked.AttemptedAt, name: name})
			}
			slices.SortFunc(claims, func(a, b claim) int { return a.at.Compare(b.at) })
			actual := make([]string, 0, len(claims))
			for i, claim := range claims {
				if i > 0 {
					require.True(t, claim.at.After(claims[i-1].at), "two jobs were claimed at the same time")
				}
				actual = append(actual, claim.name)
			}
			require.Equal(t, []string{
				"priority 1, earliest, lower ID",
				"priority 1, earliest, higher ID",
				"priority 1, later",
				"priority 1, latest",
				"priority 3, earliest",
				"priority 4",
			}, actual)
		})
	})

	// Both implementations compete for a burst of short jobs, and every job
	// runs exactly once.
	t.Run("Competition", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			jobsPerInserter, maxWorkers := 150, 8
			if env.Driver == DriverSQLite {
				jobsPerInserter, maxWorkers = 20, 2
			}
			clientIDs := map[*Adapter]string{env.Reference: "reference-competitor", env.Candidate: "candidate-competitor"}
			for _, adapter := range []*Adapter{env.Reference, env.Candidate} {
				adapter.Start(t, protocol.StartParams{ClientID: clientIDs[adapter], MaxWorkers: maxWorkers})
			}
			for _, inserter := range []*Adapter{env.Reference, env.Candidate} {
				jobs := make([]protocol.InsertJob, jobsPerInserter)
				for i := range jobs {
					jobs[i] = withDuration(echo(fmt.Sprintf("competition %s %d", inserter.Label, i), protocol.BehaviorSleep), 5*time.Millisecond)
				}
				inserter.Insert(t, protocol.InsertParams{Jobs: jobs})
			}

			worked := env.DB.WaitJobCount(t, 2*jobsPerInserter, 30*time.Second, "state = 'completed'")
			perWorker := map[string]int{}
			for _, job := range worked {
				require.Equal(t, 1, job.Attempt, "job %d ran more than once", job.ID)
				require.Len(t, job.AttemptedBy, 1)
				require.Empty(t, job.Errors)
				perWorker[job.AttemptedBy[0]]++
			}
			t.Logf("competition split: %v", perWorker)
			for _, clientID := range clientIDs {
				require.Positive(t, perWorker[clientID], "%s claimed no jobs", clientID)
			}
		})
	})

	// Clients that share a queue while each knows only its own kind, the
	// deployment Go's FetchOnlyKnownKinds exists for. The first client starts
	// alone with jobs of the other's kind ahead of its own in claim order,
	// works its own, and leaves the others available with no attempt used.
	// The second then starts and works the rest, and jobs of both kinds
	// inserted while both run go to the client that knows their kind.
	t.Run("HeterogeneousFleet", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, first, second *Adapter) {
			firstKind, secondKind := protocol.KindEchoPeer, protocol.KindEcho
			clientIDs := map[string]string{firstKind: "fleet-first", secondKind: "fleet-second"}
			jobs := map[string][]int64{}
			insert := func(kind string) {
				for i := range 3 {
					jobs[kind] = append(jobs[kind], env.DB.InsertRaw(t, RawJob{Args: &protocol.Args{Message: fmt.Sprintf("fleet %s %d", kind, i)}, Kind: kind}))
				}
			}
			// Lower IDs are claimed first, so a client that ignored the kind
			// filter would claim the other kind's jobs before its own.
			insert(secondKind)
			insert(firstKind)

			first.Start(t, protocol.StartParams{ClientID: clientIDs[firstKind], FetchOnlyKnownKinds: true, MaxWorkers: 1, WorkerKinds: []string{firstKind}})
			for _, id := range jobs[firstKind] {
				requireWorkedOnceBy(t, env.DB.WaitJob(t, id, workWait), clientIDs[firstKind])
			}
			for _, id := range jobs[secondKind] {
				requireUnclaimed(t, env, id)
			}

			second.Start(t, protocol.StartParams{ClientID: clientIDs[secondKind], FetchOnlyKnownKinds: true, MaxWorkers: 1, WorkerKinds: []string{secondKind}})
			insert(firstKind)
			insert(secondKind)
			for kind, kindIDs := range jobs {
				for _, id := range kindIDs {
					worked := env.DB.WaitJob(t, id, workWait)
					requireWorkedOnceBy(t, worked, clientIDs[kind])
					require.Equal(t, kind, worked.Kind)
				}
			}
		})
	})

	// A safe kind rename, as Go's JobArgsWithKindAliases supports: a worker
	// registered under the new kind with the old one as an alias works jobs
	// of both kinds the other implementation wrote, first with an ordinary
	// client and then with one that fetches only known kinds, whose claim
	// filter must include the alias, while a job of a kind it doesn't know
	// stays untouched.
	t.Run("KindAlias", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				for _, fetchOnlyKnownKinds := range []bool{false, true} {
					env.DB.Exec(t, "DELETE FROM river_job")
					oldKind := env.DB.InsertRaw(t, RawJob{Kind: protocol.KindEcho})
					newKind := env.DB.InsertRaw(t, RawJob{Kind: protocol.KindEchoRenamed})
					unknown := env.DB.InsertRaw(t, RawJob{Kind: protocol.KindEchoPeer})

					worker.Start(t, protocol.StartParams{
						ClientID: "renamed-worker", FetchOnlyKnownKinds: fetchOnlyKnownKinds, MaxWorkers: 2, WorkerKinds: []string{protocol.KindEchoRenamed},
					})
					for _, id := range []int64{oldKind, newKind} {
						requireWorkedOnceBy(t, env.DB.WaitJob(t, id, workWait), "renamed-worker")
					}
					require.Equal(t, protocol.KindEcho, env.DB.MustJob(t, oldKind).Kind)
					require.Equal(t, protocol.KindEchoRenamed, env.DB.MustJob(t, newKind).Kind)
					if fetchOnlyKnownKinds {
						requireUnclaimed(t, env, unknown)
					}
					worker.Stop(t, protocol.StopParams{})
				}
			}
		})
	})

	// A leader that knows only some kinds rescues jobs a client that knew
	// others abandoned, as happens when implementations with disjoint workers
	// share a database. A process that works both kinds dies holding one job
	// of each, and each implementation in turn leads with a worker for one
	// kind only. Like Go's rescuer, it retries the job of the kind it knows
	// on its retry policy and discards the one it doesn't, leaving both as
	// Go does.
	t.Run("RescuerUnknownKind", func(t *testing.T) {
		t.Parallel()

		type outcome struct {
			Attempt     int
			AttemptedBy []string
			Errors      []string
			Finalized   bool
			Kind        string
			MaxAttempts int
			RescueCount any
			// RetryDelay is the delay from the rescue to the job's new
			// scheduled_at, to the second, or zero when the rescue left
			// scheduled_at unchanged.
			RetryDelay time.Duration
			State      string
		}
		const (
			rescueAfter = time.Second
			retryDelay  = time.Minute
		)
		rescue := func(t *testing.T, env *Env, leader *Adapter) map[string]outcome {
			t.Helper()

			running := map[string]*protocol.Job{}
			for _, kind := range []string{protocol.KindEcho, protocol.KindEchoPeer} {
				job := env.Reference.InsertJob(t, withDuration(withOpts(echo("rescuer kinds "+kind, protocol.BehaviorSleep),
					protocol.InsertOpts{MaxAttempts: 3, Queue: "rescuer_kinds"}), time.Minute))
				if kind != protocol.KindEcho {
					env.DB.SetKind(t, job.ID, kind)
				}
				running[kind] = job
			}
			crasher := env.StartAdapter(t, env.Reference.Implementation)
			crasher.Start(t, protocol.StartParams{
				ClientID: "rescuer-kinds-crasher", LeaderElectionDisabled: true, MaxWorkers: 2, Queues: []string{"rescuer_kinds"},
				WorkerKinds: []string{protocol.KindEcho, protocol.KindEchoPeer},
			})
			for kind, job := range running {
				running[kind] = env.DB.WaitJob(t, job.ID, workWait, "running")
			}
			crasher.Kill(t)
			for _, job := range running {
				waitUntilRescuable(t, job, rescueAfter)
			}

			// The leader knows only the peer kind, so the echo kind is
			// unknown to it.
			leader.Start(t, protocol.StartParams{
				ClientID: "rescuer-kinds-leader", JobTimeoutMS: rescueAfter.Milliseconds(), MaxWorkers: 1,
				RescueAfterMS: rescueAfter.Milliseconds(), RetryDelayMS: retryDelay.Milliseconds(), Tuning: fastTuning,
				WorkerKinds: []string{protocol.KindEchoPeer},
			})
			rescued := map[string]*protocol.Job{
				protocol.KindEcho:     env.DB.WaitJob(t, running[protocol.KindEcho].ID, maintenanceWait, "discarded"),
				protocol.KindEchoPeer: env.DB.WaitJob(t, running[protocol.KindEchoPeer].ID, maintenanceWait, "retryable"),
			}
			leader.Stop(t, protocol.StopParams{})

			outcomes := map[string]outcome{}
			for kind, job := range rescued {
				require.Len(t, job.Errors, 1, "%s rescue of %s", leader.Label, kind)
				result := outcome{
					Attempt: job.Attempt, AttemptedBy: job.AttemptedBy, Finalized: job.FinalizedAt != nil, Kind: job.Kind,
					MaxAttempts: job.MaxAttempts, RescueCount: job.Metadata["river:rescue_count"], State: job.State,
				}
				for _, attemptError := range job.Errors {
					result.Errors = append(result.Errors, fmt.Sprintf("%d %s %q", attemptError.Attempt, attemptError.Error, attemptError.Trace))
				}
				if !job.ScheduledAt.Equal(running[kind].ScheduledAt) {
					result.RetryDelay = job.ScheduledAt.Sub(job.Errors[0].At).Round(time.Second)
				}
				outcomes[kind] = result
			}
			return outcomes
		}

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			reference := rescue(t, env, env.Reference)
			require.Equal(t, "discarded", reference[protocol.KindEcho].State)
			require.True(t, reference[protocol.KindEcho].Finalized)
			require.Zero(t, reference[protocol.KindEcho].RetryDelay)
			require.Equal(t, "retryable", reference[protocol.KindEchoPeer].State)
			require.False(t, reference[protocol.KindEchoPeer].Finalized)
			require.Equal(t, retryDelay, reference[protocol.KindEchoPeer].RetryDelay)

			other := env.Another(t)
			require.Equal(t, reference, rescue(t, other, other.Candidate), "the implementations' rescuers left abandoned jobs differently")
		})
	})

	// A job whose kind has no worker is fetched and failed with River's
	// unknown-kind error rather than skipped. The error is retryable, so a
	// job with attempts left is retried: the first retry delay, about a
	// second, is inside the scheduler interval, so the job is made available
	// again at once, and the second, about sixteen seconds, isn't, so it
	// then waits as retryable.
	t.Run("UnknownKind", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			const kind = "conformance_unregistered"
			discarded := env.DB.InsertRaw(t, RawJob{Kind: kind, MaxAttempts: 1})
			retried := env.DB.InsertRaw(t, RawJob{Kind: kind, MaxAttempts: 5})
			worker.Start(t, protocol.StartParams{ClientID: "unknown-kind", MaxWorkers: 1})
			known := inserter.InsertJob(t, echo("known kind", protocol.BehaviorComplete))
			requireWorkedOnceBy(t, env.DB.WaitJob(t, known.ID, workWait), "unknown-kind")

			failed := env.DB.WaitJob(t, discarded, workWait, "discarded")
			require.Equal(t, 1, failed.Attempt)
			require.Equal(t, []string{"unknown-kind"}, failed.AttemptedBy)
			require.Len(t, failed.Errors, 1)
			require.Equal(t, errorUnknownKind+kind, failed.Errors[0].Error)

			retryable := env.DB.WaitJob(t, retried, workWait, "retryable")
			require.Equal(t, 2, retryable.Attempt)
			require.Equal(t, []string{"unknown-kind", "unknown-kind"}, retryable.AttemptedBy)
			require.Len(t, retryable.Errors, 2)
			for _, attemptError := range retryable.Errors {
				require.Equal(t, errorUnknownKind+kind, attemptError.Error)
			}
			require.Nil(t, retryable.FinalizedAt)
		})
	})
}
