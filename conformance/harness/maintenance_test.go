package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// TestMaintenance covers maintenance one implementation's leader performs on
// rows the other wrote, where both must reach the same result.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestMaintenance(t *testing.T) {
	t.Parallel()

	// One implementation's leader inserts a unique run-on-start periodic job,
	// and a later leader of the other must skip its own run-on-start insert
	// as a duplicate. It only does when both compute the same unique key and
	// states for the job.
	t.Run("PeriodicUnique", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, first, second *Adapter) {
			start := func(leader *Adapter, clientID string) {
				leader.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1, PeriodicRunOnStart: true, PeriodicUnique: true})
				require.Equal(t, clientID, env.DB.WaitLeader(t, "").LeaderID)
				leader.WaitStats(t, "the periodic enqueuer starting", func(stats *protocol.StatsResult) bool { return stats.PeriodicStarts == 1 })
			}

			start(first, "first-periodic-leader")
			periodic := waitPeriodicJobs(t, env, protocol.PeriodicJobID, 1)[0]
			require.Equal(t, "completed", env.DB.WaitJob(t, periodic.ID, workWait).State)
			first.Stop(t, protocol.StopParams{})

			start(second, "second-periodic-leader")
			// Each leader inserts a non-unique marker job after the unique
			// one, so once the second leader's marker exists, its insert of
			// the unique job was attempted.
			waitPeriodicJobs(t, env, protocol.PeriodicMarkerJobID, 2)
			periodicJobs := periodicJobs(t, env, protocol.PeriodicJobID)
			require.Len(t, periodicJobs, 1, "the second leader inserted a unique periodic job the first had already inserted")
			require.Equal(t, periodic.ID, periodicJobs[0].ID)
		})
	})

	// A leader's scheduler handles due retries of unique jobs as River Go's
	// does. The reference prepares the same retryable jobs for each
	// implementation's leader: a unique job whose key another live job holds,
	// two unique jobs sharing a key with none live, and a job that isn't
	// unique. The leader discards the conflicting job and the later of the
	// two duplicates, marking each with unique_key_conflict, and makes the
	// others available.
	t.Run("SchedulerUniqueConflict", func(t *testing.T) {
		t.Parallel()

		type outcome struct {
			Attempt           int
			Finalized         bool
			State             string
			UniqueKeyConflict any
		}
		const queue = "scheduler_discard"
		uniqueOpts := protocol.InsertOpts{
			MaxAttempts: 3, Queue: queue,
			Unique: &protocol.UniqueOpts{ByArgs: true, ByState: []string{"available", "pending", "running", "scheduled"}},
		}
		schedule := func(t *testing.T, env *Env, leader *Adapter) map[string]outcome {
			t.Helper()

			// The retry delay exceeds River Go's scheduler interval, so the
			// retries stay retryable until a scheduler makes them due.
			env.Reference.Start(t, protocol.StartParams{
				ClientID: "scheduler-setup", LeaderElectionDisabled: true, MaxWorkers: 1, Queues: []string{queue}, RetryDelayMS: 5_500,
			})
			insertRetryable := func(message string, opts protocol.InsertOpts) *protocol.Job {
				job := env.Reference.InsertJob(t, withOpts(echo(message, protocol.BehaviorError), opts))
				return env.DB.WaitJob(t, job.ID, workWait, "retryable")
			}
			jobs := map[string]*protocol.Job{
				"conflict":         insertRetryable("conflict", uniqueOpts),
				"duplicate first":  insertRetryable("duplicate", uniqueOpts),
				"duplicate second": insertRetryable("duplicate", uniqueOpts),
				"not unique":       insertRetryable("not unique", protocol.InsertOpts{MaxAttempts: 3, Queue: queue}),
			}
			env.Reference.Stop(t, protocol.StopParams{})
			require.NotEqual(t, jobs["duplicate first"].ID, jobs["duplicate second"].ID, "a retryable job outside its unique states blocked insertion")
			// A live job takes the conflicting job's key. Nothing works its
			// queue.
			holder := env.Reference.InsertJob(t, withOpts(echo("conflict", protocol.BehaviorError), uniqueOpts))
			require.NotEqual(t, jobs["conflict"].ID, holder.ID)
			require.Equal(t, "available", holder.State)

			var latest time.Time
			for _, job := range jobs {
				if job.ScheduledAt.After(latest) {
					latest = job.ScheduledAt
				}
			}
			time.Sleep(time.Until(latest.Add(100 * time.Millisecond)))
			leader.Start(t, protocol.StartParams{ClientID: "scheduler-leader", MaxWorkers: 1, Tuning: fastTuning})
			expectedStates := map[string]string{
				"conflict": "discarded", "duplicate first": "available", "duplicate second": "discarded", "not unique": "available",
			}
			outcomes := map[string]outcome{}
			for name, job := range jobs {
				scheduled := env.DB.WaitJob(t, job.ID, maintenanceWait, expectedStates[name])
				outcomes[name] = outcome{
					Attempt: scheduled.Attempt, Finalized: scheduled.FinalizedAt != nil, State: scheduled.State,
					UniqueKeyConflict: scheduled.Metadata["unique_key_conflict"],
				}
			}
			leader.Stop(t, protocol.StopParams{})
			require.Equal(t, "available", env.DB.MustJob(t, holder.ID).State, "the scheduler changed the live job holding the key")
			return outcomes
		}

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			reference := schedule(t, env, env.Reference)
			require.Equal(t, "scheduler_discarded", reference["conflict"].UniqueKeyConflict)
			require.True(t, reference["conflict"].Finalized)
			require.Equal(t, "scheduler_discarded", reference["duplicate second"].UniqueKeyConflict)
			require.Nil(t, reference["duplicate first"].UniqueKeyConflict)

			other := env.Another(t)
			require.Equal(t, reference, schedule(t, other, other.Candidate), "the implementations' schedulers left due retries differently")
		})
	})
}
