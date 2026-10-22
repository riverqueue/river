package harness

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// runtimeMetadataKeys are the metadata keys River's runtime writes, which
// every implementation must write by the same names.
var runtimeMetadataKeys = []string{ //nolint:gochecknoglobals // constant
	"cancel_attempted_at",
	rivercommon.MetadataKeyPeriodicJobID,
	rivercommon.MetadataKeyRescueCount,
	rivercommon.MetadataKeyResumableCursor,
	rivercommon.MetadataKeyResumableStep,
	rivertype.MetadataKeyOutput,
	riverdriver.UniqueInsertMetadataKey,
	"snoozes",
	"unique_key_conflict",
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestWork(t *testing.T) {
	t.Parallel()

	// A job's attempted_by keeps its last 100 clients, whichever
	// implementation appends to it.
	t.Run("AttemptedByHistory", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, retrier, worker *Adapter) {
			history := make([]string, 0, 101)
			for i := range 98 {
				history = append(history, fmt.Sprintf("earlier-%03d", i))
			}
			id := env.DB.InsertRaw(t, RawJob{
				Args: &protocol.Args{Behavior: protocol.BehaviorError, Message: "attempted_by history"}, Attempt: 98, AttemptedBy: history, MaxAttempts: 200,
			})
			for attempt := range 3 {
				clientID := fmt.Sprintf("history-%d", attempt)
				history = append(history, clientID)
				worker.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1, RetryDelayMS: time.Minute.Milliseconds()})
				job := env.DB.WaitJob(t, id, workWait, "retryable")
				require.Equal(t, 99+attempt, job.Attempt)
				worker.Stop(t, protocol.StopParams{})
				if attempt < 2 {
					retrier.Retry(t, protocol.JobParams{ID: id})
				}
			}
			require.Equal(t, history[len(history)-100:], env.DB.MustJob(t, id).AttemptedBy)
			require.Equal(t, history[len(history)-100:], listOne(t, retrier, id).AttemptedBy)
		})
	})

	// Runtime-owned metadata one implementation writes, for output, a snooze,
	// and a cancellation the other requested, uses River's names and types,
	// and user metadata, including another implementation's extension data
	// like Go's river:log, survives it.
	t.Run("ReservedMetadata", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, worker, controller *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "reserved-metadata", MaxWorkers: 2})
			riverLog := []any{map[string]any{"attempt": float64(1), "log": "logged by an earlier attempt"}}
			opts := protocol.InsertOpts{Metadata: metadata(t, map[string]any{"river:log": riverLog, "user": "kept"})}
			output := controller.InsertJob(t, withOpts(echo("reserved output", protocol.BehaviorOutput), opts))
			snoozed := controller.InsertJob(t, withDuration(withOpts(echo("reserved snooze", protocol.BehaviorSnoozeOnce), opts), 5*time.Millisecond))
			cancelled := controller.InsertJob(t, withOpts(echo("reserved cancel", protocol.BehaviorCooperativeCancel), opts))
			env.DB.WaitJob(t, cancelled.ID, workWait, "running")
			controller.Cancel(t, protocol.JobParams{ID: cancelled.ID})

			jobs := map[string]*protocol.Job{}
			for name, id := range map[string]int64{"output": output.ID, "snoozed": snoozed.ID, "cancelled": cancelled.ID} {
				job := env.DB.WaitJob(t, id, workWait)
				require.Equal(t, "kept", job.Metadata["user"], name)
				require.Equal(t, riverLog, job.Metadata["river:log"], name)
				for key := range job.Metadata {
					if key != "user" && key != "river:log" {
						require.Contains(t, runtimeMetadataKeys, key, "%s carries metadata key %q, which River doesn't write", name, key)
					}
				}
				jobs[name] = job
			}
			require.Equal(t, "completed", jobs["output"].State)
			require.Equal(t, map[string]any{"message": "reserved output"}, jobs["output"].Metadata["output"])
			require.Equal(t, "completed", jobs["snoozed"].State)
			require.InDelta(t, 1, jobs["snoozed"].Metadata["snoozes"], 0)
			require.Equal(t, "cancelled", jobs["cancelled"].State)
			cancelAttemptedAt, ok := jobs["cancelled"].Metadata["cancel_attempted_at"].(string)
			require.True(t, ok, "cancel_attempted_at must be a time string")
			require.Regexp(t, goTimeTextPattern, cancelAttemptedAt)
		})
	})

	// Each attempt of a resumable job runs in a different implementation, so
	// each resumes from the step and cursor the other recorded. A long retry
	// delay keeps an implementation from reclaiming the next attempt before
	// it stops.
	t.Run("ResumableCursor", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, producer, consumer *Adapter) {
			job := producer.InsertJob(t, withOpts(echo("cross-implementation cursor", protocol.BehaviorResumableCursor),
				protocol.InsertOpts{MaxAttempts: 3, Metadata: metadata(t, map[string]any{"application": "retained"})}))
			for i, worker := range []*Adapter{producer, consumer, producer} {
				worker.Start(t, protocol.StartParams{ClientID: "resumable", MaxWorkers: 1, RetryDelayMS: time.Minute.Milliseconds()})
				state := "retryable"
				if i == 2 {
					state = "completed"
				}
				job = env.DB.WaitJob(t, job.ID, workWait, state)
				worker.Stop(t, protocol.StopParams{})

				require.Equal(t, i+1, job.Attempt)
				require.Equal(t, "retained", job.Metadata["application"])
				require.InDelta(t, 1, job.Metadata["first_attempt"], 0, "a completed first step ran again")
				if i == 0 {
					require.Equal(t, "first", job.Metadata["river:resumable_step"])
					cursors, ok := job.Metadata["river:resumable_cursor"].(map[string]any)
					require.True(t, ok, "cursor metadata must be an object")
					require.InDelta(t, 7, cursors["second"], 0)
				} else {
					require.Equal(t, "second", job.Metadata["river:resumable_step"])
					require.Nil(t, job.Metadata["river:resumable_cursor"], "a consumed cursor must be cleared: %v", job.Metadata)
					require.InDelta(t, 7, job.Metadata["cursor_observed"], 0)
				}
				if i < 2 {
					consumer.Retry(t, protocol.JobParams{ID: job.ID})
				}
			}
			require.Len(t, job.Errors, 2)
		})
	})

	// A job scheduled in the future is never attempted before its time, and a
	// snooze no longer than the scheduler interval leaves the job available
	// with a future scheduled_at, which the fetch honors.
	t.Run("ScheduleBoundaries", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "schedule-boundaries", MaxWorkers: 2})

			scheduled := inserter.InsertJob(t, withOpts(echo("scheduled in the future", protocol.BehaviorComplete),
				protocol.InsertOpts{ScheduledAt: new(time.Now().Add(time.Second).UTC())}))
			require.Equal(t, "scheduled", scheduled.State)
			worked := env.DB.WaitJob(t, scheduled.ID, maintenanceWait)
			require.Equal(t, "completed", worked.State)
			require.False(t, worked.AttemptedAt.Before(worked.ScheduledAt), "attempted at %s, before scheduled at %s", worked.AttemptedAt, worked.ScheduledAt)

			// A two-second snooze is inside River's five-second scheduler
			// interval, so the job stays available with a future
			// scheduled_at.
			snoozed := inserter.InsertJob(t, withDuration(echo("short snooze", protocol.BehaviorSnoozeOnce), 2*time.Second))
			var afterSnooze *protocol.Job
			WaitFor(t, "the snooze", workWait, func() bool {
				afterSnooze = env.DB.MustJob(t, snoozed.ID)
				return afterSnooze.Metadata["snoozes"] != nil
			})
			require.InDelta(t, 1, afterSnooze.Metadata["snoozes"], 0)
			if afterSnooze.State != "completed" && afterSnooze.State != "running" {
				require.Equal(t, "available", afterSnooze.State, "a snooze within the scheduler interval stays available")
			}
			worked = env.DB.WaitJob(t, snoozed.ID, workWait)
			require.Equal(t, "completed", worked.State)
			require.False(t, worked.AttemptedAt.Before(afterSnooze.ScheduledAt), "attempted at %s, before its snooze ended at %s", worked.AttemptedAt, afterSnooze.ScheduledAt)
		})
	})

	// A snooze records the snoozes counter, gives its attempt back, and with
	// a delay beyond the scheduler interval parks the job as scheduled at the
	// snooze time. The other implementation reads every step alike.
	t.Run("Snooze", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, worker, observer *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "snooze", MaxWorkers: 1})

			short := observer.InsertJob(t, withDuration(echo("short snooze", protocol.BehaviorSnoozeOnce), 5*time.Millisecond))
			worked := env.DB.WaitJob(t, short.ID, workWait)
			require.Equal(t, "completed", worked.State)
			require.Equal(t, 1, worked.Attempt, "a snooze must not consume an attempt")
			require.InDelta(t, 1, worked.Metadata["snoozes"], 0)
			require.Empty(t, worked.Errors)

			// River's scheduler interval is five seconds, so a longer snooze
			// is stored as scheduled rather than available.
			const longSnooze = 10 * time.Second
			long := observer.InsertJob(t, withDuration(echo("long snooze", protocol.BehaviorSnoozeOnce), longSnooze))
			parked := env.DB.WaitJob(t, long.ID, workWait, "scheduled")
			require.Zero(t, parked.Attempt, "a snooze must give its attempt back")
			require.InDelta(t, 1, parked.Metadata["snoozes"], 0)
			require.Empty(t, parked.Errors)
			require.Nil(t, parked.FinalizedAt)
			require.NotNil(t, parked.AttemptedAt)
			delay := parked.ScheduledAt.Sub(*parked.AttemptedAt)
			require.GreaterOrEqual(t, delay, longSnooze-100*time.Millisecond)
			require.Less(t, delay, longSnooze+2*time.Second)
			require.Equal(t, parked, listOne(t, observer, long.ID))
			require.True(t, slices.Contains(worker.Stats(t).Events, "job_snoozed"))
		})
	})
}
