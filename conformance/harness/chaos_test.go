package harness

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// errorRescued is the attempt error River records for a rescued job.
const errorRescued = "Stuck job rescued by JobRescuer"

// errorUndecodable prefixes the attempt error River records for a claimed row
// it can't decode.
const errorUndecodable = "job row couldn't be decoded: "

// TestChaos is the nightly tier's faults: killed processes, lost
// connections and notifications, failing statements, and rows an
// implementation can't decode. Every implementation must keep working
// through them and reach River Go's job states.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestChaos(t *testing.T) {
	t.Parallel()

	RequireNightly(t)

	// A completion waits on another transaction's lock on the job's row and
	// then finishes the job.
	t.Run("CompletionRowLock", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			ctx := context.Background()
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				worker.Start(t, protocol.StartParams{ClientID: "row-lock", MaxWorkers: 1})
				inserted := env.Reference.InsertJob(t, echo("row-lock "+worker.Label, protocol.BehaviorBarrierWait))
				env.DB.WaitJob(t, inserted.ID, workWait, "running")

				locker, err := env.DB.Pool(t).Begin(ctx)
				require.NoError(t, err)
				_, err = locker.Exec(ctx, "SELECT 1 FROM river_job WHERE id = $1 FOR UPDATE", inserted.ID)
				require.NoError(t, err)
				worker.Release(t, "row-lock "+worker.Label)
				env.DB.WaitLockWait(t, worker)
				require.NoError(t, locker.Commit(ctx))

				completed := env.DB.WaitJob(t, inserted.ID, workWait)
				require.Equal(t, "completed", completed.State, worker.Label)
				require.Equal(t, 1, completed.Attempt, worker.Label)
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// A completion that fails with a serialization failure is retried and
	// the job completes in its one attempt.
	t.Run("CompletionTransientFailure", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				// The sequence advances outside the aborted statement, so
				// exactly one completion fails.
				env.DB.Exec(t, `
					CREATE SEQUENCE completion_fault;
					CREATE FUNCTION fail_completion_once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
						IF OLD.state = 'running' AND NEW.state = 'completed' AND nextval('completion_fault') = 1 THEN
							RAISE EXCEPTION 'injected completion failure' USING ERRCODE = '40001';
						END IF;
						RETURN NEW;
					END $$;
					CREATE TRIGGER fail_completion_once BEFORE UPDATE ON river_job FOR EACH ROW EXECUTE FUNCTION fail_completion_once()`)

				worker.Start(t, protocol.StartParams{ClientID: "completion-retry", MaxWorkers: 1})
				inserted := env.Reference.InsertJob(t, echo("transient completion failure", protocol.BehaviorComplete))
				completed := env.DB.WaitJob(t, inserted.ID, 30*time.Second)
				require.Equal(t, "completed", completed.State, worker.Label)
				require.Equal(t, 1, completed.Attempt, worker.Label)
				require.Empty(t, completed.Errors, worker.Label)
				var faults int64
				env.DB.QueryRow(t, "SELECT last_value FROM completion_fault", nil, &faults)
				require.GreaterOrEqual(t, faults, int64(2), "%s: the injected failure never fired", worker.Label)
				worker.Stop(t, protocol.StopParams{})
				env.DB.Exec(t, `DROP TRIGGER fail_completion_once ON river_job; DROP FUNCTION fail_completion_once(); DROP SEQUENCE completion_fault`)
			}
		})
	})

	// The database becomes unreachable for a worker: its connections reset
	// and new ones are refused. The job it was working finishes while its
	// completion can't be written, and new work arrives that it can't see.
	// Once the database is back, both complete in one attempt.
	t.Run("DatabaseUnavailable", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			for _, implementation := range []*Implementation{env.Reference.Implementation, env.Candidate.Implementation} {
				proxy := startFaultProxy(t, env.DB.adapterURL)
				worker := env.StartAdapterURL(t, implementation, proxy.url)
				worker.Start(t, protocol.StartParams{ClientID: "outage"})
				barrier := "outage " + worker.Label
				inFlight := env.Reference.InsertJob(t, echo(barrier, protocol.BehaviorBarrierWait))
				env.DB.WaitJob(t, inFlight.ID, workWait, "running")

				proxy.takeDown()
				worker.Release(t, barrier)
				during := env.Reference.InsertJob(t, echo("inserted during the outage", protocol.BehaviorComplete))
				proxy.waitForRejections(t, 3)
				proxy.restore()

				for _, id := range []int64{inFlight.ID, during.ID} {
					job := env.DB.WaitJob(t, id, time.Minute)
					require.Equal(t, "completed", job.State, worker.Label)
					require.Equal(t, 1, job.Attempt, "%s job %d was rescued or retried", worker.Label, id)
					require.Empty(t, job.Errors, worker.Label)
				}
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// A claimed row an implementation can't decode doesn't strand the rows
	// claimed with it. Like River Go, an implementation fails the row's
	// attempt without working it: the error handler sees it, the attempt
	// error starts with "job row couldn't be decoded: ", the job is retried
	// on the client's retry policy or discarded at its maximum attempts, and
	// the undecodable value is left as it was. Array metadata is valid for
	// River Go but not every implementation decodes it, so each either works
	// such a row or fails it this way. Attempt errors in shapes River doesn't
	// write decode leniently and are never rewritten.
	t.Run("DecodeIsolation", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			const oddErrors = `ARRAY['{"at": "2024-01-02 03:04:05+00", "attempt": "1", "error": {"message": "boom"}, "trace": ["frame"]}'::jsonb, '42'::jsonb]`
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				env.DB.Exec(t, "DELETE FROM river_job")
				ordinary := env.Reference.InsertJob(t, echo("ordinary", protocol.BehaviorComplete))
				sparse := env.Reference.InsertJob(t, echo("sparse errors", protocol.BehaviorComplete))
				env.DB.Exec(t, `UPDATE river_job SET errors = ARRAY['{"error": "sparse", "extra": true}'::jsonb] WHERE id = $1`, sparse.ID)
				odd := env.Reference.InsertJob(t, echo("odd errors", protocol.BehaviorComplete))
				env.DB.Exec(t, `UPDATE river_job SET errors = `+oddErrors+` WHERE id = $1`, odd.ID)
				retried := env.Reference.InsertJob(t, echo("array metadata retried", protocol.BehaviorComplete))
				discarded := env.Reference.InsertJob(t, withOpts(echo("array metadata discarded", protocol.BehaviorComplete), protocol.InsertOpts{MaxAttempts: 1}))
				env.DB.Exec(t, `UPDATE river_job SET metadata = '[1]' WHERE id IN ($1, $2)`, retried.ID, discarded.ID)

				worker.Start(t, protocol.StartParams{ClientID: "decode", RetryDelayMS: time.Hour.Milliseconds()})
				for _, id := range []int64{ordinary.ID, sparse.ID, odd.ID} {
					worked := env.DB.WaitJob(t, id, workWait)
					require.Equal(t, "completed", worked.State, "%s job %d", worker.Label, id)
					require.Equal(t, 1, worked.Attempt, "%s job %d", worker.Label, id)
				}
				require.Equal(t, []protocol.AttemptError{
					{Attempt: 1, Error: `{"message":"boom"}`, Trace: `["frame"]`},
					{Error: "42"},
				}, listOne(t, worker, odd.ID).Errors, "%s decodes odd attempt errors differently", worker.Label)
				var oddText, expectedText string
				env.DB.QueryRow(t, "SELECT errors::text, ("+oddErrors+")::text FROM river_job WHERE id = $1", []any{odd.ID}, &oddText, &expectedText)
				require.Equal(t, expectedText, oddText, "%s rewrote attempt errors it only read", worker.Label)

				failed := 0
				for id, failedState := range map[int64]string{retried.ID: "retryable", discarded.ID: "discarded"} {
					if requireUndecodableOutcome(t, env, worker, id, "[1]", failedState) {
						failed++
					}
				}
				stats := worker.WaitStats(t, "every row finishing", func(stats *protocol.StatsResult) bool {
					return CountEvents(stats, "job_completed") == 3+2-failed && CountEvents(stats, "job_failed") == failed
				})
				require.Zero(t, stats.ErrorHandlerCalls, worker.Label)
				worker.Stop(t, protocol.StopParams{})

				// The error handler sees an undecodable row's failed attempt,
				// and its decision applies to the row.
				handled := env.Reference.InsertJob(t, echo("array metadata handled", protocol.BehaviorComplete))
				env.DB.Exec(t, `UPDATE river_job SET metadata = '[1]' WHERE id = $1`, handled.ID)
				afterHandled := env.Reference.InsertJob(t, echo("ordinary after the handler", protocol.BehaviorComplete))
				worker.Start(t, protocol.StartParams{ClientID: "decode-handler", ErrorHandlerCancel: true})
				env.DB.WaitJob(t, afterHandled.ID, workWait)
				handlerCalls := 0
				if requireUndecodableOutcome(t, env, worker, handled.ID, "[1]", "cancelled") {
					handlerCalls = 1
				}
				worker.WaitStats(t, "the error handler", func(stats *protocol.StatsResult) bool { return stats.ErrorHandlerCalls == handlerCalls })
				worker.Stop(t, protocol.StopParams{})
			}
		})

		// On SQLite, a JSON column changed out of band to text that isn't
		// JSON doesn't stall its queue. The value is left in place, except
		// that errors that aren't JSON are wrapped in an array as a string,
		// so the attempt error can still be appended.
		EachDriver(t, &EnvOpts{Drivers: []string{DriverSQLite}}, func(t *testing.T, env *Env) {
			columns := []string{"args", "attempted_by", "errors", "metadata", "tags"}
			for _, worker := range []*Adapter{env.Candidate, env.Reference} {
				env.DB.Exec(t, "DELETE FROM river_job")
				ordinary := env.Reference.InsertJob(t, echo("ordinary", protocol.BehaviorComplete))
				invalid := map[string]int64{}
				originals := map[string]*string{}
				for _, column := range columns {
					id := env.Reference.InsertJob(t, echo("invalid "+column, protocol.BehaviorComplete)).ID
					var original *string
					env.DB.QueryRow(t, "SELECT json("+column+") FROM river_job WHERE id = ?", []any{id}, &original)
					env.DB.Exec(t, "UPDATE river_job SET "+column+" = 'not json' WHERE id = ?", id)
					invalid[column], originals[column] = id, original
				}

				worker.Start(t, protocol.StartParams{ClientID: "invalid-json", RetryDelayMS: time.Hour.Milliseconds()})
				env.DB.WaitJob(t, ordinary.ID, workWait)
				worker.WaitStats(t, "every invalid row failing", func(stats *protocol.StatsResult) bool {
					return CountEvents(stats, "job_failed") == len(columns)
				})
				worker.Stop(t, protocol.StopParams{})

				for _, column := range columns {
					id := invalid[column]
					if column != "errors" {
						var left, leftType string
						env.DB.QueryRow(t, "SELECT CAST("+column+" AS TEXT), typeof("+column+") FROM river_job WHERE id = ?", []any{id}, &left, &leftType)
						require.Equal(t, "text", leftType, "%s %s", worker.Label, column)
						require.Equal(t, "not json", left, "%s rewrote invalid %s", worker.Label, column)
						env.DB.Exec(t, "UPDATE river_job SET "+column+" = jsonb(?) WHERE id = ?", originals[column], id)
					}

					// Read the way River Go reads rows.
					failed := listOne(t, env.Reference, id)
					require.Equal(t, "retryable", failed.State, "%s %s", worker.Label, column)
					require.Equal(t, 1, failed.Attempt, "%s %s", worker.Label, column)
					require.NotEmpty(t, failed.Errors, "%s %s", worker.Label, column)
					attemptError := failed.Errors[len(failed.Errors)-1]
					require.Equal(t, 1, attemptError.Attempt, "%s %s", worker.Label, column)
					require.True(t, strings.HasPrefix(attemptError.Error, errorUndecodable), "%s %s: %s", worker.Label, column, attemptError.Error)
					if column == "errors" {
						require.Len(t, failed.Errors, 2, worker.Label)
						require.Equal(t, "not json", failed.Errors[0].Error, worker.Label)
					}
				}
			}
		})
	})

	// The leading process of one implementation dies and the other takes
	// over. Both configure the same run-on-start periodic job, so the
	// periodic jobs and each one's periodic enqueuer starts show that exactly
	// one runs leader-only maintenance in each term.
	t.Run("LeaderDeath", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, leaderKind, follower *Adapter) {
			leader := env.StartAdapter(t, leaderKind.Implementation)
			leader.Start(t, protocol.StartParams{ClientID: "dying-leader", MaxWorkers: 1, PeriodicRunOnStart: true})
			require.Equal(t, "dying-leader", env.DB.WaitLeader(t, "").LeaderID)
			leader.WaitStats(t, "the periodic enqueuer starting", func(stats *protocol.StatsResult) bool { return stats.PeriodicStarts == 1 })
			waitPeriodicJobs(t, env, protocol.PeriodicJobID, 1)

			follower.Start(t, protocol.StartParams{ClientID: "surviving-follower", MaxWorkers: 1, PeriodicRunOnStart: true})
			// Working a job gives a follower that wrongly started leader-only
			// maintenance time to show it.
			marker := follower.InsertJob(t, echo("follower running", protocol.BehaviorComplete))
			env.DB.WaitJob(t, marker.ID, workWait)
			require.Zero(t, follower.Stats(t).PeriodicStarts, "a follower ran the leader-only periodic enqueuer")
			require.Equal(t, "dying-leader", env.DB.WaitLeader(t, "").LeaderID)
			require.Len(t, periodicJobs(t, env, protocol.PeriodicJobID), 1)

			leader.Kill(t)
			// The dead leader can't resign; expiring its lease stands in for
			// it running out.
			env.DB.ExpireLeader(t)
			require.Equal(t, "surviving-follower", env.DB.WaitLeader(t, "dying-leader").LeaderID)
			follower.WaitStats(t, "the periodic enqueuer starting", func(stats *protocol.StatsResult) bool { return stats.PeriodicStarts == 1 })
			for _, job := range waitPeriodicJobs(t, env, protocol.PeriodicJobID, 2) {
				require.Equal(t, true, job.Metadata["periodic"])
			}
			// One periodic job per term, even after more work.
			marker = follower.InsertJob(t, echo("after the takeover", protocol.BehaviorComplete))
			env.DB.WaitJob(t, marker.ID, workWait)
			require.Len(t, periodicJobs(t, env, protocol.PeriodicJobID), 2)
			require.Equal(t, "surviving-follower", env.DB.WaitLeader(t, "").LeaderID)
		})
	})

	// A worker's listener backend, then all of its connections, are
	// terminated, and after each fault an insert by the other implementation
	// wakes it through a notification.
	t.Run("ListenerReconnect", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env, worker, controller *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "reconnect", FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 1})
			env.DB.WaitListening(t, worker)
			requireNotificationRoundTrip(t, env, controller, "before the fault")

			require.GreaterOrEqual(t, env.DB.TerminateConnections(t, worker, true), 1)
			env.DB.WaitListening(t, worker)
			requireNotificationRoundTrip(t, env, controller, "after the listener fault")

			require.GreaterOrEqual(t, env.DB.TerminateConnections(t, worker, false), 1)
			env.DB.WaitListening(t, worker)
			requireNotificationRoundTrip(t, env, controller, "after the connection fault")
		})
	})

	// A job inserted without a notification is found by polling.
	t.Run("LostNotification", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				worker.Start(t, protocol.StartParams{ClientID: "poll-recovery", FetchPollIntervalMS: 250, MaxWorkers: 1})
				id := env.DB.InsertRaw(t, RawJob{})
				requireWorkedOnceBy(t, env.DB.WaitJob(t, id, workWait), "poll-recovery")
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// A process of one implementation dies holding a running attempt, and
	// the other takes over leadership, rescues the attempt, and completes the
	// job.
	t.Run("ProcessKillRescue", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, crashingKind, recovery *Adapter) {
			requireProcessKillRescue(t, env, crashingKind.Implementation, recovery)
		})
	})

	// A process dies holding a running attempt, and a restarted process of
	// the same implementation rescues and completes it.
	t.Run("ProcessKillRestart", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			for _, implementation := range []*Implementation{env.Reference.Implementation, env.Candidate.Implementation} {
				env.DB.Exec(t, "DELETE FROM river_job")
				requireProcessKillRescue(t, env, implementation, env.StartAdapter(t, implementation))
			}
		})
	})

	// Every process is replaced in turn while both implementations keep
	// inserting and working jobs, which is the skew a rolling deploy of
	// mixed implementations produces, and every job completes exactly once.
	t.Run("RollingDeployment", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			type deployment struct {
				adapter        *Adapter
				implementation *Implementation
				version        int
			}
			clientID := func(current *deployment) string {
				return fmt.Sprintf("%s-rolling-%d", current.implementation.Name, current.version)
			}
			deployments := []*deployment{
				{implementation: env.Reference.Implementation},
				{implementation: env.Candidate.Implementation},
			}
			for _, current := range deployments {
				current.adapter = env.StartAdapter(t, current.implementation)
				current.adapter.Start(t, protocol.StartParams{ClientID: clientID(current), MaxWorkers: 4})
			}
			var ids []int64
			insertBatch := func(step string) {
				for i := range 20 {
					job := deployments[i%len(deployments)].adapter.InsertJob(t, withDuration(echo(fmt.Sprintf("rolling %s %d", step, i), protocol.BehaviorSleep), 20*time.Millisecond))
					ids = append(ids, job.ID)
				}
			}
			insertBatch("initial")
			for _, current := range deployments {
				// Stop the old process gracefully, insert while it's gone,
				// then bring up a new process of the same implementation.
				current.adapter.Stop(t, protocol.StopParams{})
				insertBatch("without " + clientID(current))
				current.version++
				current.adapter = env.StartAdapter(t, current.implementation)
				current.adapter.Start(t, protocol.StartParams{ClientID: clientID(current), MaxWorkers: 4})
				insertBatch("with " + clientID(current))
			}

			completed := env.DB.WaitJobCount(t, len(ids), 30*time.Second, "state = 'completed'")
			workers := map[string]int{}
			for _, job := range completed {
				require.Equal(t, 1, job.Attempt, "job %d ran more than once", job.ID)
				require.Len(t, job.AttemptedBy, 1)
				require.Empty(t, job.Errors)
				workers[job.AttemptedBy[0]]++
			}
			t.Logf("rolling deployment work split: %v", workers)
			for _, current := range deployments {
				require.Positive(t, workers[clientID(current)], "%s did no work after its replacement", clientID(current))
			}
			require.Contains(t, []string{clientID(deployments[0]), clientID(deployments[1])}, env.DB.WaitLeader(t, "").LeaderID,
				"leadership must end with a replacement process")
		})
	})

	// A foreign transaction holds SQLite's write lock past the adapters'
	// busy timeout while a job finishes, so the first completion write
	// fails, and the job still completes.
	t.Run("SQLiteWriterLock", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverSQLite}}, func(t *testing.T, env *Env) {
			ctx := context.Background()
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				worker.Start(t, protocol.StartParams{ClientID: "writer-lock"})
				barrier := "writer-lock " + worker.Label
				inserted := env.Reference.InsertJob(t, echo(barrier, protocol.BehaviorBarrierWait))
				env.DB.WaitJob(t, inserted.ID, workWait, "running")

				conn, err := env.DB.SQLite(t).Conn(ctx)
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
				require.NoError(t, err)
				worker.Release(t, barrier)
				time.Sleep(6 * time.Second) // The fault is the lock's duration, not a wait for an outcome.
				_, err = conn.ExecContext(ctx, "ROLLBACK")
				require.NoError(t, err)
				require.NoError(t, conn.Close())

				require.Equal(t, "completed", env.DB.WaitJob(t, inserted.ID, time.Minute).State, worker.Label)
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// Both implementations run on Postgres made to look like YugabyteDB
	// without LISTEN/NOTIFY, as River Go's own tests simulate it: a schema
	// ahead of pg_catalog shadows version() and current_setting() with a
	// Yugabyte version lacking yb_enable_listen_notify, and pg_notify with a
	// function that raises, so any notification fails the operation sending
	// it. Each implementation must detect the server itself: write unique
	// jobs with a nonce, since Yugabyte lacks xmax, so the other's duplicate
	// insert returns the same job; send no notifications; and, without being
	// configured to only poll, notice the other's cancellation of its running
	// job by polling.
	t.Run("SimulatedYugabyte", func(t *testing.T) {
		t.Parallel()

		opts := &EnvOpts{
			Drivers:    []string{DriverPostgres},
			SearchPath: []string{"pg_catalog"},
			Setup: func(t *testing.T, db *Database) {
				t.Helper()

				db.Exec(t, `
					CREATE FUNCTION version() RETURNS text LANGUAGE sql AS $$ SELECT 'PostgreSQL 15.12-YB-2025.2.1.0-b1'::text $$;
					CREATE FUNCTION current_setting(setting_name text, missing_ok boolean) RETURNS text LANGUAGE sql AS $$
						SELECT CASE WHEN setting_name = 'yb_enable_listen_notify' THEN NULL::text
						ELSE pg_catalog.current_setting(setting_name, missing_ok) END $$;
					CREATE FUNCTION pg_notify(text, text) RETURNS void LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END $$;`)
			},
		}
		EachDirection(t, opts, func(t *testing.T, env *Env, controller, worker *Adapter) {
			unique := withOpts(echo("simulated yugabyte unique", protocol.BehaviorComplete), protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByArgs: true}})
			inserted := controller.InsertJob(t, unique)
			var hasNonce bool
			env.DB.QueryRow(t, "SELECT metadata ? 'river:unique_nonce' FROM river_job WHERE id = $1", []any{inserted.ID}, &hasNonce)
			require.True(t, hasNonce, "%s inserted a unique job without a nonce", controller.Label)
			require.Equal(t, inserted.ID, worker.InsertJob(t, unique).ID, "%s inserted a duplicate", worker.Label)

			worker.Start(t, protocol.StartParams{ClientID: "yugabyte", FetchPollIntervalMS: 100, MaxWorkers: 1})
			cancellable := controller.InsertJob(t, echo("simulated yugabyte cancel", protocol.BehaviorCooperativeCancel))
			env.DB.WaitJob(t, cancellable.ID, workWait, "running")
			startedAt := time.Now()
			controller.Cancel(t, protocol.JobParams{ID: cancellable.ID})
			require.Equal(t, "cancelled", env.DB.WaitJob(t, cancellable.ID, workWait).State)
			require.Less(t, time.Since(startedAt), 6*time.Second)
		})
	})
}

// requireNotificationRoundTrip requires an insert by controller to wake a
// worker that polls once a minute. A listener that has just reconnected may
// miss a notification sent before it resubscribed, so inserts repeat until
// one wakes the worker or the bound elapses.
func requireNotificationRoundTrip(t *testing.T, env *Env, controller *Adapter, label string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		inserted := controller.InsertJob(t, echo(fmt.Sprintf("%s %d", label, attempt), protocol.BehaviorComplete))
		attemptDeadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(attemptDeadline) {
			if env.DB.MustJob(t, inserted.ID).State == "completed" {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	require.FailNowf(t, "no wakeup", "%s: %s's inserts never woke the worker", label, controller.Label)
}

// requireProcessKillRescue kills a process of crashingKind while it holds a
// running attempt, and requires recovery to take over leadership, rescue the
// attempt, and complete the job.
func requireProcessKillRescue(t *testing.T, env *Env, crashingKind *Implementation, recovery *Adapter) {
	t.Helper()

	const rescueAfter = 1_500 * time.Millisecond
	queue := "process_kill"
	crashing := env.StartAdapter(t, crashingKind)
	crashing.Start(t, protocol.StartParams{ClientID: "killed-worker", MaxWorkers: 1, Queues: []string{queue}})
	inserted := recovery.InsertJob(t, withDuration(withOpts(echo("rescue after a process dies", protocol.BehaviorSleep), protocol.InsertOpts{Queue: queue}), time.Second))
	running := env.DB.WaitJob(t, inserted.ID, workWait, "running")
	require.Equal(t, []string{"killed-worker"}, running.AttemptedBy)
	crashing.Kill(t)
	// The killed process can't resign. Expiring its lease stands in for the
	// lease running out.
	env.DB.ExpireLeader(t)
	waitUntilRescuable(t, running, rescueAfter)

	recovery.Start(t, protocol.StartParams{
		ClientID: "rescuer", JobTimeoutMS: rescueAfter.Milliseconds(), MaxWorkers: 1, Queues: []string{queue},
		RescueAfterMS: rescueAfter.Milliseconds(), Tuning: fastTuning,
	})
	require.Equal(t, "rescuer", env.DB.WaitLeader(t, "killed-worker").LeaderID)
	job := env.DB.WaitJob(t, inserted.ID, maintenanceWait)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 2, job.Attempt)
	require.Equal(t, []string{"killed-worker", "rescuer"}, job.AttemptedBy)
	require.Len(t, job.Errors, 1)
	require.Equal(t, errorRescued, job.Errors[0].Error)
	require.InDelta(t, 1, job.Metadata["river:rescue_count"], 0)
	recovery.Stop(t, protocol.StopParams{})
}

// requireUndecodableOutcome waits for a worker to finish a claimed row
// whose metadata it may not decode, and checks the outcome with SQL, since
// not every implementation can read the row back. An implementation that
// decodes the row completes it. One that can't fails the attempt as River Go
// fails an undecodable row, reaching failedState, and true is returned.
// Either way, the metadata is left as it was.
func requireUndecodableOutcome(t *testing.T, env *Env, worker *Adapter, id int64, metadata, failedState string) bool {
	t.Helper()

	var (
		attempt, errorCount   int
		lastError             *string
		lastAttempt           *string
		storedMetadata, state string
		retryLater, finalized bool
	)
	WaitFor(t, "the row finishing", workWait, func() bool {
		env.DB.QueryRow(t, `SELECT state::text, attempt, coalesce(array_length(errors, 1), 0),
				errors[array_length(errors, 1)] ->> 'error', errors[array_length(errors, 1)] ->> 'attempt',
				metadata::text, scheduled_at > now() + interval '30 minutes', finalized_at IS NOT NULL
			FROM river_job WHERE id = $1`, []any{id},
			&state, &attempt, &errorCount, &lastError, &lastAttempt, &storedMetadata, &retryLater, &finalized)
		return state != "available" && state != "running"
	})
	require.Equal(t, 1, attempt, "%s job %d", worker.Label, id)
	require.Equal(t, metadata, storedMetadata, "%s rewrote metadata it couldn't decode", worker.Label)
	if state == "completed" {
		require.Zero(t, errorCount, "%s job %d", worker.Label, id)
		return false
	}

	require.Equal(t, failedState, state, "%s job %d", worker.Label, id)
	require.Equal(t, 1, errorCount, "%s job %d", worker.Label, id)
	require.NotNil(t, lastError)
	require.True(t, strings.HasPrefix(*lastError, errorUndecodable), "%s job %d attempt error: %s", worker.Label, id, *lastError)
	require.Equal(t, "1", *lastAttempt, "%s job %d", worker.Label, id)
	switch failedState {
	case "retryable":
		require.True(t, retryLater, "%s didn't retry job %d on the client's retry policy", worker.Label, id)
	case "cancelled", "discarded":
		require.True(t, finalized, "%s job %d", worker.Label, id)
	}
	return true
}
