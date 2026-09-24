//go:build riverconformance

package harness_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestResilienceConformance checks that each implementation keeps working
// through database faults and reaches Go's job states on non-happy paths:
// an unavailable database, transient completion errors, row locks, hard
// shutdown, and rows another implementation may consider malformed. Faults
// are injected by the harness itself (a TCP proxy and direct SQL) rather than
// through adapter methods, so every implementation runs the same scenarios.
func TestResilienceConformance(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	scenarios := newScenarioTracker(t, scenarioOwnerResilience)
	ctx := context.Background()
	repositoryRoot := repoRoot(t)
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)

	database, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close(context.Background())) })
	observer, err := pgx.Connect(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close(context.Background())) })

	// The reference adapter always reaches the database directly. Every
	// worker under test reaches it through its own fault proxy.
	reference := startReferenceAdapter(t, repositoryRoot, databaseURL, "go")
	reference.call(t, "migrate", map[string]any{}, nil)
	goProxy := startFaultProxy(ctx, t, databaseURL)
	candidateProxy := startFaultProxy(ctx, t, databaseURL)
	workers := []resilienceWorker{
		{
			adapter: startReferenceAdapter(t, repositoryRoot, goProxy.url, "go-proxied"),
			name:    "go",
			proxy:   goProxy,
		},
		{
			adapter: startCandidateAdapter(t, repositoryRoot, candidateProxy.url, candidateSpec.Implementation, candidateSpec, candidateSpec.Command),
			name:    candidateSpec.Implementation,
			proxy:   candidateProxy,
		},
	}

	// Subtests share one database and run in order, so none are parallel.
	t.Run("DatabaseUnavailableReconnect", func(t *testing.T) { //nolint:paralleltest // Shares the conformance database.
		for _, worker := range workers {
			reference.call(t, "reset", map[string]any{}, nil)
			worker.adapter.call(t, "start", map[string]any{"client_id": worker.name + "-outage"}, nil)
			barrier := worker.name + "-outage"
			worker.adapter.call(t, "barrier_create", map[string]any{"name": barrier}, nil)
			var inFlight, during, observed normalizedJob
			reference.call(t, "insert", map[string]any{"behavior": "barrier_wait", "message": barrier}, &inFlight)
			reference.call(t, "wait", map[string]any{"id": inFlight.ID, "states": []string{"running"}}, &observed)

			// The database becomes unreachable for the worker: established
			// connections reset and new ones are refused. The in-flight job
			// finishes while its completion cannot be written, and new work
			// arrives while the worker cannot see it.
			worker.proxy.takeDown()
			worker.adapter.call(t, "barrier_release", map[string]any{"name": barrier}, nil)
			reference.call(t, "insert", map[string]any{"message": "inserted during outage"}, &during)
			worker.proxy.waitForRejections(t, 3)
			worker.proxy.restore()

			for _, id := range []int64{inFlight.ID, during.ID} {
				job := waitForReferenceCompleted(t, reference, id, time.Minute)
				require.Equal(t, 1, job.Attempt, "%s job %d was rescued or retried", worker.name, id)
				require.Empty(t, job.Errors, "%s job %d", worker.name, id)
			}
			worker.adapter.call(t, "stop", map[string]any{}, nil)
		}
		scenarios.pass("database_unavailable_reconnect")
	})

	t.Run("CompletionTransientFailureRetry", func(t *testing.T) { //nolint:paralleltest // Shares the conformance database.
		for _, worker := range workers {
			reference.call(t, "reset", map[string]any{}, nil)
			// Fail the first running-to-completed transition with a
			// serialization failure. The sequence advances outside the
			// aborted statement, so exactly one attempt fails.
			execSQL(ctx, t, database, `
				CREATE SEQUENCE river_resilience_completion_fault;
				CREATE FUNCTION river_resilience_fail_completion_once() RETURNS trigger
				LANGUAGE plpgsql AS $$ BEGIN
					IF OLD.state = 'running' AND NEW.state = 'completed'
						AND nextval('river_resilience_completion_fault') = 1 THEN
						RAISE EXCEPTION 'injected completion failure' USING ERRCODE = '40001';
					END IF;
					RETURN NEW;
				END $$;
				CREATE TRIGGER river_resilience_fail_completion_once BEFORE UPDATE ON river_job
				FOR EACH ROW EXECUTE FUNCTION river_resilience_fail_completion_once()`)
			t.Cleanup(func() {
				execSQL(ctx, t, database, `
					DROP TRIGGER IF EXISTS river_resilience_fail_completion_once ON river_job;
					DROP FUNCTION IF EXISTS river_resilience_fail_completion_once();
					DROP SEQUENCE IF EXISTS river_resilience_completion_fault`)
			})

			worker.adapter.call(t, "start", map[string]any{"client_id": worker.name + "-completion-retry"}, nil)
			var inserted normalizedJob
			reference.call(t, "insert", map[string]any{"message": "transient completion failure"}, &inserted)
			job := waitForReferenceCompleted(t, reference, inserted.ID, 30*time.Second)
			require.Equal(t, 1, job.Attempt, worker.name)
			require.Empty(t, job.Errors, worker.name)
			var injected int64
			require.NoError(t, database.QueryRow(ctx,
				"SELECT last_value FROM river_resilience_completion_fault").Scan(&injected))
			require.GreaterOrEqual(t, injected, int64(2), "%s: the injected failure never fired", worker.name)
			worker.adapter.call(t, "stop", map[string]any{}, nil)
			execSQL(ctx, t, database, `
				DROP TRIGGER river_resilience_fail_completion_once ON river_job;
				DROP FUNCTION river_resilience_fail_completion_once();
				DROP SEQUENCE river_resilience_completion_fault`)
		}
		scenarios.pass("completion_transient_failure_retry")
	})

	t.Run("CompletionRowLockWait", func(t *testing.T) { //nolint:paralleltest // Shares the conformance database.
		for _, worker := range workers {
			reference.call(t, "reset", map[string]any{}, nil)
			worker.adapter.call(t, "start", map[string]any{"client_id": worker.name + "-row-lock"}, nil)
			barrier := worker.name + "-row-lock"
			worker.adapter.call(t, "barrier_create", map[string]any{"name": barrier}, nil)
			var inserted, observed normalizedJob
			reference.call(t, "insert", map[string]any{"behavior": "barrier_wait", "message": barrier}, &inserted)
			reference.call(t, "wait", map[string]any{"id": inserted.ID, "states": []string{"running"}}, &observed)

			locker, err := database.Begin(ctx)
			require.NoError(t, err)
			_, err = locker.Exec(ctx, "SELECT 1 FROM river_job WHERE id = $1 FOR UPDATE", inserted.ID)
			require.NoError(t, err)
			worker.adapter.call(t, "barrier_release", map[string]any{"name": barrier}, nil)
			pollUntil(t, 30*time.Second, worker.name+" completion waiting on the row lock", func() bool {
				var waiting int
				require.NoError(t, observer.QueryRow(ctx,
					"SELECT count(*) FROM pg_locks WHERE NOT granted AND locktype = 'transactionid'").Scan(&waiting))
				return waiting > 0
			})
			require.NoError(t, locker.Commit(ctx))

			job := waitForReferenceCompleted(t, reference, inserted.ID, 30*time.Second)
			require.Equal(t, 1, job.Attempt, worker.name)
			worker.adapter.call(t, "stop", map[string]any{}, nil)
		}
		scenarios.pass("completion_row_lock_wait")
	})

	t.Run("HardShutdownOutcomes", func(t *testing.T) { //nolint:paralleltest // Shares the conformance database.
		for _, worker := range workers {
			reference.call(t, "reset", map[string]any{}, nil)
			worker.adapter.call(t, "start", map[string]any{
				"client_id": worker.name + "-hard-shutdown", "max_workers": 4,
			}, nil)
			jobs := make(map[string]normalizedJob)
			for _, behavior := range []string{"cooperative_cancel", "cancel_attempted", "cancel_error", "cancel_panic"} {
				insertBehavior := behavior
				if behavior == "cancel_attempted" {
					insertBehavior = "cooperative_cancel"
				}
				var inserted, observed normalizedJob
				reference.call(t, "insert", map[string]any{"behavior": insertBehavior, "message": behavior}, &inserted)
				reference.call(t, "wait", map[string]any{"id": inserted.ID, "states": []string{"running"}}, &observed)
				jobs[behavior] = inserted
			}
			// A cancellation whose notification never reached the worker.
			execSQL(ctx, t, database, `UPDATE river_job
				SET metadata = jsonb_set(metadata, '{cancel_attempted_at}', to_jsonb('2026-01-02T03:04:05Z'::text))
				WHERE id = `+strconv.FormatInt(jobs["cancel_attempted"].ID, 10))
			worker.adapter.call(t, "stop", map[string]any{"cancel": true}, nil)

			var job normalizedJob
			reference.call(t, "get", map[string]any{"id": jobs["cooperative_cancel"].ID}, &job)
			require.Equal(t, "available", job.State, worker.name)
			require.Equal(t, 0, job.Attempt, worker.name)
			require.NotNil(t, job.AttemptedAt, "%s: an interrupted job keeps attempted_at", worker.name)
			require.Empty(t, job.Errors, worker.name)

			reference.call(t, "get", map[string]any{"id": jobs["cancel_attempted"].ID}, &job)
			require.Equal(t, "cancelled", job.State, worker.name)
			require.NotNil(t, job.FinalizedAt, worker.name)

			for _, behavior := range []string{"cancel_error", "cancel_panic"} {
				reference.call(t, "get", map[string]any{"id": jobs[behavior].ID}, &job)
				require.Contains(t, []string{"available", "retryable"}, job.State, "%s %s", worker.name, behavior)
				require.Equal(t, 1, job.Attempt, "%s %s: a genuine failure consumes its attempt", worker.name, behavior)
				require.Len(t, job.Errors, 1, "%s %s", worker.name, behavior)
			}
		}
		scenarios.pass("hard_shutdown_soft_stop_classification", "shutdown_after_cancel_attempt")
	})

	t.Run("ClaimedRowDecodeIsolation", func(t *testing.T) { //nolint:paralleltest // Shares the conformance database.
		for _, worker := range workers {
			reference.call(t, "reset", map[string]any{}, nil)
			var ordinary, sparseErrors, oddMetadata normalizedJob
			reference.call(t, "insert", map[string]any{"message": "ordinary"}, &ordinary)
			reference.call(t, "insert", map[string]any{"message": "sparse errors"}, &sparseErrors)
			reference.call(t, "insert", map[string]any{
				"message": "array metadata", "opts": map[string]any{"max_attempts": 1},
			}, &oddMetadata)
			// Go decodes attempt errors with encoding/json, which tolerates
			// missing and unknown fields. Array metadata is valid for Go but
			// cannot be decoded by every implementation.
			execSQL(ctx, t, database, `UPDATE river_job
				SET errors = ARRAY['{"error": "sparse", "extra": true}'::jsonb]
				WHERE id = `+strconv.FormatInt(sparseErrors.ID, 10))
			execSQL(ctx, t, database, `UPDATE river_job SET metadata = '[1]'::jsonb
				WHERE id = `+strconv.FormatInt(oddMetadata.ID, 10))

			worker.adapter.call(t, "start", map[string]any{"client_id": worker.name + "-decode"}, nil)
			for _, id := range []int64{ordinary.ID, sparseErrors.ID} {
				waitForReferenceCompleted(t, reference, id, 30*time.Second)
			}
			// Whatever an implementation makes of the odd row, it must not
			// strand it (or the rows claimed with it) as running.
			pollUntil(t, 30*time.Second, worker.name+" finalizing the odd row", func() bool {
				var state string
				require.NoError(t, database.QueryRow(ctx,
					"SELECT state::text FROM river_job WHERE id = $1", oddMetadata.ID).Scan(&state))
				return state == "completed" || state == "discarded"
			})
			worker.adapter.call(t, "stop", map[string]any{}, nil)
		}
		scenarios.pass("claimed_row_decode_isolation")
	})
}

// TestResilienceSQLiteConformance checks SQLite behavior under a foreign
// writer and Go-sized integers, using only the sqlite-runtime-v1 profile.
func TestResilienceSQLiteConformance(t *testing.T) { //nolint:tparallel // Subtests share one SQLite database and run in order.
	t.Parallel()
	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteResilience)

	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance-resilience.sqlite")
	const profileName = "sqlite-runtime-v1"
	goAdapter := startReferenceAdapterForProfile(
		t, repositoryRoot, databaseURL, "sqlite", profileName, "go",
	)
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateAdapter := startAdapterCommandForProfile(
		t, repositoryRoot, databaseURL, "sqlite", profileName,
		candidateSpec.Implementation, candidateSpec.Command,
	)
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	t.Run("GoIntegerRanges", func(t *testing.T) { //nolint:paralleltest // Shares the SQLite database.
		// River Go stores native integers on SQLite, so `max_attempts` can
		// exceed a 16-bit integer. Every implementation must still work it.
		for _, pair := range []struct{ inserter, worker *adapter }{
			{inserter: goAdapter, worker: candidateAdapter},
			{inserter: goAdapter, worker: goAdapter},
		} {
			var inserted, worked, stored normalizedJob
			pair.inserter.call(t, "insert", map[string]any{
				"message": "wide max attempts", "opts": map[string]any{"max_attempts": 40_000},
			}, &inserted)
			pair.worker.call(t, "work", map[string]any{
				"client_id": pair.worker.name + "-wide-integers", "id": inserted.ID,
			}, &worked)
			require.Equal(t, "completed", worked.State, pair.worker.name)
			pair.inserter.call(t, "get", map[string]any{"id": inserted.ID}, &stored)
			require.Equal(t, 40_000, stored.MaxAttempts, "working the job must not rewrite max_attempts")
		}
		scenarios.pass("sqlite_runtime_go_integer_ranges")
	})

	t.Run("CompletionUnderForeignWriterLock", func(t *testing.T) { //nolint:paralleltest // Shares the SQLite database.
		for _, pair := range []struct{ locker, worker *adapter }{
			{locker: goAdapter, worker: candidateAdapter},
			{locker: candidateAdapter, worker: goAdapter},
		} {
			pair.worker.call(t, "start", map[string]any{"client_id": pair.worker.name + "-writer-lock"}, nil)
			barrier := pair.worker.name + "-writer-lock"
			pair.worker.call(t, "barrier_create", map[string]any{"name": barrier}, nil)
			var inserted, observed normalizedJob
			pair.worker.call(t, "insert", map[string]any{"behavior": "barrier_wait", "message": barrier}, &inserted)
			pair.worker.call(t, "wait", map[string]any{"id": inserted.ID, "states": []string{"running"}}, &observed)

			// A write inside an open transaction holds SQLite's write lock.
			// Keep it past the adapters' five-second busy timeout while the
			// job finishes, so the first completion write fails.
			handle := pair.worker.name + "-writer-lock"
			pair.locker.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
			pair.locker.call(t, "tx_insert", map[string]any{
				"handle": handle, "job": map[string]any{"message": "foreign writer"},
			}, nil)
			pair.worker.call(t, "barrier_release", map[string]any{"name": barrier}, nil)
			time.Sleep(6 * time.Second) // The fault is the lock's duration, not a wait for an outcome.
			pair.locker.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)

			pollUntil(t, time.Minute, pair.worker.name+" completion after the foreign lock", func() bool {
				var job normalizedJob
				pair.locker.call(t, "get", map[string]any{"id": inserted.ID}, &job)
				return job.State == "completed"
			})
			pair.worker.call(t, "stop", map[string]any{}, nil)
		}
		scenarios.pass("sqlite_runtime_completion_under_writer_lock")
	})
}

type resilienceWorker struct {
	adapter *adapter
	name    string
	proxy   *faultProxy
}

// faultProxy forwards TCP connections to PostgreSQL and can make the database
// unavailable to one adapter: it resets established connections and refuses
// new ones until restored. Unlike terminating backends, this keeps the
// database down for that adapter while the harness and reference still work.
type faultProxy struct {
	down     atomic.Bool
	mu       sync.Mutex
	open     map[net.Conn]struct{}
	rejected atomic.Int64
	url      string
}

func startFaultProxy(ctx context.Context, t *testing.T, databaseURL string) *faultProxy {
	t.Helper()

	parsed, err := url.Parse(databaseURL)
	require.NoError(t, err, "the resilience tier needs a URL-form database URL")
	upstream := parsed.Host
	if parsed.Port() == "" {
		upstream = net.JoinHostPort(parsed.Hostname(), "5432")
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxied := *parsed
	proxied.Host = listener.Addr().String()
	proxy := &faultProxy{open: make(map[net.Conn]struct{}), url: proxied.String()}
	t.Cleanup(func() {
		_ = listener.Close()
		proxy.closeAll()
	})

	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if proxy.down.Load() {
				proxy.rejected.Add(1)
				_ = client.Close()
				continue
			}
			go proxy.forward(ctx, client, upstream)
		}
	}()
	return proxy
}

func (proxy *faultProxy) forward(ctx context.Context, client net.Conn, upstream string) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	server, err := dialer.DialContext(ctx, "tcp", upstream)
	if err != nil {
		_ = client.Close()
		return
	}
	if !proxy.track(client, server) {
		return
	}
	done := make(chan struct{}, 2)
	pipe := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		done <- struct{}{}
	}
	go pipe(server, client)
	go pipe(client, server)
	<-done
	proxy.untrack(client, server)
}

func (proxy *faultProxy) track(connections ...net.Conn) bool {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if proxy.down.Load() {
		for _, connection := range connections {
			_ = connection.Close()
		}
		return false
	}
	for _, connection := range connections {
		proxy.open[connection] = struct{}{}
	}
	return true
}

func (proxy *faultProxy) untrack(connections ...net.Conn) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
		delete(proxy.open, connection)
	}
}

func (proxy *faultProxy) closeAll() {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	for connection := range proxy.open {
		_ = connection.Close()
		delete(proxy.open, connection)
	}
}

func (proxy *faultProxy) takeDown() {
	proxy.down.Store(true)
	proxy.closeAll()
}

func (proxy *faultProxy) restore() {
	proxy.down.Store(false)
}

// waitForRejections waits until the adapter has tried to reconnect `count`
// times while the proxy is down, proving it noticed the outage.
func (proxy *faultProxy) waitForRejections(t *testing.T, count int64) {
	t.Helper()
	pollUntil(t, time.Minute, "reconnection attempts (did the client stop?)", func() bool {
		return proxy.rejected.Load() >= count
	})
}

func execSQL(ctx context.Context, t *testing.T, database *pgx.Conn, sql string) {
	t.Helper()
	_, err := database.Exec(ctx, sql)
	require.NoError(t, err)
}

// waitForReferenceCompleted polls a job through the reference adapter, which
// is connected directly and so unaffected by a worker's faults.
func waitForReferenceCompleted(t *testing.T, reference *adapter, id int64, timeout time.Duration) normalizedJob {
	t.Helper()
	var job normalizedJob
	pollUntil(t, timeout, "job "+strconv.FormatInt(id, 10)+" completing", func() bool {
		reference.call(t, "get", map[string]any{"id": id}, &job)
		return job.State == "completed"
	})
	return job
}

// pollUntil evaluates condition on the test goroutine until it holds, failing
// the test after timeout.
func pollUntil(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		require.True(t, time.Now().Before(deadline), "timed out waiting for %s", description)
		time.Sleep(20 * time.Millisecond)
	}
}
