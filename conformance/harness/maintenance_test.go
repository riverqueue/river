//go:build riverconformance

package harness_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// maintenanceImplementation is one engine whose leader-owned maintenance is
// checked. Every scenario runs against the Go reference first, which
// validates the scenario itself, and then against the candidate.
type maintenanceImplementation struct {
	adapter         *adapter
	applicationName string
	name            string
}

// maintenanceTuning shortens intervals River Go doesn't expose. Only
// implementations whose descriptor lists an option receive it; the others,
// like River Go, run each service as soon as they gain leadership.
func maintenanceTuning() map[string]any {
	return map[string]any{
		"elect_interval_ms":     50,
		"rescuer_interval_ms":   50,
		"scheduler_interval_ms": 50,
	}
}

func startParams(schema, clientID string, extra map[string]any) map[string]any {
	params := map[string]any{
		"client_id":                 clientID,
		"job_cleaner_interval_ms":   50,
		"max_workers":               1,
		"queue_cleaner_interval_ms": 50,
		"schema":                    schema,
	}
	maps.Copy(params, extra)
	return params
}

// maintenanceHarness provides direct database access for arranging rows and
// observing server state that no adapter method exposes, such as lock waits.
type maintenanceHarness struct {
	pool *pgxpool.Pool
	t    *testing.T
}

// schema creates and migrates a fresh schema through the Go reference
// migrator and drops it when the test finishes.
func (harness *maintenanceHarness) schema(migrator *adapter, name string) string {
	harness.t.Helper()

	schema := fmt.Sprintf("%s_%x", name, time.Now().UnixNano()&0xffffff)
	migrator.call(harness.t, "migrate", map[string]any{"schema": schema}, nil)
	harness.t.Cleanup(func() {
		_, err := harness.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		require.NoError(harness.t, err)
	})
	return schema
}

func (harness *maintenanceHarness) exec(sql string) {
	harness.t.Helper()

	_, err := harness.pool.Exec(context.Background(), sql)
	require.NoError(harness.t, err)
}

func (harness *maintenanceHarness) queryInt(sql string, args ...any) int64 {
	harness.t.Helper()

	var value int64
	require.NoError(harness.t, harness.pool.QueryRow(context.Background(), sql, args...).Scan(&value))
	return value
}

func (harness *maintenanceHarness) waitFor(description string, timeout time.Duration, condition func() bool) {
	harness.t.Helper()

	deadline := time.Now().Add(timeout)
	for !condition() {
		require.True(harness.t, time.Now().Before(deadline), "timed out waiting for %s", description)
		time.Sleep(20 * time.Millisecond)
	}
}

// lockWaiters counts an implementation's statements blocked on a lock.
func (harness *maintenanceHarness) lockWaiters(applicationName string) int64 {
	harness.t.Helper()

	return harness.queryInt(`
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND application_name = $1
		  AND state = 'active' AND wait_event_type = 'Lock'`, applicationName)
}

// conformanceArgsJSON is a complete `conformance_echo` argument object, so
// every implementation's worker can decode rows the harness inserts.
const conformanceArgsJSON = `{"behavior":"","duration_ms":0,"message":"maintenance"}`

func table(schema, name string) string {
	return pgx.Identifier{schema, name}.Sanitize()
}

func TestMaintenanceConformance(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RIVER_CONFORMANCE_DATABASE_URL is required")
	}
	scenarios := newScenarioTracker(t, scenarioOwnerMaintenance)
	repositoryRoot := repoRoot(t)
	goAdapter := startReferenceAdapter(t, repositoryRoot, databaseURL, "go")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateAdapter := startCandidateAdapter(t, repositoryRoot, databaseURL, candidateSpec.Implementation, candidateSpec, candidateSpec.Command)
	implementations := []maintenanceImplementation{
		{adapter: goAdapter, applicationName: "river-conformance-go", name: "go"},
		{adapter: candidateAdapter, applicationName: candidateSpec.ApplicationName, name: candidateSpec.Implementation},
	}

	pool, err := pgxpool.New(context.Background(), databaseURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	harness := &maintenanceHarness{pool: pool, t: t}
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	t.Run("CronScheduleGoldens", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		verifyCronScheduleGoldens(t, repositoryRoot, goAdapter, candidateAdapter)
		scenarios.pass("cron_schedule_goldens")
	})

	t.Run("QueueNamesAndUnknownQueueControl", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			implementation.adapter.requireCallError(t, "queue_pause", map[string]any{"name": "maintenance_missing_queue"}, "not_found")
			implementation.adapter.requireCallError(t, "queue_resume", map[string]any{"name": "maintenance_missing_queue"}, "not_found")
			implementation.adapter.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
			implementation.adapter.call(t, "queue_resume", map[string]any{"name": "*"}, nil)

			var inserted normalizedJob
			implementation.adapter.call(t, "insert", map[string]any{
				"message": "pipe queue", "opts": map[string]any{"queue": "tenant|emails"},
			}, &inserted)
			require.Equal(t, "tenant|emails", inserted.Queue, implementation.name)
			implementation.adapter.call(t, "delete", map[string]any{"id": inserted.ID}, nil)
		}
		scenarios.pass("queue_names_and_unknown_queue_control")
	})

	t.Run("MigrationMixedCaseSchema", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		// Go quotes the schema; a migrated mixed-case schema must be seen as
		// migrated rather than folded to lowercase.
		schema := harness.schema(goAdapter, "MaintMixedCase")
		var result struct {
			Existing []int `json:"existing"`
			Versions []int `json:"versions"`
		}
		candidateAdapter.call(t, "migrate", map[string]any{"schema": schema}, &result)
		require.Empty(t, result.Versions)
		require.NotEmpty(t, result.Existing)
		scenarios.pass("migration_mixed_case_schema")
	})

	t.Run("JobCleanerRetention", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyJobCleanerRetention(t, harness, goAdapter, implementation)
		}
		scenarios.pass("maintenance_job_cleaner_retention")
	})

	t.Run("QueueCleanerKeepsActiveQueues", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyQueueCleaner(t, harness, goAdapter, implementation)
		}
		scenarios.pass("maintenance_queue_cleaner_keeps_active_queues")
	})

	t.Run("ReindexerSkipsArtifacts", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyReindexer(t, harness, goAdapter, implementation)
		}
		scenarios.pass("maintenance_reindexer_skips_artifacts")
	})

	t.Run("RescuerPastFullBatchOfUnexpiredJobs", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyRescuerFullBatch(t, harness, goAdapter, implementation)
		}
		scenarios.pass("maintenance_rescuer_full_batch_of_unexpired_jobs")
	})

	t.Run("RescuerStaleSelection", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyRescuerStaleSelection(t, harness, goAdapter, implementation)
		}
		scenarios.pass("maintenance_rescuer_stale_selection")
	})

	t.Run("SameClientIDTermReplacement", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifySameClientIDTermReplacement(t, harness, goAdapter, implementation)
		}
		scenarios.pass("leadership_same_client_id_term_replacement")
	})

	t.Run("LeaderRenewalUnderSlowMaintenance", func(t *testing.T) { //nolint:paralleltest // Shares adapters.
		for _, implementation := range implementations {
			verifyRenewalUnderSlowMaintenance(t, harness, goAdapter, implementation)
		}
		scenarios.pass("leadership_renewal_under_slow_maintenance")
	})
}

func verifyCronScheduleGoldens(t *testing.T, repositoryRoot string, adapters ...*adapter) {
	t.Helper()

	var fixture struct {
		CronCases []struct {
			Expression string      `json:"expression"`
			From       time.Time   `json:"from"`
			Name       string      `json:"name"`
			Next       []time.Time `json:"next"`
		} `json:"cron_cases"`
		CronInvalid []string `json:"cron_invalid"`
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/fixtures/maintenance_values.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.NotEmpty(t, fixture.CronCases)

	for _, testCase := range fixture.CronCases {
		for _, adapter := range adapters {
			var result struct {
				Next []time.Time `json:"next"`
			}
			adapter.call(t, "cron_next", map[string]any{
				"count":      5,
				"expression": testCase.Expression,
				"from":       testCase.From.Format(time.RFC3339Nano),
			}, &result)
			require.Len(t, result.Next, len(testCase.Next), "%s adapter case %s", adapter.name, testCase.Name)
			for index, expected := range testCase.Next {
				actual := result.Next[index]
				require.True(t, expected.Equal(actual), "%s adapter case %s occurrence %d: %s != %s",
					adapter.name, testCase.Name, index, actual, expected)
				_, expectedOffset := expected.Zone()
				_, actualOffset := actual.Zone()
				require.Equal(t, expectedOffset, actualOffset, "%s adapter case %s offset", adapter.name, testCase.Name)
			}
		}
	}
	for _, expression := range fixture.CronInvalid {
		for _, adapter := range adapters {
			adapter.requireCallError(t, "cron_next", map[string]any{
				"count": 1, "expression": expression, "from": "2026-01-02T03:04:05Z",
			}, "rejected")
		}
	}
}

func insertRawJob(harness *maintenanceHarness, schema, kind, state string, attemptedAgo, finalizedAgo *time.Duration) int64 {
	harness.t.Helper()

	var attemptedAt, finalizedAt *time.Time
	if attemptedAgo != nil {
		value := time.Now().Add(-*attemptedAgo)
		attemptedAt = &value
	}
	if finalizedAgo != nil {
		value := time.Now().Add(-*finalizedAgo)
		finalizedAt = &value
	}
	attempt := 0
	if state == "running" {
		attempt = 1
	}
	return harness.queryInt(fmt.Sprintf(`
		INSERT INTO %s (args, attempt, attempted_at, attempted_by, finalized_at, kind, max_attempts, state)
		VALUES ('`+conformanceArgsJSON+`', $1, $2, CASE WHEN $2::timestamptz IS NULL THEN NULL ELSE ARRAY['dead-client'] END, $3, $4, 25, $5::text::%s)
		RETURNING id`, table(schema, "river_job"), pgx.Identifier{schema, "river_job_state"}.Sanitize()),
		attempt, attemptedAt, finalizedAt, kind, state)
}

func jobExists(harness *maintenanceHarness, schema string, id int64) bool {
	harness.t.Helper()

	return harness.queryInt("SELECT count(*) FROM "+table(schema, "river_job")+" WHERE id = $1", id) == 1
}

func verifyJobCleanerRetention(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	schema := harness.schema(migrator, "maint_job_cleaner")
	expiredCancelled := insertRawJob(harness, schema, "conformance_echo", "cancelled", nil, new(2*time.Hour))
	expiredCompleted := insertRawJob(harness, schema, "conformance_echo", "completed", nil, new(2*time.Hour))
	expiredDiscarded := insertRawJob(harness, schema, "conformance_echo", "discarded", nil, new(2*time.Hour))
	recentCancelled := insertRawJob(harness, schema, "conformance_echo", "cancelled", nil, new(time.Minute))
	running := insertRawJob(harness, schema, "conformance_echo", "running", new(time.Second), nil)

	// Completed jobs are retained forever (-1); the other finalized states
	// expire after one hour.
	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-job-cleaner", map[string]any{
		"cancelled_job_retention_ms": 3_600_000,
		"completed_job_retention_ms": -1,
		"discarded_job_retention_ms": 3_600_000,
		"queue":                      "maintenance_idle",
	}), maintenanceTuning())
	// Both expired rows are removed by one cleaner statement, so observing
	// their deletion proves a complete pass ran.
	harness.waitFor(implementation.name+" job cleaner", 30*time.Second, func() bool {
		return !jobExists(harness, schema, expiredCancelled) && !jobExists(harness, schema, expiredDiscarded)
	})
	implementation.adapter.call(t, "stop", map[string]any{}, nil)

	require.True(t, jobExists(harness, schema, expiredCompleted), "%s deleted a retained state", implementation.name)
	require.True(t, jobExists(harness, schema, recentCancelled), "%s deleted a job before its retention", implementation.name)
	require.True(t, jobExists(harness, schema, running), "%s deleted a running job", implementation.name)
}

func verifyQueueCleaner(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	schema := harness.schema(migrator, "maint_queue_cleaner")
	queues := table(schema, "river_queue")
	harness.exec("INSERT INTO " + queues + " (name, created_at, metadata, updated_at) VALUES ('stale', now(), '{}', now() - interval '25 hours')")
	harness.exec("INSERT INTO " + queues + " (name, created_at, metadata, updated_at) VALUES ('recent', now(), '{}', now() - interval '1 hour')")

	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-queue-cleaner", map[string]any{
		"queue": "maintenance_active",
	}), maintenanceTuning())
	queueExists := func(name string) bool {
		return harness.queryInt("SELECT count(*) FROM "+queues+" WHERE name = $1", name) == 1
	}
	harness.waitFor(implementation.name+" queue cleaner", 30*time.Second, func() bool {
		return !queueExists("stale") && queueExists("maintenance_active")
	})
	implementation.adapter.call(t, "stop", map[string]any{}, nil)

	require.True(t, queueExists("recent"), "%s deleted a queue within retention", implementation.name)
	require.True(t, queueExists("maintenance_active"), "%s deleted its active queue", implementation.name)
}

func indexFilenode(harness *maintenanceHarness, schema, index string) int64 {
	harness.t.Helper()

	return harness.queryInt(`
		SELECT c.relfilenode::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2`, schema, index)
}

func verifyReindexer(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	schema := harness.schema(migrator, "maint_reindexer")
	jobs := table(schema, "river_job")
	harness.exec("CREATE INDEX maint_artifact_idx ON " + jobs + " (kind)")
	harness.exec("CREATE INDEX maint_artifact_idx_ccnew1 ON " + jobs + " (kind)")
	harness.exec("CREATE INDEX maint_rebuilt_idx ON " + jobs + " (kind)")
	artifactFilenode := indexFilenode(harness, schema, "maint_artifact_idx")
	rebuiltFilenode := indexFilenode(harness, schema, "maint_rebuilt_idx")

	// Indexes are processed in order, so once the last one is rebuilt the
	// missing index and the one with a leftover artifact were already skipped.
	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-reindexer", map[string]any{
		"queue":                 "maintenance_idle",
		"reindexer_index_names": []string{"maint_missing_idx", "maint_artifact_idx", "maint_rebuilt_idx"},
		"reindexer_interval_ms": 200,
	}), maintenanceTuning())
	harness.waitFor(implementation.name+" reindex", 30*time.Second, func() bool {
		return indexFilenode(harness, schema, "maint_rebuilt_idx") != rebuiltFilenode
	})
	implementation.adapter.call(t, "stop", map[string]any{}, nil)

	require.Equal(t, artifactFilenode, indexFilenode(harness, schema, "maint_artifact_idx"),
		"%s rebuilt an index with a leftover concurrent artifact", implementation.name)
	require.Positive(t, indexFilenode(harness, schema, "maint_artifact_idx_ccnew1"))
}

func verifyRescuerFullBatch(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	schema := harness.schema(migrator, "maint_rescue_batch")
	jobs := table(schema, "river_job")
	// A full default batch (10,000) of stuck jobs whose timeout is disabled
	// precedes one eligible job. Without paging past the ignored batch, a
	// rescuer re-selects the same rows forever.
	harness.exec(fmt.Sprintf(`
		INSERT INTO %s (args, attempt, attempted_at, attempted_by, kind, max_attempts, state)
		SELECT '`+conformanceArgsJSON+`', 1, now() - interval '2 hours', ARRAY['dead-client'], 'conformance_echo', 25, 'running'
		FROM generate_series(1, 10000)`, jobs))
	eligible := insertRawJob(harness, schema, "maintenance_unregistered_kind", "running", new(2*time.Hour), nil)

	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-rescue-batch", map[string]any{
		"job_timeout_disabled": true,
		"queue":                "maintenance_idle",
		"rescue_after_ms":      60_000,
	}), maintenanceTuning())
	harness.waitFor(implementation.name+" rescue past a full batch", 60*time.Second, func() bool {
		return harness.queryInt("SELECT count(*) FROM "+jobs+" WHERE id = $1 AND state = 'discarded'", eligible) == 1
	})
	implementation.adapter.call(t, "stop", map[string]any{}, nil)

	require.Equal(t, int64(10_000), harness.queryInt(
		"SELECT count(*) FROM "+jobs+" WHERE kind = 'conformance_echo' AND state = 'running' AND errors IS NULL"),
		"%s rescued jobs whose timeout is disabled", implementation.name)
}

func verifyRescuerStaleSelection(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	ctx := context.Background()
	schema := harness.schema(migrator, "maint_rescue_stale")
	jobs := table(schema, "river_job")
	completed := insertRawJob(harness, schema, "maintenance_unregistered_kind", "running", new(2*time.Hour), nil)
	reclaimed := insertRawJob(harness, schema, "maintenance_unregistered_kind", "running", new(2*time.Hour), nil)
	eligible := insertRawJob(harness, schema, "maintenance_unregistered_kind", "running", new(2*time.Hour), nil)

	// Hold two stuck rows so the rescuer's update waits on them after it has
	// already selected them.
	tx, err := harness.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "SELECT id FROM "+jobs+" WHERE id = ANY($1) FOR UPDATE", []int64{completed, reclaimed})
	require.NoError(t, err)

	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-rescue-stale", map[string]any{
		"queue":           "maintenance_idle",
		"rescue_after_ms": 60_000,
	}), maintenanceTuning())
	// Implementations that select without row locks (like Go) block here
	// until the harness commits; ones that skip locked rows rescue the
	// eligible job first. Either way the held jobs must end up untouched.
	eligibleRescued := func() bool {
		return harness.queryInt("SELECT count(*) FROM "+jobs+" WHERE id = $1 AND state = 'discarded'", eligible) == 1
	}
	harness.waitFor(implementation.name+" rescue pass reaching the held rows", 30*time.Second, func() bool {
		return harness.lockWaiters(implementation.applicationName) > 0 || eligibleRescued()
	})

	// A worker finishes one job and another client re-claims the other before
	// the stale rescue proceeds.
	_, err = tx.Exec(ctx, "UPDATE "+jobs+" SET state = 'completed', finalized_at = now() WHERE id = $1", completed)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "UPDATE "+jobs+" SET attempt = attempt + 1, attempted_at = now() WHERE id = $1", reclaimed)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	harness.waitFor(implementation.name+" rescue of the eligible job", 30*time.Second, eligibleRescued)
	implementation.adapter.call(t, "stop", map[string]any{}, nil)

	require.Equal(t, int64(1), harness.queryInt(`
		SELECT count(*) FROM `+jobs+` WHERE id = $1 AND state = 'completed' AND errors IS NULL
		  AND NOT metadata ? 'river:rescue_count'`, completed), "%s rescued a completed job", implementation.name)
	require.Equal(t, int64(1), harness.queryInt(`
		SELECT count(*) FROM `+jobs+` WHERE id = $1 AND state = 'running' AND attempt = 2 AND errors IS NULL`,
		reclaimed), "%s rescued a re-claimed job", implementation.name)
}

type leaseRow struct {
	electedAt time.Time
	expiresAt time.Time
	leaderID  string
}

func readLease(harness *maintenanceHarness, schema string) (leaseRow, bool) {
	harness.t.Helper()

	var lease leaseRow
	err := harness.pool.QueryRow(context.Background(),
		"SELECT elected_at, expires_at, leader_id FROM "+table(schema, "river_leader")).
		Scan(&lease.electedAt, &lease.expiresAt, &lease.leaderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return leaseRow{}, false
	}
	require.NoError(harness.t, err)
	return lease, true
}

func verifySameClientIDTermReplacement(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	schema := harness.schema(migrator, "maint_term_replace")
	clientID := implementation.name + "-shared-identity"
	periodicCount := func() int64 {
		return harness.queryInt("SELECT count(*) FROM " + table(schema, "river_job") +
			" WHERE metadata ->> 'river:periodic_job_id' = 'conformance-periodic'")
	}
	implementation.adapter.startWithTuning(t, startParams(schema, clientID, map[string]any{
		"periodic_run_on_start": true,
		"queue":                 "maintenance_idle",
	}), maintenanceTuning())
	var first leaseRow
	harness.waitFor(implementation.name+" first term", 30*time.Second, func() bool {
		lease, ok := readLease(harness, schema)
		first = lease
		return ok && lease.leaderID == clientID && periodicCount() == 1
	})

	// Another process with the same client ID takes over with a newer term.
	// The original client must lose leadership instead of renewing that
	// term, and win a fresh term only after the replacement expires.
	var replacementElectedAt time.Time
	require.NoError(t, harness.pool.QueryRow(context.Background(), fmt.Sprintf(`
		WITH removed AS (DELETE FROM %[1]s RETURNING leader_id, elected_at)
		INSERT INTO %[1]s (leader_id, elected_at, expires_at)
		SELECT leader_id, elected_at + interval '1 second', now() + interval '3 seconds' FROM removed
		RETURNING elected_at`, table(schema, "river_leader"))).Scan(&replacementElectedAt))
	harness.waitFor(implementation.name+" fresh term after replacement", 45*time.Second, func() bool {
		lease, ok := readLease(harness, schema)
		return ok && lease.leaderID == clientID &&
			!lease.electedAt.Equal(first.electedAt) && !lease.electedAt.Equal(replacementElectedAt)
	})
	// Run-on-start periodic jobs are inserted once per gained term.
	harness.waitFor(implementation.name+" second run-on-start job", 30*time.Second, func() bool {
		return periodicCount() == 2
	})
	implementation.adapter.call(t, "stop", map[string]any{}, nil)
}

func verifyRenewalUnderSlowMaintenance(t *testing.T, harness *maintenanceHarness, migrator *adapter, implementation maintenanceImplementation) {
	t.Helper()

	ctx := context.Background()
	schema := harness.schema(migrator, "maint_slow_renewal")
	jobs := table(schema, "river_job")
	expired := insertRawJob(harness, schema, "conformance_echo", "completed", nil, new(48*time.Hour))

	// Holding the expired row blocks the job cleaner's delete.
	tx, err := harness.pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, "SELECT id FROM "+jobs+" WHERE id = $1 FOR UPDATE", expired)
	require.NoError(t, err)

	implementation.adapter.startWithTuning(t, startParams(schema, implementation.name+"-slow-renewal", map[string]any{
		"queue": "maintenance_idle",
	}), maintenanceTuning())
	harness.waitFor(implementation.name+" blocked job cleaner", 30*time.Second, func() bool {
		return harness.lockWaiters(implementation.applicationName) > 0
	})

	// The leader keeps renewing the same term while its maintenance is stuck:
	// the first renewal is observed while the cleaner is still blocked, and
	// the lease keeps advancing afterwards.
	initial, ok := readLease(harness, schema)
	require.True(t, ok)
	expiresAt := initial.expiresAt
	for renewal := range 2 {
		harness.waitFor(implementation.name+" renewal during blocked maintenance", 30*time.Second, func() bool {
			lease, ok := readLease(harness, schema)
			require.True(t, ok)
			require.True(t, lease.electedAt.Equal(initial.electedAt), "%s lost its term while maintenance was blocked", implementation.name)
			if lease.expiresAt.After(expiresAt) {
				expiresAt = lease.expiresAt
				return true
			}
			return false
		})
		if renewal == 0 {
			require.Positive(t, harness.lockWaiters(implementation.applicationName),
				"%s maintenance stopped waiting before the lease was renewed", implementation.name)
		}
	}
	require.NoError(t, tx.Rollback(ctx))
	implementation.adapter.call(t, "stop", map[string]any{}, nil)
}
