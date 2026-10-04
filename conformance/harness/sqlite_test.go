//go:build riverconformance

package harness_test

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//nolint:paralleltest,tparallel // Scenarios share one database and adapter processes, so they run sequentially.
func TestMixedSQLiteConformance(t *testing.T) {
	t.Parallel()

	scenarios := newScenarioTracker(t, scenarioOwnerSQLiteStorage)
	repositoryRoot := repoRoot(t)
	databaseURL := filepath.Join(t.TempDir(), "river-conformance.sqlite")
	goAdapter := startReferenceAdapterForProfile(t, repositoryRoot, databaseURL, "sqlite", "", "go")
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateSpec.requireProfile(t, profilePortableStorage)
	candidateAdapter := startAdapterCommandForProfile(
		t, repositoryRoot, databaseURL, "sqlite", "", candidateSpec.Implementation, candidateSpec, candidateSpec.Command,
	)
	scenarios.attach(goAdapter, candidateAdapter)
	pair := mixedPair{candidate: candidateAdapter, candidateSpec: candidateSpec, reference: goAdapter}

	t.Run("sqlite_profile_handshake", func(t *testing.T) {
		defer scenarios.record(t)

		verifyProfileHandshakes(t, repositoryRoot, "conformance/adapter/profiles/sqlite.json", candidateSpec, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_migration_cross_language", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteMigrations(t, readManifest(t, repositoryRoot).Migration.Latest, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_insert_get_unique_cross_language", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteCrossLanguageInsertion(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_batch_atomicity", func(t *testing.T) {
		defer scenarios.record(t)

		verifyBatchInsertion(t, goAdapter, candidateAdapter)
		pair.eachDirection(func(actor, observer *adapter) {
			verifyTransactionalBatchInsertion(t, actor, observer)
		})
	})
	t.Run("sqlite_job_rows", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteJobRows(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_unique_column_bytes", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUniqueColumnBytes(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_job_crud", func(t *testing.T) {
		defer scenarios.record(t)

		verifyDifferentialJobCRUD(t, goAdapter, candidateAdapter)
		verifyLargeMetadataRoundTrip(t, goAdapter, candidateAdapter)
		verifyBulkDeleteSafety(t, goAdapter, candidateAdapter)
		verifyDifferentialListCursors(t, goAdapter, candidateAdapter, false)
	})
	t.Run("sqlite_unsafe_int64_job_ids_rpc_list_cursors", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUnsafeInt64JobIDs(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_transactions", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteTransactions(t, goAdapter, candidateAdapter)
	})
	t.Run("sqlite_timestamp_rounding_ordering", func(t *testing.T) {
		defer scenarios.record(t)

		verifySQLiteTimestampEncoding(t, goAdapter, candidateAdapter)
	})
}

// verifyProfileHandshakes checks that the reference and candidate advertise
// exactly the named profile's capabilities and methods.
func verifyProfileHandshakes(t *testing.T, repositoryRoot, profilePath string, candidateSpec adapterSpec, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	var profile adapterProfile
	profileBytes, err := os.ReadFile(filepath.Join(repositoryRoot, profilePath))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(profileBytes, &profile))
	manifest := readManifest(t, repositoryRoot)
	for _, testCase := range []struct {
		adapter        *adapter
		implementation string
	}{
		{adapter: goAdapter, implementation: "go"},
		{adapter: candidateAdapter, implementation: candidateSpec.Implementation},
	} {
		var handshake adapterHandshake
		testCase.adapter.call(t, "handshake", map[string]any{}, &handshake)
		require.Equal(t, testCase.implementation, handshake.Implementation)
		require.Equal(t, manifest.Implementations[testCase.implementation].Version, handshake.ImplementationVersion)
		require.Equal(t, profile.Backend, handshake.Backend)
		require.Equal(t, profile.Name, handshake.Profile)
		require.Equal(t, profile.ProtocolRevision, handshake.ProtocolRevision)
		require.Equal(t, profile.Capabilities, handshake.Capabilities)
		require.Equal(t, profile.Methods, handshake.Methods)
		require.Equal(t, map[string]int{manifest.Migration.Line: manifest.Migration.Latest}, handshake.MigrationLines)
	}
	verifyRequestStrictness(t, goAdapter, candidateAdapter)
	contract, err := sharedAdapterContract()
	require.NoError(t, err)
	for method := range contract.methods {
		if !slices.Contains(profile.Methods, method) {
			for _, current := range []*adapter{goAdapter, candidateAdapter} {
				current.requireUnvalidatedCallError(t, method, map[string]any{}, "method_not_found")
			}
		}
	}
}

func verifySQLiteCrossLanguageInsertion(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		observer *adapter
		writer   *adapter
	}{
		{observer: candidateAdapter, writer: goAdapter},
		{observer: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		params := map[string]any{
			"message": "SQLite insertion from " + pair.writer.name,
			"opts": map[string]any{
				"metadata": map[string]any{"writer": pair.writer.name},
				"tags":     []string{"sqlite_cross_language"},
			},
		}
		var inserted, observed normalizedJob
		pair.writer.call(t, "insert", params, &inserted)
		require.NotNil(t, inserted.Errors)
		require.Empty(t, inserted.Errors)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.NotNil(t, observed.Errors)
		require.Equal(t, inserted, observed)

		// Each unique option must produce the same key and states in both
		// implementations, so the observer's insertion is a duplicate.
		for _, testCase := range uniqueColumnCases() {
			uniqueParams := map[string]any{
				"message": "SQLite unique " + testCase.name + " from " + pair.writer.name,
				"opts":    testCase.opts,
			}
			pair.writer.call(t, "insert", uniqueParams, &inserted)
			pair.observer.call(t, "insert", uniqueParams, &observed)
			require.Equal(t, inserted, observed, "%s: %s inserted a duplicate of %s's job", testCase.name, pair.observer.name, pair.writer.name)
		}
	}
}

func verifySQLiteMigrations(t *testing.T, latest int, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	type migrationResult struct {
		Applied  []int `json:"applied"`
		Existing []int `json:"existing"`
		Valid    bool  `json:"valid"`
	}
	expectedLatest := make([]int, latest)
	for index := range latest {
		expectedLatest[index] = index + 1
	}
	for initializerIndex, initializer := range []*adapter{goAdapter, candidateAdapter} {
		observer := []*adapter{candidateAdapter, goAdapter}[initializerIndex]
		for version := 1; version <= len(expectedLatest); version++ {
			var result migrationResult
			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)

			initializer.call(t, "migrate", map[string]any{
				"direction": "up", "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)
			require.Equal(t, version == len(expectedLatest), result.Valid)

			observer.call(t, "migrate", map[string]any{
				"direction": "down", "dry_run": true, "target_version": version,
			}, &result)
			require.Empty(t, result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)

			observer.call(t, "migrate", map[string]any{}, &result)
			require.Equal(t, expectedLatest[version:], result.Applied)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			var inserted, observed normalizedJob
			observer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("SQLite historical migration %d", version),
			}, &inserted)
			initializer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
			require.Equal(t, inserted, observed)

			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			observer.call(t, "migrate", map[string]any{
				"direction": "down", "dry_run": true, "target_version": version,
			}, &result)
			require.Empty(t, result.Applied)
			require.Equal(t, expectedLatest[:version], result.Existing)

			observer.call(t, "migrate", map[string]any{}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			observer.call(t, "migrate", map[string]any{
				"direction": "down", "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)
		}
	}
	var result migrationResult
	goAdapter.call(t, "migrate", map[string]any{}, &result)
	require.Equal(t, expectedLatest, result.Applied)
	require.Equal(t, expectedLatest, result.Existing)
	require.True(t, result.Valid)
}

// verifySQLiteTimestampEncoding has each implementation write the same
// scheduled times, which SQLite stores as millisecond text, and requires
// every writer to round them as Go does: to the nearest millisecond, with
// halfway values rounded up (toward the future even before 1970), carrying
// into the second. Both implementations must read every row back the same
// way and list them in time order.
func verifySQLiteTimestampEncoding(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	goAdapter.call(t, "reset", map[string]any{}, nil)
	testCases := []struct {
		expected    string
		expectedRaw string
		input       string
	}{
		{expected: "2026-01-02T03:04:05.123Z", expectedRaw: "2026-01-02 03:04:05.123", input: "2026-01-02T03:04:05.1234Z"},
		{expected: "2026-01-02T03:04:05.124Z", expectedRaw: "2026-01-02 03:04:05.124", input: "2026-01-02T03:04:05.1238Z"},
		{expected: "2026-01-02T03:04:05Z", expectedRaw: "2026-01-02 03:04:05.000", input: "2026-01-02T03:04:05.0004999Z"},
		{expected: "2026-01-02T03:04:05.001Z", expectedRaw: "2026-01-02 03:04:05.001", input: "2026-01-02T03:04:05.0005Z"},
		{expected: "2026-01-02T03:04:06Z", expectedRaw: "2026-01-02 03:04:06.000", input: "2026-01-02T03:04:05.9995Z"},
		{expected: "1970-01-01T00:00:00Z", expectedRaw: "1970-01-01 00:00:00.000", input: "1969-12-31T23:59:59.9995Z"},
		{expected: "1969-12-31T23:59:59.998Z", expectedRaw: "1969-12-31 23:59:59.998", input: "1969-12-31T23:59:59.9975Z"},
	}
	type insertedJob struct {
		expected time.Time
		id       int64
	}
	writers := []*adapter{goAdapter, candidateAdapter}
	inserted := make([]insertedJob, 0, len(writers)*len(testCases))
	for _, writer := range writers {
		for _, testCase := range testCases {
			var job normalizedJob
			writer.call(t, "insert", map[string]any{
				"message": "SQLite timestamp " + testCase.input,
				"opts": map[string]any{
					"scheduled_at": testCase.input,
					"tags":         []string{"sqlite_timestamps"},
				},
			}, &job)
			require.Equal(t, testCase.expected, job.ScheduledAt, "%s writing %s", writer.name, testCase.input)
			inserted = append(inserted, insertedJob{expected: parseTime(t, testCase.expected), id: job.ID})
			for _, observer := range []*adapter{goAdapter, candidateAdapter} {
				var observed normalizedJob
				observer.call(t, "get", map[string]any{"id": job.ID}, &observed)
				require.Equal(t, testCase.expected, observed.ScheduledAt,
					"%s reading %s's %s", observer.name, writer.name, testCase.input)
				var raw struct {
					CreatedAt   string `json:"created_at"`
					ScheduledAt string `json:"scheduled_at"`
				}
				observer.call(t, "raw_job_timestamps", map[string]any{"id": job.ID}, &raw)
				require.Equal(t, testCase.expectedRaw, raw.ScheduledAt, "%s's stored %s", writer.name, testCase.input)
				_, err := time.Parse("2006-01-02 15:04:05.000", raw.CreatedAt)
				require.NoError(t, err)
			}
		}
	}
	slices.SortStableFunc(inserted, func(a, b insertedJob) int {
		return cmp.Or(a.expected.Compare(b.expected), cmp.Compare(a.id, b.id))
	})
	expectedIDs := make([]int64, len(inserted))
	for index, job := range inserted {
		expectedIDs[index] = job.id
	}
	for _, observer := range []*adapter{goAdapter, candidateAdapter} {
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{
			"direction": "asc", "limit": len(inserted), "order_by": "scheduled_at", "states": []string{"scheduled"},
			"tags_all": []string{"sqlite_timestamps"},
		}, &listed)
		require.Equal(t, expectedIDs, jobIDs(listed.Jobs), "%s listing by scheduled_at", observer.name)
	}
}

func verifySQLiteTransactions(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		handle := "sqlite-commit-" + pair.actor.name
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted, inTransaction normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job": map[string]any{
				"message": "SQLite transaction commit",
				"opts":    map[string]any{"tags": []string{"sqlite_transaction"}},
			},
		}, &inserted)
		pair.actor.call(t, "tx_get", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, inserted, inTransaction)
		requireJobNotFound(t, pair.observer, inserted.ID)
		pair.actor.call(t, "tx_update", map[string]any{
			"handle": handle, "id": inserted.ID, "output": map[string]any{"committed": true},
		}, &inTransaction)
		pair.actor.call(t, "tx_cancel", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, "cancelled", inTransaction.State)
		pair.actor.call(t, "tx_retry", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &inTransaction)
		require.Equal(t, "available", inTransaction.State)
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_list", map[string]any{
			"handle": handle, "ids": []int64{inserted.ID},
		}, &listed)
		require.Equal(t, []normalizedJob{inTransaction}, listed.Jobs)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		var observed normalizedJob
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inTransaction, observed)

		handle = "sqlite-rollback-" + pair.actor.name
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle, "job": map[string]any{"message": "SQLite transaction rollback"},
		}, &inserted)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		requireJobNotFound(t, pair.observer, inserted.ID)

		handle = "sqlite-batch-error-" + pair.actor.name
		tag := strings.ReplaceAll(handle, "-", "_")
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.actor.requireCallError(t, "tx_insert_many", map[string]any{
			"handle": handle,
			"jobs": []map[string]any{
				{"message": "must not partially commit", "opts": map[string]any{"tags": []string{tag}}},
				{"message": "invalid", "opts": map[string]any{"priority": 99}},
			},
		}, "rejected")
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs)
	}
}

// rawNotification is one SQLite outbox row as `raw_notifications` returns it.
type rawNotification struct {
	ID          int64  `json:"id"`
	Payload     string `json:"payload"`
	PayloadType string `json:"payload_type"`
	Topic       string `json:"topic"`
}
