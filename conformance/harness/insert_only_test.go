//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestInsertOnlyConformance checks the insert-only-v1 profile: a client that
// only enqueues jobs, such as a producer library for a language without a
// River worker runtime. The candidate inserts; the Go reference observes and
// works every job.
//
//nolint:paralleltest // Scenarios share one database and adapter processes, so they run sequentially.
func TestInsertOnlyConformance(t *testing.T) {
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerInsertOnly)
	repositoryRoot := repoRoot(t)
	candidateSpec := conformanceCandidateSpec(t, repositoryRoot, false)
	candidateSpec.requireProfile(t, profileInsertOnly)
	observer := newPostgresObserver(t, databaseURL)
	reference := startReferenceAdapter(t, repositoryRoot, databaseURL, "go")
	candidate := startAdapterCommandForProfile(t, repositoryRoot, databaseURL, "postgres", profileInsertOnly,
		candidateSpec.Implementation, candidateSpec.Command)
	candidate.applicationName = candidateSpec.ApplicationName
	candidate.spec = candidateSpec
	scenarios.attach(reference, candidate)

	t.Run("insert_only_profile_handshake", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertOnlyHandshake(t, repositoryRoot, candidateSpec, candidate)
	})
	reference.call(t, "migrate", map[string]any{}, nil)
	t.Run("insert_only_insert_reference_work", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertOnlyInsert(t, candidate, reference)
	})
	t.Run("insert_only_typed_batch", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertOnlyBatch(t, candidate, reference)
	})
	t.Run("insert_only_transactional_insert", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertOnlyTransactions(t, observer, candidate, reference)
	})
	t.Run("insert_only_unique_insert", func(t *testing.T) {
		defer scenarios.record(t)

		verifyUniqueKeyGoldens(t, repositoryRoot, candidate)
		verifyInsertOnlyUnique(t, candidate, reference)
	})
	t.Run("insert_only_insert_notification", func(t *testing.T) {
		defer scenarios.record(t)

		verifyInsertNotificationWakeup(t, candidate, reference)
	})
}

// verifyInsertOnlyHandshake requires the candidate to advertise exactly the
// insert-only profile and reject every other contract method.
func verifyInsertOnlyHandshake(t *testing.T, repositoryRoot string, candidateSpec adapterSpec, candidate *adapter) {
	t.Helper()

	var profile adapterProfile
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/adapter/profiles/insert-only.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &profile))
	manifest := readManifest(t, repositoryRoot)
	var handshake adapterHandshake
	candidate.call(t, "handshake", map[string]any{}, &handshake)
	require.Equal(t, candidateSpec.Implementation, handshake.Implementation)
	require.Equal(t, manifest.Implementations[candidateSpec.Implementation].Version, handshake.ImplementationVersion)
	require.Equal(t, profile.Backend, handshake.Backend)
	require.Equal(t, profile.Name, handshake.Profile)
	require.Equal(t, profile.ProtocolRevision, handshake.ProtocolRevision)
	require.Equal(t, profile.Capabilities, handshake.Capabilities)
	require.Equal(t, profile.Methods, handshake.Methods)
	require.Equal(t, map[string]int{manifest.Migration.Line: manifest.Migration.Latest}, handshake.MigrationLines)
	contract, err := sharedAdapterContract()
	require.NoError(t, err)
	for method := range contract.methods {
		if !slices.Contains(profile.Methods, method) {
			candidate.requireUnvalidatedCallError(t, method, map[string]any{}, "method_not_found")
		}
	}
	candidate.requireUnvalidatedCallError(t, "insert", map[string]any{"message": "unknown", "unexpected": true}, "invalid_params")
}

// verifyInsertOnlyInsert compares a candidate insert with the same insert
// made by the reference, field by field, and has the reference work it.
func verifyInsertOnlyInsert(t *testing.T, candidate, reference *adapter) {
	t.Helper()

	reference.call(t, "reset", map[string]any{}, nil)
	scheduledAt := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond).Format(time.RFC3339Nano)
	for _, params := range []map[string]any{
		{"message": "defaults"},
		{"message": "options", "opts": map[string]any{
			"max_attempts": 3, "metadata": map[string]any{"source": "insert-only"}, "priority": 2,
			"queue": "insert_only", "tags": []string{"insert_only"},
		}},
		{"message": "scheduled", "opts": map[string]any{"scheduled_at": scheduledAt}},
		{"message": "pending", "opts": map[string]any{"pending": true}},
	} {
		var fromCandidate, fromReference, observed normalizedJob
		candidate.call(t, "insert", params, &fromCandidate)
		reference.call(t, "insert", params, &fromReference)
		reference.call(t, "get", map[string]any{"id": fromCandidate.ID}, &observed)
		require.Equal(t, fromCandidate, observed)
		require.Equal(t, comparableJob(fromReference), comparableJob(fromCandidate), "%s insert differs from the reference", params["message"])
	}

	var inserted, worked normalizedJob
	candidate.call(t, "insert", map[string]any{"message": "worked by the reference"}, &inserted)
	reference.call(t, "work", map[string]any{"client_id": "go-insert-only-worker", "id": inserted.ID}, &worked)
	require.Equal(t, "completed", worked.State)
	require.Equal(t, []string{"go-insert-only-worker"}, worked.AttemptedBy)
}

// comparableJob clears the fields that legitimately differ between two
// separately inserted jobs.
func comparableJob(job normalizedJob) normalizedJob {
	job.CreatedAt = ""
	job.ID = 0
	if job.State == "available" || job.State == "pending" {
		job.ScheduledAt = ""
	}
	return job
}

// verifyInsertOnlyBatch checks typed batch results in input order, including
// a duplicate of a unique job the reference inserted.
func verifyInsertOnlyBatch(t *testing.T, candidate, reference *adapter) {
	t.Helper()

	reference.call(t, "reset", map[string]any{}, nil)
	uniqueParams := map[string]any{
		"message": "insert-only duplicate", "opts": map[string]any{"unique": map[string]any{"by_args": true}},
	}
	var existing normalizedJob
	reference.call(t, "insert", uniqueParams, &existing)
	var inserted struct {
		Results []normalizedInsertResult `json:"results"`
	}
	candidate.call(t, "insert_many", map[string]any{"jobs": []map[string]any{
		{"message": "batch first", "opts": map[string]any{"metadata": map[string]any{"batch_index": 0}}},
		uniqueParams,
		{"message": "batch pending", "opts": map[string]any{"pending": true}},
	}}, &inserted)
	require.Len(t, inserted.Results, 3)
	require.False(t, inserted.Results[0].UniqueSkippedAsDuplicate)
	require.EqualValues(t, 0, inserted.Results[0].Job.Metadata["batch_index"])
	require.True(t, inserted.Results[1].UniqueSkippedAsDuplicate)
	require.Equal(t, existing, inserted.Results[1].Job)
	require.Equal(t, "pending", inserted.Results[2].Job.State)
	for _, result := range inserted.Results {
		var observed normalizedJob
		reference.call(t, "get", map[string]any{"id": result.Job.ID}, &observed)
		require.Equal(t, result.Job, observed)
	}
	candidate.requireCallError(t, "insert_many", map[string]any{"jobs": []map[string]any{}}, "rejected")
}

// verifyInsertOnlyTransactions checks that transactional inserts become
// visible and publish insert notifications only on commit.
func verifyInsertOnlyTransactions(t *testing.T, observer *postgresObserver, candidate, reference *adapter) {
	t.Helper()

	insertChannel := observer.currentSchema(t) + ".river_insert"
	for _, commit := range []bool{false, true} {
		reference.call(t, "reset", map[string]any{}, nil)
		listener := observer.listen(t, insertChannel)
		outcome := "rollback"
		if commit {
			outcome = "commit"
		}
		handle := "insert-only-" + outcome
		tag := strings.ReplaceAll(handle, "-", "_")
		candidate.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var single normalizedJob
		candidate.call(t, "tx_insert", map[string]any{
			"handle": handle, "job": map[string]any{"message": handle, "opts": map[string]any{"tags": []string{tag}}},
		}, &single)
		var batch struct {
			Results []normalizedInsertResult `json:"results"`
		}
		candidate.call(t, "tx_insert_many", map[string]any{
			"handle": handle, "jobs": []map[string]any{{"message": handle + " batch", "opts": map[string]any{"tags": []string{tag}}}},
		}, &batch)
		require.Len(t, batch.Results, 1)
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		reference.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs, "transactional inserts became visible before commit")
		if commit {
			candidate.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
			require.NotEmpty(t, listener.receiveUntilMarker(t, observer, handle+"-marker"), "commit published no insert notification")
			reference.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.ElementsMatch(t, []int64{single.ID, batch.Results[0].Job.ID}, jobIDs(listed.Jobs))
		} else {
			candidate.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
			require.Empty(t, listener.receiveUntilMarker(t, observer, handle+"-marker"), "rollback published an insert notification")
			reference.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.Empty(t, listed.Jobs)
		}
	}
}

// verifyInsertOnlyUnique checks that unique inserts from the candidate and
// the reference resolve to the same row in both orders and for each unique
// dimension.
func verifyInsertOnlyUnique(t *testing.T, candidate, reference *adapter) {
	t.Helper()

	for _, opts := range []map[string]any{
		{"unique": map[string]any{"by_args": true}},
		{"scheduled_at": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), "unique": map[string]any{"by_period_ms": 60_000}},
		{"queue": "unique_queue", "unique": map[string]any{"by_queue": true}},
	} {
		for _, order := range [][2]*adapter{{candidate, reference}, {reference, candidate}} {
			reference.call(t, "reset", map[string]any{}, nil)
			params := map[string]any{"message": "insert-only unique", "opts": opts}
			var first, second normalizedJob
			order[0].call(t, "insert", params, &first)
			order[1].call(t, "insert", params, &second)
			require.Equal(t, first, second, "%s then %s with %v", order[0].name, order[1].name, opts)
			require.NotNil(t, first.UniqueKey)
		}
	}
}
