//go:build riverconformance

package harness_test

import (
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

// verifyCustomSchema migrates a custom schema with one implementation, then
// has the other insert and work a job in it. It also checks that the worker
// accepts the longest portable schema name and rejects invalid names.
func verifyCustomSchema(t *testing.T, schema string, migrator, worker *adapter) {
	t.Helper()

	migrator.call(t, "migrate", map[string]any{"schema": schema}, nil)
	migrator.call(t, "reset", map[string]any{"schema": schema}, nil)

	var inserted, observed, worked normalizedJob
	worker.call(t, "insert", map[string]any{
		"message": "custom schema", "schema": schema,
	}, &inserted)
	migrator.call(t, "get", map[string]any{
		"id": inserted.ID, "schema": schema,
	}, &observed)
	require.Equal(t, inserted, observed)
	worker.call(t, "work", map[string]any{
		"id": inserted.ID, "schema": schema,
	}, &worked)
	require.Equal(t, "completed", worked.State)
	migrator.call(t, "get", map[string]any{
		"id": inserted.ID, "schema": schema,
	}, &observed)
	require.Equal(t, worked, observed)

	boundarySchema := strings.Repeat("s", 46)
	migrator.call(t, "migrate", map[string]any{"schema": boundarySchema}, nil)
	var boundaryJob normalizedJob
	worker.call(t, "insert", map[string]any{
		"message": "maximum portable schema", "schema": boundarySchema,
	}, &boundaryJob)
	require.Positive(t, boundaryJob.ID)
	worker.requireCallError(t, "insert", map[string]any{
		"message": "schema too long", "schema": strings.Repeat("s", 47),
	}, "rejected")
	// Any other schema name works in both implementations as long as it's
	// quoted, like Go's `SafeIdentifier`, so only the length is portable to
	// reject here.
}

// verifyConcurrentUniqueConflicts proves that a unique insert blocks on
// another implementation's uncommitted conflicting insert and then returns
// the committed winner. The loser's backend is observed waiting on a lock in
// PostgreSQL before the winner commits, so a slow response cannot pass as a
// blocked one.
func verifyConcurrentUniqueConflicts(t *testing.T, observer *postgresObserver, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	allStates := []string{
		"available",
		"cancelled",
		"completed",
		"discarded",
		"pending",
		"retryable",
		"running",
		"scheduled",
	}
	fixedScheduledAt := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	testCases := []struct {
		name string
		opts map[string]any
	}{
		{
			name: "by_args",
			opts: map[string]any{"unique": map[string]any{"by_args": true}},
		},
		{
			name: "by_period",
			opts: map[string]any{
				"scheduled_at": fixedScheduledAt,
				"unique":       map[string]any{"by_period_ms": 60_000},
			},
		},
		{
			name: "by_queue",
			opts: map[string]any{
				"queue":  "unique_queue",
				"unique": map[string]any{"by_queue": true},
			},
		},
		{
			name: "by_state",
			opts: map[string]any{"unique": map[string]any{"by_state": allStates}},
		},
	}

	for _, testCase := range testCases {
		for _, direction := range []struct {
			loser  *adapter
			winner *adapter
		}{
			{loser: candidateAdapter, winner: goAdapter},
			{loser: goAdapter, winner: candidateAdapter},
		} {
			loser, winner := direction.loser, direction.winner
			goAdapter.call(t, "reset", map[string]any{}, nil)
			winnerHandle := fmt.Sprintf("%s-%s-winner", testCase.name, winner.name)
			loserHandle := fmt.Sprintf("%s-%s-loser", testCase.name, loser.name)
			winner.call(t, "tx_begin", map[string]any{"handle": winnerHandle}, nil)
			loser.call(t, "tx_begin", map[string]any{"handle": loserHandle}, nil)

			params := map[string]any{
				"handle": winnerHandle,
				"job": map[string]any{
					"message": "concurrent unique " + testCase.name,
					"opts":    testCase.opts,
				},
			}
			var winnerJob normalizedJob
			winner.call(t, "tx_insert", params, &winnerJob)

			loserParams := map[string]any{"handle": loserHandle, "job": params["job"]}
			type loserResult struct {
				err error
				job normalizedJob
			}
			resultCh := make(chan loserResult, 1)
			go func() {
				var job normalizedJob
				err := loser.callWithoutTest("tx_insert", loserParams, &job)
				resultCh <- loserResult{err: err, job: job}
			}()

			observer.waitForLockWait(t, loser.applicationName)
			select {
			case result := <-resultCh:
				t.Fatalf("%s unique insert returned while %s's conflict was uncommitted (%s): %+v", loser.name, winner.name, testCase.name, result)
			default:
			}
			winner.call(t, "tx_commit", map[string]any{"handle": winnerHandle}, nil)

			var result loserResult
			select {
			case result = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s unique insert remained blocked after %s committed (%s)", loser.name, winner.name, testCase.name)
			}
			require.NoError(t, result.err)
			loser.call(t, "tx_commit", map[string]any{"handle": loserHandle}, nil)
			require.Equal(t, winnerJob, result.job)

			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			goAdapter.call(t, "list", map[string]any{}, &listed)
			require.Equal(t, []normalizedJob{winnerJob}, listed.Jobs)
		}
	}

	// Sequential inserts with the same unique arguments from both
	// implementations resolve to one row.
	goAdapter.call(t, "reset", map[string]any{}, nil)
	uniqueParams := map[string]any{
		"message": "cross-language unique",
		"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
	}
	var uniqueGo, uniqueCandidate normalizedJob
	goAdapter.call(t, "insert", uniqueParams, &uniqueGo)
	candidateAdapter.call(t, "insert", uniqueParams, &uniqueCandidate)
	require.Equal(t, uniqueGo, uniqueCandidate)
}

// verifyConcurrentCancelRetryRace races a cancel, then a retry, between the
// implementations. The winner holds the job's row lock in an open
// transaction until the loser's request is observed waiting on it, so the
// loser's statement starts before the winner commits. Its update then
// matches nothing, and it must return the winner's committed row rather than
// the row as its statement first saw it.
func verifyConcurrentCancelRetryRace(t *testing.T, observer *postgresObserver, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, direction := range []struct {
		loser  *adapter
		winner *adapter
	}{
		{loser: candidateAdapter, winner: goAdapter},
		{loser: goAdapter, winner: candidateAdapter},
	} {
		loser, winner := direction.loser, direction.winner
		goAdapter.call(t, "reset", map[string]any{}, nil)

		race := func(t *testing.T, operation string, id int64) {
			t.Helper()

			handle := fmt.Sprintf("%s-%s-winner", operation, winner.name)
			winner.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
			var winnerJob normalizedJob
			winner.call(t, "tx_"+operation, map[string]any{"handle": handle, "id": id}, &winnerJob)

			type loserResult struct {
				err error
				job normalizedJob
			}
			resultCh := make(chan loserResult, 1)
			go func() {
				var job normalizedJob
				err := loser.callWithoutTest(operation, map[string]any{"id": id}, &job)
				resultCh <- loserResult{err: err, job: job}
			}()

			observer.waitForLockWait(t, loser.applicationName)
			select {
			case result := <-resultCh:
				t.Fatalf("%s %s returned while %s's was uncommitted: %+v", loser.name, operation, winner.name, result)
			default:
			}
			winner.call(t, "tx_commit", map[string]any{"handle": handle}, nil)

			var result loserResult
			select {
			case result = <-resultCh:
			case <-time.After(5 * time.Second):
				t.Fatalf("%s %s remained blocked after %s committed", loser.name, operation, winner.name)
			}
			require.NoError(t, result.err)
			require.Equal(t, winnerJob, result.job,
				"%s lost a %s race to %s and must return the committed row", loser.name, operation, winner.name)

			var committed normalizedJob
			goAdapter.call(t, "get", map[string]any{"id": id}, &committed)
			require.Equal(t, winnerJob, committed)
		}

		var job normalizedJob
		goAdapter.call(t, "insert", map[string]any{
			"message": "cancel and retry race",
			"opts":    map[string]any{"scheduled_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)},
		}, &job)
		race(t, "cancel", job.ID)
		race(t, "retry", job.ID)
	}
}

// verifyBatchInsertion checks typed batch insertion results, ordering,
// duplicate reporting, repeated unique keys, invalid unique options, and
// atomic rejection outside a transaction.
func verifyBatchInsertion(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		pair.actor.requireCallError(t, "insert_many", map[string]any{
			"jobs": []map[string]any{},
		}, "rejected")
		uniqueParams := map[string]any{
			"message": "typed batch duplicate " + pair.actor.name,
			"opts":    map[string]any{"unique": map[string]any{"by_args": true}},
		}
		var existing normalizedJob
		pair.actor.call(t, "insert", uniqueParams, &existing)

		jobs := []map[string]any{
			{
				"message": "typed batch first " + pair.actor.name,
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 0},
					"priority": 2,
					"tags":     []string{"typed_batch_" + pair.actor.name},
				},
			},
			uniqueParams,
			{
				"message": "typed batch third " + pair.actor.name,
				"opts": map[string]any{
					"pending": true,
					"tags":    []string{"typed_batch_" + pair.actor.name},
				},
			},
		}
		var inserted struct {
			Results []normalizedInsertResult `json:"results"`
		}
		pair.actor.call(t, "insert_many", map[string]any{"jobs": jobs}, &inserted)
		require.Len(t, inserted.Results, 3)
		for _, result := range inserted.Results {
			require.NotNil(t, result.Job.Errors)
			require.Empty(t, result.Job.Errors)
		}
		require.False(t, inserted.Results[0].UniqueSkippedAsDuplicate)
		require.EqualValues(t, 0, inserted.Results[0].Job.Metadata["batch_index"])
		require.Equal(t, 2, inserted.Results[0].Job.Priority)
		require.Equal(t, existing, inserted.Results[1].Job)
		require.True(t, inserted.Results[1].UniqueSkippedAsDuplicate)
		require.False(t, inserted.Results[2].UniqueSkippedAsDuplicate)
		require.Equal(t, "pending", inserted.Results[2].Job.State)

		var observed normalizedJob
		for _, result := range inserted.Results {
			observed = normalizedJob{}
			pair.observer.call(t, "get", map[string]any{"id": result.Job.ID}, &observed)
			require.Equal(t, result.Job, observed)
		}

		invalidTag := "invalid_batch_" + pair.actor.name
		pair.actor.requireCallError(t, "insert_many", map[string]any{"jobs": []map[string]any{
			{"message": "must roll back", "opts": map[string]any{"tags": []string{invalidTag}}},
			{"message": "invalid priority", "opts": map[string]any{"priority": 99}},
		}}, "rejected")
		var invalidRows struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{invalidTag}}, &invalidRows)
		require.Empty(t, invalidRows.Jobs)

		// A unique key may appear only once in a batch among jobs whose state
		// it covers. PostgreSQL reports a database error and SQLite a
		// rejection, so only the failure and its atomicity are compared.
		repeatedTag := "repeated_key_batch_" + pair.actor.name
		repeated := map[string]any{
			"message": "repeated unique key " + pair.actor.name,
			"opts": map[string]any{
				"tags":   []string{repeatedTag},
				"unique": map[string]any{"by_args": true},
			},
		}
		response := pair.actor.callResponse(t, "insert_many", map[string]any{
			"jobs": []map[string]any{repeated, repeated},
		})
		require.NotNil(t, response.Error, "%s adapter inserted a batch repeating a unique key", pair.actor.name)
		var repeatedRows struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.observer.call(t, "list", map[string]any{"tags_all": []string{repeatedTag}}, &repeatedRows)
		require.Empty(t, repeatedRows.Jobs)

		// Excluding the kind needs arguments, queue, or period in the key.
		pair.actor.requireCallError(t, "insert", map[string]any{
			"message": "unique without kind " + pair.actor.name,
			"opts":    map[string]any{"unique": map[string]any{"exclude_kind": true}},
		}, "rejected")
	}
}

func verifyLargeBatchInsertion(t *testing.T, adapters ...*adapter) {
	t.Helper()

	const batchSize = 6_000
	for _, actor := range adapters {
		actor.call(t, "reset", map[string]any{}, nil)
		jobs := make([]map[string]any, batchSize)
		for index := range jobs {
			jobs[index] = map[string]any{
				"message": fmt.Sprintf("large ordinary batch %s %d", actor.name, index),
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": index},
				},
			}
		}
		var inserted struct {
			Results []normalizedInsertResult `json:"results"`
		}
		actor.call(t, "insert_many", map[string]any{"jobs": jobs}, &inserted)
		require.Len(t, inserted.Results, batchSize)
		for index, result := range inserted.Results {
			require.EqualValues(t, index, result.Job.Metadata["batch_index"], "result %d is out of input order", index)
		}
	}
}

// verifyTransactionalBatchInsertion checks that typed batches inserted in a
// caller-managed transaction are invisible to the other implementation until
// commit and never visible after rollback.
func verifyTransactionalBatchInsertion(t *testing.T, actor, observer *adapter) {
	t.Helper()

	actor.call(t, "reset", map[string]any{}, nil)
	emptyHandle := "batch-empty-" + actor.name
	actor.call(t, "tx_begin", map[string]any{"handle": emptyHandle}, nil)
	actor.requireCallError(t, "tx_insert_many", map[string]any{
		"handle": emptyHandle,
		"jobs":   []map[string]any{},
	}, "rejected")
	actor.call(t, "tx_commit", map[string]any{"handle": emptyHandle}, nil)
	for _, commit := range []bool{false, true} {
		actor.call(t, "reset", map[string]any{}, nil)
		outcome := "rollback"
		if commit {
			outcome = "commit"
		}
		handle := fmt.Sprintf("batch-%s-typed-%s", actor.name, outcome)
		tag := strings.ReplaceAll(handle, "-", "_")
		actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		jobs := []map[string]any{
			{
				"message": handle + " first",
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 0},
					"priority": 2,
					"tags":     []string{tag},
				},
			},
			{
				"message": handle + " second",
				"opts": map[string]any{
					"metadata": map[string]any{"batch_index": 1},
					"priority": 3,
					"tags":     []string{tag},
				},
			},
		}
		var result struct {
			Results []normalizedInsertResult `json:"results"`
		}
		actor.call(t, "tx_insert_many", map[string]any{"handle": handle, "jobs": jobs}, &result)
		require.Len(t, result.Results, 2)
		require.EqualValues(t, 0, result.Results[0].Job.Metadata["batch_index"])
		require.EqualValues(t, 1, result.Results[1].Job.Metadata["batch_index"])

		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
		require.Empty(t, listed.Jobs)
		if commit {
			actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
			observer.call(t, "list", map[string]any{
				"direction": "asc", "order_by": "id", "tags_all": []string{tag},
			}, &listed)
			require.Len(t, listed.Jobs, 2)
			require.Equal(t, []int{2, 3}, []int{listed.Jobs[0].Priority, listed.Jobs[1].Priority})
		} else {
			actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
			observer.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.Empty(t, listed.Jobs)
		}
	}
}

// verifyDifferentialJobCRUD writes with one implementation and reads,
// updates, cancels, retries, and deletes with alternating implementations.
func verifyDifferentialJobCRUD(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		writerTag := "writer_" + pair.writer.name
		pair.writer.call(t, "reset", map[string]any{}, nil)
		var inserted, observed normalizedJob
		pair.writer.call(t, "insert", map[string]any{
			"message": "differential CRUD",
			"opts": map[string]any{
				"metadata": map[string]any{"writer": pair.writer.name},
				"priority": 3,
				"tags":     []string{"all_jobs", writerTag},
			},
		}, &inserted)
		pair.reader.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)

		listParams := map[string]any{
			"ids": []int64{inserted.ID}, "tags_all": []string{"all_jobs", writerTag},
		}
		var readerList, writerList struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.writer.call(t, "list", listParams, &writerList)
		pair.reader.call(t, "list", listParams, &readerList)
		require.Equal(t, writerList, readerList)
		require.Equal(t, []normalizedJob{inserted}, writerList.Jobs)

		var updated normalizedJob
		pair.reader.call(t, "update", map[string]any{
			"id": inserted.ID, "output": map[string]any{"updated_by": pair.reader.name},
		}, &updated)
		require.Equal(t, map[string]any{"updated_by": pair.reader.name}, updated.Metadata["output"])
		pair.writer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, updated, observed)

		var cancelled normalizedJob
		pair.writer.call(t, "cancel", map[string]any{"id": inserted.ID}, &cancelled)
		pair.reader.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, cancelled, observed)
		require.Equal(t, "cancelled", cancelled.State)

		var retried normalizedJob
		pair.reader.call(t, "retry", map[string]any{"id": inserted.ID}, &retried)
		pair.writer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, retried, observed)
		require.Equal(t, "available", retried.State)
		require.Nil(t, retried.FinalizedAt)

		var deleted normalizedJob
		pair.writer.call(t, "delete", map[string]any{"id": inserted.ID}, &deleted)
		require.Equal(t, retried, deleted)
		requireJobNotFound(t, pair.reader, inserted.ID)
	}
}

// verifyBulkDeleteSafety deletes an explicit ID set across implementations
// and requires both implementations to refuse an unfiltered bulk delete.
func verifyBulkDeleteSafety(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		bulkIDs := make([]int64, 0, 2)
		for index := range 2 {
			var bulk normalizedJob
			pair.writer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("bulk delete %d", index),
			}, &bulk)
			bulkIDs = append(bulkIDs, bulk.ID)
		}
		var survivor normalizedJob
		pair.writer.call(t, "insert", map[string]any{"message": "bulk delete survivor"}, &survivor)
		var bulkDeleted struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.reader.call(t, "delete_many", map[string]any{"ids": bulkIDs}, &bulkDeleted)
		require.ElementsMatch(t, bulkIDs, jobIDs(bulkDeleted.Jobs))
		for _, id := range bulkIDs {
			requireJobNotFound(t, pair.writer, id)
		}
		for _, current := range []*adapter{pair.writer, pair.reader} {
			current.requireCallError(t, "delete_many", map[string]any{}, "rejected")
		}
		var observed normalizedJob
		pair.writer.call(t, "get", map[string]any{"id": survivor.ID}, &observed)
		require.Equal(t, survivor, observed)
	}
}

// verifyJobCleanerQueueFilters runs batches of the job cleaner's deletion with
// each implementation over jobs the other finalized. Retained jobs in queues
// `kept1`/`kept2` are inserted before jobs in `deleted1`/`deleted2`, so they
// hold the lowest IDs and outnumber a batch of 2. A query that limits
// candidates before applying queue filters would select only retained jobs,
// delete nothing, and stop the cleaner from making progress.
func verifyJobCleanerQueueFilters(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	queues := []string{"kept1", "kept2", "kept1", "kept2", "kept1", "kept2", "deleted1", "deleted2", "deleted1", "deleted2", "deleted1"}
	for _, pair := range []struct {
		cleaner *adapter
		writer  *adapter
	}{
		{cleaner: candidateAdapter, writer: goAdapter},
		{cleaner: goAdapter, writer: candidateAdapter},
	} {
		for _, testCase := range []struct {
			name              string
			queuesExcluded    []string
			queuesIncluded    []string // nil omits the inclusion filter
			wantBatches       []int    // jobs deleted by each successive batch
			wantDeletedQueues []string // queues whose jobs are eligible
		}{
			// `kept1` appears in both lists; exclusion takes precedence.
			{name: "both", queuesExcluded: []string{"kept1", "kept2"}, queuesIncluded: []string{"deleted1", "deleted2", "kept1"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			// An empty exclusion list excludes nothing.
			{name: "empty excluded", queuesExcluded: []string{}, wantBatches: []int{2, 2, 2, 2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2", "kept1", "kept2"}},
			// An empty inclusion list matches no queues, unlike an absent one.
			{name: "empty included", queuesIncluded: []string{}, wantBatches: []int{0}},
			{name: "excluded", queuesExcluded: []string{"kept1", "kept2"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			{name: "included", queuesIncluded: []string{"deleted1", "deleted2"}, wantBatches: []int{2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2"}},
			{name: "missing included", queuesIncluded: []string{"missing"}, wantBatches: []int{0}},
			{name: "no filters", wantBatches: []int{2, 2, 2, 2, 2, 1, 0}, wantDeletedQueues: []string{"deleted1", "deleted2", "kept1", "kept2"}},
		} {
			pair.writer.call(t, "reset", map[string]any{}, nil)
			allIDs := make([]int64, 0, len(queues))
			var eligibleIDs []int64
			for _, queue := range queues {
				var job normalizedJob
				pair.writer.call(t, "insert", map[string]any{
					"message": "job cleaner queue filters", "opts": map[string]any{"queue": queue},
				}, &job)
				pair.writer.call(t, "cancel", map[string]any{"id": job.ID}, nil)
				allIDs = append(allIDs, job.ID)
				if slices.Contains(testCase.wantDeletedQueues, queue) {
					eligibleIDs = append(eligibleIDs, job.ID)
				}
			}

			params := map[string]any{
				// Every job was finalized just now, so a future horizon makes
				// each one old enough to delete.
				"before": time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
				"limit":  2,
			}
			if testCase.queuesExcluded != nil {
				params["queues_excluded"] = testCase.queuesExcluded
			}
			if testCase.queuesIncluded != nil {
				params["queues_included"] = testCase.queuesIncluded
			}
			var deletedTotal int
			for batch, wantDeleted := range testCase.wantBatches {
				var result struct {
					Deleted int `json:"deleted"`
				}
				pair.cleaner.call(t, "delete_finalized", params, &result)
				require.Equal(t, wantDeleted, result.Deleted, "%s batch %d over %s's jobs (%s)", pair.cleaner.name, batch, pair.writer.name, testCase.name)
				deletedTotal += result.Deleted

				// Batches delete the oldest eligible jobs first, so exactly
				// the first deletedTotal eligible jobs are gone.
				var listed struct {
					Jobs []normalizedJob `json:"jobs"`
				}
				pair.writer.call(t, "list", map[string]any{"ids": allIDs, "limit": len(allIDs), "order_by": "id"}, &listed)
				require.Equal(t,
					slices.DeleteFunc(slices.Clone(allIDs), func(id int64) bool { return slices.Contains(eligibleIDs[:deletedTotal], id) }),
					jobIDs(listed.Jobs),
					"%s batch %d over %s's jobs (%s)", pair.cleaner.name, batch, pair.writer.name, testCase.name,
				)
			}
			require.Len(t, eligibleIDs, deletedTotal, "%s over %s's jobs (%s)", pair.cleaner.name, pair.writer.name, testCase.name)
		}
	}
}

// verifyDifferentialListCursors pages through a filtered list with cursors
// emitted by one implementation and consumed by the other.
func verifyDifferentialListCursors(t *testing.T, goAdapter, candidateAdapter *adapter, filterMetadata bool) {
	t.Helper()

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		paginationIDs := make([]int64, 0, 3)
		for index := range 3 {
			var paginationJob normalizedJob
			pair.writer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("pagination %d", index),
				"opts": map[string]any{
					"metadata":     map[string]any{"pagination_writer": pair.writer.name},
					"priority":     index + 1,
					"scheduled_at": fmt.Sprintf("2099-01-01T00:00:0%dZ", index+1),
					"tags":         []string{"pagination_jobs"},
				},
			}, &paginationJob)
			paginationIDs = append(paginationIDs, paginationJob.ID)
		}
		// A job outside every filter must never appear.
		var excluded normalizedJob
		pair.writer.call(t, "insert", map[string]any{"message": "pagination excluded"}, &excluded)
		type jobPage struct {
			Cursor *string         `json:"cursor"`
			Jobs   []normalizedJob `json:"jobs"`
		}
		pageParams := func(after *string) map[string]any {
			params := map[string]any{
				"direction":  "desc",
				"limit":      2,
				"order_by":   "scheduled_at",
				"priorities": []int{1, 2, 3},
				"states":     []string{"scheduled"},
				"tags_all":   []string{"pagination_jobs"},
			}
			if filterMetadata {
				params["metadata"] = map[string]any{"pagination_writer": pair.writer.name}
			}
			if after != nil {
				params["after"] = *after
			}
			return params
		}
		var readerPage, writerPage jobPage
		pair.reader.call(t, "list", pageParams(nil), &readerPage)
		pair.writer.call(t, "list", pageParams(nil), &writerPage)
		require.Equal(t, writerPage, readerPage)
		require.Equal(t, []int64{paginationIDs[2], paginationIDs[1]}, jobIDs(writerPage.Jobs))
		require.NotNil(t, writerPage.Cursor)

		var readerSecondPage, writerSecondPage jobPage
		pair.reader.call(t, "list", pageParams(writerPage.Cursor), &readerSecondPage)
		pair.writer.call(t, "list", pageParams(readerPage.Cursor), &writerSecondPage)
		require.Equal(t, writerSecondPage, readerSecondPage)
		require.Equal(t, []int64{paginationIDs[0]}, jobIDs(writerSecondPage.Jobs))
	}
}

// jobListCursorKind is a job kind that Go's `encoding/json` escapes (`<`,
// `>`, and `&` become `\u003c`, `\u003e`, and `\u0026`) and whose cursor
// text always contains `-`, wherever the kind falls in the Base64 groups:
// one of three consecutive `~` bytes ends a group, and its low six bits
// encode as `-`.
const jobListCursorKind = "conformance_cursor<>&~~~"

// verifyJobListCursorInterchange checks that job-list cursors are
// interchangeable for each sort field: both engines emit byte-identical
// cursor text for the same page, and each resumes from the other's cursor
// to the same next page, in both directions. Time ordering over mixed states
// uses the first listed state's field for every job and its cursor, with
// nulls last ascending and first descending.
func verifyJobListCursorInterchange(t *testing.T, first, second *adapter) {
	t.Helper()

	type jobPage struct {
		Cursor *string         `json:"cursor"`
		Jobs   []normalizedJob `json:"jobs"`
	}
	type listCase struct {
		kind    string
		orderBy string
		// order lists the kind's jobs by insertion index in ascending list
		// order, or nil for insertion order.
		order  []int
		states []string
	}
	const echoKind = "conformance_echo"
	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: second, writer: first},
		{reader: first, writer: second},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		idsByKind := make(map[string][]int64, 2)
		for index := range 3 {
			// Scheduled times have fractional seconds that Go encodes with
			// trailing zeros trimmed, like `.12`.
			var scheduled, raw normalizedJob
			pair.writer.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("cursor %d", index),
				"opts": map[string]any{
					"scheduled_at": fmt.Sprintf("2099-01-01T00:00:0%d.%d2Z", index+1, index+1),
				},
			}, &scheduled)
			idsByKind[echoKind] = append(idsByKind[echoKind], scheduled.ID)
			// A raw row's `scheduled_at` comes from a column default that
			// SQLite stores in a non-canonical format, so this kind is
			// ordered only by ID until it is cancelled.
			pair.writer.call(t, "raw_insert_no_notify", map[string]any{
				"kind": jobListCursorKind, "message": fmt.Sprintf("cursor %d", index),
			}, &raw)
			idsByKind[jobListCursorKind] = append(idsByKind[jobListCursorKind], raw.ID)
		}

		verifyCases := func(cases []listCase) {
			for _, current := range cases {
				for _, direction := range []string{"asc", "desc"} {
					description := fmt.Sprintf("%s -> %s: kind %s ordered by %s %s",
						pair.writer.name, pair.reader.name, current.kind, current.orderBy, direction)
					expected := slices.Clone(idsByKind[current.kind])
					if current.order != nil {
						expected = expected[:0]
						for _, index := range current.order {
							expected = append(expected, idsByKind[current.kind][index])
						}
					}
					if direction == "desc" {
						slices.Reverse(expected)
					}
					params := func(after *string) map[string]any {
						params := map[string]any{
							"direction": direction,
							"kinds":     []string{current.kind},
							"limit":     2,
							"order_by":  current.orderBy,
						}
						if current.states != nil {
							params["states"] = current.states
						}
						if after != nil {
							params["after"] = *after
						}
						return params
					}

					var readerPage, writerPage jobPage
					pair.writer.call(t, "list", params(nil), &writerPage)
					pair.reader.call(t, "list", params(nil), &readerPage)
					require.Equal(t, expected[:2], jobIDs(writerPage.Jobs), description)
					require.Equal(t, writerPage, readerPage, description)
					require.NotNil(t, writerPage.Cursor, description)
					cursor := *writerPage.Cursor
					if current.kind == jobListCursorKind {
						require.Contains(t, cursor, "-", description)
					}

					var resumed jobPage
					pair.reader.call(t, "list", params(&cursor), &resumed)
					require.Equal(t, expected[2:], jobIDs(resumed.Jobs), description)
				}
			}
		}
		verifyCases([]listCase{
			{kind: echoKind, orderBy: "id"},
			{kind: echoKind, orderBy: "scheduled_at", states: []string{"scheduled"}},
			{kind: echoKind, orderBy: "time", states: []string{"scheduled"}},
			{kind: jobListCursorKind, orderBy: "id"},
		})
		// Cancelling in ID order sets increasing `finalized_at` times.
		for _, kind := range []string{echoKind, jobListCursorKind} {
			for _, id := range idsByKind[kind] {
				pair.writer.call(t, "cancel", map[string]any{"id": id}, nil)
			}
		}
		verifyCases([]listCase{
			{kind: echoKind, orderBy: "finalized_at", states: []string{"cancelled"}},
			{kind: echoKind, orderBy: "time", states: []string{"cancelled"}},
			{kind: jobListCursorKind, orderBy: "finalized_at", states: []string{"cancelled"}},
			{kind: jobListCursorKind, orderBy: "time", states: []string{"cancelled"}},
		})

		// Retrying the middle job makes it available again, scheduled now and
		// without a finalized time. Listed with cancelled jobs, every job is
		// ordered by the first state's field, so a page can end on a job of
		// the other state, and the retried job's null `finalized_at` sorts
		// last ascending.
		echoIDs := idsByKind[echoKind]
		pair.writer.call(t, "retry", map[string]any{"id": echoIDs[1]}, nil)
		verifyCases([]listCase{
			{kind: echoKind, orderBy: "time", order: []int{0, 2, 1}, states: []string{"cancelled", "available"}},
			{kind: echoKind, orderBy: "time", order: []int{1, 0, 2}, states: []string{"available", "cancelled"}},
		})
		// With the last job retried too, pages end on a null `finalized_at`.
		pair.writer.call(t, "retry", map[string]any{"id": echoIDs[2]}, nil)
		verifyCases([]listCase{
			{kind: echoKind, orderBy: "time", order: []int{0, 1, 2}, states: []string{"cancelled", "available"}},
		})
	}
}

// verifyDifferentialQueueCRUD compares persisted queue rows and metadata
// updates across implementations, including the `metadata_changed` control
// notification an update sends, which River Go's producers hand to their
// extension.
func verifyDifferentialQueueCRUD(t *testing.T, observer *postgresObserver, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	controlChannel := observer.currentSchema(t) + ".river_control"

	for _, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		pair.writer.call(t, "start", map[string]any{
			"client_id": pair.writer.name + "-queue-crud", "max_workers": 1,
		}, nil)
		pair.writer.call(t, "stop", map[string]any{}, nil)
		var readerQueue, updatedQueue, writerQueue normalizedQueue
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &writerQueue)
		pair.reader.call(t, "queue_get", map[string]any{"name": "default"}, &readerQueue)
		require.Equal(t, writerQueue, readerQueue)
		require.Equal(t, "default", writerQueue.Name)
		require.Nil(t, writerQueue.PausedAt)
		listener := observer.listen(t, controlChannel)
		pair.reader.call(t, "queue_update", map[string]any{
			"metadata": map[string]any{"updated_by": pair.reader.name}, "name": "default",
		}, &updatedQueue)
		require.Equal(t, map[string]any{"updated_by": pair.reader.name}, updatedQueue.Metadata)
		payloads := listener.receiveUntilMarker(t, observer, pair.reader.name+"-queue-update-marker")
		require.Len(t, payloads, 1, "%s: one control notification per metadata update", pair.reader.name)
		require.JSONEq(t,
			`{"action":"metadata_changed","metadata":{"updated_by":"`+pair.reader.name+`"},"queue":"default"}`,
			payloads[0], pair.reader.name)
		pair.writer.call(t, "queue_get", map[string]any{"name": "default"}, &writerQueue)
		require.Equal(t, updatedQueue, writerQueue)
		var readerQueues, writerQueues struct {
			Queues []normalizedQueue `json:"queues"`
		}
		pair.reader.call(t, "queue_list", map[string]any{}, &readerQueues)
		pair.writer.call(t, "queue_list", map[string]any{}, &writerQueues)
		require.Equal(t, writerQueues, readerQueues)
		require.Contains(t, writerQueues.Queues, updatedQueue)
	}
}

func verifyUnsafeInt64JobIDs(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const firstUnsafeID int64 = 9_007_199_254_740_993
	type jobPage struct {
		Cursor *string         `json:"cursor"`
		Jobs   []normalizedJob `json:"jobs"`
	}
	for pairIndex, pair := range []struct {
		reader *adapter
		writer *adapter
	}{
		{reader: candidateAdapter, writer: goAdapter},
		{reader: goAdapter, writer: candidateAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		ids := []int64{
			firstUnsafeID + int64(pairIndex*10),
			firstUnsafeID + int64(pairIndex*10) + 1,
		}
		for _, id := range ids {
			var inserted struct {
				ID int64 `json:"id"`
			}
			pair.writer.call(t, "raw_insert_exact_json", map[string]any{"id": id}, &inserted)
			require.Equal(t, id, inserted.ID)

			var observed normalizedJob
			pair.reader.call(t, "get", map[string]any{"id": id}, &observed)
			require.Equal(t, id, observed.ID)
		}

		listParams := func(after *string) map[string]any {
			params := map[string]any{
				"direction": "asc",
				"ids":       ids,
				"limit":     1,
				"order_by":  "id",
			}
			if after != nil {
				params["after"] = *after
			}
			return params
		}
		var readerFirst, writerFirst jobPage
		pair.reader.call(t, "list", listParams(nil), &readerFirst)
		pair.writer.call(t, "list", listParams(nil), &writerFirst)
		require.Equal(t, writerFirst, readerFirst)
		require.Equal(t, []int64{ids[0]}, normalizedJobIDs(writerFirst.Jobs))
		require.NotNil(t, writerFirst.Cursor)

		var readerSecond, writerSecond jobPage
		pair.reader.call(t, "list", listParams(writerFirst.Cursor), &readerSecond)
		pair.writer.call(t, "list", listParams(readerFirst.Cursor), &writerSecond)
		require.Equal(t, writerSecond, readerSecond)
		require.Equal(t, []int64{ids[1]}, normalizedJobIDs(writerSecond.Jobs))

		var cancelled, observed normalizedJob
		pair.reader.call(t, "cancel", map[string]any{"id": ids[0]}, &cancelled)
		pair.writer.call(t, "get", map[string]any{"id": ids[0]}, &observed)
		require.Equal(t, cancelled, observed)
		require.Equal(t, ids[0], cancelled.ID)
		require.Equal(t, "cancelled", cancelled.State)
	}
}

func verifyJobRowRoundTrip(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		observer *adapter
	}{
		{inserter: goAdapter, observer: candidateAdapter},
		{inserter: candidateAdapter, observer: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var exactInserted struct {
			ID int64 `json:"id"`
		}
		pair.inserter.call(t, "raw_insert_exact_json", map[string]any{}, &exactInserted)
		var exactAtInserter, exactAtObserver struct {
			Decimal  string `json:"decimal"`
			Integer  string `json:"integer"`
			Negative string `json:"negative"`
		}
		pair.inserter.call(t, "raw_job_exact_json", map[string]any{"id": exactInserted.ID}, &exactAtInserter)
		pair.observer.call(t, "raw_job_exact_json", map[string]any{"id": exactInserted.ID}, &exactAtObserver)
		require.Equal(t, exactAtInserter, exactAtObserver)
		require.Equal(t, "0.12345678901234567890123456789", exactAtObserver.Decimal)
		require.Equal(t, "9223372036854775807", exactAtObserver.Integer)
		require.Equal(t, "-9223372036854775808", exactAtObserver.Negative)

		var inserted, observed normalizedJob
		pair.inserter.call(t, "raw_insert_full_row", map[string]any{}, &inserted)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
		require.Equal(t, inserted, observed)
		require.Equal(t, map[string]any{
			"nested": map[string]any{"enabled": true},
			"values": []any{float64(1), "two", nil},
		}, observed.Args)
		require.Equal(t, 3, observed.Attempt)
		require.NotNil(t, observed.AttemptedAt)
		require.Equal(t, "2026-01-02T03:04:06.123456Z", *observed.AttemptedAt)
		require.Equal(t, []string{"go-client", "candidate-client"}, observed.AttemptedBy)
		require.Equal(t, "2026-01-02T03:04:05.6789Z", observed.CreatedAt)
		require.Len(t, observed.Errors, 1)
		require.Equal(t, "2026-01-02T03:04:06.123456Z", observed.Errors[0].At)
		require.Equal(t, 3, observed.Errors[0].Attempt)
		require.Equal(t, "worker failed: escaped \"detail\"", observed.Errors[0].Error)
		require.Equal(t, "frame one\nframe two", observed.Errors[0].Trace)
		require.NotNil(t, observed.FinalizedAt)
		require.Equal(t, "2026-01-02T03:04:07.000001Z", *observed.FinalizedAt)
		require.Equal(t, "conformance_full_row", observed.Kind)
		require.Equal(t, 4, observed.MaxAttempts)
		require.Equal(t, map[string]any{
			"output":             map[string]any{"ok": true},
			"river:rescue_count": float64(2),
			"user":               "metadata",
		}, observed.Metadata)
		require.Equal(t, 2, observed.Priority)
		require.Equal(t, "priority_jobs", observed.Queue)
		require.Equal(t, "2026-01-02T03:04:05.999999Z", observed.ScheduledAt)
		require.Equal(t, "discarded", observed.State)
		require.Equal(t, []string{"alpha_tag", "beta_tag"}, observed.Tags)
		require.NotNil(t, observed.UniqueKey)
		require.Equal(t, strings.Repeat("ab", 32), *observed.UniqueKey)
		require.Equal(t, []string{
			"available", "completed", "pending", "retryable", "running", "scheduled",
		}, observed.UniqueStates)
	}
}

// verifyLargeMetadataRoundTrip keeps large numeric values in a string-valued
// RPC parameter so neither adapter's JSON-RPC decoder can round them before
// the database sees them.
func verifyLargeMetadataRoundTrip(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const metadataJSON = `{"negative":-9223372036854775808,"big_integer":123456789012345678901234567890,"beyond_float":1e400,"long_decimal":0.1000000000000000055511151231257827}`
	type exactTokens struct {
		BigInteger  string `json:"big_integer"`
		BeyondFloat string `json:"beyond_float"`
		LongDecimal string `json:"long_decimal"`
	}
	for _, pair := range []struct {
		writer *adapter
		reader *adapter
	}{
		{writer: goAdapter, reader: candidateAdapter},
		{writer: candidateAdapter, reader: goAdapter},
	} {
		pair.writer.call(t, "reset", map[string]any{}, nil)
		var inserted struct {
			ID int64 `json:"id"`
		}
		pair.writer.call(t, "raw_insert_exact_json", map[string]any{"metadata_json": metadataJSON}, &inserted)
		read := func(actor *adapter) exactTokens {
			t.Helper()
			var tokens exactTokens
			actor.call(t, "raw_job_exact_json", map[string]any{"id": inserted.ID}, &tokens)
			return tokens
		}
		before := read(pair.writer)
		require.Equal(t, before, read(pair.reader))
		require.Equal(t, "123456789012345678901234567890", before.BigInteger)
		require.Equal(t, "0.1000000000000000055511151231257827", before.LongDecimal)
		require.NotEmpty(t, before.BeyondFloat)

		pair.reader.call(t, "update", map[string]any{"id": inserted.ID, "output": "preserved"}, nil)
		require.Equal(t, before, read(pair.writer))
		require.Equal(t, before, read(pair.reader))
	}
}

// verifyTransactionalJobCRUD runs job CRUD inside one implementation's
// transaction and observes commit and rollback from the other.
func verifyTransactionalJobCRUD(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)

		handle := pair.actor.name + "-transactional-crud-commit"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var inserted normalizedJob
		pair.actor.call(t, "tx_insert", map[string]any{
			"handle": handle,
			"job": map[string]any{
				"message": "transactional CRUD",
				"opts": map[string]any{
					"metadata": map[string]any{"actor": pair.actor.name},
					"tags":     []string{"transactional_crud"},
				},
			},
		}, &inserted)
		requireJobNotFound(t, pair.observer, inserted.ID)

		var transactionalJob normalizedJob
		pair.actor.call(t, "tx_get", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, inserted, transactionalJob)
		pair.actor.call(t, "tx_update", map[string]any{
			"handle": handle, "id": inserted.ID,
			"output": map[string]any{"updated_by": pair.actor.name},
		}, &transactionalJob)
		require.Equal(t, map[string]any{"updated_by": pair.actor.name}, transactionalJob.Metadata["output"])
		var transactionalJobs struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_list", map[string]any{
			"handle": handle, "ids": []int64{inserted.ID},
		}, &transactionalJobs)
		require.Equal(t, []normalizedJob{transactionalJob}, transactionalJobs.Jobs)

		pair.actor.call(t, "tx_cancel", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, "cancelled", transactionalJob.State)
		pair.actor.call(t, "tx_retry", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &transactionalJob)
		require.Equal(t, "available", transactionalJob.State)
		requireJobNotFound(t, pair.observer, inserted.ID)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)

		var observedJob normalizedJob
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)

		handle = pair.actor.name + "-transactional-crud-rollback"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var deleted normalizedJob
		pair.actor.call(t, "tx_delete", map[string]any{
			"handle": handle, "id": inserted.ID,
		}, &deleted)
		require.Equal(t, transactionalJob, deleted)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "get", map[string]any{"id": inserted.ID}, &observedJob)
		require.Equal(t, transactionalJob, observedJob)

		bulkIDs := make([]int64, 0, 2)
		for index := range 2 {
			var bulk normalizedJob
			pair.actor.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("transactional bulk delete %d", index),
			}, &bulk)
			bulkIDs = append(bulkIDs, bulk.ID)
		}
		handle = pair.actor.name + "-transactional-bulk-delete"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var deletedMany struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		pair.actor.call(t, "tx_delete_many", map[string]any{
			"handle": handle, "ids": bulkIDs,
		}, &deletedMany)
		require.ElementsMatch(t, bulkIDs, jobIDs(deletedMany.Jobs))
		for _, id := range bulkIDs {
			pair.observer.call(t, "get", map[string]any{"id": id}, &observedJob)
			require.Equal(t, id, observedJob.ID)
		}
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		for _, id := range bulkIDs {
			requireJobNotFound(t, pair.observer, id)
		}
	}
}

// verifyTransactionalQueueOperations updates, pauses, and resumes a queue in
// one implementation's transaction and observes commit and rollback from the
// other.
func verifyTransactionalQueueOperations(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		actor    *adapter
		observer *adapter
	}{
		{actor: goAdapter, observer: candidateAdapter},
		{actor: candidateAdapter, observer: goAdapter},
	} {
		pair.actor.call(t, "reset", map[string]any{}, nil)
		pair.actor.call(t, "start", map[string]any{
			"client_id": pair.actor.name + "-transactional-queues", "max_workers": 1,
		}, nil)
		pair.actor.call(t, "stop", map[string]any{}, nil)

		var queueBefore normalizedQueue
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &queueBefore)

		handle := pair.actor.name + "-transactional-queue-commit"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		var queueInTransaction normalizedQueue
		pair.actor.call(t, "tx_queue_update", map[string]any{
			"handle":   handle,
			"metadata": map[string]any{"updated_by": pair.actor.name},
			"name":     "default",
		}, &queueInTransaction)
		require.Equal(t, map[string]any{"updated_by": pair.actor.name}, queueInTransaction.Metadata)
		pair.actor.call(t, "tx_queue_pause", map[string]any{
			"handle": handle, "name": "default",
		}, nil)
		pair.actor.call(t, "tx_queue_get", map[string]any{
			"handle": handle, "name": "default",
		}, &queueInTransaction)
		require.NotNil(t, queueInTransaction.PausedAt)
		var queuesInTransaction struct {
			Queues []normalizedQueue `json:"queues"`
		}
		pair.actor.call(t, "tx_queue_list", map[string]any{
			"handle": handle,
		}, &queuesInTransaction)
		require.Contains(t, queuesInTransaction.Queues, queueInTransaction)

		var observedQueue normalizedQueue
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueBefore, observedQueue)
		pair.actor.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)

		handle = pair.actor.name + "-transactional-queue-rollback"
		pair.actor.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
		pair.actor.call(t, "tx_queue_resume", map[string]any{
			"handle": handle, "name": "default",
		}, nil)
		pair.actor.call(t, "tx_queue_get", map[string]any{
			"handle": handle, "name": "default",
		}, &observedQueue)
		require.Nil(t, observedQueue.PausedAt)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)
		pair.actor.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
		pair.observer.call(t, "queue_get", map[string]any{"name": "default"}, &observedQueue)
		require.Equal(t, queueInTransaction, observedQueue)
	}
}

func verifyHistoricalMigrations(t *testing.T, latest int, adapters ...*adapter) {
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
	for initializerIndex, initializer := range adapters {
		upgrader := adapters[(initializerIndex+1)%len(adapters)]
		for version := 1; version <= latest; version++ {
			schema := fmt.Sprintf("river_conformance_history_%s_%d", initializer.name, version)
			var result migrationResult
			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": -1,
			}, &result)
			initializer.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema, "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			require.Equal(t, version == latest, result.Valid)

			upgrader.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema,
			}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			var inserted, observed normalizedJob
			upgrader.call(t, "insert", map[string]any{
				"message": fmt.Sprintf("historical migration %d", version), "schema": schema,
			}, &inserted)
			initializer.call(t, "get", map[string]any{
				"id": inserted.ID, "schema": schema,
			}, &observed)
			require.Equal(t, inserted, observed)

			initializer.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": version,
			}, &result)
			require.Equal(t, expectedLatest[:version], result.Existing)
			upgrader.call(t, "migrate", map[string]any{
				"direction": "up", "schema": schema,
			}, &result)
			require.Equal(t, expectedLatest, result.Existing)
			require.True(t, result.Valid)
			upgrader.call(t, "migrate", map[string]any{
				"direction": "down", "schema": schema, "target_version": -1,
			}, &result)
			require.Empty(t, result.Existing)
		}
	}
}

// verifyDeterministicControls evaluates each implementation's production
// default retry policy at fixed clock and seed inputs and requires the delay
// to fall within the bounds generated from River's Go retry policy.
func verifyDeterministicControls(t *testing.T, repositoryRoot string, adapters ...*adapter) {
	t.Helper()

	var fixture struct {
		RetryCases []struct {
			ErrorCount int    `json:"error_count"`
			JobID      int64  `json:"job_id"`
			MaxDelayNS int64  `json:"max_delay_ns"`
			MinDelayNS int64  `json:"min_delay_ns"`
			Now        string `json:"now"`
			Seed       uint64 `json:"seed"`
		} `json:"retry_cases"`
	}
	contents, err := os.ReadFile(filepath.Join(repositoryRoot, "conformance/fixtures/protocol_values.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(contents, &fixture))
	require.NotEmpty(t, fixture.RetryCases)
	for _, testCase := range fixture.RetryCases {
		for _, adapter := range adapters {
			adapter.call(t, "clock_set", map[string]any{"now": testCase.Now}, nil)
			adapter.call(t, "rng_seed", map[string]any{"seed": testCase.Seed}, nil)
			var result struct {
				DelayNS int64 `json:"delay_ns"`
			}
			adapter.call(t, "retry_delay", map[string]any{
				"error_count": testCase.ErrorCount,
				"job_id":      testCase.JobID,
			}, &result)
			require.GreaterOrEqual(t, result.DelayNS, testCase.MinDelayNS, "%s adapter error_count %d", adapter.name, testCase.ErrorCount)
			require.LessOrEqual(t, result.DelayNS, testCase.MaxDelayNS, "%s adapter error_count %d", adapter.name, testCase.ErrorCount)
		}
	}
}
