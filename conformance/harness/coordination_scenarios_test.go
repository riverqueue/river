//go:build riverconformance

package harness_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// verifyUnknownKind checks that a job whose kind has no registered worker is
// fetched and failed with the canonical unknown-kind error rather than being
// skipped. The error is retryable, so a job with attempts left is retried.
func verifyUnknownKind(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		var discarded, retryable normalizedJob
		pair.inserter.call(t, "raw_insert_no_notify", map[string]any{
			"kind": "conformance_unregistered", "message": "must fail compatibly",
			"opts": map[string]any{"max_attempts": 1},
		}, &discarded)
		pair.inserter.call(t, "raw_insert_no_notify", map[string]any{
			"kind": "conformance_unregistered", "message": "must be retried",
			"opts": map[string]any{"max_attempts": 5},
		}, &retryable)
		workerID := pair.worker.name + "-unknown-kind"
		pair.worker.call(t, "start", map[string]any{
			"client_id": workerID, "max_workers": 1,
		}, nil)
		var known normalizedJob
		pair.inserter.call(t, "insert", map[string]any{"message": "known kind from " + pair.inserter.name}, &known)
		pair.worker.call(t, "wait", map[string]any{"id": known.ID}, &known)
		require.Equal(t, "completed", known.State)

		pair.worker.call(t, "wait", map[string]any{
			"id": discarded.ID, "states": []string{"discarded"},
		}, &discarded)
		require.Equal(t, 1, discarded.Attempt)
		require.Equal(t, []string{workerID}, discarded.AttemptedBy)
		require.Len(t, discarded.Errors, 1)
		require.Equal(t,
			"job kind is not registered in the client's Workers bundle: conformance_unregistered",
			discarded.Errors[0].Error,
		)

		// The first retry delay (about one second) is inside the scheduler
		// interval, so the failed job is made available again immediately
		// and retried. The second delay (about sixteen seconds) is not, so
		// the job then waits as retryable.
		pair.worker.call(t, "wait", map[string]any{
			"id": retryable.ID, "states": []string{"retryable"},
		}, &retryable)
		require.Equal(t, 2, retryable.Attempt)
		require.Equal(t, []string{workerID, workerID}, retryable.AttemptedBy)
		require.Len(t, retryable.Errors, 2)
		for _, attemptError := range retryable.Errors {
			require.Equal(t, discarded.Errors[0].Error, attemptError.Error)
		}
		require.Nil(t, retryable.FinalizedAt)
		pair.worker.call(t, "stop", map[string]any{}, nil)
	}
}

// verifyInsertNotificationWakeup proves an insert from one implementation
// wakes the other's worker through a notification. The worker polls only
// once a minute, so prompt completion cannot come from polling.
func verifyInsertNotificationWakeup(t *testing.T, controller, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-notification-only", "fetch_poll_interval_ms": 60_000,
		"max_workers": 1,
	}, nil)
	startedAt := time.Now()
	var inserted normalizedJob
	controller.call(t, "insert", map[string]any{
		"message": "cross-language insert notification",
	}, &inserted)
	worker.call(t, "wait", map[string]any{"id": inserted.ID}, &inserted)
	require.Equal(t, "completed", inserted.State)
	require.Less(t, time.Since(startedAt), 5*time.Second)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyPauseResumeNotification pauses a queue from one implementation and
// proves the other's running worker stops working it until it is resumed.
// The worker first reports that it applied the pause. A marker job on a
// second, unpaused queue then proves the worker kept fetching after the
// paused job was inserted, and the paused job's attempt must start no
// earlier than the resume.
func verifyPauseResumeNotification(t *testing.T, controller, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-pause-resume", "instrumented": true, "max_workers": 1,
	}, nil)
	worker.call(t, "queue_add", map[string]any{"max_workers": 1, "name": "pause_marker"}, nil)

	controller.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "queue_paused")
	})
	var paused, marker normalizedJob
	controller.call(t, "insert", map[string]any{"message": "inserted while paused"}, &paused)
	controller.call(t, "insert", map[string]any{
		"message": "unpaused marker", "opts": map[string]any{"queue": "pause_marker"},
	}, &marker)
	worker.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
	require.Equal(t, "completed", marker.State)
	worker.call(t, "get", map[string]any{"id": paused.ID}, &paused)
	require.Equal(t, "available", paused.State, "a paused queue was worked")

	controller.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
	var queue normalizedQueue
	controller.call(t, "queue_get", map[string]any{"name": "default"}, &queue)
	require.Nil(t, queue.PausedAt)
	resumedAt := parseTime(t, queue.UpdatedAt)
	worker.call(t, "wait", map[string]any{"id": paused.ID}, &paused)
	require.Equal(t, "completed", paused.State)
	require.NotNil(t, paused.AttemptedAt)
	require.False(t, parseTime(t, *paused.AttemptedAt).Before(resumedAt),
		"paused job attempted at %s before the queue resumed at %s", *paused.AttemptedAt, queue.UpdatedAt)
	waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "queue_resumed")
	})
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyRemoteCancelNotification cancels a running job from the other
// implementation and requires the cancellation to reach the worker through a
// control notification, recording the cancellation request in metadata.
func verifyRemoteCancelNotification(t *testing.T, controller, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-remote-cancel", "fetch_poll_interval_ms": 60_000,
		"max_workers": 1,
	}, nil)
	var cancellable normalizedJob
	controller.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "cross-language cancel notification",
	}, &cancellable)
	worker.call(t, "wait", map[string]any{
		"id": cancellable.ID, "states": []string{"running"},
	}, &cancellable)
	startedAt := time.Now()
	var requested normalizedJob
	controller.call(t, "cancel", map[string]any{"id": cancellable.ID}, &requested)
	require.Equal(t, "running", requested.State, "cancelling a running job only requests cancellation")
	cancelAttemptedAt, ok := requested.Metadata["cancel_attempted_at"].(string)
	require.True(t, ok, "cancel_attempted_at metadata must be a timestamp string: %v", requested.Metadata)
	parseTime(t, cancelAttemptedAt)
	worker.call(t, "wait", map[string]any{"id": cancellable.ID}, &cancellable)
	require.Equal(t, "cancelled", cancellable.State)
	require.Less(t, time.Since(startedAt), 5*time.Second)
	require.Equal(t, cancelAttemptedAt, cancellable.Metadata["cancel_attempted_at"])
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyPollOnlyRemoteCancellation cancels a running job from the other
// implementation while the worker runs without notifications. The worker
// polls its running jobs for cancellation requests every two seconds, so
// the job is cancelled without a control notification reaching it.
func verifyPollOnlyRemoteCancellation(t *testing.T, controller, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-poll-only-cancel", "fetch_poll_interval_ms": 100,
		"max_workers": 1, "poll_only": true,
	}, nil)
	var cancellable normalizedJob
	controller.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "poll-only cancel",
	}, &cancellable)
	worker.call(t, "wait", map[string]any{
		"id": cancellable.ID, "states": []string{"running"},
	}, &cancellable)
	startedAt := time.Now()
	controller.call(t, "cancel", map[string]any{"id": cancellable.ID}, nil)
	worker.call(t, "wait", map[string]any{"id": cancellable.ID}, &cancellable)
	require.Equal(t, "cancelled", cancellable.State)
	require.Len(t, cancellable.Errors, 1)
	require.Equal(t, "JobCancelError: job cancelled remotely", cancellable.Errors[0].Error)
	require.Less(t, time.Since(startedAt), 6*time.Second)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyCooperativeRemoteCancellation checks the canonical persisted outcome
// and event of a worker that honors a remote cancellation.
func verifyCooperativeRemoteCancellation(t *testing.T, controller, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-cooperative-cancel", "fetch_poll_interval_ms": 60_000,
		"instrumented": true, "max_workers": 1,
	}, nil)
	var cancellable normalizedJob
	controller.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "cooperative cancellation",
	}, &cancellable)
	worker.call(t, "wait", map[string]any{
		"id": cancellable.ID, "states": []string{"running"},
	}, &cancellable)
	controller.call(t, "cancel", map[string]any{"id": cancellable.ID}, &cancellable)
	worker.call(t, "wait", map[string]any{"id": cancellable.ID}, &cancellable)
	require.Equal(t, "cancelled", cancellable.State)
	require.Equal(t, 1, cancellable.Attempt)
	require.NotNil(t, cancellable.FinalizedAt)
	require.Len(t, cancellable.Errors, 1)
	require.Equal(t, "JobCancelError: job cancelled remotely", cancellable.Errors[0].Error)
	stats := waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "job_cancelled")
	})
	require.NotContains(t, stats.Events, "job_failed")
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyRemoteQueueSubscriptionEvents checks that a pause or resume issued
// by one implementation produces exactly one subscription event in the other
// and that repeated requests are not delivered again. Control notifications
// are processed in order, so waiting for the next state change proves any
// event from a repeated request would already have been observed.
func verifyRemoteQueueSubscriptionEvents(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		controller *adapter
		observer   *adapter
	}{
		{controller: candidateAdapter, observer: goAdapter},
		{controller: goAdapter, observer: candidateAdapter},
	} {
		pair.observer.call(t, "reset", map[string]any{}, nil)
		pair.observer.call(t, "start", map[string]any{
			"client_id":              pair.observer.name + "-remote-queue-subscriber",
			"fetch_poll_interval_ms": 60_000,
			"instrumented":           true,
			"max_workers":            1,
		}, nil)

		var warmup normalizedJob
		pair.controller.call(t, "insert", map[string]any{
			"message": "activate remote queue subscriber",
		}, &warmup)
		pair.observer.call(t, "wait", map[string]any{"id": warmup.ID}, &warmup)
		require.Equal(t, "completed", warmup.State)

		waitForEventCounts := func(paused, resumed int) {
			stats := waitForRuntimeStats(t, pair.observer, func(stats runtimeStats) bool {
				return countRuntimeEvent(stats, "queue_paused") >= paused &&
					countRuntimeEvent(stats, "queue_resumed") >= resumed
			})
			require.Equal(t, paused, countRuntimeEvent(stats, "queue_paused"))
			require.Equal(t, resumed, countRuntimeEvent(stats, "queue_resumed"))
		}
		pair.controller.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
		waitForEventCounts(1, 0)
		pair.controller.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
		pair.controller.call(t, "queue_resume", map[string]any{"name": "*"}, nil)
		waitForEventCounts(1, 1)
		pair.controller.call(t, "queue_resume", map[string]any{"name": "*"}, nil)
		pair.controller.call(t, "queue_pause", map[string]any{"name": "*"}, nil)
		waitForEventCounts(2, 1)
		pair.controller.call(t, "queue_resume", map[string]any{"name": "*"}, nil)
		waitForEventCounts(2, 2)

		pair.observer.call(t, "stop", map[string]any{}, nil)
	}
}

// verifyTransactionalNotificationWakeups checks that transactional batch
// inserts notify only on commit. Commit must wake a worker that polls once a
// minute. Rollback must publish nothing: the harness listens to the raw
// insert channel and sends its own marker after the rollback, so any
// notification the rolled-back transaction leaked would arrive first.
func verifyTransactionalNotificationWakeups(t *testing.T, observer *postgresObserver, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const method = "tx_insert_many"
	insertChannel := observer.currentSchema(t) + ".river_insert"
	for _, pair := range []struct {
		controller *adapter
		worker     *adapter
	}{
		{controller: candidateAdapter, worker: goAdapter},
		{controller: goAdapter, worker: candidateAdapter},
	} {
		for _, commit := range []bool{false, true} {
			pair.worker.call(t, "reset", map[string]any{}, nil)
			pair.worker.call(t, "start", map[string]any{
				"client_id":              pair.worker.name + "-transaction-notification",
				"fetch_poll_interval_ms": 60_000,
				"max_workers":            2,
			}, nil)
			listener := observer.listen(t, insertChannel)

			outcome := "rollback"
			if commit {
				outcome = "commit"
			}
			handle := fmt.Sprintf("notification-%s-%s-%s", pair.controller.name, method, outcome)
			tag := strings.ReplaceAll(handle, "-", "_")
			pair.controller.call(t, "tx_begin", map[string]any{"handle": handle}, nil)
			jobs := []map[string]any{
				{"message": handle + " first", "opts": map[string]any{"tags": []string{tag}}},
				{"message": handle + " second", "opts": map[string]any{"tags": []string{tag}}},
			}
			var inserted struct {
				Results []normalizedInsertResult `json:"results"`
			}
			pair.controller.call(t, method, map[string]any{
				"handle": handle, "jobs": jobs,
			}, &inserted)
			require.Len(t, inserted.Results, 2)

			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			pair.worker.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
			require.Empty(t, listed.Jobs, "transactional batch became visible before commit")

			if commit {
				startedAt := time.Now()
				pair.controller.call(t, "tx_commit", map[string]any{"handle": handle}, nil)
				waitForListedJobCount(t, pair.worker, map[string]any{
					"states": []string{"completed"}, "tags_all": []string{tag},
				}, 2)
				require.Less(t, time.Since(startedAt), 5*time.Second,
					"committed transactional insert did not wake a 60-second polling worker")
				require.NotEmpty(t, listener.receiveUntilMarker(t, observer, handle+"-marker"),
					"commit published no insert notification")
			} else {
				pair.controller.call(t, "tx_rollback", map[string]any{"handle": handle}, nil)
				require.Empty(t, listener.receiveUntilMarker(t, observer, handle+"-marker"),
					"rolled-back transaction published an insert notification")
				pair.worker.call(t, "list", map[string]any{"tags_all": []string{tag}}, &listed)
				require.Empty(t, listed.Jobs)
			}
			pair.worker.call(t, "stop", map[string]any{}, nil)
		}
	}
}

// verifyClaimOrder checks the order in which a client claims available
// jobs, which every implementation writes in its own SQL: Go claims by
// priority, then scheduled_at, then ID. One implementation inserts jobs
// whose ID order, scheduled_at order, and priority order all differ,
// including two with equal priority and scheduled_at, and the other works
// them one at a time once its scheduler makes them all available together.
// Each job sleeps briefly, so the claims' attempted_at times are distinct and
// record the order.
func verifyClaimOrder(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		pair.inserter.call(t, "reset", map[string]any{}, nil)
		base := time.Now().UTC().Truncate(time.Millisecond)
		ids := make(map[string]int64)
		// In insertion (ID) order.
		for _, job := range []struct {
			name     string
			priority int
			ago      time.Duration
		}{
			{name: "priority 1, latest", priority: 1, ago: 30 * time.Second},
			{name: "priority 4", priority: 4, ago: time.Minute},
			{name: "priority 1, later", priority: 1, ago: time.Minute},
			{name: "priority 3, earliest", priority: 3, ago: 3 * time.Minute},
			{name: "priority 1, earliest, lower ID", priority: 1, ago: 2 * time.Minute},
			{name: "priority 1, earliest, higher ID", priority: 1, ago: 2 * time.Minute},
		} {
			var inserted normalizedJob
			pair.inserter.call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 5, "message": "claim order " + job.name,
				"opts": map[string]any{
					"priority": job.priority, "scheduled_at": base.Add(-job.ago).Format(time.RFC3339Nano),
				},
			}, &inserted)
			// Like Go, an explicit schedule inserts the job scheduled even
			// when it's due, and the leader's scheduler makes it available.
			require.Equal(t, "scheduled", inserted.State, job.name)
			ids[job.name] = inserted.ID
		}
		expected := []string{
			"priority 1, earliest, lower ID",
			"priority 1, earliest, higher ID",
			"priority 1, later",
			"priority 1, latest",
			"priority 3, earliest",
			"priority 4",
		}

		clientID := pair.worker.name + "-claim-order"
		pair.worker.startWithTuning(t, map[string]any{"client_id": clientID, "max_workers": 1},
			map[string]any{"elect_interval_ms": 20, "scheduler_interval_ms": 20})
		type claim struct {
			at   time.Time
			name string
		}
		claims := make([]claim, 0, len(ids))
		for name, id := range ids {
			var worked normalizedJob
			pair.worker.call(t, "wait", map[string]any{"id": id}, &worked)
			require.Equal(t, "completed", worked.State, name)
			require.Equal(t, []string{clientID}, worked.AttemptedBy, name)
			require.NotNil(t, worked.AttemptedAt, name)
			claims = append(claims, claim{at: parseTime(t, *worked.AttemptedAt), name: name})
		}
		pair.worker.call(t, "stop", map[string]any{}, nil)

		slices.SortFunc(claims, func(a, b claim) int { return a.at.Compare(b.at) })
		actual := make([]string, len(claims))
		for index, claim := range claims {
			if index > 0 {
				require.True(t, claim.at.After(claims[index-1].at), "%s claimed two jobs at the same time", pair.worker.name)
			}
			actual[index] = claim.name
		}
		require.Equal(t, expected, actual, "%s claimed jobs out of order", pair.worker.name)
	}
}
