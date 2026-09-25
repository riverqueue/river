//go:build riverconformance

package harness_test

import (
	"encoding/json"
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

	insertChannel := observer.currentSchema(t) + ".river_insert"
	for _, pair := range []struct {
		controller *adapter
		worker     *adapter
	}{
		{controller: candidateAdapter, worker: goAdapter},
		{controller: goAdapter, worker: candidateAdapter},
	} {
		for _, method := range []string{"tx_insert_many", "tx_insert_many_fast"} {
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
					Count   int                      `json:"count"`
					Results []normalizedInsertResult `json:"results"`
				}
				pair.controller.call(t, method, map[string]any{
					"handle": handle, "jobs": jobs,
				}, &inserted)
				if method == "tx_insert_many_fast" {
					require.Equal(t, 2, inserted.Count)
				} else {
					require.Len(t, inserted.Results, 2)
				}

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
}

// verifyLeadershipRequestLifecycle requests resignation from each
// implementation, directly and in transactions, while the other leads. On
// PostgreSQL the harness also listens to the raw leadership channel to prove
// a rolled-back request publishes nothing.
func verifyLeadershipRequestLifecycle(t *testing.T, observer *postgresObserver, first, second *adapter) {
	t.Helper()

	for _, pair := range []struct {
		leader    *adapter
		requester *adapter
	}{
		{leader: first, requester: second},
		{leader: second, requester: first},
	} {
		pair.leader.call(t, "reset", map[string]any{}, nil)
		pair.leader.call(t, "start", map[string]any{
			"client_id": pair.leader.name + "-resign-lifecycle", "max_workers": 1,
		}, nil)
		initial := waitForLeaderTerm(t, pair.leader, "")
		require.Equal(t, pair.leader.name+"-resign-lifecycle", initial.LeaderID)

		pair.requester.call(t, "request_resign", map[string]any{}, nil)
		afterDirect := waitForLeaderTerm(t, pair.leader, initial.ElectedAt)

		var listener *postgresNotificationListener
		if observer != nil {
			listener = observer.listen(t, observer.currentSchema(t)+".river_leadership")
		}
		rollbackHandle := pair.requester.name + "-resign-rollback"
		pair.requester.call(t, "tx_begin", map[string]any{"handle": rollbackHandle}, nil)
		pair.requester.call(t, "request_resign", map[string]any{"handle": rollbackHandle}, nil)
		pair.requester.call(t, "tx_rollback", map[string]any{"handle": rollbackHandle}, nil)
		if listener != nil {
			require.Empty(t, listener.receiveUntilMarker(t, observer, rollbackHandle+"-marker"),
				"rolled-back resignation request published a notification")
		}
		require.Equal(t, afterDirect.ElectedAt, readLeader(t, pair.leader).ElectedAt)

		commitHandle := pair.requester.name + "-resign-commit"
		pair.requester.call(t, "tx_begin", map[string]any{"handle": commitHandle}, nil)
		pair.requester.call(t, "request_resign", map[string]any{"handle": commitHandle}, nil)
		pair.requester.call(t, "tx_commit", map[string]any{"handle": commitHandle}, nil)
		if listener != nil {
			// The leader may already have answered with a resigned
			// notification; only resignation requests are counted.
			requests := 0
			for _, payload := range listener.receiveUntilMarker(t, observer, commitHandle+"-marker") {
				var notification struct {
					Action string `json:"action"`
				}
				require.NoError(t, json.Unmarshal([]byte(payload), &notification))
				if notification.Action == "request_resign" {
					requests++
				}
			}
			require.Equal(t, 1, requests, "committed resignation request was not published exactly once")
		}
		_ = waitForLeaderTerm(t, pair.leader, afterDirect.ElectedAt)
		pair.leader.call(t, "stop", map[string]any{}, nil)
	}
}

// verifyGracefulLeaderFailover moves leadership between the reference and the
// candidate with resignation requests and graceful stops in both directions,
// requiring both implementations to agree on the single current leader.
func verifyGracefulLeaderFailover(t *testing.T, pair mixedPair) {
	t.Helper()

	goID := "go-mixed-worker"
	candidateID := pair.candidateSpec.Implementation + "-mixed-worker"
	pair.reference.call(t, "reset", map[string]any{}, nil)
	pair.reference.call(t, "start", map[string]any{"client_id": goID, "max_workers": 2}, nil)
	pair.candidate.call(t, "start", map[string]any{"client_id": candidateID, "max_workers": 2}, nil)
	firstTerm := waitForLeaderTerm(t, pair.reference, "")
	pair.reference.call(t, "request_resign", map[string]any{}, nil)
	secondTerm := waitForLeaderTerm(t, pair.reference, firstTerm.ElectedAt)
	pair.candidate.call(t, "request_resign", map[string]any{}, nil)
	thirdTerm := waitForLeaderTerm(t, pair.candidate, secondTerm.ElectedAt)
	require.Equal(t, thirdTerm, readLeader(t, pair.reference), "implementations disagree about the leader")

	leaderAdapter, leaderID := pair.reference, goID
	followerAdapter, followerID := pair.candidate, candidateID
	if thirdTerm.LeaderID == candidateID {
		leaderAdapter, leaderID = pair.candidate, candidateID
		followerAdapter, followerID = pair.reference, goID
	} else {
		require.Equal(t, goID, thirdTerm.LeaderID)
	}
	leaderAdapter.call(t, "stop", map[string]any{}, nil)
	require.Equal(t, followerID, waitForLeader(t, followerAdapter, leaderID))
	leaderAdapter.call(t, "start", map[string]any{"client_id": leaderID, "max_workers": 2}, nil)
	followerAdapter.call(t, "stop", map[string]any{}, nil)
	require.Equal(t, leaderID, waitForLeader(t, leaderAdapter, followerID))
	require.Equal(t, readLeader(t, leaderAdapter), readLeader(t, followerAdapter))
	leaderAdapter.call(t, "stop", map[string]any{}, nil)
}

// verifyLeaderElectionDisabled starts a client with leader election disabled
// alongside an eligible client of another implementation. The disabled
// client must reject periodic jobs, work the periodic job the eligible
// leader enqueues into its queue, run no leader-only maintenance, and never
// become leader, including after the eligible leader stops and after the
// disabled client restarts. Where the implementation allows it, the disabled
// client uses a short election interval, so one that still took part in
// elections would become leader within the scenario.
func verifyLeaderElectionDisabled(t *testing.T, disabled, eligible *adapter) {
	t.Helper()

	// SQLite can't filter job lists by metadata, so periodic jobs are
	// selected from the full list.
	periodicJobs := func() []normalizedJob {
		var result struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		disabled.call(t, "list", map[string]any{"limit": 100}, &result)
		var periodic []normalizedJob
		for _, job := range result.Jobs {
			if job.Metadata["river:periodic_job_id"] == "conformance-periodic" {
				periodic = append(periodic, job)
			}
		}
		return periodic
	}
	disabledID := disabled.spec.Implementation + "-election-disabled"
	eligibleID := eligible.spec.Implementation + "-election-eligible"
	disabledParams := map[string]any{
		"client_id": disabledID, "instrumented": true, "leader_election_disabled": true, "max_workers": 1,
	}
	fastElection := map[string]any{"elect_interval_ms": 20}
	disabled.call(t, "reset", map[string]any{}, nil)

	disabled.requireCallError(t, "start", map[string]any{
		"client_id": disabledID, "leader_election_disabled": true, "periodic_run_on_start": true,
	}, "rejected")
	disabled.startWithTuning(t, disabledParams, fastElection)
	var marker normalizedJob
	eligible.call(t, "insert", map[string]any{"message": "before an eligible client starts"}, &marker)
	disabled.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
	require.Equal(t, []string{disabledID}, marker.AttemptedBy)
	require.Empty(t, readLeader(t, eligible).LeaderID, "a client with leader election disabled became leader")

	// The eligible client works a separate queue, so only the disabled
	// client works the periodic job it enqueues into the default queue.
	eligible.startWithTuning(t, map[string]any{
		"client_id": eligibleID, "instrumented": true, "max_workers": 1,
		"periodic_run_on_start": true, "queue": "election_eligible",
	}, fastElection)
	require.Equal(t, eligibleID, waitForLeader(t, disabled, ""))
	waitForRuntimeStats(t, eligible, func(stats runtimeStats) bool { return stats.PeriodicStarts == 1 })
	deadline := time.Now().Add(5 * time.Second)
	for len(periodicJobs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	enqueued := periodicJobs()
	require.Len(t, enqueued, 1, "the eligible leader did not enqueue its periodic job")
	periodic := enqueued[0]
	disabled.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
	require.Equal(t, "completed", periodic.State)
	require.Equal(t, []string{disabledID}, periodic.AttemptedBy)
	stats := waitForRuntimeStats(t, disabled, func(runtimeStats) bool { return true })
	require.Zero(t, stats.PeriodicStarts, "a client with leader election disabled ran the periodic enqueuer")

	eligible.call(t, "stop", map[string]any{}, nil)
	for _, step := range []string{"after the eligible leader stops", "after a restart"} {
		if step == "after a restart" {
			disabled.call(t, "stop", map[string]any{}, nil)
			disabled.startWithTuning(t, disabledParams, fastElection)
		}
		eligible.call(t, "insert", map[string]any{"message": step}, &marker)
		disabled.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
		require.Equal(t, "completed", marker.State)
		require.Equal(t, []string{disabledID}, marker.AttemptedBy)
		require.Empty(t, readLeader(t, eligible).LeaderID, "a client with leader election disabled became leader %s", step)
	}
	stats = waitForRuntimeStats(t, disabled, func(runtimeStats) bool { return true })
	require.Zero(t, stats.PeriodicStarts, "a client with leader election disabled ran the periodic enqueuer")
	require.Len(t, periodicJobs(), 1)
	disabled.call(t, "stop", map[string]any{}, nil)
}

// verifyListenerReconnect terminates each worker's listener backend and then
// all of its database connections, and requires a notification round trip
// from the other implementation after each fault.
func verifyListenerReconnect(t *testing.T, pair mixedPair) {
	t.Helper()

	pair.eachDirection(func(worker, controller *adapter) {
		worker.call(t, "reset", map[string]any{}, nil)
		worker.call(t, "start", map[string]any{
			"client_id": worker.name + "-reconnect", "fetch_poll_interval_ms": 60_000, "max_workers": 1,
		}, nil)
		waitForListener(t, worker)
		requireNotificationRoundTrip(t, controller, worker, "before_fault")

		var disconnected struct {
			Count int `json:"count"`
		}
		worker.call(t, "fault_disconnect_listeners", map[string]any{}, &disconnected)
		require.GreaterOrEqual(t, disconnected.Count, 1)
		waitForListener(t, worker)
		requireNotificationRoundTrip(t, controller, worker, "after_listener_fault")

		controller.call(t, "fault_disconnect_application", map[string]any{
			"application_name": worker.applicationName,
		}, &disconnected)
		require.GreaterOrEqual(t, disconnected.Count, 1)
		waitForListener(t, worker)
		requireNotificationRoundTrip(t, controller, worker, "after_application_fault")
		worker.call(t, "stop", map[string]any{}, nil)
	})
}

// requireNotificationRoundTrip requires an insert by the controller to wake a
// worker that polls once a minute. A listener that has just reconnected may
// miss a notification sent before it resubscribed, so inserts repeat until
// one wakes the worker or the bound elapses.
func requireNotificationRoundTrip(t *testing.T, controller, worker *adapter, label string) {
	t.Helper()

	tag := "round_trip_" + label
	deadline := time.Now().Add(10 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		var inserted normalizedJob
		controller.call(t, "insert", map[string]any{
			"message": fmt.Sprintf("%s %d", label, attempt), "opts": map[string]any{"tags": []string{tag}},
		}, &inserted)
		attemptDeadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(attemptDeadline) {
			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			worker.call(t, "list", map[string]any{"states": []string{"completed"}, "tags_all": []string{tag}}, &listed)
			if len(listed.Jobs) > 0 {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	t.Fatalf("%s: %s inserts never woke %s's listener", label, controller.name, worker.name)
}

// verifyLostNotificationPollRecovery inserts without a notification and
// requires the worker's poll loop to find the job.
func verifyLostNotificationPollRecovery(t *testing.T, inserter, worker *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-poll-recovery", "fetch_poll_interval_ms": 250, "max_workers": 1,
	}, nil)
	var notificationLost normalizedJob
	inserter.call(t, "raw_insert_no_notify", map[string]any{"message": "poll recovery"}, &notificationLost)
	worker.call(t, "wait", map[string]any{"id": notificationLost.ID}, &notificationLost)
	require.Equal(t, "completed", notificationLost.State)
	require.Equal(t, []string{worker.name + "-poll-recovery"}, notificationLost.AttemptedBy)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifySkipLockedCompetition has both implementations compete for a burst
// of short jobs and requires every job to run exactly once.
func verifySkipLockedCompetition(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const jobsPerInserter = 150
	goID, candidateID := goAdapter.name+"-competitor", candidateAdapter.name+"-competitor"
	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "start", map[string]any{"client_id": goID, "max_workers": 8}, nil)
	candidateAdapter.call(t, "start", map[string]any{"client_id": candidateID, "max_workers": 8}, nil)
	for _, inserter := range []*adapter{goAdapter, candidateAdapter} {
		jobs := make([]map[string]any, jobsPerInserter)
		for index := range jobs {
			jobs[index] = map[string]any{
				"behavior": "sleep", "duration_ms": 5,
				"message": fmt.Sprintf("competition %s %d", inserter.name, index),
				"opts":    map[string]any{"tags": []string{"competition"}},
			}
		}
		var inserted struct {
			Count int `json:"count"`
		}
		inserter.call(t, "insert_many_fast", map[string]any{"jobs": jobs}, &inserted)
		require.Equal(t, jobsPerInserter, inserted.Count)
	}
	worked := waitForListedJobCountWithin(t, goAdapter, map[string]any{
		"limit": 2 * jobsPerInserter, "states": []string{"completed"}, "tags_all": []string{"competition"},
	}, 2*jobsPerInserter, 30*time.Second)
	perWorker := make(map[string]int)
	for _, job := range worked {
		require.Equal(t, 1, job.Attempt, "job %d ran more than once", job.ID)
		require.Len(t, job.AttemptedBy, 1)
		require.Empty(t, job.Errors)
		perWorker[job.AttemptedBy[0]]++
	}
	require.Positive(t, perWorker[goID], "Go worker claimed no jobs")
	require.Positive(t, perWorker[candidateID], "candidate worker claimed no jobs")
	require.Len(t, perWorker, 2)
	t.Logf("competition split: %v", perWorker)
	goAdapter.call(t, "stop", map[string]any{}, nil)
	candidateAdapter.call(t, "stop", map[string]any{}, nil)
}

// verifyIgnoredCancellationHardAbort stops a disposable candidate process
// whose worker ignores cancellation and requires the attempt to be returned
// to the queue. Go cannot abort a goroutine that ignores its context, so this
// scenario exercises the candidate's runtime only.
func verifyIgnoredCancellationHardAbort(t *testing.T, repositoryRoot, databaseURL string, pair mixedPair) {
	t.Helper()

	pair.reference.call(t, "reset", map[string]any{}, nil)
	stuck := startCandidateAdapter(t, repositoryRoot, databaseURL, "candidate-stuck", pair.candidateSpec, pair.candidateSpec.RestartCommand)
	stuckClientID := pair.candidateSpec.Implementation + "-stuck-worker"
	stuck.call(t, "start", map[string]any{
		"client_id": stuckClientID, "max_workers": 1, "queue": "ignored",
	}, nil)
	var stuckJob normalizedJob
	pair.reference.call(t, "insert", map[string]any{
		"behavior": "ignored_cancel",
		"message":  "ignored cancellation",
		"opts":     map[string]any{"queue": "ignored"},
	}, &stuckJob)
	pair.reference.call(t, "wait", map[string]any{
		"id": stuckJob.ID, "states": []string{"running"},
	}, &stuckJob)
	stuck.call(t, "stop", map[string]any{"cancel": true}, nil)
	pair.reference.call(t, "get", map[string]any{"id": stuckJob.ID}, &stuckJob)
	require.Equal(t, "available", stuckJob.State)
	require.Equal(t, 0, stuckJob.Attempt)
}

// verifyProcessKillRestartAndRescue kills a candidate process mid-attempt and
// requires a restarted candidate process to rescue and complete the job.
func verifyProcessKillRestartAndRescue(t *testing.T, repositoryRoot, databaseURL string, pair mixedPair) {
	t.Helper()

	pair.reference.call(t, "reset", map[string]any{}, nil)
	crashing := startCandidateAdapter(t, repositoryRoot, databaseURL, "candidate-crashing", pair.candidateSpec, pair.candidateSpec.RestartCommand)
	crashingClientID := pair.candidateSpec.Implementation + "-crashing-worker"
	crashing.call(t, "start", map[string]any{
		"client_id": crashingClientID, "max_workers": 1,
	}, nil)
	var crashJob normalizedJob
	pair.reference.call(t, "insert", map[string]any{
		"behavior": "sleep", "duration_ms": 1_000, "message": "process death rescue",
	}, &crashJob)
	pair.reference.call(t, "wait", map[string]any{
		"id": crashJob.ID, "states": []string{"running"},
	}, &crashJob)
	crashing.kill(t)
	pair.reference.call(t, "fault_expire_leader", map[string]any{}, nil)

	recovery := startCandidateAdapter(t, repositoryRoot, databaseURL, "candidate-recovery", pair.candidateSpec, pair.candidateSpec.RestartCommand)
	recoveryClientID := pair.candidateSpec.Implementation + "-recovery-worker"
	recovery.startWithTuning(t, map[string]any{
		"client_id":       recoveryClientID,
		"job_timeout_ms":  1_500,
		"max_workers":     1,
		"rescue_after_ms": 1_500,
	}, map[string]any{"elect_interval_ms": 20, "rescuer_interval_ms": 20, "scheduler_interval_ms": 20})
	recovery.call(t, "wait", map[string]any{"id": crashJob.ID}, &crashJob)
	require.Equal(t, "completed", crashJob.State)
	require.Equal(t, 2, crashJob.Attempt)
	require.Equal(t, []string{crashingClientID, recoveryClientID}, crashJob.AttemptedBy)
	require.EqualValues(t, 1, crashJob.Metadata["river:rescue_count"])
	recovery.call(t, "stop", map[string]any{}, nil)
}
