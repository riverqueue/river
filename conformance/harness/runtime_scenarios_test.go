//go:build riverconformance

package harness_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type runtimeStats struct {
	ErrorHandlerCalls   int      `json:"error_handler_calls"`
	Events              []string `json:"events"`
	PeriodicStarts      int      `json:"periodic_starts"`
	ResumableFirstRuns  int      `json:"resumable_first_runs"`
	ResumableSecondRuns int      `json:"resumable_second_runs"`
	StuckJobs           int      `json:"stuck_jobs"`
	Trace               []string `json:"trace"`
}

func verifyBarrierWaitAndRelease(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-barrier", "max_workers": 2,
	}, nil)
	current.call(t, "barrier_create", map[string]any{"name": "runtime"}, nil)
	var inserted, running, worked normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "barrier_wait", "message": "runtime",
	}, &inserted)
	current.call(t, "wait", map[string]any{
		"id": inserted.ID, "states": []string{"running"},
	}, &running)
	require.Equal(t, "running", running.State)
	require.Equal(t, 1, running.Attempt)
	current.call(t, "barrier_release", map[string]any{"name": "runtime"}, nil)
	current.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
	require.Equal(t, "completed", worked.State)
	require.Equal(t, running.AttemptedAt, worked.AttemptedAt)
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyWorkerOutcomes checks the persisted row for each terminal worker
// outcome in one implementation.
func verifyWorkerOutcomes(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-outcomes", "max_workers": 2,
	}, nil)
	for _, testCase := range []struct {
		behavior    string
		errorText   string
		maxAttempts int
		state       string
	}{
		{behavior: "cancel", state: "cancelled"},
		{behavior: "discard", maxAttempts: 1, state: "discarded"},
		{behavior: "error", errorText: "conformance retryable error", maxAttempts: 1, state: "discarded"},
	} {
		params := map[string]any{"behavior": testCase.behavior, "message": testCase.behavior}
		if testCase.maxAttempts > 0 {
			params["opts"] = map[string]any{"max_attempts": testCase.maxAttempts}
		}
		var inserted, worked normalizedJob
		current.call(t, "insert", params, &inserted)
		current.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
		require.Equal(t, testCase.state, worked.State, "%s behavior", testCase.behavior)
		require.Equal(t, 1, worked.Attempt, "%s behavior", testCase.behavior)
		require.NotNil(t, worked.FinalizedAt, "%s behavior", testCase.behavior)
		require.Len(t, worked.Errors, 1, "%s behavior", testCase.behavior)
		require.Equal(t, 1, worked.Errors[0].Attempt, "%s behavior", testCase.behavior)
		if testCase.errorText != "" {
			require.Equal(t, testCase.errorText, worked.Errors[0].Error)
		}
	}

	var outputInserted, outputWorked normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "output", "message": "runtime output",
	}, &outputInserted)
	current.call(t, "wait", map[string]any{"id": outputInserted.ID}, &outputWorked)
	require.Equal(t, "completed", outputWorked.State)
	require.Empty(t, outputWorked.Errors)
	require.Equal(t, map[string]any{"message": "runtime output"}, outputWorked.Metadata["output"])
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyPanicAttemptTrace checks that a panic is persisted with its value and
// a stack trace that the other implementation reads unchanged.
func verifyPanicAttemptTrace(t *testing.T, worker, observer *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-panic", "max_workers": 1,
	}, nil)
	var inserted, worked, observed normalizedJob
	worker.call(t, "insert", map[string]any{
		"behavior": "panic", "message": "panic", "opts": map[string]any{"max_attempts": 1},
	}, &inserted)
	worker.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
	require.Equal(t, "discarded", worked.State)
	require.Equal(t, 1, worked.Attempt)
	require.Len(t, worked.Errors, 1)
	require.Contains(t, worked.Errors[0].Error, "conformance worker panic")
	require.Equal(t, 1, worked.Errors[0].Attempt)
	require.NotEmpty(t, worked.Errors[0].Trace)
	observer.call(t, "get", map[string]any{"id": inserted.ID}, &observed)
	require.Equal(t, worked, observed)
	worker.call(t, "stop", map[string]any{}, nil)
}

func verifyTransactionalCompletion(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-transactional-completion", "max_workers": 1,
	}, nil)
	var inserted, worked normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "transactional_complete", "message": "transactional completion",
	}, &inserted)
	current.call(t, "wait", map[string]any{"id": inserted.ID}, &worked)
	require.Equal(t, "completed", worked.State)
	require.Empty(t, worked.Errors)
	require.Equal(t, true, worked.Metadata["transactional_completion"])
	current.call(t, "stop", map[string]any{}, nil)
}

// verifySnoozeTransition checks the persisted snooze transition: the
// `snoozes` counter, an attempt that is given back, and a delay longer than
// the scheduler interval parking the job as `scheduled` at the snooze time.
func verifySnoozeTransition(t *testing.T, worker, observer *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-snooze", "max_workers": 1,
	}, nil)

	var short normalizedJob
	worker.call(t, "insert", map[string]any{
		"behavior": "snooze_once", "duration_ms": 5, "message": "short snooze",
	}, &short)
	worker.call(t, "wait", map[string]any{"id": short.ID}, &short)
	require.Equal(t, "completed", short.State)
	require.Equal(t, 1, short.Attempt, "a snooze must not consume an attempt")
	require.EqualValues(t, 1, short.Metadata["snoozes"])
	require.Empty(t, short.Errors)

	// Both implementations default to a five-second scheduler interval; a
	// longer snooze is persisted as scheduled rather than available.
	const longSnooze = 10 * time.Second
	var long normalizedJob
	worker.call(t, "insert", map[string]any{
		"behavior": "snooze_once", "duration_ms": longSnooze.Milliseconds(), "message": "long snooze",
	}, &long)
	worker.call(t, "wait", map[string]any{"id": long.ID, "states": []string{"scheduled"}}, &long)
	require.Equal(t, 0, long.Attempt, "a snooze must give its attempt back")
	require.EqualValues(t, 1, long.Metadata["snoozes"])
	require.Empty(t, long.Errors)
	require.Nil(t, long.FinalizedAt)
	require.NotNil(t, long.AttemptedAt)
	delay := parseTime(t, long.ScheduledAt).Sub(parseTime(t, *long.AttemptedAt))
	require.GreaterOrEqual(t, delay, longSnooze-100*time.Millisecond)
	require.Less(t, delay, longSnooze+2*time.Second)
	var observed normalizedJob
	observer.call(t, "get", map[string]any{"id": long.ID}, &observed)
	require.Equal(t, long, observed)
	worker.call(t, "stop", map[string]any{}, nil)
}

func verifyExternalTerminalCompletionRace(t *testing.T, worker, externalizer *adapter) {
	t.Helper()

	externalizer.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-completion-race", "instrumented": true, "max_workers": 1,
	}, nil)

	for index, testCase := range []struct {
		behavior      string
		expectsOutput bool
		externalState string
	}{
		{behavior: "barrier_output", expectsOutput: true, externalState: "completed"},
		{behavior: "barrier_output", expectsOutput: true, externalState: "discarded"},
		{behavior: "barrier_wait", externalState: "completed"},
	} {
		barrierName := fmt.Sprintf("completion-race-%s-%d", testCase.externalState, index)
		worker.call(t, "barrier_create", map[string]any{"name": barrierName}, nil)
		var inserted, running normalizedJob
		externalizer.call(t, "insert", map[string]any{
			"behavior": testCase.behavior, "message": barrierName,
		}, &inserted)
		externalizer.call(t, "wait", map[string]any{
			"id": inserted.ID, "states": []string{"running"},
		}, &running)

		var external normalizedJob
		externalizer.call(t, "raw_finalize", map[string]any{
			"id": inserted.ID,
			"metadata": map[string]any{
				"external": testCase.externalState,
				"shared":   "external",
			},
			"state": testCase.externalState,
		}, &external)
		require.Equal(t, testCase.externalState, external.State)
		require.NotNil(t, external.FinalizedAt)
		if testCase.externalState == "discarded" {
			require.Equal(t, []normalizedAttemptError{{
				At:      "2026-02-03T04:05:06.789Z",
				Attempt: 1,
				Error:   "external discard",
				Trace:   "external trace",
			}}, external.Errors)
		} else {
			require.Empty(t, external.Errors)
		}

		worker.call(t, "barrier_release", map[string]any{"name": barrierName}, nil)
		waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
			return len(stats.Events) == index+1
		})
		var completed normalizedJob
		externalizer.call(t, "get", map[string]any{"id": inserted.ID}, &completed)
		if testCase.expectsOutput {
			require.Equal(t, map[string]any{"race": "worker"}, completed.Metadata["output"])
		} else {
			require.NotContains(t, completed.Metadata, "output")
		}
		require.Equal(t, testCase.externalState, completed.State)
		require.Equal(t, external.FinalizedAt, completed.FinalizedAt)
		require.Equal(t, external.Errors, completed.Errors)
		require.Equal(t, testCase.externalState, completed.Metadata["external"])
		require.Equal(t, "external", completed.Metadata["shared"])
	}

	stats := waitForRuntimeStats(t, worker, func(stats runtimeStats) bool {
		return len(stats.Events) == 3
	})
	require.Equal(t, []string{"job_completed", "job_failed", "job_completed"}, stats.Events)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyExtensionOrder checks global hook and middleware ordering around
// insertion and work.
func verifyExtensionOrder(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-extension-order", "instrumented": true, "max_workers": 1,
	}, nil)
	var ordinary normalizedJob
	current.call(t, "insert", map[string]any{"message": "extension order"}, &ordinary)
	current.call(t, "wait", map[string]any{"id": ordinary.ID}, &ordinary)
	require.Equal(t, "completed", ordinary.State)
	stats := waitForRuntimeStats(t, current, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "job_completed")
	})
	// Like River Go, hooks run inside middleware: insertion middleware wraps
	// the insert-begin hooks, and work middleware wraps the work hooks and
	// the worker.
	requireOrderedSubsequence(t, stats.Trace, []string{
		"middleware:insert_before", "hook:insert_begin", "middleware:insert_after",
	})
	requireOrderedSubsequence(t, stats.Trace, []string{
		"middleware:work_before", "hook:work_begin", "hook:work_end", "middleware:work_after",
	})
	requireOrderedSubsequence(t, stats.Trace, []string{"middleware:insert_after", "hook:work_begin"})
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyResumableRetry checks that a completed resumable step is skipped on
// the retry after a later step fails.
func verifyResumableRetry(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-resumable-retry", "instrumented": true,
		"max_workers": 1, "retry_delay_ms": 5,
	}, nil)
	var resumable normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "resumable", "message": "resumable", "opts": map[string]any{"max_attempts": 2},
	}, &resumable)
	current.call(t, "wait", map[string]any{"id": resumable.ID}, &resumable)
	require.Equal(t, "completed", resumable.State)
	require.Equal(t, 2, resumable.Attempt)
	require.Len(t, resumable.Errors, 1)
	require.Equal(t, "first", resumable.Metadata["river:resumable_step"])
	stats := waitForRuntimeStats(t, current, func(stats runtimeStats) bool {
		return slices.Contains(stats.Events, "job_completed") && slices.Contains(stats.Events, "job_failed")
	})
	require.Equal(t, 1, stats.ResumableFirstRuns, "a completed step must not run again")
	require.Equal(t, 2, stats.ResumableSecondRuns)
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyDynamicQueues adds, reconfigures, and removes a queue on a running
// client. Reconfiguration is proven by running two blocked jobs at once
// after raising the queue's worker limit from one to two.
func verifyDynamicQueues(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-dynamic-queues", "max_workers": 1,
	}, nil)
	current.call(t, "queue_add", map[string]any{"max_workers": 1, "name": "dynamic"}, nil)
	current.call(t, "queue_add", map[string]any{"max_workers": 2, "name": "dynamic"}, nil)
	current.call(t, "barrier_create", map[string]any{"name": "dynamic-concurrency"}, nil)
	blocked := make([]normalizedJob, 2)
	for index := range blocked {
		current.call(t, "insert", map[string]any{
			"behavior": "barrier_wait", "message": "dynamic-concurrency",
			"opts": map[string]any{"queue": "dynamic"},
		}, &blocked[index])
	}
	for _, job := range blocked {
		var running normalizedJob
		current.call(t, "wait", map[string]any{"id": job.ID, "states": []string{"running"}}, &running)
	}
	current.call(t, "barrier_release", map[string]any{"name": "dynamic-concurrency"}, nil)
	for _, job := range blocked {
		var completed normalizedJob
		current.call(t, "wait", map[string]any{"id": job.ID}, &completed)
		require.Equal(t, "completed", completed.State)
		require.Equal(t, "dynamic", completed.Queue)
	}

	current.call(t, "queue_remove", map[string]any{"name": "dynamic"}, nil)
	var orphaned, marker normalizedJob
	current.call(t, "insert", map[string]any{
		"message": "removed queue", "opts": map[string]any{"queue": "dynamic"},
	}, &orphaned)
	current.call(t, "insert", map[string]any{"message": "default queue marker"}, &marker)
	current.call(t, "wait", map[string]any{"id": marker.ID}, &marker)
	require.Equal(t, "completed", marker.State)
	current.call(t, "get", map[string]any{"id": orphaned.ID}, &orphaned)
	require.Equal(t, "available", orphaned.State, "a removed queue must not be worked")
	current.call(t, "stop", map[string]any{}, nil)
}

func verifyPeriodicRunOnStart(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-periodic", "instrumented": true,
		"max_workers": 1, "periodic_run_on_start": true,
	}, nil)
	periodic := waitForListedJob(t, current, map[string]any{
		"metadata": map[string]any{"river:periodic_job_id": "conformance-periodic"},
	})
	current.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
	require.Equal(t, "completed", periodic.State)
	require.Equal(t, true, periodic.Metadata["periodic"])
	stats := waitForRuntimeStats(t, current, func(stats runtimeStats) bool {
		return stats.PeriodicStarts == 1
	})
	require.Equal(t, 1, stats.PeriodicStarts)
	var listed struct {
		Jobs []normalizedJob `json:"jobs"`
	}
	current.call(t, "list", map[string]any{
		"metadata": map[string]any{"river:periodic_job_id": "conformance-periodic"},
	}, &listed)
	require.Len(t, listed.Jobs, 1, "run-on-start must enqueue exactly once per leadership term")
	current.call(t, "stop", map[string]any{}, nil)
}

func verifyErrorHandlerCancel(t *testing.T, current *adapter) {
	t.Helper()

	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-error-handler", "error_handler_cancel": true,
		"instrumented": true, "max_workers": 1,
	}, nil)
	var handled normalizedJob
	current.call(t, "insert", map[string]any{
		"behavior": "error", "message": "error handler cancellation",
		"opts": map[string]any{"max_attempts": 3},
	}, &handled)
	current.call(t, "wait", map[string]any{"id": handled.ID}, &handled)
	require.Equal(t, "cancelled", handled.State)
	require.Equal(t, 1, handled.Attempt)
	require.Len(t, handled.Errors, 1)
	require.Equal(t, "conformance retryable error", handled.Errors[0].Error)
	stats := waitForRuntimeStats(t, current, func(stats runtimeStats) bool {
		return stats.ErrorHandlerCalls == 1 && slices.Contains(stats.Events, "job_cancelled")
	})
	require.Equal(t, 1, stats.ErrorHandlerCalls)
	current.call(t, "stop", map[string]any{}, nil)
}

// verifyTimeoutCancellation checks that a job timeout cancels a cooperative
// worker and records the failed attempt.
func verifyTimeoutCancellation(t *testing.T, worker, observer *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-timeout", "job_timeout_ms": 20, "max_workers": 1,
	}, nil)
	var job, observed normalizedJob
	worker.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "timeout cancellation",
		"opts": map[string]any{"max_attempts": 1},
	}, &job)
	worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "discarded", job.State)
	require.Equal(t, 1, job.Attempt)
	require.Len(t, job.Errors, 1)
	require.NotEmpty(t, job.Errors[0].Error)
	require.NotNil(t, job.AttemptedAt)
	require.NotNil(t, job.FinalizedAt)
	require.GreaterOrEqual(t, parseTime(t, *job.FinalizedAt).Sub(parseTime(t, *job.AttemptedAt)), 20*time.Millisecond)
	observer.call(t, "get", map[string]any{"id": job.ID}, &observed)
	require.Equal(t, job, observed)
	worker.call(t, "stop", map[string]any{}, nil)
}

// verifyCompletionBatching completes many jobs at once and requires the
// completions to share write transactions. PostgreSQL assigns one
// transaction ID per writing transaction, so completing N jobs one at a time
// would consume at least N IDs.
func verifyCompletionBatching(t *testing.T, observer *postgresObserver, current *adapter) {
	t.Helper()

	const jobCount = 1_000
	current.call(t, "reset", map[string]any{}, nil)
	current.call(t, "start", map[string]any{
		"client_id": current.name + "-completion-batching", "fetch_poll_interval_ms": 1_000,
		"max_workers": jobCount,
	}, nil)
	current.call(t, "barrier_create", map[string]any{"name": "completion-batching"}, nil)
	jobs := make([]map[string]any, jobCount)
	for index := range jobs {
		jobs[index] = map[string]any{"behavior": "barrier_wait", "message": "completion-batching"}
	}
	var inserted struct {
		Count int `json:"count"`
	}
	current.call(t, "insert_many_fast", map[string]any{"jobs": jobs}, &inserted)
	require.Equal(t, jobCount, inserted.Count)
	waitForListedJobCountWithin(t, current, map[string]any{
		"limit": jobCount, "states": []string{"running"},
	}, jobCount, 20*time.Second)

	before := observer.nextTransactionID(t)
	current.call(t, "barrier_release", map[string]any{"name": "completion-batching"}, nil)
	completed := waitForListedJobCountWithin(t, current, map[string]any{
		"limit": jobCount, "states": []string{"completed"},
	}, jobCount, 20*time.Second)
	writes := observer.nextTransactionID(t) - before
	for _, job := range completed {
		require.Equal(t, 1, job.Attempt)
		require.Empty(t, job.Errors)
	}
	require.Less(t, writes, int64(jobCount/4),
		"%s used %d write transactions to complete %d jobs; completions are not batched", current.name, writes, jobCount)
	t.Logf("%s completed %d jobs in %d write transactions", current.name, jobCount, writes)
	current.call(t, "stop", map[string]any{}, nil)
}

func verifyRefetchedAttemptCancellation(t *testing.T, worker, canceller *adapter) {
	t.Helper()

	worker.call(t, "reset", map[string]any{}, nil)
	worker.call(t, "start", map[string]any{
		"client_id": worker.name + "-refetched-cancel", "max_workers": 1,
	}, nil)
	var job normalizedJob
	canceller.call(t, "insert", map[string]any{
		"behavior": "snooze_then_cancel", "duration_ms": 1, "message": "refetched cancellation",
	}, &job)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		worker.call(t, "get", map[string]any{"id": job.ID}, &job)
		if job.State == "running" && job.Metadata["snoozes"] != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.Equal(t, "running", job.State)
	require.NotNil(t, job.Metadata["snoozes"])
	canceller.call(t, "cancel", map[string]any{"id": job.ID}, &job)
	worker.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "cancelled", job.State)
	worker.call(t, "stop", map[string]any{}, nil)
}

func parseTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339Nano, value)
	require.NoError(t, err)
	return parsed
}
