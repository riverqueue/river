//go:build riverconformance

package harness_test

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// uniqueColumnCases are unique options whose stored key and state mask every
// implementation must write identically. The period case schedules the job
// at a fixed time, so its period, derived from the scheduled time, doesn't
// depend on when the scenario runs.
func uniqueColumnCases() []struct {
	name string
	opts map[string]any
} {
	allStates := []string{"available", "cancelled", "completed", "discarded", "pending", "retryable", "running", "scheduled"}
	return []struct {
		name string
		opts map[string]any
	}{
		{name: "by_args", opts: map[string]any{"unique": map[string]any{"by_args": true}}},
		{name: "by_args_exclude_kind", opts: map[string]any{"unique": map[string]any{"by_args": true, "exclude_kind": true}}},
		{name: "by_period", opts: map[string]any{
			"scheduled_at": "2031-02-03T04:05:06.789Z",
			"unique":       map[string]any{"by_period_ms": 3_600_000},
		}},
		{name: "by_queue", opts: map[string]any{"queue": "unique_queue", "unique": map[string]any{"by_queue": true}}},
		{name: "by_state", opts: map[string]any{"unique": map[string]any{"by_state": []string{"available", "pending", "running", "scheduled"}}}},
		{name: "combined", opts: map[string]any{
			"queue":        "unique_queue",
			"scheduled_at": "2031-02-03T04:05:06.789Z",
			"unique": map[string]any{
				"by_args": true, "by_period_ms": 86_400_000, "by_queue": true, "by_state": allStates,
			},
		}},
	}
}

// uniqueColumns is the part of raw_job_row that stores a job's uniqueness.
type uniqueColumns struct {
	Key        *string
	KeyType    *string
	States     *string
	StatesType *string
}

func readUniqueColumns(t *testing.T, reader *adapter, id int64) uniqueColumns {
	t.Helper()

	var row rawJobRow
	reader.call(t, "raw_job_row", map[string]any{"id": id}, &row)
	return uniqueColumns{Key: row.UniqueKey, KeyType: row.UniqueKeyType, States: row.UniqueStates, StatesType: row.UniqueStatesType}
}

// verifyUniqueColumnBytes has each implementation insert the same unique jobs
// and requires the stored `unique_key` and `unique_states` to be identical
// byte for byte, including their SQLite storage types, as read by both
// implementations. A job without unique options stores neither.
func verifyUniqueColumnBytes(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	write := func(writer *adapter, params map[string]any) uniqueColumns {
		t.Helper()

		goAdapter.call(t, "reset", map[string]any{}, nil)
		var inserted normalizedJob
		writer.call(t, "insert", params, &inserted)
		columns := readUniqueColumns(t, goAdapter, inserted.ID)
		require.Equal(t, columns, readUniqueColumns(t, candidateAdapter, inserted.ID),
			"%s and %s render the unique columns %s wrote differently", goAdapter.name, candidateAdapter.name, writer.name)
		return columns
	}

	for _, testCase := range uniqueColumnCases() {
		params := map[string]any{"message": "unique columns " + testCase.name, "opts": testCase.opts}
		reference := write(goAdapter, params)
		require.NotNil(t, reference.Key, "%s: Go stored no unique key", testCase.name)
		require.NotNil(t, reference.States, "%s: Go stored no unique states", testCase.name)
		require.Equal(t, reference, write(candidateAdapter, params),
			"%s: %s and %s stored different unique columns", testCase.name, goAdapter.name, candidateAdapter.name)
	}

	params := map[string]any{"message": "not unique"}
	require.Equal(t, uniqueColumns{}, write(goAdapter, params))
	require.Equal(t, uniqueColumns{}, write(candidateAdapter, params))
}

// verifyClaimTimeCancellation cancels a job from canceller between the moment
// claimer's claim of it commits and the moment claimer starts working it.
// The claimer holds its claim on a barrier, so the job is already running
// but has no executor when the cancellation arrives. The claimer must start
// the job's worker already cancelled, which its runtime stats report, so a
// cancellation that only arrived after the claim was released fails the
// scenario instead of passing through the ordinary cancellation path.
func verifyClaimTimeCancellation(t *testing.T, canceller, claimer *adapter, listens bool) {
	t.Helper()

	claimer.call(t, "reset", map[string]any{}, nil)
	barrier := "claim-time-cancel-" + claimer.name
	claimer.call(t, "barrier_create", map[string]any{"name": barrier}, nil)
	clientID := claimer.name + "-claim-time-cancel"
	claimer.call(t, "start", map[string]any{
		"claim_barrier": barrier, "client_id": clientID, "max_workers": 1,
	}, nil)
	if listens {
		// Remote cancellation arrives by notification, so the claimer must
		// be listening before the claim it holds.
		waitForListener(t, claimer)
	}

	var job normalizedJob
	canceller.call(t, "insert", map[string]any{
		"behavior": "cooperative_cancel", "message": "claim-time cancellation from " + canceller.name,
	}, &job)
	canceller.call(t, "wait", map[string]any{"id": job.ID, "states": []string{"running"}}, &job)
	require.Equal(t, []string{clientID}, job.AttemptedBy)

	var requested normalizedJob
	canceller.call(t, "cancel", map[string]any{"id": job.ID}, &requested)
	require.Equal(t, "running", requested.State, "cancelling a claimed job only requests cancellation")
	// Give the claimer time to receive the notification while it still holds
	// the claim. SQLite listeners poll every 50 ms, and PostgreSQL delivers
	// notifications at commit.
	time.Sleep(time.Second)
	claimer.call(t, "barrier_release", map[string]any{"name": barrier}, nil)

	canceller.call(t, "wait", map[string]any{"id": job.ID}, &job)
	require.Equal(t, "cancelled", job.State, "%s did not cancel a job %s cancelled during its claim", claimer.name, canceller.name)
	require.Equal(t, 1, job.Attempt)
	require.Len(t, job.Errors, 1)
	require.Equal(t, "JobCancelError: job cancelled remotely", job.Errors[0].Error)
	var stats runtimeStats
	claimer.call(t, "runtime_stats", map[string]any{}, &stats)
	require.Equal(t, 1, stats.CancelledAtStart,
		"%s started a job %s cancelled during its claim without its cancellation", claimer.name, canceller.name)
	claimer.call(t, "stop", map[string]any{}, nil)
}

// notificationCapture reads the notifications published since its previous
// read.
type notificationCapture interface {
	next(t *testing.T) []rawNotification
}

// postgresNotificationCapture listens to River's PostgreSQL channels on
// harness connections. Payloads are grouped by channel, each in commit order.
type postgresNotificationCapture struct {
	listeners []*postgresNotificationListener
	marker    int
	observer  *postgresObserver
	schema    string
}

func newPostgresNotificationCapture(t *testing.T, observer *postgresObserver) *postgresNotificationCapture {
	t.Helper()

	capture := &postgresNotificationCapture{observer: observer, schema: observer.currentSchema(t)}
	for _, topic := range []string{"river_control", "river_insert", "river_leadership"} {
		capture.listeners = append(capture.listeners, observer.listen(t, capture.schema+"."+topic))
	}
	return capture
}

func (capture *postgresNotificationCapture) next(t *testing.T) []rawNotification {
	t.Helper()

	var notifications []rawNotification
	for _, listener := range capture.listeners {
		capture.marker++
		marker := fmt.Sprintf("notification-capture-marker-%d", capture.marker)
		for _, payload := range listener.receiveUntilMarker(t, capture.observer, marker) {
			notifications = append(notifications, rawNotification{
				Payload: payload, Topic: strings.TrimPrefix(listener.channel, capture.schema+"."),
			})
		}
	}
	return notifications
}

// sqliteNotificationCapture reads SQLite outbox rows through an observing
// adapter, in ID order, with IDs cleared.
type sqliteNotificationCapture struct {
	afterID  int64
	observer *adapter
}

func newSQLiteNotificationCapture(t *testing.T, observer *adapter) *sqliteNotificationCapture {
	t.Helper()

	capture := &sqliteNotificationCapture{observer: observer}
	_ = capture.next(t)
	return capture
}

func (capture *sqliteNotificationCapture) next(t *testing.T) []rawNotification {
	t.Helper()

	notifications := rawNotificationsAfter(t, capture.observer, capture.afterID)
	for index := range notifications {
		capture.afterID = notifications[index].ID
		notifications[index].ID = 0
	}
	return notifications
}

// notificationOperation is the notifications one operation published.
type notificationOperation struct {
	name          string
	notifications []rawNotification
}

var notificationJobIDPattern = regexp.MustCompile(`"job_id":\d+`)

// notificationQueueMetadata is written with its keys out of order, with
// characters Go escapes, and with an escape Go keeps from the caller's text
// but a re-encoder drops, so member order and escaping show in the bytes.
const notificationQueueMetadata = `{"zeta":"<&>","alpha":1,"path":"a\/b"}`

// publishNotificationOperations has actor perform every operation that
// publishes a notification and returns what each published, with job IDs
// replaced. The client it starts uses a fixed ID, so leadership payloads
// name the same leader whichever implementation runs it.
func publishNotificationOperations(t *testing.T, actor *adapter, capture notificationCapture) []notificationOperation {
	t.Helper()

	var operations []notificationOperation
	record := func(name string) {
		t.Helper()

		notifications := capture.next(t)
		for index := range notifications {
			notifications[index].Payload = notificationJobIDPattern.ReplaceAllString(notifications[index].Payload, `"job_id":0`)
		}
		operations = append(operations, notificationOperation{name: name, notifications: notifications})
	}

	actor.call(t, "reset", map[string]any{}, nil)
	_ = capture.next(t)
	var job normalizedJob
	actor.call(t, "insert", map[string]any{
		"message": "notification bytes", "opts": map[string]any{"queue": "notification_bytes"},
	}, &job)
	record("insert")
	actor.call(t, "cancel", map[string]any{"id": job.ID}, nil)
	record("cancel")
	// Outlast any insert notification throttling, so a retry that notifies
	// isn't suppressed by the insertion above.
	time.Sleep(250 * time.Millisecond)
	actor.call(t, "retry", map[string]any{"id": job.ID}, nil)
	record("retry")

	const clientID = "notification-bytes"
	actor.call(t, "start", map[string]any{"client_id": clientID, "max_workers": 1}, nil)
	require.Equal(t, clientID, waitForLeader(t, actor, ""))
	record("start")
	actor.call(t, "queue_update", map[string]any{
		"metadata": json.RawMessage(notificationQueueMetadata), "name": "default",
	}, nil)
	record("queue_update")
	actor.call(t, "queue_pause", map[string]any{"name": "default"}, nil)
	record("queue_pause")
	actor.call(t, "queue_resume", map[string]any{"name": "default"}, nil)
	record("queue_resume")
	term := readLeader(t, actor)
	actor.call(t, "request_resign", map[string]any{}, nil)
	_ = waitForLeaderTerm(t, actor, term.ElectedAt)
	record("request_resign")
	actor.call(t, "stop", map[string]any{}, nil)
	record("stop")
	return operations
}

// metadataTextPending lists implementations whose queue updates don't yet
// keep the caller's metadata text, so their `metadata_changed` payloads are
// reported rather than failed until they do.
var metadataTextPending = map[string]bool{"javascript": true} //nolint:gochecknoglobals // fixed lookup table

// goSortedJSON re-encodes a JSON document the way Go encodes a map: keys
// sorted at every level, with Go's escaping.
func goSortedJSON(t *testing.T, document string) string {
	t.Helper()

	var decoded any
	require.NoError(t, json.Unmarshal([]byte(document), &decoded))
	encoded, err := json.Marshal(decoded)
	require.NoError(t, err)
	return string(encoded)
}

// verifyNotificationPayloadBytes has each implementation perform the same
// operations and requires the notifications they publish (insert, cancel,
// queue metadata changes, pause, resume, resignation requests, and
// resignations) to match Go's byte for byte: topic, payload text, and on
// SQLite the payload's storage type.
//
// One difference is reported rather than failed for the implementations in
// metadataTextPending: re-encoding `metadata_changed` metadata from its
// parsed value (sorted keys and canonical escapes) instead of keeping the
// caller's text.
func verifyNotificationPayloadBytes(t *testing.T, goAdapter, candidateAdapter *adapter, newCapture func(actor *adapter) notificationCapture) {
	t.Helper()

	reference := publishNotificationOperations(t, goAdapter, newCapture(goAdapter))
	candidate := publishNotificationOperations(t, candidateAdapter, newCapture(candidateAdapter))
	require.Len(t, candidate, len(reference))
	byName := make(map[string][]rawNotification, len(reference))
	for _, operation := range reference {
		byName[operation.name] = operation.notifications
	}
	require.Len(t, byName["insert"], 1, "Go published no insert notification")
	for _, name := range []string{"cancel", "queue_update", "queue_pause", "queue_resume", "request_resign"} {
		require.NotEmpty(t, byName[name], "Go published no notification for %s", name)
	}

	for index, expected := range reference {
		actual := candidate[index]
		require.Equal(t, expected.name, actual.name)
		if sameNotifications(expected.notifications, actual.notifications) {
			continue
		}
		switch {
		case metadataTextPending[candidateAdapter.spec.Implementation] &&
			expected.name == "queue_update" && len(expected.notifications) == 1 && len(actual.notifications) == 1 &&
			actual.notifications[0].Topic == expected.notifications[0].Topic &&
			actual.notifications[0].PayloadType == expected.notifications[0].PayloadType &&
			actual.notifications[0].Payload == goSortedJSON(t, expected.notifications[0].Payload):
			t.Logf("KNOWN DIVERGENCE: %s re-encodes metadata_changed metadata from its parsed value (sorted keys, canonical escapes); Go keeps the caller's text:\n  go:        %s\n  %s: %s",
				candidateAdapter.name, expected.notifications[0].Payload, candidateAdapter.name, actual.notifications[0].Payload)
		default:
			require.Equal(t, expected.notifications, actual.notifications,
				"%s: %s and %s published different notifications", expected.name, goAdapter.name, candidateAdapter.name)
		}
	}
}

func sameNotifications(expected, actual []rawNotification) bool {
	if len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

// verifyUniquePeriodicJob has one implementation's leader insert a unique
// run-on-start periodic job and then requires a later leader of the other
// implementation to skip its own run-on-start insertion as a duplicate, in
// both directions. It only skips when both compute the same unique key and
// states for the periodic job.
func verifyUniquePeriodicJob(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		first, second *adapter
	}{
		{first: goAdapter, second: candidateAdapter},
		{first: candidateAdapter, second: goAdapter},
	} {
		pair.first.call(t, "reset", map[string]any{}, nil)
		start := func(leader *adapter) {
			t.Helper()

			clientID := leader.name + "-periodic-unique"
			leader.call(t, "start", map[string]any{
				"client_id": clientID, "instrumented": true, "max_workers": 1,
				"periodic_run_on_start": true, "periodic_unique": true,
			}, nil)
			require.Equal(t, clientID, waitForLeader(t, leader, ""))
			_ = waitForRuntimeStats(t, leader, func(stats runtimeStats) bool { return stats.PeriodicStarts == 1 })
		}

		start(pair.first)
		periodic := waitForPeriodicJob(t, pair.first, "conformance-periodic")
		pair.first.call(t, "wait", map[string]any{"id": periodic.ID}, &periodic)
		require.Equal(t, "completed", periodic.State)
		pair.first.call(t, "stop", map[string]any{}, nil)

		start(pair.second)
		// Each leader inserts a non-unique marker job after the unique
		// job, so once the second leader's marker exists, its attempt to
		// insert the unique job has been made.
		var periodicJobs []normalizedJob
		deadline := time.Now().Add(10 * time.Second)
		for {
			var listed struct {
				Jobs []normalizedJob `json:"jobs"`
			}
			pair.second.call(t, "list", map[string]any{}, &listed)
			markers := 0
			periodicJobs = periodicJobs[:0]
			for _, job := range listed.Jobs {
				switch job.Metadata["river:periodic_job_id"] {
				case "conformance-periodic-marker":
					markers++
				case "conformance-periodic":
					periodicJobs = append(periodicJobs, job)
				}
			}
			if markers == 2 {
				break
			}
			require.True(t, time.Now().Before(deadline), "%s inserted no periodic marker job", pair.second.name)
			time.Sleep(10 * time.Millisecond)
		}
		require.Len(t, periodicJobs, 1, "%s inserted a unique periodic job %s already inserted", pair.second.name, pair.first.name)
		require.Equal(t, periodic.ID, periodicJobs[0].ID)
		pair.second.call(t, "stop", map[string]any{}, nil)
	}
}

// waitForPeriodicJob waits for a job inserted by the periodic job with the
// given ID and returns it.
func waitForPeriodicJob(t *testing.T, observer *adapter, periodicJobID string) normalizedJob {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var listed struct {
			Jobs []normalizedJob `json:"jobs"`
		}
		observer.call(t, "list", map[string]any{}, &listed)
		for _, job := range listed.Jobs {
			if job.Metadata["river:periodic_job_id"] == periodicJobID {
				return job
			}
		}
		require.True(t, time.Now().Before(deadline), "no job from periodic job %s", periodicJobID)
		time.Sleep(10 * time.Millisecond)
	}
}
