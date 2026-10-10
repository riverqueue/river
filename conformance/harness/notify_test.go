package harness

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// semanticNotification is a notification compared as JSON rather than text:
// its topic, its SQLite storage type, and its decoded payload, with any job
// ID cleared once checked.
type semanticNotification struct {
	Payload     any
	PayloadType string
	Topic       string
}

// semanticNotifications decodes notifications. A payload's job ID, which
// differs between writers, must be the JSON integer jobID, and is cleared.
func semanticNotifications(t *testing.T, notifications []Notification, jobID int64) []semanticNotification {
	t.Helper()

	semantic := make([]semanticNotification, len(notifications))
	for i, notification := range notifications {
		var payload any
		require.NoError(t, json.Unmarshal([]byte(notification.Payload), &payload), "%s payload isn't JSON: %s", notification.Topic, notification.Payload)
		if fields, ok := payload.(map[string]any); ok {
			if _, ok := fields["job_id"]; ok {
				var raw struct {
					JobID json.RawMessage `json:"job_id"`
				}
				require.NoError(t, json.Unmarshal([]byte(notification.Payload), &raw))
				id, err := strconv.ParseInt(string(raw.JobID), 10, 64)
				require.NoError(t, err, "%s job_id isn't a JSON integer: %s", notification.Topic, notification.Payload)
				require.Equal(t, jobID, id, "%s names another job: %s", notification.Topic, notification.Payload)
				fields["job_id"] = 0
			}
		}
		semantic[i] = semanticNotification{Payload: payload, PayloadType: notification.PayloadType, Topic: notification.Topic}
	}
	return semantic
}

// requireStatsCounts waits until the observer's queue events reach the
// counts, and requires them not to exceed them.
func requireQueueEventCounts(t *testing.T, observer *Adapter, paused, resumed int) {
	t.Helper()

	stats := observer.WaitStats(t, fmt.Sprintf("%d pauses and %d resumes", paused, resumed), func(stats *protocol.StatsResult) bool {
		return CountEvents(stats, "queue_paused") >= paused && CountEvents(stats, "queue_resumed") >= resumed
	})
	require.Equal(t, paused, CountEvents(stats, "queue_paused"))
	require.Equal(t, resumed, CountEvents(stats, "queue_resumed"))
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestNotifications(t *testing.T) {
	t.Parallel()

	// An insert from one implementation wakes the other's worker through a
	// notification. The worker polls only once a minute, so prompt
	// completion can't come from polling.
	t.Run("InsertWakeup", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "insert-wakeup", FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 1})
			env.DB.WaitListening(t, worker)
			startedAt := time.Now()
			inserted := controller.InsertJob(t, echo("insert wakeup", protocol.BehaviorComplete))
			require.Equal(t, "completed", env.DB.WaitJob(t, inserted.ID, workWait).State)
			require.Less(t, time.Since(startedAt), 5*time.Second)
		})
	})

	// Pausing a queue from one implementation stops the other's running
	// worker from working it until it's resumed. The worker reports applying
	// the pause, a marker job in another queue then proves it kept fetching,
	// and the paused job's attempt starts no earlier than the resume.
	t.Run("PauseResume", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "pause-resume", MaxWorkers: 1, Queues: []string{"default", "pause_marker"}})
			controller.Queue(t, protocol.QueueParams{Action: protocol.QueueActionPause, Name: "default"})
			requireQueueEventCounts(t, worker, 1, 0)

			paused := controller.InsertJob(t, echo("inserted while paused", protocol.BehaviorComplete))
			marker := controller.InsertJob(t, withOpts(echo("unpaused marker", protocol.BehaviorComplete), protocol.InsertOpts{Queue: "pause_marker"}))
			require.Equal(t, "completed", env.DB.WaitJob(t, marker.ID, workWait).State)
			require.Equal(t, "available", env.DB.MustJob(t, paused.ID).State, "a paused queue was worked")

			controller.Queue(t, protocol.QueueParams{Action: protocol.QueueActionResume, Name: "default"})
			queue := env.DB.Queue(t, "default")
			require.Nil(t, queue.PausedAt)
			worked := env.DB.WaitJob(t, paused.ID, workWait)
			require.Equal(t, "completed", worked.State)
			require.False(t, worked.AttemptedAt.Before(queue.UpdatedAt), "paused job attempted at %s, before the queue resumed at %s", worked.AttemptedAt, queue.UpdatedAt)
			requireQueueEventCounts(t, worker, 1, 1)
		})
	})

	// Each implementation publishes the same notifications as the reference
	// for the same operations: whether each is sent, how many and in which
	// order, the topic, on SQLite the payload's storage type, and the payload
	// as JSON, so key order, escaping, and whitespace don't matter.
	t.Run("Payloads", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			reference := publishNotifications(t, env, env.Reference)
			other := env.Another(t)
			candidate := publishNotifications(t, other, other.Candidate)

			byName := map[string][]semanticNotification{}
			for _, operation := range reference {
				byName[operation.name] = operation.notifications
			}
			require.Len(t, byName["insert"], 1, "the reference published no insert notification")
			for _, name := range []string{"cancel", "queue_update", "queue_pause", "queue_resume", "request_resign"} {
				require.NotEmpty(t, byName[name], "the reference published no notification for %s", name)
			}
			require.Len(t, candidate, len(reference))
			for i, expected := range reference {
				require.Equal(t, expected, candidate[i], "%s: the implementations published different notifications", expected.name)
			}
		})
	})

	// A pause or resume of every queue by one implementation produces
	// exactly one subscription event in the other, and repeating it doesn't
	// deliver it again. Control notifications are processed in order, so
	// waiting for the next change proves any event from a repeat would
	// already have been seen.
	t.Run("QueueSubscriptionEvents", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, observer *Adapter) {
			observer.Start(t, protocol.StartParams{ClientID: "queue-subscriber", FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 1})
			warmup := controller.InsertJob(t, echo("activate the subscriber", protocol.BehaviorComplete))
			require.Equal(t, "completed", env.DB.WaitJob(t, warmup.ID, workWait).State)

			queue := func(action string) { controller.Queue(t, protocol.QueueParams{Action: action, Name: "*"}) }
			queue(protocol.QueueActionPause)
			requireQueueEventCounts(t, observer, 1, 0)
			queue(protocol.QueueActionPause)
			queue(protocol.QueueActionResume)
			requireQueueEventCounts(t, observer, 1, 1)
			queue(protocol.QueueActionResume)
			queue(protocol.QueueActionPause)
			requireQueueEventCounts(t, observer, 2, 1)
			queue(protocol.QueueActionResume)
			requireQueueEventCounts(t, observer, 2, 2)
		})
	})

	// A metadata update by one implementation of a queue the other created
	// sends one metadata_changed control notification, which River Go's
	// producers hand to their extension. Queue changes made in a transaction
	// are seen only when it commits.
	t.Run("QueueUpdates", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, creator, updater *Adapter) {
			creator.Start(t, protocol.StartParams{ClientID: "queue-creator", MaxWorkers: 1})
			creator.Stop(t, protocol.StopParams{})
			before := env.DB.Queue(t, "default")
			require.Nil(t, before.PausedAt)

			notifications := env.DB.Listen(t)
			updater.Queue(t, protocol.QueueParams{Action: protocol.QueueActionUpdate, Metadata: metadata(t, map[string]any{"updated_by": "updater"}), Name: "default"})
			require.Equal(t, map[string]any{"updated_by": "updater"}, env.DB.Queue(t, "default").Metadata)
			published := notifications.Next(t)
			require.Len(t, published, 1, "one control notification per metadata update")
			require.Equal(t, "river_control", published[0].Topic)
			require.JSONEq(t, `{"action":"metadata_changed","metadata":{"updated_by":"updater"},"queue":"default"}`, published[0].Payload)

			updater.TxBegin(t, "queue_commit")
			updater.Queue(t, protocol.QueueParams{Action: protocol.QueueActionUpdate, Metadata: metadata(t, map[string]any{"updated_by": "transaction"}), Name: "default", Tx: "queue_commit"})
			updater.Queue(t, protocol.QueueParams{Action: protocol.QueueActionPause, Name: "default", Tx: "queue_commit"})
			unchanged := env.DB.Queue(t, "default")
			require.Equal(t, map[string]any{"updated_by": "updater"}, unchanged.Metadata)
			require.Nil(t, unchanged.PausedAt)
			updater.TxEnd(t, "queue_commit", true)
			committed := env.DB.Queue(t, "default")
			require.Equal(t, map[string]any{"updated_by": "transaction"}, committed.Metadata)
			require.NotNil(t, committed.PausedAt)

			updater.TxBegin(t, "queue_rollback")
			updater.Queue(t, protocol.QueueParams{Action: protocol.QueueActionResume, Name: "default", Tx: "queue_rollback"})
			updater.TxEnd(t, "queue_rollback", false)
			require.Equal(t, committed, env.DB.Queue(t, "default"))
		})
	})

	// Inserts and cancellations made in a transaction publish their
	// notifications only when it commits. A committed insert wakes a worker
	// that polls once a minute; a rolled-back one, and cancelling an already
	// finalized job, publish nothing.
	t.Run("Transactional", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			notifications := env.DB.Listen(t)
			job := controller.InsertJob(t, echo("transactional cancel", protocol.BehaviorComplete))
			_ = notifications.Next(t)

			controller.TxBegin(t, "cancel_rollback")
			controller.Cancel(t, protocol.JobParams{ID: job.ID, Tx: "cancel_rollback"})
			controller.TxEnd(t, "cancel_rollback", false)
			require.Empty(t, notifications.Next(t), "a rolled-back cancellation published")

			controller.TxBegin(t, "cancel_commit")
			controller.Cancel(t, protocol.JobParams{ID: job.ID, Tx: "cancel_commit"})
			controller.TxEnd(t, "cancel_commit", true)
			published := notifications.Next(t)
			require.Len(t, published, 1)
			require.Equal(t, "river_control", published[0].Topic)
			require.JSONEq(t, fmt.Sprintf(`{"action":"cancel","job_id":%d,"queue":"default"}`, job.ID), published[0].Payload)
			if env.Driver == DriverSQLite {
				require.Equal(t, "text", published[0].PayloadType)
			}

			controller.Cancel(t, protocol.JobParams{ID: job.ID})
			require.Empty(t, notifications.Next(t), "cancelling a finalized job published")

			worker.Start(t, protocol.StartParams{ClientID: "transactional-wakeup", FetchPollIntervalMS: time.Minute.Milliseconds(), MaxWorkers: 2})
			env.DB.WaitListening(t, worker)
			for _, commit := range []bool{false, true} {
				tx := fmt.Sprintf("insert_%t", commit)
				// Outlast insert notification throttling, so the committed
				// insert isn't suppressed by the rolled-back one.
				time.Sleep(250 * time.Millisecond)
				controller.TxBegin(t, tx)
				controller.Insert(t, protocol.InsertParams{Tx: tx, Jobs: []protocol.InsertJob{
					withOpts(echo(tx+" first", protocol.BehaviorComplete), protocol.InsertOpts{Tags: []string{tx}}),
					withOpts(echo(tx+" second", protocol.BehaviorComplete), protocol.InsertOpts{Tags: []string{tx}}),
				}})
				require.Empty(t, worker.List(t, protocol.ListParams{TagsAll: []string{tx}}).Jobs, "a transactional insert was visible before commit")

				startedAt := time.Now()
				controller.TxEnd(t, tx, commit)
				if !commit {
					require.Empty(t, notifications.Next(t), "a rolled-back insert published")
					require.Empty(t, worker.List(t, protocol.ListParams{TagsAll: []string{tx}}).Jobs)
					continue
				}
				WaitFor(t, "the committed jobs completing", workWait, func() bool {
					return len(worker.List(t, protocol.ListParams{States: []string{"completed"}, TagsAll: []string{tx}}).Jobs) == 2
				})
				require.Less(t, time.Since(startedAt), 5*time.Second, "a committed insert didn't wake a worker polling once a minute")
				require.True(t, slices.ContainsFunc(notifications.Next(t), func(n Notification) bool { return n.Topic == "river_insert" }),
					"a committed insert published no insert notification")
			}
		})
	})
}

// notificationOperation is the notifications one operation published.
type notificationOperation struct {
	name          string
	notifications []semanticNotification
}

// publishNotifications has actor perform every operation that publishes a
// notification and returns what each published. Its client has a fixed ID,
// so leadership payloads name the same leader whichever implementation runs.
func publishNotifications(t *testing.T, env *Env, actor *Adapter) []notificationOperation {
	t.Helper()

	notifications := env.DB.Listen(t)
	var (
		job        *protocol.Job
		operations []notificationOperation
	)
	record := func(name string) {
		var jobID int64
		if job != nil {
			jobID = job.ID
		}
		operations = append(operations, notificationOperation{name: name, notifications: semanticNotifications(t, notifications.Next(t), jobID)})
	}

	job = actor.InsertJob(t, withOpts(echo("notification payloads", protocol.BehaviorComplete), protocol.InsertOpts{Queue: "notification_payloads"}))
	record("insert")
	actor.Cancel(t, protocol.JobParams{ID: job.ID})
	record("cancel")
	// Outlast insert notification throttling, so a retry that notifies isn't
	// suppressed by the insert above.
	time.Sleep(250 * time.Millisecond)
	actor.Retry(t, protocol.JobParams{ID: job.ID})
	record("retry")

	const clientID = "notification-payloads"
	actor.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1})
	term := env.DB.WaitLeader(t, "")
	require.Equal(t, clientID, term.LeaderID)
	record("start")
	actor.Queue(t, protocol.QueueParams{Action: protocol.QueueActionUpdate, Metadata: json.RawMessage(`{"zeta":"z","alpha":1}`), Name: "default"})
	record("queue_update")
	actor.Queue(t, protocol.QueueParams{Action: protocol.QueueActionPause, Name: "default"})
	record("queue_pause")
	actor.Queue(t, protocol.QueueParams{Action: protocol.QueueActionResume, Name: "default"})
	record("queue_resume")
	actor.RequestResign(t, protocol.RequestResignParams{})
	env.DB.WaitNewTerm(t, term.ElectedAt)
	record("request_resign")
	actor.Stop(t, protocol.StopParams{})
	record("stop")
	return operations
}
