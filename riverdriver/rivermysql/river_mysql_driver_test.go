package rivermysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()
		ctx := schemaTemplateParam(ctx, "")
		// Just verify it doesn't panic
		_ = ctx
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()
		ctx := schemaTemplateParam(ctx, "custom_schema")
		_ = ctx
	})
}

func TestDriverProperties(t *testing.T) {
	t.Parallel()

	driver := New(nil)
	require.Equal(t, "?", driver.ArgPlaceholder())
	require.Equal(t, riverdriver.DatabaseNameMySQL, driver.DatabaseName())
	require.True(t, driver.SupportsListener())
	require.True(t, driver.SupportsListenNotify())
	require.Equal(t, time.Microsecond, driver.TimePrecision())
	require.False(t, driver.PoolIsSet())
}

func TestJobInsertAndGet(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert a job
	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{"test": true}`),
		Kind:        "test_job",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateAvailable,
		Tags:        []string{"tag1"},
	})
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Positive(t, job.ID)
	require.Equal(t, "test_job", job.Kind)
	require.Equal(t, rivertype.JobStateAvailable, job.State)
	require.Equal(t, "default", job.Queue)
	require.Equal(t, 1, job.Priority)
	require.Equal(t, 3, job.MaxAttempts)
	require.JSONEq(t, `{"test": true}`, string(job.EncodedArgs))
	require.Equal(t, []string{"tag1"}, job.Tags)

	// Get the job by ID
	fetched, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     job.ID,
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, job.ID, fetched.ID)
	require.Equal(t, job.Kind, fetched.Kind)
}

func TestJobGetAvailable(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert some available jobs
	for i := range 3 {
		_, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
			EncodedArgs: []byte(`{}`),
			Kind:        fmt.Sprintf("job_%d", i),
			MaxAttempts: 3,
			Priority:    1,
			Queue:       "default",
			Schema:      schema,
			State:       rivertype.JobStateAvailable,
			Tags:        []string{},
		})
		require.NoError(t, err)
	}

	// Get available jobs (needs transaction for FOR UPDATE)
	txExec, err := exec.Begin(ctx)
	require.NoError(t, err)
	defer txExec.Rollback(ctx)

	jobs, err := txExec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
		ClientID:       "test-client",
		MaxAttemptedBy: 4,
		MaxToLock:      2,
		Queue:          "default",
		Schema:         schema,
	})
	require.NoError(t, err)
	require.Len(t, jobs, 2)

	for _, job := range jobs {
		require.Equal(t, rivertype.JobStateRunning, job.State)
	}

	require.NoError(t, txExec.Commit(ctx))
}

func TestJobCancel(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert a job
	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "test_cancel",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateAvailable,
		Tags:        []string{},
	})
	require.NoError(t, err)

	// Cancel the job
	cancelled, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
		ID:                job.ID,
		CancelAttemptedAt: time.Now().UTC(),
		ControlTopic:      "test_topic",
		Schema:            schema,
	})
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateCancelled, cancelled.State)
	require.NotNil(t, cancelled.FinalizedAt)
}

func TestJobDelete(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert a completed job
	now := time.Now().UTC().Truncate(time.Microsecond)
	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		FinalizedAt: &now,
		Kind:        "test_delete",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateCompleted,
		Tags:        []string{},
	})
	require.NoError(t, err)

	// Delete the job
	deleted, err := exec.JobDelete(ctx, &riverdriver.JobDeleteParams{
		ID:     job.ID,
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, job.ID, deleted.ID)

	// Verify it's gone
	_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     job.ID,
		Schema: schema,
	})
	require.ErrorIs(t, err, rivertype.ErrNotFound)
}

func TestJobCountByState(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert jobs in different states
	for range 3 {
		_, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
			EncodedArgs: []byte(`{}`),
			Kind:        "test_count",
			MaxAttempts: 3,
			Priority:    1,
			Queue:       "default",
			Schema:      schema,
			State:       rivertype.JobStateAvailable,
			Tags:        []string{},
		})
		require.NoError(t, err)
	}

	count, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  rivertype.JobStateAvailable,
	})
	require.NoError(t, err)
	require.Equal(t, 3, count)
}

func TestLeaderElection(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Attempt to elect a leader
	leader, err := exec.LeaderAttemptElect(ctx, &riverdriver.LeaderElectParams{
		LeaderID: "test-leader",
		Schema:   schema,
		TTL:      30 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "test-leader", leader.LeaderID)

	// Get the elected leader
	fetched, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, "test-leader", fetched.LeaderID)

	// Re-elect should succeed
	reelected, err := exec.LeaderAttemptReelect(ctx, &riverdriver.LeaderReelectParams{
		ElectedAt: leader.ElectedAt,
		LeaderID:  "test-leader",
		Schema:    schema,
		TTL:       30 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, "test-leader", reelected.LeaderID)

	// Resign
	resigned, err := exec.LeaderResign(ctx, &riverdriver.LeaderResignParams{
		ElectedAt:       leader.ElectedAt,
		LeaderID:        "test-leader",
		LeadershipTopic: "leadership",
		Schema:          schema,
	})
	require.NoError(t, err)
	require.True(t, resigned)
}

func TestListenerWaitForNotificationDoesNotSkipLowerUncommittedID(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	type waitForNotificationResult struct {
		err          error
		found        bool
		notification *riverdriver.Notification
	}

	var (
		ctx      = t.Context()
		dbPool   = riversharedtest.DBPoolMySQL(ctx, t)
		driver   = New(dbPool)
		schema   = riverdbtest.TestSchema(ctx, t, driver, nil)
		listener = driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema}).(*Listener) //nolint:forcetypeassert
		table    = mysqlIdentifier(schema) + "." + mysqlIdentifier("river_notification")
	)
	insertNotificationSQL := "INSERT INTO " + table + " (payload, topic) VALUES (?, ?)" //nolint:gosec

	listener.pollInterval = time.Millisecond
	require.NoError(t, listener.Connect(ctx))
	t.Cleanup(func() { require.NoError(t, listener.Close(context.Background())) })
	require.NoError(t, listener.Listen(ctx, "topic"))

	txLower, err := dbPool.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txLower.Rollback() })

	_, err = txLower.ExecContext(ctx, insertNotificationSQL, "lower", "topic")
	require.NoError(t, err)

	insertCtx, cancel := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
	defer cancel()

	_, err = dbPool.ExecContext(insertCtx, insertNotificationSQL, "higher", "topic")
	require.NoError(t, err)

	started := testsignal.TestSignal[struct{}]{}
	started.Init(t)
	waitForNotification := testsignal.TestSignal[waitForNotificationResult]{}
	waitForNotification.Init(t)

	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		started.Signal(struct{}{})

		notification, found, err := listener.waitForNotificationOnce(waitCtx)
		waitForNotification.Signal(waitForNotificationResult{
			err:          err,
			found:        found,
			notification: notification,
		})
	}()

	started.WaitOrTimeout()

	select {
	case res := <-waitForNotification.WaitC():
		require.NoError(t, res.err)
		require.FailNow(t, "listener returned before lower transaction committed", "notification: %+v", res.notification)
	case <-time.After(250 * time.Millisecond):
	}

	require.NoError(t, txLower.Commit())

	res := waitForNotification.WaitOrTimeout()
	require.NoError(t, res.err)
	require.True(t, res.found)
	require.Equal(t, &riverdriver.Notification{Payload: "lower", Topic: "topic"}, res.notification)

	waitHigherCtx, cancel := context.WithTimeout(ctx, riversharedtest.WaitTimeout())
	defer cancel()

	notification, err := listener.WaitForNotification(waitHigherCtx)
	require.NoError(t, err)
	require.Equal(t, &riverdriver.Notification{Payload: "higher", Topic: "topic"}, notification)
}

func TestQueueOperations(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	now := time.Now().UTC().Truncate(time.Microsecond)

	// Create a queue
	queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
		Metadata: []byte(`{}`),
		Name:     "test_queue",
		Now:      &now,
		Schema:   schema,
	})
	require.NoError(t, err)
	require.Equal(t, "test_queue", queue.Name)
	require.Nil(t, queue.PausedAt)

	// Get the queue
	fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   "test_queue",
		Schema: schema,
	})
	require.NoError(t, err)
	require.Equal(t, "test_queue", fetched.Name)

	// List queues
	queues, err := exec.QueueList(ctx, &riverdriver.QueueListParams{
		Max:    100,
		Schema: schema,
	})
	require.NoError(t, err)
	require.Len(t, queues, 1)

	// Pause queue
	err = exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
		Name:   "test_queue",
		Schema: schema,
	})
	require.NoError(t, err)

	paused, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   "test_queue",
		Schema: schema,
	})
	require.NoError(t, err)
	require.NotNil(t, paused.PausedAt)

	// Resume queue
	err = exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
		Name:   "test_queue",
		Schema: schema,
	})
	require.NoError(t, err)

	resumed, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   "test_queue",
		Schema: schema,
	})
	require.NoError(t, err)
	require.Nil(t, resumed.PausedAt)
}

func TestTransactions(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Begin and commit
	tx, err := exec.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "tx_test",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateAvailable,
		Tags:        []string{},
	})
	require.NoError(t, err)

	require.NoError(t, tx.Commit(ctx))

	// Verify job exists
	count, err := exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  rivertype.JobStateAvailable,
	})
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Begin and rollback
	tx2, err := exec.Begin(ctx)
	require.NoError(t, err)

	_, err = tx2.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "tx_test_rollback",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateAvailable,
		Tags:        []string{},
	})
	require.NoError(t, err)

	require.NoError(t, tx2.Rollback(ctx))

	// Verify second job was rolled back
	count, err = exec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{
		Schema: schema,
		State:  rivertype.JobStateAvailable,
	})
	require.NoError(t, err)
	require.Equal(t, 1, count) // still 1
}

func TestJobInsertFastMany(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
		Jobs: []*riverdriver.JobInsertFastParams{
			{
				EncodedArgs: []byte(`{"i": 1}`),
				Kind:        "fast_job",
				MaxAttempts: 3,
				Priority:    1,
				Queue:       "default",
				State:       rivertype.JobStateAvailable,
				Tags:        []string{},
			},
			{
				EncodedArgs: []byte(`{"i": 2}`),
				Kind:        "fast_job",
				MaxAttempts: 3,
				Priority:    1,
				Queue:       "default",
				State:       rivertype.JobStateAvailable,
				Tags:        []string{},
			},
		},
		Schema: schema,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)

	for _, result := range results {
		require.NotNil(t, result.Job)
		require.False(t, result.UniqueSkippedAsDuplicate)
	}
}

func TestJobMetadata(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert a job with metadata
	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "metadata_test",
		MaxAttempts: 3,
		Metadata:    []byte(`{"key": "value"}`),
		Priority:    1,
		Queue:       "default",
		Schema:      schema,
		State:       rivertype.JobStateAvailable,
		Tags:        []string{},
	})
	require.NoError(t, err)

	var metadata map[string]any
	require.NoError(t, json.Unmarshal(job.Metadata, &metadata))
	require.Equal(t, "value", metadata["key"])

	// Update metadata
	updated, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
		ID:              job.ID,
		MetadataDoMerge: true,
		Metadata:        []byte(`{"new_key": "new_value"}`),
		Schema:          schema,
	})
	require.NoError(t, err)

	require.NoError(t, json.Unmarshal(updated.Metadata, &metadata))
	require.Equal(t, "value", metadata["key"])
	require.Equal(t, "new_value", metadata["new_key"])
}

func TestJobSchedule(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		exec   = driver.GetExecutor()
	)

	// Insert a scheduled job with a past scheduled_at
	past := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Microsecond)
	_, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
		EncodedArgs: []byte(`{}`),
		Kind:        "scheduled_job",
		MaxAttempts: 3,
		Priority:    1,
		Queue:       "default",
		ScheduledAt: &past,
		Schema:      schema,
		State:       rivertype.JobStateScheduled,
		Tags:        []string{},
	})
	require.NoError(t, err)

	// Schedule should find and transition the job
	now := time.Now().UTC()
	results, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
		Max:    100,
		Now:    &now,
		Schema: schema,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, rivertype.JobStateAvailable, results[0].Job.State)
}

func TestNotifyMany(t *testing.T) {
	t.Parallel()

	driver := New(nil)
	require.NotNil(t, driver.GetListener(&riverdriver.GetListenenerParams{}))
}

func TestNotifyManyInsertsNotifications(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx    = t.Context()
		driver = New(riversharedtest.DBPoolMySQL(ctx, t))
		exec   = driver.GetExecutor()
		schema = riverdbtest.TestSchema(ctx, t, driver, nil)
	)

	err := exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{"test1", "test2"},
		Schema:  schema,
		Topic:   "test_topic",
	})
	require.NoError(t, err)

	var count int
	require.NoError(t, exec.QueryRow(ctx, "SELECT count(*) FROM "+mysqlIdentifier(schema)+".river_notification WHERE topic = ? AND payload IN (?, ?)", "test_topic", "test1", "test2").Scan(&count))
	require.Equal(t, 2, count)
}

func TestPGAdvisoryXactLock(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	var (
		ctx  = t.Context()
		exec = New(riversharedtest.DBPoolMySQL(ctx, t)).GetExecutor()
	)

	_, err := exec.PGAdvisoryXactLock(ctx, 12345)
	require.ErrorIs(t, err, riverdriver.ErrNotImplemented)
}
