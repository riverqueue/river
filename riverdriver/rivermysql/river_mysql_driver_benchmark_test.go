package rivermysql

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// BenchmarkJSONTableBatch includes a transaction and rollback in each iteration,
// so updates always act on running jobs and inserts don't grow the tables.
func BenchmarkJSONTableBatch(b *testing.B) {
	riversharedtest.SkipIfMySQLNotEnabled(b)

	ctx := b.Context()
	driver := New(riversharedtest.DBPoolMySQL(ctx, b))
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)
	exec := driver.GetExecutor()
	now := time.Now().UTC().Truncate(time.Microsecond)
	attemptedAt := now.Add(-time.Hour)

	for _, operation := range []string{"Insert", "Notify", "Rescue", "SetState"} {
		for _, size := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/%d", operation, size), func(b *testing.B) {
				insertParams := &riverdriver.JobInsertFastManyParams{Schema: schema}
				notifyParams := &riverdriver.NotifyManyParams{Schema: schema, Topic: "river_insert"}
				rescueParams := &riverdriver.JobRescueManyParams{Schema: schema, StuckHorizon: now}
				stateParams := &riverdriver.JobSetStateIfRunningManyParams{Schema: schema}
				for range size {
					insertParams.Jobs = append(insertParams.Jobs, &riverdriver.JobInsertFastParams{
						EncodedArgs: []byte(`{"message":"benchmark","account_id":123456789}`),
						Kind:        "benchmark",
						MaxAttempts: 25,
						Priority:    1,
						Queue:       "default",
						State:       rivertype.JobStateAvailable,
						Tags:        []string{"benchmark"},
					})
					notifyParams.Payload = append(notifyParams.Payload, `{"queue":"default"}`)
					if operation != "Rescue" && operation != "SetState" {
						continue
					}
					jobs, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
						Jobs: []*riverdriver.JobInsertFullParams{{
							Attempt: 1, AttemptedAt: &attemptedAt, EncodedArgs: []byte(`{}`),
							Kind: "benchmark", MaxAttempts: 25, Priority: 1, Queue: "default",
							State: rivertype.JobStateRunning, Tags: []string{},
						}},
						Schema: schema,
					})
					require.NoError(b, err)
					id := jobs[0].ID
					rescueParams.ID = append(rescueParams.ID, id)
					rescueParams.Error = append(rescueParams.Error, []byte(`{"attempt":1,"error":"stuck job"}`))
					rescueParams.FinalizedAt = append(rescueParams.FinalizedAt, nil)
					rescueParams.ScheduledAt = append(rescueParams.ScheduledAt, now)
					rescueParams.State = append(rescueParams.State, string(rivertype.JobStateAvailable))
					stateParams.ID = append(stateParams.ID, id)
					stateParams.Attempt = append(stateParams.Attempt, nil)
					stateParams.ErrData = append(stateParams.ErrData, nil)
					stateParams.FinalizedAt = append(stateParams.FinalizedAt, &now)
					stateParams.MetadataDoMerge = append(stateParams.MetadataDoMerge, true)
					stateParams.MetadataUpdates = append(stateParams.MetadataUpdates, []byte(`{"result":"ok"}`))
					stateParams.ScheduledAt = append(stateParams.ScheduledAt, nil)
					stateParams.State = append(stateParams.State, rivertype.JobStateCompleted)
				}

				b.ReportAllocs()
				for b.Loop() {
					tx, err := exec.Begin(ctx)
					require.NoError(b, err)
					switch operation {
					case "Insert":
						count, err := tx.JobInsertFastManyNoReturning(ctx, insertParams)
						require.NoError(b, err)
						require.Equal(b, size, count)
					case "Notify":
						require.NoError(b, tx.NotifyMany(ctx, notifyParams))
					case "Rescue":
						_, err := tx.JobRescueMany(ctx, rescueParams)
						require.NoError(b, err)
					case "SetState":
						jobs, err := tx.JobSetStateIfRunningMany(ctx, stateParams)
						require.NoError(b, err)
						require.Len(b, jobs, size)
					}
					require.NoError(b, tx.Rollback(ctx))
				}
			})
		}
	}
}

func BenchmarkJobGetAvailableKnownKinds(b *testing.B) {
	riversharedtest.SkipIfMySQLNotEnabled(b)

	ctx := b.Context()
	driver := New(riversharedtest.DBPoolMySQL(ctx, b))
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)
	exec := driver.GetExecutor()
	kinds := make([]string, 100)
	for i := range kinds {
		kinds[i] = fmt.Sprintf("kind_%03d", i)
	}
	jobs := make([]*riverdriver.JobInsertFastParams, 10_000)
	for i := range jobs {
		jobs[i] = &riverdriver.JobInsertFastParams{
			EncodedArgs: []byte(`{}`),
			Kind:        kinds[i%len(kinds)],
			MaxAttempts: 1,
			Priority:    1,
			Queue:       "default",
			State:       rivertype.JobStateAvailable,
			Tags:        []string{},
		}
	}
	_, err := exec.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{Jobs: jobs, Schema: schema})
	require.NoError(b, err)

	for _, testCase := range []struct {
		kinds []string
		name  string
	}{
		{kinds: kinds, name: "AllKnown100Kinds"},
		{kinds: nil, name: "Disabled"},
		{kinds: kinds[:1], name: "OnePercentKnown"},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tx, err := exec.Begin(ctx)
				require.NoError(b, err)
				result, err := tx.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
					ClientID:       "benchmark",
					Kind:           testCase.kinds,
					MaxAttemptedBy: 4,
					MaxToLock:      100,
					Queue:          "default",
					Schema:         schema,
				})
				require.NoError(b, err)
				require.Len(b, result.Jobs, 100)
				require.NoError(b, tx.Rollback(ctx))
			}
		})
	}
}

// BenchmarkJobInsertManyReturning measures the path used by standard InsertMany,
// including fetching returned jobs and classifying uniqueness conflicts.
func BenchmarkJobInsertManyReturning(b *testing.B) {
	riversharedtest.SkipIfMySQLNotEnabled(b)

	ctx := b.Context()
	driver := New(riversharedtest.DBPoolMySQL(ctx, b))
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)
	exec := driver.GetExecutor()
	for _, mode := range []string{"MixedDuplicates", "Plain", "Unique"} {
		for _, size := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("%s/%d", mode, size), func(b *testing.B) {
				params := &riverdriver.JobInsertFastManyParams{Schema: schema}
				for i := range size {
					job := &riverdriver.JobInsertFastParams{
						EncodedArgs: []byte(fmt.Sprintf(`{"message":"benchmark","index":%d}`, i)),
						Kind:        "benchmark",
						MaxAttempts: 25,
						Priority:    1,
						Queue:       "default",
						State:       rivertype.JobStateAvailable,
						Tags:        []string{"benchmark"},
					}
					if mode != "Plain" {
						job.UniqueKey = fmt.Appendf(nil, "%s/%d/%d", mode, size, i)
						job.UniqueStates = 0xff
					}
					params.Jobs = append(params.Jobs, job)
					if mode == "MixedDuplicates" && i%2 == 0 {
						_, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{job}, Schema: schema})
						require.NoError(b, err)
					}
				}

				b.ReportAllocs()
				for b.Loop() {
					tx, err := exec.Begin(ctx)
					require.NoError(b, err)
					jobs, err := tx.JobInsertFastMany(ctx, params)
					require.NoError(b, err)
					require.Len(b, jobs, size)
					require.NoError(b, tx.Rollback(ctx))
				}
			})
		}
	}
}

func BenchmarkListenerNotificationDrain(b *testing.B) {
	riversharedtest.SkipIfMySQLNotEnabled(b)

	ctx := b.Context()
	pool := riversharedtest.DBPoolMySQL(ctx, b)
	driver := New(pool)
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)
	payloads := make([]string, 256)
	for i := range payloads {
		payloads[i] = strconv.Itoa(i)
	}
	require.NoError(b, driver.GetExecutor().NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: payloads,
		Schema:  schema,
		Topic:   "benchmark",
	}))

	b.ReportAllocs()
	for b.Loop() {
		listener := &Listener{
			dbPool:      pool,
			isConnected: true,
			replacer:    &driver.replacer,
			schema:      schema,
			topics:      map[string]int64{"benchmark": 0},
		}
		for _, payload := range payloads {
			notification, err := listener.WaitForNotification(ctx)
			require.NoError(b, err)
			require.Equal(b, payload, notification.Payload)
		}
	}
}
