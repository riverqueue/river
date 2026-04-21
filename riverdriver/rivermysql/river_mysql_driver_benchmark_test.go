package rivermysql

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

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
