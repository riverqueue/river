package riverpgxv5_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func BenchmarkJobCountByQueueAndState(b *testing.B) {
	ctx := context.Background()

	dbPool := riversharedtest.DBPool(ctx, b)
	driver := riverpgxv5.New(dbPool)
	exec := driver.GetExecutor()
	schema := riverdbtest.TestSchema(ctx, b, driver, nil)
	numJobs := queueStateCountBenchmarkNumJobs(b)

	seedQueueStateCountBenchmarkData(ctx, b, exec, schema, numJobs)

	queueNamesTwo := queueStateCountBenchmarkQueueNames(2)
	queueNamesTen := queueStateCountBenchmarkQueueNames(10)

	for _, benchmarkCase := range []struct {
		name       string
		queueNames []string
	}{
		{name: "TwoQueues", queueNames: queueNamesTwo},
		{name: "TenQueues", queueNames: queueNamesTen},
	} {
		b.Run(benchmarkCase.name, func(b *testing.B) {
			b.ReportAllocs()

			params := &riverdriver.JobCountByQueueAndStateParams{
				QueueNames: benchmarkCase.queueNames,
				Schema:     schema,
			}

			b.ResetTimer()
			for range b.N {
				results, err := driver.GetExecutor().JobCountByQueueAndState(ctx, params)
				require.NoError(b, err)
				require.NotEmpty(b, results)
			}
		})
	}
}

func queueStateCountBenchmarkNumJobs(b *testing.B) int {
	b.Helper()

	numJobs := 20_000
	if numJobsEnv := os.Getenv("RIVER_BENCH_QUEUE_STATE_COUNT_NUM_JOBS"); numJobsEnv != "" {
		parsedNumJobs, err := strconv.Atoi(numJobsEnv)
		require.NoError(b, err)
		require.Greater(b, parsedNumJobs, 0)

		numJobs = parsedNumJobs
	}

	return numJobs
}

func queueStateCountBenchmarkQueueNames(numQueues int) []string {
	queueNames := make([]string, numQueues)
	for i := range numQueues {
		queueNames[i] = fmt.Sprintf("queue_%03d", i+1)
	}

	return queueNames
}

func queueStateCountBenchmarkQueue(jobNum int) string {
	// Rotate queue more slowly than state so every queue gets every state.
	return fmt.Sprintf("queue_%03d", ((jobNum/8)%100)+1)
}

func queueStateCountBenchmarkState(jobNum int) rivertype.JobState {
	switch jobNum % 8 {
	case 0:
		return rivertype.JobStateRunning
	case 1:
		return rivertype.JobStateAvailable
	case 2:
		return rivertype.JobStateCompleted
	case 3:
		return rivertype.JobStateCancelled
	case 4:
		return rivertype.JobStateDiscarded
	case 5:
		return rivertype.JobStateRetryable
	case 6:
		return rivertype.JobStateScheduled
	default:
		return rivertype.JobStatePending
	}
}

func seedQueueStateCountBenchmarkData(ctx context.Context, b *testing.B, exec riverdriver.Executor, schema string, numJobs int) {
	b.Helper()

	const insertBatchSize = 5000

	now := time.Now().UTC()

	for start := 0; start < numJobs; start += insertBatchSize {
		end := min(start+insertBatchSize, numJobs)
		insertParams := make([]*riverdriver.JobInsertFullParams, 0, end-start)

		for jobNum := start; jobNum < end; jobNum++ {
			finalizedAt, scheduledAt, state := queueStateCountBenchmarkTimestamps(now, jobNum)

			insertParams = append(insertParams, &riverdriver.JobInsertFullParams{
				EncodedArgs: []byte(`{}`),
				FinalizedAt: finalizedAt,
				Kind:        "benchmark",
				MaxAttempts: 25,
				Metadata:    []byte(`{}`),
				Priority:    (jobNum % 4) + 1,
				Queue:       queueStateCountBenchmarkQueue(jobNum),
				ScheduledAt: &scheduledAt,
				State:       state,
			})
		}

		_, err := exec.JobInsertFullMany(ctx, &riverdriver.JobInsertFullManyParams{
			Jobs:   insertParams,
			Schema: schema,
		})
		require.NoError(b, err)
	}

	countsByState, err := exec.JobCountByAllStates(ctx, &riverdriver.JobCountByAllStatesParams{Schema: schema})
	require.NoError(b, err)

	var numRows int
	for _, numJobsForState := range countsByState {
		numRows += numJobsForState
	}
	require.Equal(b, numJobs, numRows)
}

func queueStateCountBenchmarkTimestamps(now time.Time, jobNum int) (*time.Time, time.Time, rivertype.JobState) {
	scheduledAt := now.Add(-time.Duration(jobNum%100000) * time.Second)
	state := queueStateCountBenchmarkState(jobNum)

	if state != rivertype.JobStateCancelled && state != rivertype.JobStateCompleted && state != rivertype.JobStateDiscarded {
		return nil, scheduledAt, state
	}

	finalizedAt := scheduledAt.Add(time.Second)
	return &finalizedAt, scheduledAt, state
}
