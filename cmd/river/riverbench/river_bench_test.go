package riverbench

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestBenchmarkerInsertJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		benchmarker *Benchmarker[pgx.Tx]
		client      *river.Client[pgx.Tx]
	}

	setup := func(ctx context.Context, t *testing.T) *testBundle {
		t.Helper()

		var (
			driver = riverpgxv5.New(riversharedtest.DBPool(ctx, t))
			logger = riversharedtest.Logger(t)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		// Insert-only client since jobs should stay in the database so that
		// their args can be inspected.
		client, err := river.NewClient(driver, &river.Config{
			Logger: logger,
			Schema: schema,
		})
		require.NoError(t, err)

		return &testBundle{
			benchmarker: NewBenchmarker(driver, &Config{Logger: logger, Schema: schema}),
			client:      client,
		}
	}

	t.Run("NumbersJobsSequentiallyAcrossBatches", func(t *testing.T) {
		t.Parallel()

		bundle := setup(ctx, t)

		// Spans more than one batch to check that numbering continues from
		// one batch into the next.
		const numTotalJobs = insertBatchSize + 2

		var (
			minJobsReady    = make(chan struct{})
			numJobsInserted atomic.Int64
			numJobsLeft     atomic.Int64
		)

		bundle.benchmarker.insertJobs(ctx, bundle.client, minJobsReady, &numJobsInserted, &numJobsLeft, numTotalJobs, make(chan struct{}))
		require.Equal(t, int64(numTotalJobs), numJobsInserted.Load())

		listRes, err := bundle.client.JobList(ctx, river.NewJobListParams().Kinds((BenchmarkArgs{}).Kind()).First(numTotalJobs))
		require.NoError(t, err)
		require.Len(t, listRes.Jobs, numTotalJobs)

		jobNums := make([]int, len(listRes.Jobs))
		for i, job := range listRes.Jobs {
			var args BenchmarkArgs
			require.NoError(t, json.Unmarshal(job.EncodedArgs, &args))
			jobNums[i] = args.Num
		}
		slices.Sort(jobNums)

		expectedJobNums := make([]int, numTotalJobs)
		for i := range expectedJobNums {
			expectedJobNums[i] = i + 1
		}
		require.Equal(t, expectedJobNums, jobNums)
	})
}
