package river_test

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

// Example_hookMetricEmit demonstrates consuming River metrics with a
// rivertype.HookMetricEmit plugin.
func Example_hookMetricEmit() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	metricChan := make(chan rivertype.Metric, 100)

	workers := river.NewWorkers()
	river.AddWorker(workers, river.WorkFunc(func(ctx context.Context, job *river.Job[NoOpArgs]) error {
		return nil
	}))

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), initTestConfig(ctx, dbPool, &river.Config{
		Plugins: []rivertype.Plugin{
			river.HookMetricEmitFunc(func(ctx context.Context, params *rivertype.HookMetricEmitParams) {
				// Avoid blocking River's job-fetch loop. A production hook might
				// hand the metric to an asynchronous OpenTelemetry or Datadog
				// exporter instead.
				select {
				case metricChan <- params.Metric:
				default:
				}
			}),
		},
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	}))
	if err != nil {
		panic(err)
	}

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	_, err = riverClient.Insert(ctx, NoOpArgs{}, nil)
	if err != nil {
		panic(err)
	}

	var (
		countMetric    *rivertype.JobGetAvailableCountMetric
		durationMetric *rivertype.JobGetAvailableDurationMetric
	)
	for countMetric == nil || durationMetric == nil {
		metric := riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), metricChan, 1)[0]
		switch metric := metric.(type) {
		case *rivertype.JobGetAvailableCountMetric:
			if metric.Count > 0 {
				countMetric = metric
			}
		case *rivertype.JobGetAvailableDurationMetric:
			durationMetric = metric
		}
	}

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	fmt.Printf("%s emitted for queue %q\n", durationMetric.Name(), durationMetric.Queue)
	fmt.Printf("%s emitted with count %d for queue %q\n", countMetric.Name(), countMetric.Count, countMetric.Queue)

	// Output:
	// job_get_available_duration emitted for queue "default"
	// job_get_available_count emitted with count 1 for queue "default"
}
