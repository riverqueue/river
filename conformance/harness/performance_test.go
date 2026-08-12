//go:build riverconformance

package harness_test

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type benchmarkMetrics struct {
	p95        time.Duration
	throughput float64
}

func TestPerformanceGate(t *testing.T) {
	// This opt-in release gate owns the shared conformance database for the
	// duration of all three same-host comparison runs.
	if os.Getenv("RIVER_CONFORMANCE_PERFORMANCE") != "1" {
		t.Skip("RIVER_CONFORMANCE_PERFORMANCE=1 is required")
	}
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	require.NotEmpty(t, databaseURL)
	jobs := 200
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_JOBS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		jobs = parsed
	}
	require.GreaterOrEqual(t, jobs, 20)

	root := repoRoot(t)
	goAdapter := startAdapter(t, root, databaseURL, "go-performance", "go", "run", "./internal/cmd/riverconformanceadapter")
	rustAdapter := startAdapter(t, root, databaseURL, "rust-performance", "cargo", "run", "--release", "--quiet", "--manifest-path", "rust/Cargo.toml", "-p", "riverqueue-conformance")
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	for _, mode := range []string{"enqueue", "worker", "mixed"} {
		_ = runAdapterBenchmark(t, goAdapter, mode, max(20, jobs/10))
		_ = runAdapterBenchmark(t, rustAdapter, mode, max(20, jobs/10))

		goRuns, rustRuns := make([]benchmarkMetrics, 0, 3), make([]benchmarkMetrics, 0, 3)
		for range 3 {
			goRuns = append(goRuns, runAdapterBenchmark(t, goAdapter, mode, jobs))
			rustRuns = append(rustRuns, runAdapterBenchmark(t, rustAdapter, mode, jobs))
		}
		goMetrics, rustMetrics := medianMetrics(goRuns), medianMetrics(rustRuns)
		t.Logf("%s: Go %.1f jobs/s p95=%s; Rust %.1f jobs/s p95=%s", mode, goMetrics.throughput, goMetrics.p95, rustMetrics.throughput, rustMetrics.p95)
		require.GreaterOrEqual(t, rustMetrics.throughput, goMetrics.throughput*0.80,
			"Rust must sustain at least 80%% of Go throughput in %s", mode)
		require.LessOrEqual(t, rustMetrics.p95, goMetrics.p95*5/4,
			"Rust p95 must be at most 1.25x Go in %s", mode)
	}
}

func TestMixedSoak(t *testing.T) {
	// This opt-in soak owns the shared conformance database. CI sets 10m,
	// release candidates use 1h, and the scheduled job uses 6h.
	durationString := os.Getenv("RIVER_CONFORMANCE_SOAK_DURATION")
	if durationString == "" {
		t.Skip("RIVER_CONFORMANCE_SOAK_DURATION is required")
	}
	duration, err := time.ParseDuration(durationString)
	require.NoError(t, err)
	require.Positive(t, duration)
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	require.NotEmpty(t, databaseURL)

	root := repoRoot(t)
	goAdapter := startAdapter(t, root, databaseURL, "go-soak", "go", "run", "./internal/cmd/riverconformanceadapter")
	rustAdapter := startAdapter(t, root, databaseURL, "rust-soak", "cargo", "run", "--quiet", "--manifest-path", "rust/Cargo.toml", "-p", "riverqueue-conformance")
	goAdapter.call(t, "migrate", map[string]any{}, nil)
	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "start", map[string]any{"client_id": "go-soak", "max_workers": 8}, nil)
	rustAdapter.call(t, "start", map[string]any{"client_id": "rust-soak", "max_workers": 8}, nil)

	deadline := time.Now().Add(duration)
	jobsCompleted := 0
	for time.Now().Before(deadline) {
		ids := make([]int64, 0, 20)
		for index := range 20 {
			inserter := goAdapter
			if index%2 == 1 {
				inserter = rustAdapter
			}
			var job normalizedJob
			inserter.call(t, "insert", map[string]any{"message": fmt.Sprintf("soak-%d", jobsCompleted+index)}, &job)
			ids = append(ids, job.ID)
		}
		for _, id := range ids {
			var job normalizedJob
			rustAdapter.call(t, "wait", map[string]any{"id": id}, &job)
			require.Equal(t, "completed", job.State)
			require.Equal(t, 1, job.Attempt)
			require.Len(t, job.AttemptedBy, 1)
		}
		jobsCompleted += len(ids)
		for _, adapter := range []*adapter{goAdapter, rustAdapter} {
			var connections struct {
				Count int `json:"count"`
			}
			adapter.call(t, "connection_count", map[string]any{}, &connections)
			require.LessOrEqual(t, connections.Count, 20, "%s database connections grew without bound", adapter.name)
		}
	}
	goAdapter.call(t, "stop", map[string]any{}, nil)
	rustAdapter.call(t, "stop", map[string]any{}, nil)
	t.Logf("completed %d mixed jobs over %s", jobsCompleted, duration)
}

func medianMetrics(runs []benchmarkMetrics) benchmarkMetrics {
	throughputs := make([]float64, len(runs))
	p95s := make([]time.Duration, len(runs))
	for index, run := range runs {
		throughputs[index] = run.throughput
		p95s[index] = run.p95
	}
	sort.Float64s(throughputs)
	sort.Slice(p95s, func(left, right int) bool { return p95s[left] < p95s[right] })
	return benchmarkMetrics{p95: p95s[len(p95s)/2], throughput: throughputs[len(throughputs)/2]}
}

func runAdapterBenchmark(t *testing.T, adapter *adapter, mode string, jobs int) benchmarkMetrics {
	t.Helper()

	// A small deterministic work interval keeps worker and mixed p95 focused on
	// the full execution pipeline without making a sub-millisecond no-op
	// baseline (and host scheduler jitter) determine the release result.
	const workDuration = 10 * time.Millisecond

	adapter.call(t, "reset", map[string]any{}, nil)
	if mode == "enqueue" {
		var result struct {
			DurationNS int64 `json:"duration_ns"`
			P95NS      int64 `json:"p95_ns"`
		}
		adapter.call(t, "benchmark_enqueue", map[string]any{"jobs": jobs}, &result)
		duration := time.Duration(result.DurationNS)
		return benchmarkMetrics{
			p95:        time.Duration(result.P95NS),
			throughput: float64(jobs) / duration.Seconds(),
		}
	}
	ids := make([]int64, 0, jobs)
	latencies := make([]time.Duration, 0, jobs)
	if mode == "worker" {
		for index := range jobs {
			var job normalizedJob
			adapter.call(t, "insert", map[string]any{
				"behavior":    "sleep",
				"duration_ms": workDuration.Milliseconds(),
				"message":     fmt.Sprintf("worker-%d", index),
			}, &job)
			ids = append(ids, job.ID)
		}
	}
	maxWorkers := 32
	if mode == "mixed" {
		// Keep the producer/worker overlap from turning p95 into a queue-depth
		// comparison; throughput still includes all concurrent insertion and
		// execution work.
		maxWorkers = 128
	}
	adapter.call(t, "start", map[string]any{
		"client_id": adapter.name + "-benchmark", "max_workers": maxWorkers,
	}, nil)
	startedAt := time.Now()
	if mode == "mixed" {
		for index := range jobs {
			var job normalizedJob
			adapter.call(t, "insert", map[string]any{
				"behavior":    "sleep",
				"duration_ms": workDuration.Milliseconds(),
				"message":     fmt.Sprintf("%s-%d", mode, index),
			}, &job)
			ids = append(ids, job.ID)
		}
	}
	for _, id := range ids {
		var job normalizedJob
		adapter.call(t, "wait", map[string]any{"id": id}, &job)
		startField := job.CreatedAt
		if mode == "worker" {
			require.NotNil(t, job.AttemptedAt)
			startField = *job.AttemptedAt
		}
		require.NotNil(t, job.FinalizedAt)
		startTime, err := time.Parse(time.RFC3339Nano, startField)
		require.NoError(t, err)
		finalizedAt, err := time.Parse(time.RFC3339Nano, *job.FinalizedAt)
		require.NoError(t, err)
		latencies = append(latencies, finalizedAt.Sub(startTime))
	}
	adapter.call(t, "stop", map[string]any{}, nil)
	elapsed := time.Since(startedAt)
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	p95Index := max(0, int(math.Ceil(float64(len(latencies))*0.95))-1)
	return benchmarkMetrics{
		p95:        latencies[p95Index],
		throughput: float64(jobs) / elapsed.Seconds(),
	}
}
