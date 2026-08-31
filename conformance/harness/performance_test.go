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

type benchmarkGate struct {
	p95Denominator  int64
	p95Numerator    int64
	throughputRatio float64
}

func TestPerformanceGate(t *testing.T) {
	// This opt-in release gate owns the shared conformance database for the
	// duration of all three same-host comparison runs.
	if os.Getenv("RIVER_CONFORMANCE_PERFORMANCE") != "1" {
		t.Skip("RIVER_CONFORMANCE_PERFORMANCE=1 is required")
	}
	databaseURL := os.Getenv("RIVER_CONFORMANCE_DATABASE_URL")
	require.NotEmpty(t, databaseURL)
	scenarios := newScenarioTracker(t, scenarioOwnerPerformance)
	jobs := 200
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_JOBS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		jobs = parsed
	}
	require.GreaterOrEqual(t, jobs, 20)

	root := repoRoot(t)
	goAdapter := startAdapter(t, root, databaseURL, "go-performance", "go", "run", "./internal/cmd/riverconformanceadapter")
	candidateSpec := conformanceCandidateSpec(t, root, true)
	candidateAdapter := startAdapterCommand(t, root, databaseURL, candidateSpec.Implementation+"-performance", candidateSpec.Command)
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	for _, mode := range []string{"enqueue", "worker", "mixed"} {
		_ = runAdapterBenchmark(t, goAdapter, mode, max(20, jobs/10))
		_ = runAdapterBenchmark(t, candidateAdapter, mode, max(20, jobs/10))

		goRuns, candidateRuns := make([]benchmarkMetrics, 0, 3), make([]benchmarkMetrics, 0, 3)
		for range 3 {
			goRuns = append(goRuns, runAdapterBenchmark(t, goAdapter, mode, jobs))
			candidateRuns = append(candidateRuns, runAdapterBenchmark(t, candidateAdapter, mode, jobs))
		}
		goMetrics, candidateMetrics := medianMetrics(goRuns), medianMetrics(candidateRuns)
		gate := benchmarkGateForMode(mode, candidateSpec.Implementation)
		t.Logf("%s: Go %.1f jobs/s p95=%s; %s %.1f jobs/s p95=%s",
			mode, goMetrics.throughput, goMetrics.p95,
			candidateSpec.Implementation, candidateMetrics.throughput, candidateMetrics.p95)
		require.GreaterOrEqual(t, candidateMetrics.throughput, goMetrics.throughput*gate.throughputRatio,
			"%s must sustain at least %.0f%% of Go throughput in %s", candidateSpec.Implementation, gate.throughputRatio*100, mode)
		require.LessOrEqual(t, candidateMetrics.p95, goMetrics.p95*time.Duration(gate.p95Numerator)/time.Duration(gate.p95Denominator),
			"%s p95 exceeds the %.2fx Go bound in %s", candidateSpec.Implementation,
			float64(gate.p95Numerator)/float64(gate.p95Denominator), mode)
		scenarios.pass("release_" + mode + "_performance")
	}
}

func benchmarkGateForMode(mode, implementation string) benchmarkGate {
	if implementation == "javascript" {
		// JavaScript's alpha gate catches accidentally serialized work and
		// gross regressions without requiring event-loop scheduling and
		// database-driver latency to match the native implementations.
		if mode == "enqueue" {
			return benchmarkGate{p95Denominator: 1, p95Numerator: 3, throughputRatio: 0.25}
		}
		return benchmarkGate{p95Denominator: 1, p95Numerator: 2, throughputRatio: 0.50}
	}
	if mode == "enqueue" {
		// Enqueue uses equivalent ordinary insertion mechanisms but remains
		// driver/runtime-language sensitive. It is a regression guard, not an
		// incentive to add a candidate-only fast producer path.
		return benchmarkGate{p95Denominator: 1, p95Numerator: 2, throughputRatio: 0.40}
	}
	return benchmarkGate{p95Denominator: 4, p95Numerator: 5, throughputRatio: 0.80}
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
	scenarios := newScenarioTracker(t, scenarioOwnerSoak)

	root := repoRoot(t)
	goAdapter := startAdapter(t, root, databaseURL, "go-soak", "go", "run", "./internal/cmd/riverconformanceadapter")
	candidateSpec := conformanceCandidateSpec(t, root, false)
	candidateAdapter := startAdapterCommand(t, root, databaseURL, candidateSpec.Implementation+"-soak", candidateSpec.Command)
	goAdapter.call(t, "migrate", map[string]any{}, nil)
	goAdapter.call(t, "reset", map[string]any{}, nil)
	goAdapter.call(t, "start", map[string]any{"client_id": "go-soak", "max_workers": 8}, nil)
	candidateAdapter.call(t, "start", map[string]any{
		"client_id": candidateSpec.Implementation + "-soak", "max_workers": 8,
	}, nil)

	deadline := time.Now().Add(duration)
	jobsCompleted := 0
	for time.Now().Before(deadline) {
		ids := make([]int64, 0, 20)
		for index := range 20 {
			inserter := goAdapter
			if index%2 == 1 {
				inserter = candidateAdapter
			}
			var job normalizedJob
			inserter.call(t, "insert", map[string]any{"message": fmt.Sprintf("soak-%d", jobsCompleted+index)}, &job)
			ids = append(ids, job.ID)
		}
		for _, id := range ids {
			var job normalizedJob
			candidateAdapter.call(t, "wait", map[string]any{"id": id}, &job)
			require.Equal(t, "completed", job.State)
			require.Equal(t, 1, job.Attempt)
			require.Len(t, job.AttemptedBy, 1)
		}
		jobsCompleted += len(ids)
		for _, adapter := range []*adapter{goAdapter, candidateAdapter} {
			var connections struct {
				Count int `json:"count"`
			}
			adapter.call(t, "connection_count", map[string]any{}, &connections)
			require.LessOrEqual(t, connections.Count, 20, "%s database connections grew without bound", adapter.name)
		}
	}
	goAdapter.call(t, "stop", map[string]any{}, nil)
	candidateAdapter.call(t, "stop", map[string]any{}, nil)
	t.Logf("completed %d mixed jobs over %s", jobsCompleted, duration)
	scenarios.pass("mixed_connection_pool_bound", "mixed_soak")
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
