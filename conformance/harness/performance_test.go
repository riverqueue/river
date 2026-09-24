//go:build riverconformance

package harness_test

import (
	"fmt"
	"math"
	"os"
	"slices"
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

// TestPerformanceGate compares release builds of the reference and the
// candidate on the same host. Each attempt takes the median of three runs
// per implementation, and a mode passes when any of up to three attempts
// meets the candidate's declared bounds, so one noisy sample on a shared
// runner cannot fail the gate while a sustained regression still does.
func TestPerformanceGate(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	// This opt-in release gate owns the shared conformance database for the
	// duration of all same-host comparison runs.
	requireOptIn(t, "RIVER_CONFORMANCE_PERFORMANCE")
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerPerformance)
	jobs := performanceJobs(t)

	root := repoRoot(t)
	goAdapter := startReferenceAdapter(t, root, databaseURL, "go-performance")
	candidateSpec := conformanceCandidateSpec(t, root, true)
	candidateAdapter := startCandidateAdapter(t, root, databaseURL, candidateSpec.Implementation+"-performance", candidateSpec, candidateSpec.Command)
	goAdapter.call(t, "migrate", map[string]any{}, nil)

	for _, mode := range []string{"enqueue", "worker", "mixed"} {
		_ = runAdapterBenchmark(t, goAdapter, mode, max(20, jobs/10))
		_ = runAdapterBenchmark(t, candidateAdapter, mode, max(20, jobs/10))
		gateModeWithRetries(t, mode, func() []benchmarkMetrics {
			return []benchmarkMetrics{
				medianBenchmark(t, goAdapter, mode, jobs),
				medianBenchmark(t, candidateAdapter, mode, jobs),
			}
		}, func(metrics []benchmarkMetrics) []string {
			return benchmarkViolations(mode, candidateSpec, metrics[1], metrics[0])
		}, func(metrics []benchmarkMetrics) {
			t.Logf("%s: Go %.1f jobs/s p95=%s; %s %.1f jobs/s p95=%s",
				mode, metrics[0].throughput, metrics[0].p95,
				candidateSpec.Implementation, metrics[1].throughput, metrics[1].p95)
		})
		scenarios.pass("release_" + mode + "_performance")
	}
}

// gateModeWithRetries measures a benchmark mode up to
// RIVER_CONFORMANCE_PERFORMANCE_ATTEMPTS times (default three) and fails the
// test only if every attempt violates a bound.
func gateModeWithRetries(
	t *testing.T,
	mode string,
	measure func() []benchmarkMetrics,
	violationsFunc func([]benchmarkMetrics) []string,
	logFunc func([]benchmarkMetrics),
) {
	t.Helper()

	attempts := 3
	if value := os.Getenv("RIVER_CONFORMANCE_PERFORMANCE_ATTEMPTS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, parsed)
		attempts = parsed
	}
	var violations []string
	for attempt := 1; attempt <= attempts; attempt++ {
		metrics := measure()
		logFunc(metrics)
		violations = violationsFunc(metrics)
		if len(violations) == 0 {
			return
		}
		t.Logf("%s attempt %d/%d outside bounds: %v", mode, attempt, attempts, violations)
	}
	require.Empty(t, violations, "%s stayed outside its performance bounds in %d attempts", mode, attempts)
}

// benchmarkViolations compares a candidate's metrics with a reference using
// the bounds its descriptor declares.
func benchmarkViolations(mode string, spec adapterSpec, candidate, reference benchmarkMetrics) []string {
	bound := spec.performanceBound(mode)
	var violations []string
	if minimum := reference.throughput * bound.MinThroughputRatio; candidate.throughput < minimum {
		violations = append(violations, fmt.Sprintf("%s throughput %.1f jobs/s is below %.0f%% of %.1f jobs/s",
			spec.Implementation, candidate.throughput, bound.MinThroughputRatio*100, reference.throughput))
	}
	if maximum := time.Duration(float64(reference.p95) * bound.MaxP95Ratio); candidate.p95 > maximum {
		violations = append(violations, fmt.Sprintf("%s p95 %s exceeds %.2fx of %s",
			spec.Implementation, candidate.p95, bound.MaxP95Ratio, reference.p95))
	}
	return violations
}

func medianBenchmark(t *testing.T, current *adapter, mode string, jobs int) benchmarkMetrics {
	t.Helper()

	runs := make([]benchmarkMetrics, 0, 3)
	for range 3 {
		runs = append(runs, runAdapterBenchmark(t, current, mode, jobs))
	}
	return medianMetrics(runs)
}

// slowestMetrics combines the lowest throughput and highest p95 of a group.
func slowestMetrics(metrics []benchmarkMetrics) benchmarkMetrics {
	slowest := metrics[0]
	for _, current := range metrics[1:] {
		slowest.throughput = min(slowest.throughput, current.throughput)
		slowest.p95 = max(slowest.p95, current.p95)
	}
	return slowest
}

func TestMixedSoak(t *testing.T) { //nolint:paralleltest // Owns the shared PostgreSQL database.
	// This opt-in soak owns the shared conformance database. CI sets 10m,
	// release candidates use 1h, and the scheduled job uses 6h.
	duration, err := time.ParseDuration(requireEnv(t, "RIVER_CONFORMANCE_SOAK_DURATION"))
	require.NoError(t, err)
	require.Positive(t, duration)
	databaseURL := requireEnv(t, "RIVER_CONFORMANCE_DATABASE_URL")
	scenarios := newScenarioTracker(t, scenarioOwnerSoak)

	root := repoRoot(t)
	goAdapter := startReferenceAdapter(t, root, databaseURL, "go-soak")
	candidateSpec := conformanceCandidateSpec(t, root, false)
	candidateAdapter := startCandidateAdapter(t, root, databaseURL, candidateSpec.Implementation+"-soak", candidateSpec, candidateSpec.Command)
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
	slices.Sort(p95s)
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
	slices.Sort(latencies)
	p95Index := max(0, int(math.Ceil(float64(len(latencies))*0.95))-1)
	return benchmarkMetrics{
		p95:        latencies[p95Index],
		throughput: float64(jobs) / elapsed.Seconds(),
	}
}
