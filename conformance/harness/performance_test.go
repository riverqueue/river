package harness

import (
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// connectionLimit bounds each adapter's Postgres connections.
const connectionLimit = 20

// benchmarkMetrics are one benchmark run's results.
type benchmarkMetrics struct {
	p95        time.Duration
	throughput float64
}

// TestPerformance is the nightly tier's performance checks: completions
// share write transactions, connections stay bounded under load, and each
// implementation's throughput and latency stay within its bounds relative
// to the reference's.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestPerformance(t *testing.T) {
	t.Parallel()

	RequireNightly(t)

	// Many jobs completing at once share write transactions. Postgres
	// assigns one transaction ID per writing transaction, so completing N
	// jobs one at a time would use at least N.
	t.Run("CompletionBatching", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			const jobCount = 1_000
			for _, worker := range []*Adapter{env.Reference, env.Candidate} {
				env.DB.Exec(t, "DELETE FROM river_job")
				worker.Start(t, protocol.StartParams{ClientID: "completion-batching", FetchPollIntervalMS: 1_000, MaxWorkers: jobCount})
				jobs := make([]protocol.InsertJob, jobCount)
				for i := range jobs {
					jobs[i] = echo("completion-batching", protocol.BehaviorBarrierWait)
				}
				worker.Insert(t, protocol.InsertParams{Jobs: jobs})
				env.DB.WaitJobCount(t, jobCount, 20*time.Second, "state = 'running'")

				before := env.DB.NextTransactionID(t)
				worker.Release(t, "completion-batching")
				completed := env.DB.WaitJobCount(t, jobCount, 20*time.Second, "state = 'completed'")
				writes := env.DB.NextTransactionID(t) - before
				for _, job := range completed {
					require.Equal(t, 1, job.Attempt)
					require.Empty(t, job.Errors)
				}
				t.Logf("%s completed %d jobs in %d write transactions", worker.Label, jobCount, writes)
				require.Less(t, writes, int64(jobCount/4), "%s completions aren't batched", worker.Label)
				worker.Stop(t, protocol.StopParams{})
			}
		})
	})

	// Far more workers than either implementation's pool holds complete every
	// job exactly once while each adapter's connections stay bounded.
	t.Run("PoolPressure", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			const jobCount = 600
			adapters := []*Adapter{env.Reference, env.Candidate}
			for _, adapter := range adapters {
				adapter.Start(t, protocol.StartParams{ClientID: adapter.Label + " pool pressure", MaxWorkers: 100})
			}
			for _, inserter := range adapters {
				jobs := make([]protocol.InsertJob, jobCount/2)
				for i := range jobs {
					jobs[i] = withDuration(echo(fmt.Sprintf("pool pressure %d", i), protocol.BehaviorSleep), 10*time.Millisecond)
				}
				inserter.Insert(t, protocol.InsertParams{Jobs: jobs})
			}
			peak := map[string]int{}
			var completed []*protocol.Job
			WaitFor(t, "every job completing", time.Minute, func() bool {
				for _, adapter := range adapters {
					count := env.DB.ConnectionCount(t, adapter)
					peak[adapter.Label] = max(peak[adapter.Label], count)
					require.LessOrEqual(t, count, connectionLimit, "%s connections grew under pool pressure", adapter.Label)
				}
				completed = env.DB.Jobs(t, "state = 'completed'")
				return len(completed) == jobCount
			})
			for _, job := range completed {
				require.Equal(t, 1, job.Attempt)
				require.Empty(t, job.Errors)
			}
			t.Logf("peak connections under pool pressure: %v", peak)
		})
	})

	// The candidate's throughput and p95 latency stay within its bounds
	// relative to the reference's, for enqueueing, working, and both at
	// once. Each attempt takes the median of three runs, and a mode passes
	// when any of three attempts meets the bounds, so one noisy sample on a
	// shared runner can't fail it while a sustained regression still does.
	t.Run("Throughput", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
			const jobs = 200
			for _, mode := range []string{"enqueue", "worker", "mixed"} {
				bound := env.Candidate.Implementation.Performance[mode]
				var violations []string
				for attempt := 1; attempt <= 3; attempt++ {
					reference := medianBenchmark(t, env, env.Reference, mode, jobs)
					candidate := medianBenchmark(t, env, env.Candidate, mode, jobs)
					t.Logf("%s: reference %.1f jobs/s p95 %s; candidate %.1f jobs/s p95 %s",
						mode, reference.throughput, reference.p95, candidate.throughput, candidate.p95)
					violations = benchmarkViolations(bound, candidate, reference)
					if len(violations) == 0 {
						break
					}
					t.Logf("%s attempt %d outside its bounds: %v", mode, attempt, violations)
				}
				require.Empty(t, violations, "%s stayed outside its bounds", mode)
			}
		})
	})
}

// TestSoak runs mixed traffic through both implementations, and the peer
// when there is one, for RIVER_CONFORMANCE_SOAK, restarting the leader
// regularly, and requires every job to complete exactly once while every
// engine works and connections stay bounded.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestSoak(t *testing.T) {
	t.Parallel()

	soak := os.Getenv("RIVER_CONFORMANCE_SOAK")
	if soak == "" {
		t.Skip("set RIVER_CONFORMANCE_SOAK to a duration to soak")
	}
	duration, err := time.ParseDuration(soak)
	require.NoError(t, err)
	if deadline, ok := t.Deadline(); ok {
		require.Less(t, duration+time.Minute, time.Until(deadline), "the soak would outlast go test's -timeout")
	}
	peer := loadConfig(t).peer

	EachDriver(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env) {
		engines := []*Adapter{env.Reference, env.Candidate}
		if peer != nil {
			engines = append(engines, env.StartAdapter(t, peer))
		}
		clientIDs := map[string]*Adapter{}
		for i, engine := range engines {
			clientID := fmt.Sprintf("soak-%d-%s", i, engine.Implementation.Name)
			clientIDs[clientID] = engine
			engine.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 8})
		}

		deadline := time.Now().Add(duration)
		workersSeen := map[string]bool{}
		completed := 0
		for batch := 1; time.Now().Before(deadline); batch++ {
			ids := make([]int64, 0, 10*len(engines))
			for i := range 10 * len(engines) {
				job := engines[i%len(engines)].InsertJob(t, withDuration(echo(fmt.Sprintf("soak %d", completed+i), protocol.BehaviorSleep), 5*time.Millisecond))
				ids = append(ids, job.ID)
			}
			for _, id := range ids {
				job := env.DB.WaitJob(t, id, time.Minute)
				require.Equal(t, "completed", job.State)
				require.Equal(t, 1, job.Attempt)
				require.Len(t, job.AttemptedBy, 1)
				workersSeen[job.AttemptedBy[0]] = true
			}
			completed += len(ids)
			for _, engine := range engines {
				require.LessOrEqual(t, env.DB.ConnectionCount(t, engine), connectionLimit, "%s connections grew without bound", engine.Label)
			}
			if batch%10 == 0 {
				leaderID := env.DB.WaitLeader(t, "").LeaderID
				leader := clientIDs[leaderID]
				leader.Stop(t, protocol.StopParams{})
				env.DB.WaitLeader(t, leaderID)
				leader.Start(t, protocol.StartParams{ClientID: leaderID, MaxWorkers: 8})
			}
		}
		for clientID := range clientIDs {
			require.True(t, workersSeen[clientID], "%s worked no soak jobs", clientID)
		}
		t.Logf("completed %d jobs over %s", completed, duration)
	})
}

// benchmarkViolations compares a candidate's metrics with the reference's.
func benchmarkViolations(bound PerformanceBound, candidate, reference benchmarkMetrics) []string {
	var violations []string
	if minimum := reference.throughput * bound.MinThroughputRatio; candidate.throughput < minimum {
		violations = append(violations, fmt.Sprintf("throughput %.1f jobs/s is below %.0f%% of %.1f jobs/s",
			candidate.throughput, bound.MinThroughputRatio*100, reference.throughput))
	}
	if maximum := time.Duration(float64(reference.p95) * bound.MaxP95Ratio); candidate.p95 > maximum {
		violations = append(violations, fmt.Sprintf("p95 %s exceeds %.2fx of %s", candidate.p95, bound.MaxP95Ratio, reference.p95))
	}
	return violations
}

// medianBenchmark returns the median of three runs of a benchmark.
func medianBenchmark(t *testing.T, env *Env, adapter *Adapter, mode string, jobs int) benchmarkMetrics {
	t.Helper()

	p95s := make([]time.Duration, 0, 3)
	throughputs := make([]float64, 0, 3)
	for range 3 {
		metrics := runBenchmark(t, env, adapter, mode, jobs)
		p95s = append(p95s, metrics.p95)
		throughputs = append(throughputs, metrics.throughput)
	}
	slices.Sort(p95s)
	slices.Sort(throughputs)
	return benchmarkMetrics{p95: p95s[1], throughput: throughputs[1]}
}

// runBenchmark runs one benchmark. Enqueueing times each insert request.
// Working times jobs inserted beforehand from their attempt to their
// completion, and mixed times jobs from their insertion to their completion
// while they're inserted. Each job works for 10 ms, so p95 measures the whole
// pipeline rather than a no-op that host jitter would dominate.
func runBenchmark(t *testing.T, env *Env, adapter *Adapter, mode string, jobs int) benchmarkMetrics {
	t.Helper()

	env.DB.Exec(t, "DELETE FROM river_job")
	job := func(i int) protocol.InsertJob {
		return withDuration(echo(fmt.Sprintf("%s %d", mode, i), protocol.BehaviorSleep), 10*time.Millisecond)
	}
	percentile95 := func(latencies []time.Duration) time.Duration {
		slices.Sort(latencies)
		return latencies[max(0, int(math.Ceil(float64(len(latencies))*0.95))-1)]
	}

	if mode == "enqueue" {
		latencies := make([]time.Duration, jobs)
		startedAt := time.Now()
		for i := range jobs {
			insertStartedAt := time.Now()
			adapter.InsertJob(t, job(i))
			latencies[i] = time.Since(insertStartedAt)
		}
		return benchmarkMetrics{p95: percentile95(latencies), throughput: float64(jobs) / time.Since(startedAt).Seconds()}
	}

	if mode == "worker" {
		batch := make([]protocol.InsertJob, jobs)
		for i := range batch {
			batch[i] = job(i)
		}
		adapter.Insert(t, protocol.InsertParams{Jobs: batch})
	}
	// Mixed runs more workers, so p95 compares the pipelines rather than
	// queue depth; throughput still includes inserting.
	maxWorkers := 32
	if mode == "mixed" {
		maxWorkers = 128
	}
	adapter.Start(t, protocol.StartParams{ClientID: adapter.Label + " benchmark", MaxWorkers: maxWorkers})
	startedAt := time.Now()
	if mode == "mixed" {
		for i := range jobs {
			adapter.InsertJob(t, job(i))
		}
	}
	completed := env.DB.WaitJobCount(t, jobs, time.Minute, "state = 'completed'")
	elapsed := time.Since(startedAt)
	adapter.Stop(t, protocol.StopParams{})

	latencies := make([]time.Duration, len(completed))
	for i, job := range completed {
		start := job.CreatedAt
		if mode == "worker" {
			start = *job.AttemptedAt
		}
		latencies[i] = job.FinalizedAt.Sub(start)
	}
	return benchmarkMetrics{p95: percentile95(latencies), throughput: float64(jobs) / elapsed.Seconds()}
}
