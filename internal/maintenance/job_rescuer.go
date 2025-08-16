package maintenance

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/circuitbreaker"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	JobRescuerRescueAfterDefault = time.Hour
	JobRescuerIntervalDefault    = 30 * time.Second
)

// Test-only properties.
type JobRescuerTestSignals struct {
	FetchedBatch testsignal.TestSignal[struct{}] // notifies when runOnce has fetched a batch of jobs
	UpdatedBatch testsignal.TestSignal[struct{}] // notifies when runOnce has updated rescued jobs from a batch
}

func (ts *JobRescuerTestSignals) Init(tb testutil.TestingTB) {
	ts.FetchedBatch.Init(tb)
	ts.UpdatedBatch.Init(tb)
}

type JobRescuerConfig struct {
	riversharedmaintenance.BatchSizes

	// ClientRetryPolicy is the default retry policy to use for workers that don't
	// override NextRetry.
	ClientRetryPolicy jobexecutor.ClientRetryPolicy

	// Interval is the amount of time to wait between runs of the rescuer.
	Interval time.Duration

	// RescueAfter is the amount of time for a job to be active before it is
	// considered stuck and should be rescued.
	RescueAfter time.Duration

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string

	WorkUnitFactoryFunc func(kind string) workunit.WorkUnitFactory
}

func (c *JobRescuerConfig) mustValidate() *JobRescuerConfig {
	c.MustValidate()

	if c.ClientRetryPolicy == nil {
		panic("RescuerConfig.ClientRetryPolicy must be set")
	}
	if c.Interval <= 0 {
		panic("RescuerConfig.Interval must be above zero")
	}
	if c.RescueAfter <= 0 {
		panic("RescuerConfig.JobDuration must be above zero")
	}
	if c.WorkUnitFactoryFunc == nil {
		panic("RescuerConfig.WorkUnitFactoryFunc must be set")
	}

	return c
}

// JobRescuer periodically rescues jobs that have been executing for too long
// and are considered to be "stuck".
type JobRescuer struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *JobRescuerConfig
	TestSignals JobRescuerTestSignals

	exec riverdriver.Executor

	// Circuit breaker that tracks consecutive timeout failures from the central
	// query. The query starts by using the full/default batch size, but after
	// this breaker trips (after N consecutive timeouts occur in a row), it
	// switches to a smaller batch. We assume that a database that's degraded is
	// likely to stay degraded over a longer term, so after the circuit breaks,
	// it stays broken until the program is restarted.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

func NewRescuer(archetype *baseservice.Archetype, config *JobRescuerConfig, exec riverdriver.Executor) *JobRescuer {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &JobRescuer{
		Config: (&JobRescuerConfig{
			BatchSizes:          batchSizes,
			ClientRetryPolicy:   config.ClientRetryPolicy,
			Interval:            cmp.Or(config.Interval, JobRescuerIntervalDefault),
			RescueAfter:         cmp.Or(config.RescueAfter, JobRescuerRescueAfterDefault),
			Schema:              config.Schema,
			WorkUnitFactoryFunc: config.WorkUnitFactoryFunc,
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
	})
}

func (s *JobRescuer) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		ticker := timeutil.NewTickerWithInitialTick(ctx, s.Config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			res, err := s.runOnce(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					s.Logger.ErrorContext(ctx, s.Name+": Error rescuing jobs", slog.String("error", err.Error()))
				}
				continue
			}

			if res.NumJobsDiscarded > 0 || res.NumJobsRetried > 0 {
				s.Logger.InfoContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
					slog.Int64("num_jobs_discarded", res.NumJobsDiscarded),
					slog.Int64("num_jobs_retry_scheduled", res.NumJobsRetried),
				)
			}
		}
	}()

	return nil
}

func (s *JobRescuer) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.Config.Reduced
	}
	return s.Config.Default
}

type rescuerRunOnceResult struct {
	NumJobsCancelled int64
	NumJobsDiscarded int64
	NumJobsRetried   int64
}

type metadataWithCancelAttemptedAt struct {
	CancelAttemptedAt time.Time `json:"cancel_attempted_at"`
}

func (s *JobRescuer) runOnce(ctx context.Context) (*rescuerRunOnceResult, error) {
	res := &rescuerRunOnceResult{}

	for {
		stuckJobs, err := s.getStuckJobs(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return nil, fmt.Errorf("error fetching stuck jobs: %w", err)
		}

		s.reducedBatchSizeBreaker.ResetIfNotOpen()

		s.TestSignals.FetchedBatch.Signal(struct{}{})

		now := time.Now().UTC()

		rescueManyParams := riverdriver.JobRescueManyParams{
			ID:          make([]int64, 0, len(stuckJobs)),
			Error:       make([][]byte, 0, len(stuckJobs)),
			FinalizedAt: make([]*time.Time, 0, len(stuckJobs)),
			ScheduledAt: make([]time.Time, 0, len(stuckJobs)),
			Schema:      s.Config.Schema,
			State:       make([]string, 0, len(stuckJobs)),
		}

		for _, job := range stuckJobs {
			var metadata metadataWithCancelAttemptedAt
			if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
				return nil, fmt.Errorf("error unmarshaling job metadata: %w", err)
			}

			errorData, err := json.Marshal(rivertype.AttemptError{
				At:      now,
				Attempt: max(job.Attempt, 0),
				Error:   "Stuck job rescued by JobRescuer",
				Trace:   "",
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling error JSON: %w", err)
			}

			addRescueParam := func(state rivertype.JobState, finalizedAt *time.Time, scheduledAt time.Time) {
				rescueManyParams.ID = append(rescueManyParams.ID, job.ID)
				rescueManyParams.Error = append(rescueManyParams.Error, errorData)
				rescueManyParams.FinalizedAt = append(rescueManyParams.FinalizedAt, finalizedAt)
				rescueManyParams.ScheduledAt = append(rescueManyParams.ScheduledAt, scheduledAt)
				rescueManyParams.State = append(rescueManyParams.State, string(state))
			}

			if !metadata.CancelAttemptedAt.IsZero() {
				res.NumJobsCancelled++
				addRescueParam(rivertype.JobStateCancelled, &now, job.ScheduledAt) // reused previous scheduled value
				continue
			}

			retryDecision, retryAt := s.makeRetryDecision(ctx, job, now)

			switch retryDecision {
			case jobRetryDecisionDiscard:
				res.NumJobsDiscarded++
				addRescueParam(rivertype.JobStateDiscarded, &now, job.ScheduledAt) // reused previous scheduled value

			case jobRetryDecisionIgnore:
				// job not timed out yet due to kind-specific timeout value; ignore

			case jobRetryDecisionRetry:
				res.NumJobsRetried++
				addRescueParam(rivertype.JobStateRetryable, nil, retryAt)
			}
		}

		if len(rescueManyParams.ID) > 0 {
			_, err = s.exec.JobRescueMany(ctx, &rescueManyParams)
			if err != nil {
				return nil, fmt.Errorf("error rescuing stuck jobs: %w", err)
			}
		}

		s.TestSignals.UpdatedBatch.Signal(struct{}{})

		// Number of rows fetched was less than query `LIMIT` which means work is
		// done for this round:
		if len(stuckJobs) < s.batchSize() {
			break
		}

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}

	return res, nil
}

func (s *JobRescuer) getStuckJobs(ctx context.Context) ([]*rivertype.JobRow, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	defer cancelFunc()

	stuckHorizon := time.Now().Add(-s.Config.RescueAfter)

	return s.exec.JobGetStuck(ctx, &riverdriver.JobGetStuckParams{
		Max:          s.batchSize(),
		Schema:       s.Config.Schema,
		StuckHorizon: stuckHorizon,
	})
}

// jobRetryDecision is a signal from makeRetryDecision as to what to do with a
// particular job that appears to be eligible for rescue.
type jobRetryDecision int

const (
	jobRetryDecisionDiscard jobRetryDecision = iota // discard the job
	jobRetryDecisionIgnore                          // don't retry or discard the job
	jobRetryDecisionRetry                           // retry the job
)

// makeRetryDecision decides whether or not a rescued job should be retried, and if so,
// when.
func (s *JobRescuer) makeRetryDecision(ctx context.Context, job *rivertype.JobRow, now time.Time) (jobRetryDecision, time.Time) {
	workUnitFactory := s.Config.WorkUnitFactoryFunc(job.Kind)
	if workUnitFactory == nil {
		s.Logger.ErrorContext(ctx, s.Name+": Attempted to rescue unhandled job kind, discarding",
			slog.String("job_kind", job.Kind), slog.Int64("job_id", job.ID))
		return jobRetryDecisionDiscard, time.Time{}
	}

	workUnit := workUnitFactory.MakeUnit(job)
	if err := workUnit.UnmarshalJob(); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error unmarshaling job args: %s"+err.Error(),
			slog.String("job_kind", job.Kind), slog.Int64("job_id", job.ID))
	}

	if workUnit.Timeout() != 0 && now.Sub(*job.AttemptedAt) < workUnit.Timeout() {
		return jobRetryDecisionIgnore, time.Time{}
	}

	nextRetry := workUnit.NextRetry()
	if nextRetry.IsZero() {
		nextRetry = s.Config.ClientRetryPolicy.NextRetry(job)
	}

	if job.Attempt < max(job.MaxAttempts, 0) {
		return jobRetryDecisionRetry, nextRetry
	}

	return jobRetryDecisionDiscard, time.Time{}
}
