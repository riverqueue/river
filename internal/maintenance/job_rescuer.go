package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

const (
	JobRescuerRescueAfterDefault = time.Hour
	JobRescuerIntervalDefault    = 30 * time.Second
)

type ClientRetryPolicy interface {
	NextRetry(job *rivertype.JobRow) time.Time
}

// Test-only properties.
type JobRescuerTestSignals struct {
	FetchedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has fetched a batch of jobs
	UpdatedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has updated rescued jobs from a batch
}

func (ts *JobRescuerTestSignals) Init() {
	ts.FetchedBatch.Init()
	ts.UpdatedBatch.Init()
}

type JobRescuerConfig struct {
	// ClientRetryPolicy is the default retry policy to use for workers that don't
	// overide NextRetry.
	ClientRetryPolicy ClientRetryPolicy

	// Interval is the amount of time to wait between runs of the rescuer.
	Interval time.Duration

	// RescueAfter is the amount of time for a job to be active before it is
	// considered stuck and should be rescued.
	RescueAfter time.Duration

	WorkUnitFactoryFunc func(kind string) workunit.WorkUnitFactory
}

func (c *JobRescuerConfig) mustValidate() *JobRescuerConfig {
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
	baseservice.BaseService

	// exported for test purposes
	Config      *JobRescuerConfig
	TestSignals JobRescuerTestSignals

	batchSize int // configurable for test purposes
	exec      riverdriver.Executor
}

func NewRescuer(archetype *baseservice.Archetype, config *JobRescuerConfig, exec riverdriver.Executor) *JobRescuer {
	return baseservice.Init(archetype, &JobRescuer{
		Config: (&JobRescuerConfig{
			ClientRetryPolicy:   config.ClientRetryPolicy,
			Interval:            valutil.ValOrDefault(config.Interval, JobRescuerIntervalDefault),
			RescueAfter:         valutil.ValOrDefault(config.RescueAfter, JobRescuerRescueAfterDefault),
			WorkUnitFactoryFunc: config.WorkUnitFactoryFunc,
		}).mustValidate(),

		batchSize: BatchSizeDefault,
		exec:      exec,
	})
}

func (s *JobRescuer) Run(ctx context.Context) {
	s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStarted)
	defer s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStopped)

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

		s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
			slog.Int64("num_jobs_discarded", res.NumJobsDiscarded),
			slog.Int64("num_jobs_retry_scheduled", res.NumJobsRetried),
		)
	}
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
			return nil, fmt.Errorf("error fetching stuck jobs: %w", err)
		}

		s.TestSignals.FetchedBatch.Signal(struct{}{})

		now := time.Now().UTC()

		rescueManyParams := riverdriver.JobRescueManyParams{
			ID:          make([]int64, 0, len(stuckJobs)),
			Error:       make([][]byte, 0, len(stuckJobs)),
			FinalizedAt: make([]time.Time, 0, len(stuckJobs)),
			ScheduledAt: make([]time.Time, 0, len(stuckJobs)),
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
				Error:   "Stuck job rescued by Rescuer",
				Trace:   "TODO",
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling error JSON: %w", err)
			}

			addRescueParam := func(state rivertype.JobState, finalizedAt *time.Time, scheduledAt time.Time) {
				rescueManyParams.ID = append(rescueManyParams.ID, job.ID)
				rescueManyParams.Error = append(rescueManyParams.Error, errorData)
				rescueManyParams.FinalizedAt = append(rescueManyParams.FinalizedAt, ptrutil.ValOrDefault(finalizedAt, time.Time{}))
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
		if len(stuckJobs) < s.batchSize {
			break
		}

		s.Logger.InfoContext(ctx, s.Name+": Rescued batch of jobs",
			slog.Int64("num_jobs_discarded", res.NumJobsDiscarded),
			slog.Int64("num_jobs_retried", res.NumJobsRetried),
		)

		s.CancellableSleepRandomBetween(ctx, BatchBackoffMin, BatchBackoffMax)
	}

	return res, nil
}

func (s *JobRescuer) getStuckJobs(ctx context.Context) ([]*rivertype.JobRow, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFunc()

	stuckHorizon := time.Now().Add(-s.Config.RescueAfter)

	return s.exec.JobGetStuck(ctx, &riverdriver.JobGetStuckParams{
		Max:          s.batchSize,
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
