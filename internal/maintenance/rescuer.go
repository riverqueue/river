package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

const (
	RescueAfterDefault     = time.Hour
	RescuerIntervalDefault = 30 * time.Second
)

type ClientRetryPolicy interface {
	NextRetry(job *rivertype.JobRow) time.Time
}

// Test-only properties.
type RescuerTestSignals struct {
	FetchedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has fetched a batch of jobs
	UpdatedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has updated rescued jobs from a batch
}

func (ts *RescuerTestSignals) Init() {
	ts.FetchedBatch.Init()
	ts.UpdatedBatch.Init()
}

type RescuerConfig struct {
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

func (c *RescuerConfig) mustValidate() *RescuerConfig {
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

// Rescuer periodically rescues jobs that have been executing for too long
// and are considered to be "stuck".
type Rescuer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	Config      *RescuerConfig
	TestSignals RescuerTestSignals

	batchSize  int // configurable for test purposes
	dbExecutor dbutil.Executor
	queries    *dbsqlc.Queries
}

func NewRescuer(archetype *baseservice.Archetype, config *RescuerConfig, executor dbutil.Executor) *Rescuer {
	return baseservice.Init(archetype, &Rescuer{
		Config: (&RescuerConfig{
			ClientRetryPolicy:   config.ClientRetryPolicy,
			Interval:            valutil.ValOrDefault(config.Interval, RescuerIntervalDefault),
			RescueAfter:         valutil.ValOrDefault(config.RescueAfter, RescueAfterDefault),
			WorkUnitFactoryFunc: config.WorkUnitFactoryFunc,
		}).mustValidate(),

		batchSize:  BatchSizeDefault,
		dbExecutor: executor,
		queries:    dbsqlc.New(),
	})
}

func (s *Rescuer) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	// Jitter start up slightly so services don't all perform their first run at
	// exactly the same time.
	s.CancellableSleepRandomBetween(ctx, JitterMin, JitterMax)

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		s.Logger.InfoContext(ctx, s.Name+logPrefixRunLoopStarted)
		defer s.Logger.InfoContext(ctx, s.Name+logPrefixRunLoopStopped)

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
	}()

	return nil
}

type rescuerRunOnceResult struct {
	NumJobsCancelled int64
	NumJobsDiscarded int64
	NumJobsRetried   int64
}

type metadataWithCancelAttemptedAt struct {
	CancelAttemptedAt time.Time `json:"cancel_attempted_at"`
}

func (s *Rescuer) runOnce(ctx context.Context) (*rescuerRunOnceResult, error) {
	res := &rescuerRunOnceResult{}

	for {
		stuckJobs, err := s.getStuckJobs(ctx)
		if err != nil {
			return nil, fmt.Errorf("error fetching stuck jobs: %w", err)
		}

		s.TestSignals.FetchedBatch.Signal(struct{}{})

		now := time.Now().UTC()

		rescueManyParams := dbsqlc.JobRescueManyParams{
			ID:          make([]int64, len(stuckJobs)),
			Error:       make([][]byte, len(stuckJobs)),
			FinalizedAt: make([]time.Time, len(stuckJobs)),
			ScheduledAt: make([]time.Time, len(stuckJobs)),
			State:       make([]string, len(stuckJobs)),
		}

		for i, job := range stuckJobs {
			rescueManyParams.ID[i] = job.ID

			var metadata metadataWithCancelAttemptedAt
			if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
				return nil, fmt.Errorf("error unmarshaling job metadata: %w", err)
			}

			rescueManyParams.Error[i], err = json.Marshal(rivertype.AttemptError{
				At:      now,
				Attempt: max(int(job.Attempt), 0),
				Error:   "Stuck job rescued by Rescuer",
				Trace:   "TODO",
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling error JSON: %w", err)
			}

			if !metadata.CancelAttemptedAt.IsZero() {
				res.NumJobsCancelled++
				rescueManyParams.FinalizedAt[i] = now
				rescueManyParams.ScheduledAt[i] = job.ScheduledAt // reuse previous value
				rescueManyParams.State[i] = string(dbsqlc.JobStateCancelled)
				continue
			}
			shouldRetry, retryAt := s.makeRetryDecision(ctx, job)
			if shouldRetry {
				res.NumJobsRetried++
				rescueManyParams.ScheduledAt[i] = retryAt
				rescueManyParams.State[i] = string(dbsqlc.JobStateRetryable)
			} else {
				res.NumJobsDiscarded++
				rescueManyParams.FinalizedAt[i] = now
				rescueManyParams.ScheduledAt[i] = job.ScheduledAt // reuse previous value
				rescueManyParams.State[i] = string(dbsqlc.JobStateDiscarded)
			}
		}

		err = s.queries.JobRescueMany(ctx, s.dbExecutor, rescueManyParams)
		if err != nil {
			return nil, fmt.Errorf("error rescuing stuck jobs: %w", err)
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

func (s *Rescuer) getStuckJobs(ctx context.Context) ([]*dbsqlc.RiverJob, error) {
	ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFunc()

	stuckHorizon := time.Now().Add(-s.Config.RescueAfter)

	return s.queries.JobGetStuck(ctx, s.dbExecutor, dbsqlc.JobGetStuckParams{
		StuckHorizon: stuckHorizon,
		LimitCount:   int32(s.batchSize),
	})
}

// makeRetryDecision decides whether or not a rescued job should be retried, and if so,
// when.
func (s *Rescuer) makeRetryDecision(ctx context.Context, internalJob *dbsqlc.RiverJob) (bool, time.Time) {
	job := dbsqlc.JobRowFromInternal(internalJob)

	workUnitFactory := s.Config.WorkUnitFactoryFunc(job.Kind)
	if workUnitFactory == nil {
		s.Logger.ErrorContext(ctx, s.Name+": Attempted to rescue unhandled job kind, discarding",
			slog.String("job_kind", job.Kind), slog.Int64("job_id", job.ID))
		return false, time.Time{}
	}

	workUnit := workUnitFactory.MakeUnit(job)
	if err := workUnit.UnmarshalJob(); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error unmarshaling job args: %s"+err.Error(),
			slog.String("job_kind", job.Kind), slog.Int64("job_id", job.ID))
	}

	nextRetry := workUnit.NextRetry()
	if nextRetry.IsZero() {
		nextRetry = s.Config.ClientRetryPolicy.NextRetry(job)
	}
	return job.Attempt < max(int(internalJob.MaxAttempts), 0), nextRetry
}
