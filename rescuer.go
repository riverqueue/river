package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
)

const (
	defaultRescueAfter     = time.Hour
	defaultRescuerInterval = 30 * time.Second
)

// Test-only properties.
type rescuerTestSignals struct {
	FetchedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has fetched a batch of jobs
	UpdatedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce has updated rescued jobs from a batch
}

func (ts *rescuerTestSignals) Init() {
	ts.FetchedBatch.Init()
	ts.UpdatedBatch.Init()
}

type rescuerConfig struct {
	// ClientRetryPolicy is the default retry policy to use for workers that don't
	// overide NextRetry.
	ClientRetryPolicy ClientRetryPolicy

	// Interval is the amount of time to wait between runs of the rescuer.
	Interval time.Duration

	// RescueAfter is the amount of time for a job to be active before it is
	// considered stuck and should be rescued.
	RescueAfter time.Duration

	// Workers is the bundle of workers for
	Workers *Workers
}

func (c *rescuerConfig) mustValidate() *rescuerConfig {
	if c.ClientRetryPolicy == nil {
		panic("RescuerConfig.ClientRetryPolicy must be set")
	}
	if c.Interval <= 0 {
		panic("RescuerConfig.Interval must be above zero")
	}
	if c.RescueAfter <= 0 {
		panic("RescuerConfig.JobDuration must be above zero")
	}
	if c.Workers == nil {
		panic("RescuerConfig.Workers must be set")
	}

	return c
}

// rescuer periodically rescues jobs that have been executing for too long
// and are considered to be "stuck".
type rescuer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	Config      *rescuerConfig
	TestSignals rescuerTestSignals

	batchSize  int // configurable for test purposes
	dbExecutor dbutil.Executor
	queries    *dbsqlc.Queries
}

func newRescuer(archetype *baseservice.Archetype, config *rescuerConfig, executor dbutil.Executor) *rescuer {
	return baseservice.Init(archetype, &rescuer{
		Config: (&rescuerConfig{
			ClientRetryPolicy: config.ClientRetryPolicy,
			Interval:          valutil.ValOrDefault(config.Interval, defaultRescuerInterval),
			RescueAfter:       valutil.ValOrDefault(config.RescueAfter, defaultRescueAfter),
			Workers:           config.Workers,
		}).mustValidate(),

		batchSize:  maintenance.DefaultBatchSize,
		dbExecutor: executor,
		queries:    dbsqlc.New(),
	})
}

func (s *rescuer) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	// Jitter start up slightly so services don't all perform their first run at
	// exactly the same time.
	s.CancellableSleepRandomBetween(ctx, maintenance.JitterMin, maintenance.JitterMax)

	go func() {
		s.Logger.InfoContext(ctx, s.Name+": Run loop started")
		defer s.Logger.InfoContext(ctx, s.Name+": Run loop stopped")

		defer close(stopped)

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

			s.Logger.InfoContext(ctx, s.Name+": Ran successfully",
				slog.Int64("num_jobs_discarded", res.NumJobsDiscarded),
				slog.Int64("num_jobs_retry_scheduled", res.NumJobsRetried),
			)
		}
	}()

	return nil
}

type rescuerRunOnceResult struct {
	NumJobsDiscarded int64
	NumJobsRetried   int64
}

func (s *rescuer) runOnce(ctx context.Context) (*rescuerRunOnceResult, error) {
	res := &rescuerRunOnceResult{}

	for {
		stuckJobs, err := s.getStuckJobs(ctx)
		if err != nil {
			return nil, fmt.Errorf("error fetching stuck jobs: %w", err)
		}

		s.TestSignals.FetchedBatch.Signal(struct{}{})

		// Return quickly in case there's no work to do.
		if len(stuckJobs) < 1 {
			return res, nil
		}

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

			rescueManyParams.Error[i], err = json.Marshal(AttemptError{
				At:    now,
				Error: "Stuck job rescued by Rescuer",
				Num:   max(int(job.Attempt), 0),
				Trace: "TODO",
			})
			if err != nil {
				return nil, fmt.Errorf("error marshaling error JSON: %w", err)
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

		s.CancellableSleepRandomBetween(ctx, maintenance.BatchBackoffMin, maintenance.BatchBackoffMax)
	}

	return res, nil
}

func (s *rescuer) getStuckJobs(ctx context.Context) ([]*dbsqlc.RiverJob, error) {
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
func (s *rescuer) makeRetryDecision(ctx context.Context, internalJob *dbsqlc.RiverJob) (bool, time.Time) {
	job := (*JobRow)(dbsqlc.JobRowFromInternal(internalJob))
	workerInfo, ok := s.Config.Workers.workersMap[job.Kind]
	if !ok {
		s.Logger.ErrorContext(ctx, s.Name+": Attempted to rescue unhandled job kind, discarding",
			slog.String("job_kind", job.Kind), slog.Int64("job_id", job.ID))
		return false, time.Time{}
	}

	workUnit := workerInfo.workUnitFactory.MakeUnit(job)
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
