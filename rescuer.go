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
	DiscardedJobs rivercommon.TestSignal[struct{}] // notifies when runOnce has discarded jobs from the batch
	FetchedBatch  rivercommon.TestSignal[struct{}] // notifies when runOnce has fetched a batch of jobs
	RetriedJobs   rivercommon.TestSignal[struct{}] // notifies when runOnce has retried jobs from the batch
}

func (ts *rescuerTestSignals) Init() {
	ts.DiscardedJobs.Init()
	ts.FetchedBatch.Init()
	ts.RetriedJobs.Init()
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

	batchSize  int32 // configurable for test purposes
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
				slog.Int64("num_jobs_retry_scheduled", res.NumJobsRetryScheduled),
			)
		}
	}()

	return nil
}

type rescuerRunOnceResult struct {
	NumJobsRetryScheduled int64
	NumJobsDiscarded      int64
}

func (s *rescuer) runOnce(ctx context.Context) (*rescuerRunOnceResult, error) {
	res := &rescuerRunOnceResult{}

	for {
		stuckJobs, err := s.getStuckJobs(ctx)
		if err != nil {
			return nil, fmt.Errorf("error fetching stuck jobs: %w", err)
		}

		s.TestSignals.FetchedBatch.Signal(struct{}{})

		jobIDsToDiscard := make([]int64, 0, len(stuckJobs))
		attemptErrorsForDiscard := make([]AttemptError, 0, len(stuckJobs))

		jobIDsToRetry := make([]int64, 0, len(stuckJobs))
		attemptErrorsForRetry := make([]AttemptError, 0, len(stuckJobs))
		timestampsForRetry := make([]time.Time, 0, len(stuckJobs))

		now := time.Now().UTC()

		for _, job := range stuckJobs {
			shouldRetry, retryAt := s.makeRetryDecision(ctx, job)
			if shouldRetry {
				jobIDsToRetry = append(jobIDsToRetry, job.ID)
				timestampsForRetry = append(timestampsForRetry, retryAt)
				attemptError := AttemptError{
					At:    now,
					Error: "Stuck job rescued by Rescuer",
					Num:   max(int(job.Attempt), 0),
					Trace: "TODO",
				}
				attemptErrorsForRetry = append(attemptErrorsForRetry, attemptError)
			} else {
				jobIDsToDiscard = append(jobIDsToDiscard, job.ID)
				attemptError := AttemptError{
					At:    now,
					Error: "Stuck job rescued by Rescuer",
					Num:   max(int(job.Attempt), 0),
					Trace: "TODO",
				}
				attemptErrorsForDiscard = append(attemptErrorsForDiscard, attemptError)
			}
		}

		if len(jobIDsToRetry) > 0 {
			marshaledRetryErrors, err := marshalAllErrors(attemptErrorsForRetry)
			if err != nil {
				return nil, fmt.Errorf("error marshaling retry errors: %w", err)
			}

			err = s.queries.JobUpdateStuckForRetry(ctx, s.dbExecutor, dbsqlc.JobUpdateStuckForRetryParams{
				ID:          jobIDsToRetry,
				ScheduledAt: timestampsForRetry,
				Errors:      marshaledRetryErrors,
			})
			if err != nil {
				return nil, fmt.Errorf("error updating stuck jobs for retry: %w", err)
			}
			s.TestSignals.RetriedJobs.Signal(struct{}{})
		}

		if len(jobIDsToDiscard) > 0 {
			marshaledDiscardErrors, err := marshalAllErrors(attemptErrorsForDiscard)
			if err != nil {
				return nil, fmt.Errorf("error marshaling discard errors: %w", err)
			}

			err = s.queries.JobUpdateStuckForDiscard(ctx, s.dbExecutor, dbsqlc.JobUpdateStuckForDiscardParams{
				ID:     jobIDsToDiscard,
				Errors: marshaledDiscardErrors,
			})
			if err != nil {
				return nil, fmt.Errorf("error updating stuck jobs for discard: %w", err)
			}
			s.TestSignals.DiscardedJobs.Signal(struct{}{})
		}

		numDiscarded := int64(len(jobIDsToDiscard))
		numRetried := int64(len(jobIDsToRetry))
		res.NumJobsDiscarded += numDiscarded
		res.NumJobsRetryScheduled += numRetried

		// Number of rows fetched was less than query `LIMIT` which means work is
		// done for this round:
		if int32(len(stuckJobs)) < s.batchSize {
			break
		}

		s.Logger.InfoContext(ctx, s.Name+": Rescued batch of jobs",
			slog.Int64("num_jobs_discarded", numDiscarded),
			slog.Int64("num_jobs_retried", numRetried),
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
		LimitCount:   s.batchSize,
	})
}

// makeRetryDecision decides whether or not a rescued job should be retried, and if so,
// when.
func (s *rescuer) makeRetryDecision(ctx context.Context, internalJob *dbsqlc.RiverJob) (bool, time.Time) {
	job := jobRowFromInternal(internalJob)
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

func marshalAllErrors(errors []AttemptError) ([][]byte, error) {
	results := make([][]byte, len(errors))

	for i, attemptErr := range errors {
		payload, err := json.Marshal(attemptErr)
		if err != nil {
			return nil, err
		}
		results[i] = payload
	}

	return results, nil
}
