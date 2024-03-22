package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
)

const (
	CancelledJobRetentionPeriodDefault = 24 * time.Hour
	CompletedJobRetentionPeriodDefault = 24 * time.Hour
	DiscardedJobRetentionPeriodDefault = 7 * 24 * time.Hour
	JobCleanerIntervalDefault          = 30 * time.Second
)

// Test-only properties.
type JobCleanerTestSignals struct {
	DeletedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *JobCleanerTestSignals) Init() {
	ts.DeletedBatch.Init()
}

type JobCleanerConfig struct {
	// CancelledJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	CancelledJobRetentionPeriod time.Duration

	// CompletedJobRetentionPeriod is the amount of time to keep completed jobs
	// around before they're removed permanently.
	CompletedJobRetentionPeriod time.Duration

	// DiscardedJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	DiscardedJobRetentionPeriod time.Duration

	// Interval is the amount of time to wait between runs of the cleaner.
	Interval time.Duration
}

func (c *JobCleanerConfig) mustValidate() *JobCleanerConfig {
	if c.CancelledJobRetentionPeriod <= 0 {
		panic("JobCleanerConfig.CancelledJobRetentionPeriod must be above zero")
	}
	if c.CompletedJobRetentionPeriod <= 0 {
		panic("JobCleanerConfig.CompletedJobRetentionPeriod must be above zero")
	}
	if c.DiscardedJobRetentionPeriod <= 0 {
		panic("JobCleanerConfig.DiscardedJobRetentionPeriod must be above zero")
	}
	if c.Interval <= 0 {
		panic("JobCleanerConfig.Interval must be above zero")
	}

	return c
}

// JobCleaner periodically removes finalized jobs that are cancelled, completed,
// or discarded. Each state's retention time can be configured individually.
type JobCleaner struct {
	queueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *JobCleanerConfig
	TestSignals JobCleanerTestSignals

	batchSize int // configurable for test purposes
	exec      riverdriver.Executor
}

func NewJobCleaner(archetype *baseservice.Archetype, config *JobCleanerConfig, exec riverdriver.Executor) *JobCleaner {
	return baseservice.Init(archetype, &JobCleaner{
		Config: (&JobCleanerConfig{
			CancelledJobRetentionPeriod: valutil.ValOrDefault(config.CancelledJobRetentionPeriod, CancelledJobRetentionPeriodDefault),
			CompletedJobRetentionPeriod: valutil.ValOrDefault(config.CompletedJobRetentionPeriod, CompletedJobRetentionPeriodDefault),
			DiscardedJobRetentionPeriod: valutil.ValOrDefault(config.DiscardedJobRetentionPeriod, DiscardedJobRetentionPeriodDefault),
			Interval:                    valutil.ValOrDefault(config.Interval, JobCleanerIntervalDefault),
		}).mustValidate(),

		batchSize: BatchSizeDefault,
		exec:      exec,
	})
}

func (s *JobCleaner) Start(ctx context.Context) error { //nolint:dupl
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

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
					s.Logger.ErrorContext(ctx, s.Name+": Error cleaning jobs", slog.String("error", err.Error()))
				}
				continue
			}

			s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
				slog.Int("num_jobs_deleted", res.NumJobsDeleted),
			)
		}
	}()

	return nil
}

type jobCleanerRunOnceResult struct {
	NumJobsDeleted int
}

func (s *JobCleaner) runOnce(ctx context.Context) (*jobCleanerRunOnceResult, error) {
	res := &jobCleanerRunOnceResult{}

	for {
		// Wrapped in a function so that defers run as expected.
		numDeleted, err := func() (int, error) {
			ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
			defer cancelFunc()

			numDeleted, err := s.exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledFinalizedAtHorizon: time.Now().Add(-s.Config.CancelledJobRetentionPeriod),
				CompletedFinalizedAtHorizon: time.Now().Add(-s.Config.CompletedJobRetentionPeriod),
				DiscardedFinalizedAtHorizon: time.Now().Add(-s.Config.DiscardedJobRetentionPeriod),
				Max:                         s.batchSize,
			})
			if err != nil {
				return 0, fmt.Errorf("error deleting completed jobs: %w", err)
			}

			return numDeleted, nil
		}()
		if err != nil {
			return nil, err
		}

		s.TestSignals.DeletedBatch.Signal(struct{}{})

		res.NumJobsDeleted += numDeleted
		// Deleted was less than query `LIMIT` which means work is done.
		if numDeleted < s.batchSize {
			break
		}

		s.Logger.InfoContext(ctx, s.Name+": Deleted batch of jobs",
			slog.Int("num_jobs_deleted", numDeleted),
		)

		s.CancellableSleepRandomBetween(ctx, BatchBackoffMin, BatchBackoffMax)
	}

	return res, nil
}
