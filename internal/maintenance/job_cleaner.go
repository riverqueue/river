package maintenance

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

const (
	CancelledJobRetentionPeriodDefault = 24 * time.Hour
	CompletedJobRetentionPeriodDefault = 24 * time.Hour
	DiscardedJobRetentionPeriodDefault = 7 * 24 * time.Hour
	JobCleanerIntervalDefault          = 30 * time.Second
	JobCleanerTimeoutDefault           = 30 * time.Second
)

// Test-only properties.
type JobCleanerTestSignals struct {
	DeletedBatch testsignal.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *JobCleanerTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedBatch.Init(tb)
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

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string

	// Timeout of the individual queries in the job cleaner.
	Timeout time.Duration
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
	if c.Timeout <= 0 {
		panic("JobCleanerConfig.Timeout must be above zero")
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
			CancelledJobRetentionPeriod: cmp.Or(config.CancelledJobRetentionPeriod, CancelledJobRetentionPeriodDefault),
			CompletedJobRetentionPeriod: cmp.Or(config.CompletedJobRetentionPeriod, CompletedJobRetentionPeriodDefault),
			DiscardedJobRetentionPeriod: cmp.Or(config.DiscardedJobRetentionPeriod, DiscardedJobRetentionPeriodDefault),
			Interval:                    cmp.Or(config.Interval, JobCleanerIntervalDefault),
			Schema:                      config.Schema,
			Timeout:                     cmp.Or(config.Timeout, JobCleanerTimeoutDefault),
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

			if res.NumJobsDeleted > 0 {
				s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
					slog.Int("num_jobs_deleted", res.NumJobsDeleted),
				)
			}
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
			ctx, cancelFunc := context.WithTimeout(ctx, s.Config.Timeout)
			defer cancelFunc()

			numDeleted, err := s.exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledFinalizedAtHorizon: time.Now().Add(-s.Config.CancelledJobRetentionPeriod),
				CompletedFinalizedAtHorizon: time.Now().Add(-s.Config.CompletedJobRetentionPeriod),
				DiscardedFinalizedAtHorizon: time.Now().Add(-s.Config.DiscardedJobRetentionPeriod),
				Max:                         s.batchSize,
				Schema:                      s.Config.Schema,
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

		s.Logger.DebugContext(ctx, s.Name+": Deleted batch of jobs",
			slog.Int("num_jobs_deleted", numDeleted),
		)

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(BatchBackoffMin, BatchBackoffMax))
	}

	return res, nil
}
