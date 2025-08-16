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
	"github.com/riverqueue/river/rivershared/circuitbreaker"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

// Test-only properties.
type JobCleanerTestSignals struct {
	DeletedBatch testsignal.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *JobCleanerTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedBatch.Init(tb)
}

type JobCleanerConfig struct {
	riversharedmaintenance.BatchSizes

	// CancelledJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of cancelled jobs.
	CancelledJobRetentionPeriod time.Duration

	// CompletedJobRetentionPeriod is the amount of time to keep completed jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of completed jobs.
	CompletedJobRetentionPeriod time.Duration

	// DiscardedJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of discarded jobs.
	DiscardedJobRetentionPeriod time.Duration

	// Interval is the amount of time to wait between runs of the cleaner.
	Interval time.Duration

	// QueuesExcluded are queues that'll be excluded from cleaning.
	QueuesExcluded []string

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string

	// Timeout of the individual queries in the job cleaner.
	Timeout time.Duration
}

func (c *JobCleanerConfig) mustValidate() *JobCleanerConfig {
	c.MustValidate()

	if c.CancelledJobRetentionPeriod < -1 {
		panic("JobCleanerConfig.CancelledJobRetentionPeriod must be above zero")
	}
	if c.CompletedJobRetentionPeriod < -1 {
		panic("JobCleanerConfig.CompletedJobRetentionPeriod must be above zero")
	}
	if c.DiscardedJobRetentionPeriod < -1 {
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
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *JobCleanerConfig
	TestSignals JobCleanerTestSignals

	exec riverdriver.Executor

	// Circuit breaker that tracks consecutive timeout failures from the central
	// query. The query starts by using the full/default batch size, but after
	// this breaker trips (after N consecutive timeouts occur in a row), it
	// switches to a smaller batch. We assume that a database that's degraded is
	// likely to stay degraded over a longer term, so after the circuit breaks,
	// it stays broken until the program is restarted.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

func NewJobCleaner(archetype *baseservice.Archetype, config *JobCleanerConfig, exec riverdriver.Executor) *JobCleaner {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &JobCleaner{
		Config: (&JobCleanerConfig{
			BatchSizes:                  batchSizes,
			CancelledJobRetentionPeriod: cmp.Or(config.CancelledJobRetentionPeriod, riversharedmaintenance.CancelledJobRetentionPeriodDefault),
			CompletedJobRetentionPeriod: cmp.Or(config.CompletedJobRetentionPeriod, riversharedmaintenance.CompletedJobRetentionPeriodDefault),
			DiscardedJobRetentionPeriod: cmp.Or(config.DiscardedJobRetentionPeriod, riversharedmaintenance.DiscardedJobRetentionPeriodDefault),
			QueuesExcluded:              config.QueuesExcluded,
			Interval:                    cmp.Or(config.Interval, riversharedmaintenance.JobCleanerIntervalDefault),
			Schema:                      config.Schema,
			Timeout:                     cmp.Or(config.Timeout, riversharedmaintenance.JobCleanerTimeoutDefault),
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
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
					s.Logger.ErrorContext(ctx, s.Name+": Error cleaning jobs", slog.String("error", err.Error()))
				}
				continue
			}

			if res.NumJobsDeleted > 0 {
				s.Logger.InfoContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
					slog.Int("num_jobs_deleted", res.NumJobsDeleted),
				)
			}
		}
	}()

	return nil
}

func (s *JobCleaner) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.Config.Reduced
	}
	return s.Config.Default
}

type jobCleanerRunOnceResult struct {
	NumJobsDeleted int
}

func (s *JobCleaner) runOnce(ctx context.Context) (*jobCleanerRunOnceResult, error) {
	res := &jobCleanerRunOnceResult{}

	for {
		// Wrapped in a function so that defers run as expected.
		numDeleted, err := func() (int, error) {
			// In the special case that all retentions are indefinite, don't
			// bother issuing the query at all as an optimization.
			if s.Config.CompletedJobRetentionPeriod == -1 &&
				s.Config.CancelledJobRetentionPeriod == -1 &&
				s.Config.DiscardedJobRetentionPeriod == -1 {
				return 0, nil
			}

			ctx, cancelFunc := context.WithTimeout(ctx, s.Config.Timeout)
			defer cancelFunc()

			numDeleted, err := s.exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CancelledDoDelete:           s.Config.CancelledJobRetentionPeriod != -1,
				CancelledFinalizedAtHorizon: time.Now().Add(-s.Config.CancelledJobRetentionPeriod),
				CompletedDoDelete:           s.Config.CompletedJobRetentionPeriod != -1,
				CompletedFinalizedAtHorizon: time.Now().Add(-s.Config.CompletedJobRetentionPeriod),
				DiscardedDoDelete:           s.Config.DiscardedJobRetentionPeriod != -1,
				DiscardedFinalizedAtHorizon: time.Now().Add(-s.Config.DiscardedJobRetentionPeriod),
				Max:                         s.batchSize(),
				QueuesExcluded:              s.Config.QueuesExcluded,
				Schema:                      s.Config.Schema,
			})
			if err != nil {
				return 0, fmt.Errorf("error cleaning jobs: %w", err)
			}

			s.reducedBatchSizeBreaker.ResetIfNotOpen()

			return numDeleted, nil
		}()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return nil, err
		}

		s.TestSignals.DeletedBatch.Signal(struct{}{})

		res.NumJobsDeleted += numDeleted
		// Deleted was less than query `LIMIT` which means work is done.
		if numDeleted < s.batchSize() {
			break
		}

		s.Logger.DebugContext(ctx, s.Name+": Deleted batch of jobs",
			slog.Int("num_jobs_deleted", numDeleted),
		)

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}

	return res, nil
}
