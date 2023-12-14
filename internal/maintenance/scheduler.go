package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
)

const (
	SchedulerIntervalDefault = 5 * time.Second
	SchedulerLimitDefault    = 10_000
)

// Test-only properties.
type SchedulerTestSignals struct {
	ScheduledBatch rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *SchedulerTestSignals) Init() {
	ts.ScheduledBatch.Init()
}

type SchedulerConfig struct {
	// Interval is the amount of time between periodic checks for jobs to
	// be moved from "scheduled" to "available".
	Interval time.Duration

	// Limit is the maximum number of jobs to transition at once from
	// "scheduled" to "available" during periodic scheduling checks.
	Limit int
}

func (c *SchedulerConfig) mustValidate() *SchedulerConfig {
	if c.Interval <= 0 {
		panic("SchedulerConfig.Interval must be above zero")
	}
	if c.Limit <= 0 {
		panic("SchedulerConfig.Limit must be above zero")
	}

	return c
}

// Scheduler periodically moves jobs in `scheduled` or `retryable` state and
// which are ready to run over to `available` so that they're eligible to be
// worked.
type Scheduler struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	TestSignals SchedulerTestSignals

	config *SchedulerConfig
	exec   riverdriver.Executor
}

func NewScheduler(archetype *baseservice.Archetype, config *SchedulerConfig, exec riverdriver.Executor) *Scheduler {
	return baseservice.Init(archetype, &Scheduler{
		config: (&SchedulerConfig{
			Interval: valutil.ValOrDefault(config.Interval, SchedulerIntervalDefault),
			Limit:    valutil.ValOrDefault(config.Limit, SchedulerLimitDefault),
		}).mustValidate(),
		exec: exec,
	})
}

func (s *Scheduler) Start(ctx context.Context) error { //nolint:dupl
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

		ticker := timeutil.NewTickerWithInitialTick(ctx, s.config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			res, err := s.runOnce(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					s.Logger.ErrorContext(ctx, s.Name+": Error scheduling jobs", slog.String("error", err.Error()))
				}
				continue
			}
			s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
				slog.Int("num_jobs_scheduled", res.NumCompletedJobsScheduled),
			)
		}
	}()

	return nil
}

type schedulerRunOnceResult struct {
	NumCompletedJobsScheduled int
}

func (s *Scheduler) runOnce(ctx context.Context) (*schedulerRunOnceResult, error) {
	res := &schedulerRunOnceResult{}

	for {
		// Wrapped in a function so that defers run as expected.
		numScheduled, err := func() (int, error) {
			ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
			defer cancelFunc()

			numScheduled, err := s.exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				InsertTopic: string(notifier.NotificationTopicInsert),
				Max:         s.config.Limit,
				Now:         s.TimeNowUTC(),
			})
			if err != nil {
				return 0, fmt.Errorf("error scheduling jobs: %w", err)
			}

			return numScheduled, nil
		}()
		if err != nil {
			return nil, err
		}

		s.TestSignals.ScheduledBatch.Signal(struct{}{})

		res.NumCompletedJobsScheduled += numScheduled
		// Scheduled was less than query `LIMIT` which means work is done.
		if numScheduled < s.config.Limit {
			break
		}

		s.Logger.InfoContext(ctx, s.Name+": Scheduled batch of jobs",
			slog.Int("num_completed_jobs_scheduled", numScheduled),
		)

		s.CancellableSleepRandomBetween(ctx, BatchBackoffMin, BatchBackoffMax)
	}

	return res, nil
}
