package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
)

const (
	JobSchedulerIntervalDefault = 5 * time.Second
	JobSchedulerLimitDefault    = 10_000
)

// Test-only properties.
type JobSchedulerTestSignals struct {
	NotifiedQueues rivercommon.TestSignal[[]string] // notifies when queues are sent an insert notification
	ScheduledBatch rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *JobSchedulerTestSignals) Init() {
	ts.NotifiedQueues.Init()
	ts.ScheduledBatch.Init()
}

// NotifyInsert is a function to call to emit notifications for queues where
// jobs were scheduled.
type NotifyInsertFunc func(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error

type JobSchedulerConfig struct {
	// Interval is the amount of time between periodic checks for jobs to
	// be moved from "scheduled" to "available".
	Interval time.Duration

	// Limit is the maximum number of jobs to transition at once from
	// "scheduled" to "available" during periodic scheduling checks.
	Limit int

	// NotifyInsert is a function to call to emit notifications for queues
	// where jobs were scheduled.
	NotifyInsert NotifyInsertFunc
}

func (c *JobSchedulerConfig) mustValidate() *JobSchedulerConfig {
	if c.Interval <= 0 {
		panic("SchedulerConfig.Interval must be above zero")
	}
	if c.Limit <= 0 {
		panic("SchedulerConfig.Limit must be above zero")
	}

	return c
}

// JobScheduler periodically moves jobs in `scheduled` or `retryable` state and
// which are ready to run over to `available` so that they're eligible to be
// worked.
type JobScheduler struct {
	baseservice.BaseService

	// exported for test purposes
	TestSignals JobSchedulerTestSignals

	config *JobSchedulerConfig
	exec   riverdriver.Executor
}

func NewJobScheduler(archetype *baseservice.Archetype, config *JobSchedulerConfig, exec riverdriver.Executor) *JobScheduler {
	return baseservice.Init(archetype, &JobScheduler{
		config: (&JobSchedulerConfig{
			Interval:     valutil.ValOrDefault(config.Interval, JobSchedulerIntervalDefault),
			Limit:        valutil.ValOrDefault(config.Limit, JobSchedulerLimitDefault),
			NotifyInsert: config.NotifyInsert,
		}).mustValidate(),
		exec: exec,
	})
}

func (s *JobScheduler) Run(ctx context.Context) {
	s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStarted)
	defer s.Logger.DebugContext(ctx, s.Name+logPrefixRunLoopStopped)

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
}

type schedulerRunOnceResult struct {
	NumCompletedJobsScheduled int
}

func (s *JobScheduler) runOnce(ctx context.Context) (*schedulerRunOnceResult, error) {
	res := &schedulerRunOnceResult{}

	for {
		// Wrapped in a function so that defers run as expected.
		numScheduled, err := func() (int, error) {
			ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
			defer cancelFunc()

			tx, err := s.exec.Begin(ctx)
			if err != nil {
				return 0, fmt.Errorf("error starting transaction: %w", err)
			}
			defer tx.Rollback(ctx)

			now := s.TimeNowUTC()
			nowWithLookAhead := now.Add(s.config.Interval)

			scheduledJobs, err := tx.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: s.config.Limit,
				Now: nowWithLookAhead,
			})
			if err != nil {
				return 0, fmt.Errorf("error scheduling jobs: %w", err)
			}

			queues := make([]string, 0, len(scheduledJobs))

			// Notify about scheduled jobs with a scheduled_at in the past, or just
			// slightly in the future (this loop, the notify, and tx commit will take
			// a small amount of time). This isn't going to be perfect, but the goal
			// is to roughly try to guess when the clients will attempt to fetch jobs.
			notificationHorizon := s.TimeNowUTC().Add(5 * time.Millisecond)

			for _, job := range scheduledJobs {
				if job.ScheduledAt.After(notificationHorizon) {
					continue
				}

				queues = append(queues, job.Queue)
			}

			if len(queues) > 0 {
				if err := s.config.NotifyInsert(ctx, tx, queues); err != nil {
					return 0, fmt.Errorf("error notifying insert: %w", err)
				}
				s.TestSignals.NotifiedQueues.Signal(queues)
			}

			return len(scheduledJobs), tx.Commit(ctx)
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
