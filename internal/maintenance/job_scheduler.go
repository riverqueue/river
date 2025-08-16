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

const (
	JobSchedulerIntervalDefault = 5 * time.Second
)

// Test-only properties.
type JobSchedulerTestSignals struct {
	NotifiedQueues testsignal.TestSignal[[]string] // notifies when queues are sent an insert notification
	ScheduledBatch testsignal.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *JobSchedulerTestSignals) Init(tb testutil.TestingTB) {
	ts.NotifiedQueues.Init(tb)
	ts.ScheduledBatch.Init(tb)
}

// NotifyInsert is a function to call to emit notifications for queues where
// jobs were scheduled.
type NotifyInsertFunc func(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error

type JobSchedulerConfig struct {
	riversharedmaintenance.BatchSizes

	// Interval is the amount of time between periodic checks for jobs to
	// be moved from "scheduled" to "available".
	Interval time.Duration

	// NotifyInsert is a function to call to emit notifications for queues
	// where jobs were scheduled.
	NotifyInsert NotifyInsertFunc

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *JobSchedulerConfig) mustValidate() *JobSchedulerConfig {
	c.MustValidate()

	if c.Interval <= 0 {
		panic("SchedulerConfig.Interval must be above zero")
	}
	if c.Default <= 0 {
		panic("SchedulerConfig.Limit must be above zero")
	}

	return c
}

// JobScheduler periodically moves jobs in `scheduled` or `retryable` state and
// which are ready to run over to `available` so that they're eligible to be
// worked.
type JobScheduler struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	TestSignals JobSchedulerTestSignals

	config *JobSchedulerConfig
	exec   riverdriver.Executor

	// Circuit breaker that tracks consecutive timeout failures from the central
	// query. The query starts by using the full/default batch size, but after
	// this breaker trips (after N consecutive timeouts occur in a row), it
	// switches to a smaller batch. We assume that a database that's degraded is
	// likely to stay degraded over a longer term, so after the circuit breaks,
	// it stays broken until the program is restarted.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

func NewJobScheduler(archetype *baseservice.Archetype, config *JobSchedulerConfig, exec riverdriver.Executor) *JobScheduler {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &JobScheduler{
		config: (&JobSchedulerConfig{
			BatchSizes:   batchSizes,
			Interval:     cmp.Or(config.Interval, JobSchedulerIntervalDefault),
			NotifyInsert: config.NotifyInsert,
			Schema:       config.Schema,
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
	})
}

func (s *JobScheduler) Start(ctx context.Context) error { //nolint:dupl
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

			if res.NumCompletedJobsScheduled > 0 {
				s.Logger.InfoContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
					slog.Int("num_jobs_scheduled", res.NumCompletedJobsScheduled),
				)
			}
		}
	}()

	return nil
}

func (s *JobScheduler) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.config.Reduced
	}
	return s.config.Default
}

type schedulerRunOnceResult struct {
	NumCompletedJobsScheduled int
}

func (s *JobScheduler) runOnce(ctx context.Context) (*schedulerRunOnceResult, error) {
	res := &schedulerRunOnceResult{}

	for {
		// Wrapped in a function so that defers run as expected.
		numScheduled, err := func() (int, error) {
			ctx, cancelFunc := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
			defer cancelFunc()

			tx, err := s.exec.Begin(ctx)
			if err != nil {
				return 0, fmt.Errorf("error starting transaction: %w", err)
			}
			defer tx.Rollback(ctx)

			now := s.Time.NowUTC()
			nowWithLookAhead := now.Add(s.config.Interval)

			scheduledJobResults, err := tx.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max:    s.batchSize(),
				Now:    &nowWithLookAhead,
				Schema: s.config.Schema,
			})
			if err != nil {
				return 0, fmt.Errorf("error scheduling jobs: %w", err)
			}

			s.reducedBatchSizeBreaker.ResetIfNotOpen()

			queues := make([]string, 0, len(scheduledJobResults))

			// Notify about scheduled jobs with a scheduled_at in the past, or just
			// slightly in the future (this loop, the notify, and tx commit will take
			// a small amount of time). This isn't going to be perfect, but the goal
			// is to roughly try to guess when the clients will attempt to fetch jobs.
			notificationHorizon := s.Time.NowUTC().Add(5 * time.Millisecond)

			for _, result := range scheduledJobResults {
				if result.Job.ScheduledAt.After(notificationHorizon) {
					continue
				}

				queues = append(queues, result.Job.Queue)
			}

			if len(queues) > 0 {
				if err := s.config.NotifyInsert(ctx, tx, queues); err != nil {
					return 0, fmt.Errorf("error notifying insert: %w", err)
				}
				s.TestSignals.NotifiedQueues.Signal(queues)
			}

			return len(scheduledJobResults), tx.Commit(ctx)
		}()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return nil, err
		}

		s.TestSignals.ScheduledBatch.Signal(struct{}{})

		res.NumCompletedJobsScheduled += numScheduled
		// Scheduled was less than query `LIMIT` which means work is done.
		if numScheduled < s.batchSize() {
			break
		}

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}

	return res, nil
}
