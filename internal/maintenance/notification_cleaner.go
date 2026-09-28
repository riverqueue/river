package maintenance

import (
	"cmp"
	"context"
	"errors"
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
	"github.com/riverqueue/river/rivershared/util/timeoututil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
)

const (
	NotificationCleanerIntervalDefault        = time.Minute
	NotificationCleanerRetentionPeriodDefault = 5 * time.Minute
)

// NotificationCleanerTestSignals are internal signals used exclusively in tests.
type NotificationCleanerTestSignals struct {
	DeletedBatch testsignal.TestSignal[struct{}] // notifies when a delete batch finishes
}

func (ts *NotificationCleanerTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedBatch.Init(tb)
}

type NotificationCleanerConfig struct {
	riversharedmaintenance.BatchSizes

	// Interval is the amount of time to wait between cleaner runs.
	Interval time.Duration

	// RetentionPeriod is the amount of time to keep notification rows around
	// before they're removed.
	RetentionPeriod time.Duration

	// Schema where River tables are located. Empty string omits schema.
	Schema string

	// Timeout is the timeout for each delete query.
	Timeout time.Duration
}

func (c *NotificationCleanerConfig) mustValidate() *NotificationCleanerConfig {
	c.MustValidate()

	if c.Interval <= 0 {
		panic("NotificationCleanerConfig.Interval must be above zero")
	}
	if c.RetentionPeriod <= 0 {
		panic("NotificationCleanerConfig.RetentionPeriod must be above zero")
	}
	if c.Timeout <= 0 {
		panic("NotificationCleanerConfig.Timeout must be above zero")
	}

	return c
}

// NotificationCleaner removes expired notifications for drivers that emulate
// listen/notify with a stored notification log (SQLite and FoundationDB).
// By default, the elected leader runs it every minute to remove notifications
// older than five minutes.
//
// FoundationDB stores notification payloads in a per-schema log and uses a watch
// on a shared sequence key to wake listeners. Through NotificationDeleteBefore,
// this cleaner uses the driver's expiry index to delete old log records and
// their index entries in bounded transactions. The watched sequence key is
// retained so notification IDs remain monotonic even when the log is empty.
// Cleanup is based on age, not listener consumption: slow listeners may miss
// expired notifications, so River retains polling as a fallback.
type NotificationCleaner struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *NotificationCleanerConfig
	TestSignals NotificationCleanerTestSignals

	exec riverdriver.Executor

	// After repeated timeouts, keep using smaller batches until restart.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

// NewNotificationCleaner returns a notification cleaner.
func NewNotificationCleaner(archetype *baseservice.Archetype, config *NotificationCleanerConfig, exec riverdriver.Executor) *NotificationCleaner {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &NotificationCleaner{
		Config: (&NotificationCleanerConfig{
			BatchSizes:      batchSizes,
			Interval:        cmp.Or(config.Interval, NotificationCleanerIntervalDefault),
			RetentionPeriod: cmp.Or(config.RetentionPeriod, NotificationCleanerRetentionPeriodDefault),
			Schema:          config.Schema,
			Timeout:         cmp.Or(config.Timeout, riversharedmaintenance.TimeoutDefault),
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
	})
}

func (s *NotificationCleaner) Start(ctx context.Context) error { //nolint:dupl
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
					s.Logger.ErrorContext(ctx, s.Name+": Error cleaning notifications", slog.String("error", err.Error()))
				}
				continue
			}

			if res.NumNotificationsDeleted > 0 {
				s.Logger.InfoContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
					slog.Int("num_notifications_deleted", res.NumNotificationsDeleted),
				)
			}
		}
	}()

	return nil
}

func (s *NotificationCleaner) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.Config.Reduced
	}
	return s.Config.Default
}

type notificationCleanerRunOnceResult struct {
	NumNotificationsDeleted int
}

func (s *NotificationCleaner) runOnce(ctx context.Context) (*notificationCleanerRunOnceResult, error) {
	res := &notificationCleanerRunOnceResult{}
	// Keep a fixed horizon so new expirations don't extend a cleanup pass.
	createdAtHorizon := time.Now().Add(-s.Config.RetentionPeriod)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		numDeleted, err := timeoututil.WithTimeoutV(ctx, s.Config.Timeout, s.Name+".runOnce", func(ctx context.Context) (int, error) {
			numDeleted, err := s.exec.NotificationDeleteBefore(ctx, &riverdriver.NotificationDeleteBeforeParams{
				CreatedAtHorizon: createdAtHorizon,
				Max:              s.batchSize(),
				Schema:           s.Config.Schema,
			})
			if err != nil {
				return 0, err
			}

			s.reducedBatchSizeBreaker.ResetIfNotOpen()

			return numDeleted, nil
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.reducedBatchSizeBreaker.Trip()
			}

			return nil, err
		}

		s.TestSignals.DeletedBatch.Signal(struct{}{})
		res.NumNotificationsDeleted += numDeleted

		if numDeleted < s.batchSize() {
			return res, nil
		}

		// Each delete commits independently. Yield between batches so job
		// inserts and updates can make progress.
		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}
}
