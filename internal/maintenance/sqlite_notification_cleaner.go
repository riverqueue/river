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
	SQLiteNotificationCleanerIntervalDefault        = time.Minute
	SQLiteNotificationCleanerRetentionPeriodDefault = 5 * time.Minute
)

// SQLiteNotificationCleanerTestSignals are internal signals used exclusively in tests.
type SQLiteNotificationCleanerTestSignals struct {
	DeletedBatch testsignal.TestSignal[struct{}] // notifies when a delete batch finishes
}

func (ts *SQLiteNotificationCleanerTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedBatch.Init(tb)
}

type SQLiteNotificationCleanerConfig struct {
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

func (c *SQLiteNotificationCleanerConfig) mustValidate() *SQLiteNotificationCleanerConfig {
	c.MustValidate()

	if c.Interval <= 0 {
		panic("SQLiteNotificationCleanerConfig.Interval must be above zero")
	}
	if c.RetentionPeriod <= 0 {
		panic("SQLiteNotificationCleanerConfig.RetentionPeriod must be above zero")
	}
	if c.Timeout <= 0 {
		panic("SQLiteNotificationCleanerConfig.Timeout must be above zero")
	}

	return c
}

// SQLiteNotificationCleaner periodically removes old rows from SQLite's
// notification outbox. It is only needed for the SQLite driver's emulated
// listen/notify support.
type SQLiteNotificationCleaner struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *SQLiteNotificationCleanerConfig
	TestSignals SQLiteNotificationCleanerTestSignals

	exec riverdriver.Executor

	// After repeated timeouts, keep using smaller batches until restart.
	reducedBatchSizeBreaker *circuitbreaker.CircuitBreaker
}

// NewSQLiteNotificationCleaner returns a SQLite notification cleaner.
func NewSQLiteNotificationCleaner(archetype *baseservice.Archetype, config *SQLiteNotificationCleanerConfig, exec riverdriver.Executor) *SQLiteNotificationCleaner {
	batchSizes := config.WithDefaults()

	return baseservice.Init(archetype, &SQLiteNotificationCleaner{
		Config: (&SQLiteNotificationCleanerConfig{
			BatchSizes:      batchSizes,
			Interval:        cmp.Or(config.Interval, SQLiteNotificationCleanerIntervalDefault),
			RetentionPeriod: cmp.Or(config.RetentionPeriod, SQLiteNotificationCleanerRetentionPeriodDefault),
			Schema:          config.Schema,
			Timeout:         cmp.Or(config.Timeout, riversharedmaintenance.TimeoutDefault),
		}).mustValidate(),
		exec:                    exec,
		reducedBatchSizeBreaker: riversharedmaintenance.ReducedBatchSizeBreaker(batchSizes),
	})
}

func (s *SQLiteNotificationCleaner) Start(ctx context.Context) error { //nolint:dupl
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
					s.Logger.ErrorContext(ctx, s.Name+": Error cleaning SQLite notifications", slog.String("error", err.Error()))
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

func (s *SQLiteNotificationCleaner) batchSize() int {
	if s.reducedBatchSizeBreaker.Open() {
		return s.Config.Reduced
	}
	return s.Config.Default
}

type sqliteNotificationCleanerRunOnceResult struct {
	NumNotificationsDeleted int
}

func (s *SQLiteNotificationCleaner) runOnce(ctx context.Context) (*sqliteNotificationCleanerRunOnceResult, error) {
	res := &sqliteNotificationCleanerRunOnceResult{}
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

		// Each delete commits independently. Yield SQLite's writer lock before
		// the next batch so job inserts and updates can make progress.
		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(riversharedmaintenance.BatchBackoffMin, riversharedmaintenance.BatchBackoffMax))
	}
}
