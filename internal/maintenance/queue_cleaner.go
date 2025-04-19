package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
)

const (
	queueCleanerIntervalDefault = time.Hour
	QueueRetentionPeriodDefault = 24 * time.Hour
)

// Test-only properties.
type QueueCleanerTestSignals struct {
	DeletedBatch testsignal.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *QueueCleanerTestSignals) Init() {
	ts.DeletedBatch.Init()
}

type QueueCleanerConfig struct {
	// Interval is the amount of time to wait between runs of the cleaner.
	Interval time.Duration

	// RetentionPeriod is the amount of time to keep queues around before they're
	// removed.
	RetentionPeriod time.Duration

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *QueueCleanerConfig) mustValidate() *QueueCleanerConfig {
	if c.Interval <= 0 {
		panic("QueueCleanerConfig.Interval must be above zero")
	}
	if c.RetentionPeriod <= 0 {
		panic("QueueCleanerConfig.RetentionPeriod must be above zero")
	}

	return c
}

// QueueCleaner periodically removes queues from the river_queue table that have
// not been updated in a while, indicating that they are no longer active.
type QueueCleaner struct {
	queueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *QueueCleanerConfig
	TestSignals QueueCleanerTestSignals

	batchSize int // configurable for test purposes
	exec      riverdriver.Executor
}

func NewQueueCleaner(archetype *baseservice.Archetype, config *QueueCleanerConfig, exec riverdriver.Executor) *QueueCleaner {
	return baseservice.Init(archetype, &QueueCleaner{
		Config: (&QueueCleanerConfig{
			Interval:        valutil.ValOrDefault(config.Interval, queueCleanerIntervalDefault),
			RetentionPeriod: valutil.ValOrDefault(config.RetentionPeriod, QueueRetentionPeriodDefault),
			Schema:          config.Schema,
		}).mustValidate(),

		batchSize: BatchSizeDefault,
		exec:      exec,
	})
}

func (s *QueueCleaner) Start(ctx context.Context) error {
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
					s.Logger.ErrorContext(ctx, s.Name+": Error cleaning queues", slog.String("error", err.Error()))
				}
				continue
			}

			if len(res.QueuesDeleted) > 0 {
				s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
					slog.String("queues_deleted", strings.Join(res.QueuesDeleted, ",")),
				)
			}
		}
	}()

	return nil
}

type queueCleanerRunOnceResult struct {
	QueuesDeleted []string
}

func (s *QueueCleaner) runOnce(ctx context.Context) (*queueCleanerRunOnceResult, error) {
	res := &queueCleanerRunOnceResult{QueuesDeleted: make([]string, 0, 10)}

	for {
		// Wrapped in a function so that defers run as expected.
		queuesDeleted, err := func() ([]string, error) {
			ctx, cancelFunc := context.WithTimeout(ctx, 30*time.Second)
			defer cancelFunc()

			queuesDeleted, err := s.exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{
				Max:              s.batchSize,
				Schema:           s.Config.Schema,
				UpdatedAtHorizon: time.Now().Add(-s.Config.RetentionPeriod),
			})
			if err != nil {
				return nil, fmt.Errorf("error deleting expired queues: %w", err)
			}

			return queuesDeleted, nil
		}()
		if err != nil {
			return nil, err
		}

		s.TestSignals.DeletedBatch.Signal(struct{}{})

		res.QueuesDeleted = append(res.QueuesDeleted, queuesDeleted...)
		// Deleted was less than query `LIMIT` which means work is done.
		if len(queuesDeleted) < s.batchSize {
			break
		}

		serviceutil.CancellableSleep(ctx, randutil.DurationBetween(BatchBackoffMin, BatchBackoffMax))
	}

	return res, nil
}
