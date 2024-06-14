package maintenance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/timeutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
)

const (
	queueCleanerIntervalDefault = time.Hour
	QueueRetentionPeriodDefault = 24 * time.Hour
)

// Test-only properties.
type QueueCleanerTestSignals struct {
	DeletedBatch rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
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
	baseservice.BaseService

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
		}).mustValidate(),

		batchSize: BatchSizeDefault,
		exec:      exec,
	})
}

func (s *QueueCleaner) Run(ctx context.Context) {
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

		s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
			slog.Int("num_queues_deleted", len(res.QueuesDeleted)),
		)
	}
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

		s.Logger.InfoContext(ctx, s.Name+": Deleted batch of queues",
			slog.String("queues_deleted", strings.Join(queuesDeleted, ",")),
		)

		s.CancellableSleepRandomBetween(ctx, BatchBackoffMin, BatchBackoffMax)
	}

	return res, nil
}
