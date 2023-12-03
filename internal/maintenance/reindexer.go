package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/valutil"
)

const (
	ReindexerIntervalDefault = 24 * time.Hour
	ReindexerTimeoutDefault  = 15 * time.Second
)

var defaultIndexNames = []string{} //nolint:gochecknoglobals

// Test-only properties.
type ReindexerTestSignals struct {
	Reindexed rivercommon.TestSignal[struct{}] // notifies when a run finishes executing reindexes for all indexes
}

func (ts *ReindexerTestSignals) Init() {
	ts.Reindexed.Init()
}

type ReindexerConfig struct {
	// IndexNames is a list of indexes to reindex on each run.
	IndexNames []string

	// ScheduleFunc returns the next scheduled run time for the reindexer given the
	// current time.
	ScheduleFunc func(time.Time) time.Time

	// Timeout is the amount of time to wait for a single reindex query to return.
	Timeout time.Duration
}

func (c *ReindexerConfig) mustValidate() *ReindexerConfig {
	if c.ScheduleFunc == nil {
		panic("ReindexerConfig.ScheduleFunc must be set")
	}
	if c.Timeout <= 0 {
		panic("ReindexerConfig.Timeout must be above zero")
	}

	return c
}

// Reindexer periodically executes a REINDEX command on the important job
// indexes to rebuild them and fix bloat issues.
type Reindexer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// exported for test purposes
	Config      *ReindexerConfig
	TestSignals ReindexerTestSignals

	batchSize  int64 // configurable for test purposes
	dbExecutor dbutil.Executor
}

func NewReindexer(archetype *baseservice.Archetype, config *ReindexerConfig, dbExecutor dbutil.Executor) *Reindexer {
	indexNames := defaultIndexNames
	if config.IndexNames != nil {
		indexNames = config.IndexNames
	}

	scheduleFunc := config.ScheduleFunc
	if scheduleFunc == nil {
		scheduleFunc = (&defaultReindexerSchedule{}).Next
	}

	return baseservice.Init(archetype, &Reindexer{
		Config: (&ReindexerConfig{
			IndexNames:   indexNames,
			ScheduleFunc: scheduleFunc,
			Timeout:      valutil.ValOrDefault(config.Timeout, ReindexerTimeoutDefault),
		}).mustValidate(),

		batchSize:  BatchSizeDefault,
		dbExecutor: dbExecutor,
	})
}

func (s *Reindexer) Start(ctx context.Context) error {
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

		// On each run, we calculate the new schedule based on the previous run's
		// start time. This ensures that we don't accidentally skip a run as time
		// elapses during the run.
		lastRunAt := time.Now().UTC()

		for {
			nextRunAt := s.Config.ScheduleFunc(lastRunAt)
			timerCtx, timerCancel := context.WithDeadline(ctx, nextRunAt)

			select {
			case <-ctx.Done():
				timerCancel()
				return
			case <-timerCtx.Done():
			}
			lastRunAt = nextRunAt

			for _, indexName := range s.Config.IndexNames {
				if err := s.reindexOne(ctx, indexName); err != nil {
					if !errors.Is(err, context.Canceled) {
						s.Logger.ErrorContext(ctx, s.Name+": Error reindexing", slog.String("error", err.Error()), slog.String("index_name", indexName))
					}
					continue
				}
				s.TestSignals.Reindexed.Signal(struct{}{})
			}
			// TODO: maybe we should log differently if some of these fail?
			s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully, slog.Int("num_reindexes_initiated", len(s.Config.IndexNames)))
		}
	}()

	return nil
}

func (s *Reindexer) reindexOne(ctx context.Context, indexName string) error {
	ctx, cancel := context.WithTimeout(ctx, s.Config.Timeout)
	defer cancel()

	_, err := s.dbExecutor.Exec(ctx, "REINDEX INDEX CONCURRENTLY "+indexName)
	if err != nil {
		return err
	}

	s.Logger.InfoContext(ctx, s.Name+": Initiated reindex", slog.String("index_name", indexName))
	return nil
}

// defaultReindexerSchedule is a default schedule for the reindexer job which
// runs at midnight UTC daily.
type defaultReindexerSchedule struct{}

// Next returns the next scheduled time for the reindexer job.
func (s *defaultReindexerSchedule) Next(t time.Time) time.Time {
	return t.Add(24 * time.Hour).Truncate(24 * time.Hour)
}
