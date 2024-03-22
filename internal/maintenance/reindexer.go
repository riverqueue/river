package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
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
	queueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *ReindexerConfig
	TestSignals ReindexerTestSignals

	batchSize int64 // configurable for test purposes
	exec      riverdriver.Executor
}

func NewReindexer(archetype *baseservice.Archetype, config *ReindexerConfig, exec riverdriver.Executor) *Reindexer {
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

		batchSize: BatchSizeDefault,
		exec:      exec,
	})
}

func (s *Reindexer) Start(ctx context.Context) error {
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

		nextRunAt := s.Config.ScheduleFunc(time.Now().UTC())

		s.Logger.InfoContext(ctx, s.Name+": Scheduling first run", slog.Time("next_run_at", nextRunAt))

		timerUntilNextRun := time.NewTimer(time.Until(nextRunAt))

		for {
			select {
			case <-timerUntilNextRun.C:
				for _, indexName := range s.Config.IndexNames {
					if err := s.reindexOne(ctx, indexName); err != nil {
						if !errors.Is(err, context.Canceled) {
							s.Logger.ErrorContext(ctx, s.Name+": Error reindexing", slog.String("error", err.Error()), slog.String("index_name", indexName))
						}
						continue
					}
				}

				s.TestSignals.Reindexed.Signal(struct{}{})

				// On each run, we calculate the new schedule based on the
				// previous run's start time. This ensures that we don't
				// accidentally skip a run as time elapses during the run.
				nextRunAt = s.Config.ScheduleFunc(nextRunAt)

				// TODO: maybe we should log differently if some of these fail?
				s.Logger.InfoContext(ctx, s.Name+logPrefixRanSuccessfully,
					slog.Time("next_run_at", nextRunAt), slog.Int("num_reindexes_initiated", len(s.Config.IndexNames)))

				// Reset the timer after the insert loop has finished so it's
				// paused during work. Makes its firing more deterministic.
				timerUntilNextRun.Reset(time.Until(nextRunAt))

			case <-ctx.Done():
				// Clean up timer resources. We know it has _not_ received from
				// the timer since its last reset because that would have led us
				// to the case above instead of here.
				if !timerUntilNextRun.Stop() {
					<-timerUntilNextRun.C
				}
				return
			}
		}
	}()

	return nil
}

func (s *Reindexer) reindexOne(ctx context.Context, indexName string) error {
	ctx, cancel := context.WithTimeout(ctx, s.Config.Timeout)
	defer cancel()

	_, err := s.exec.Exec(ctx, "REINDEX INDEX CONCURRENTLY "+indexName)
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
