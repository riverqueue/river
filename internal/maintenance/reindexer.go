package maintenance

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const (
	ReindexerIntervalDefault = 24 * time.Hour

	// We've had user reports of builds taking 45 seconds on large tables, so
	// set a timeout of that plus a little margin. Use of `CONCURRENTLY` should
	// prevent index operations that run a little long from impacting work from
	// an operational standpoint.
	//
	// https://github.com/riverqueue/river/issues/909#issuecomment-2909949466
	ReindexerTimeoutDefault = 1 * time.Minute
)

var defaultIndexNames = []string{ //nolint:gochecknoglobals
	"river_job_args_index",
	"river_job_kind",
	"river_job_metadata_index",
	"river_job_pkey",
	"river_job_prioritized_fetching_index",
	"river_job_state_and_finalized_at_index",
	"river_job_unique_idx",
}

// Test-only properties.
type ReindexerTestSignals struct {
	Reindexed testsignal.TestSignal[struct{}] // notifies when a run finishes executing reindexes for all indexes
}

func (ts *ReindexerTestSignals) Init(tb testutil.TestingTB) {
	ts.Reindexed.Init(tb)
}

type ReindexerConfig struct {
	// IndexNames is a list of indexes to reindex on each run.
	IndexNames []string

	// ScheduleFunc returns the next scheduled run time for the reindexer given the
	// current time.
	ScheduleFunc func(time.Time) time.Time

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string

	// Timeout is the amount of time to wait for a single reindex query to run
	// before cancelling it via context.
	Timeout time.Duration
}

func (c *ReindexerConfig) mustValidate() *ReindexerConfig {
	if c.ScheduleFunc == nil {
		panic("ReindexerConfig.ScheduleFunc must be set")
	}
	if c.Timeout < -1 {
		panic("ReindexerConfig.Timeout must be above zero")
	}

	return c
}

// Reindexer periodically executes a REINDEX command on the important job
// indexes to rebuild them and fix bloat issues.
type Reindexer struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *ReindexerConfig
	TestSignals ReindexerTestSignals

	exec                     riverdriver.Executor // driver executor
	skipReindexArtifactCheck bool                 // lets the reindex artifact check be skipped for test purposes
}

func NewReindexer(archetype *baseservice.Archetype, config *ReindexerConfig, exec riverdriver.Executor) *Reindexer {
	indexNames := defaultIndexNames
	if config.IndexNames != nil {
		indexNames = config.IndexNames
	}

	scheduleFunc := config.ScheduleFunc
	if scheduleFunc == nil {
		scheduleFunc = (&DefaultReindexerSchedule{}).Next
	}

	return baseservice.Init(archetype, &Reindexer{
		Config: (&ReindexerConfig{
			IndexNames:   indexNames,
			ScheduleFunc: scheduleFunc,
			Schema:       config.Schema,
			Timeout:      cmp.Or(config.Timeout, ReindexerTimeoutDefault),
		}).mustValidate(),

		exec: exec,
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

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		nextRunAt := s.Config.ScheduleFunc(time.Now().UTC())

		s.Logger.DebugContext(ctx, s.Name+": Scheduling first run", slog.Time("next_run_at", nextRunAt))

		timerUntilNextRun := time.NewTimer(time.Until(nextRunAt))

		for {
			select {
			case <-timerUntilNextRun.C:
				for _, indexName := range s.Config.IndexNames {
					if _, err := s.reindexOne(ctx, indexName); err != nil {
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
				s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
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

func (s *Reindexer) reindexOne(ctx context.Context, indexName string) (bool, error) {
	var cancel func()
	if s.Config.Timeout > -1 {
		ctx, cancel = context.WithTimeout(ctx, s.Config.Timeout)
		defer cancel()
	}

	// Make sure that no `CONCURRENTLY` artifacts from a previous reindexing run
	// exist before trying to reindex. When using `CONCURRENTLY`, Postgres
	// creates a new index suffixed with `_ccnew` before swapping it in as the
	// new index. The existing index is renamed `_ccold` before being dropped
	// concurrently.
	//
	// If one of these artifacts exists, it probably means that a previous
	// reindex attempt timed out, and attempting to reindex again is likely
	// slated for the same fate. We opt to log a warning and no op instead of
	// trying to clean up the artifacts of a previously failed run for the same
	// reason: even with the artifacts removed, if a previous reindex failed
	// then a new one is likely to as well, so cleaning up would result in a
	// forever loop of failed index builds that'd put unnecessary pressure on
	// the underlying database.
	//
	// https://www.postgresql.org/docs/current/sql-reindex.html#SQL-REINDEX-CONCURRENTLY
	if !s.skipReindexArtifactCheck {
		for _, reindexArtifactName := range []string{indexName + "_ccnew", indexName + "_ccold"} {
			reindexArtifactExists, err := s.exec.IndexExists(ctx, &riverdriver.IndexExistsParams{Index: reindexArtifactName, Schema: s.Config.Schema})
			if err != nil {
				return false, err
			}
			if reindexArtifactExists {
				s.Logger.WarnContext(ctx, s.Name+": Found reindex artifact likely resulting from previous partially completed reindex attempt; skipping reindex",
					slog.String("artifact_name", reindexArtifactName), slog.String("index_name", indexName), slog.Duration("timeout", s.Config.Timeout))
				return false, nil
			}
		}
	}

	if err := s.exec.IndexReindex(ctx, &riverdriver.IndexReindexParams{Index: indexName, Schema: s.Config.Schema}); err != nil {
		// This should be quite rare because the reindexer has a slow run
		// period, but it's possible for the reindexer to be stopped while it's
		// trying to rebuild an index, and doing so would normally put in the
		// reindexer into permanent purgatory because the cancellation would
		// leave a concurrent index artifact which would cause the reindexer to
		// skip work on future runs.
		//
		// So here, in the case of a cancellation due to stop, take a little
		// extra time to drop any artifacts that may result from the cancelled
		// build. This will slow shutdown somewhat, but should still be
		// reasonably fast since we're only dropping indexes rather than
		// building them.
		if errors.Is(context.Cause(ctx), startstop.ErrStop) {
			ctx := context.WithoutCancel(ctx)

			ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			s.Logger.InfoContext(ctx, s.Name+": Signaled to stop during index build; attempting to clean up concurrent artifacts")

			for _, reindexArtifactName := range []string{indexName + "_ccnew", indexName + "_ccold"} {
				if err := s.exec.IndexDropIfExists(ctx, &riverdriver.IndexDropIfExistsParams{Index: reindexArtifactName, Schema: s.Config.Schema}); err != nil {
					s.Logger.ErrorContext(ctx, s.Name+": Error dropping reindex artifact", slog.String("artifact_name", reindexArtifactName), slog.String("error", err.Error()))
				}
			}
		}

		return false, err
	}

	s.Logger.InfoContext(ctx, s.Name+": Initiated reindex", slog.String("index_name", indexName))
	return true, nil
}

// DefaultReindexerSchedule is a default schedule for the reindexer job which
// runs at midnight UTC daily.
type DefaultReindexerSchedule struct{}

// Next returns the next scheduled time for the reindexer job.
func (s *DefaultReindexerSchedule) Next(t time.Time) time.Time {
	return t.Add(24 * time.Hour).Truncate(24 * time.Hour)
}
