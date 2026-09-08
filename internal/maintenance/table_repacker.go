package maintenance

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

const (
	// TableRepackerTimeoutDefault is the default timeout of the table repacker.
	// Repacking a large table may take a while, so the default timeout is quite
	// generous.
	TableRepackerTimeoutDefault = 5 * time.Minute

	// PostgresVersionRepackMinimum is the minimum version of Postgres that
	// supports REPACK (CONCURRENTLY). Postgres 19 is the first version to
	// include this feature.
	PostgresVersionRepackMinimum = 190000
)

// TableRepackerTestSignals are internal signals used exclusively in tests.
type TableRepackerTestSignals struct {
	Repacked testsignal.TestSignal[struct{}] // notifies when a run finishes
}

func (ts *TableRepackerTestSignals) Init(tb testutil.TestingTB) {
	ts.Repacked.Init(tb)
}

type TableRepackerConfig struct {
	// ScheduleFunc returns the next scheduled run time for the repacker given
	// the current time.
	ScheduleFunc func(time.Time) time.Time

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string

	// Timeout is the amount of time to wait for a repack query to run before
	// cancelling it via context.
	Timeout time.Duration
}

func (c *TableRepackerConfig) mustValidate() *TableRepackerConfig {
	if c.ScheduleFunc == nil {
		panic("TableRepackerConfig.ScheduleFunc must be set")
	}
	if c.Timeout < -1 {
		panic("TableRepackerConfig.Timeout must be above zero")
	}

	return c
}

// TableRepacker periodically executes a REPACK (CONCURRENTLY) command on the
// river_job table to reclaim space from dead tuples. This is a more efficient
// alternative to VACUUM FULL because it doesn't take an exclusive lock on the
// table.
//
// REPACK (CONCURRENTLY) requires Postgres >= 19. On older versions, the
// repacker falls back to VACUUM FULL, which takes an exclusive lock and blocks
// all access to the table for the duration of the operation. This fallback is
// not safe for production use and will only be used if explicitly configured.
type TableRepacker struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *TableRepackerConfig
	TestSignals TableRepackerTestSignals

	exec          riverdriver.Executor
	useVacuumFull bool // falls back to VACUUM FULL for Postgres < 19
}

func NewTableRepacker(archetype *baseservice.Archetype, config *TableRepackerConfig, exec riverdriver.Executor) *TableRepacker {
	scheduleFunc := config.ScheduleFunc
	if scheduleFunc == nil {
		scheduleFunc = (&DefaultTableRepackerSchedule{}).Next
	}

	return baseservice.Init(archetype, &TableRepacker{
		Config: (&TableRepackerConfig{
			ScheduleFunc: scheduleFunc,
			Schema:       config.Schema,
			Timeout:      cmp.Or(config.Timeout, TableRepackerTimeoutDefault),
		}).mustValidate(),

		exec: exec,
	})
}

func (s *TableRepacker) Start(ctx context.Context) error {
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

		// Detect the Postgres version to determine whether we can use REPACK
		// (CONCURRENTLY) or need to fall back to VACUUM FULL. This only needs
		// to be done once at start. If the version query fails (e.g. on
		// non-Postgres databases like SQLite), the service disables itself.
		if !s.useVacuumFull {
			useVacuumFull, err := s.detectVacuumFallback(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.Logger.WarnContext(ctx, s.Name+": Couldn't detect Postgres version; disabling table repacker (this is expected on non-Postgres databases)",
					slog.String("error", err.Error()))
				return
			}
			s.useVacuumFull = useVacuumFull
		}

		nextRunAt := s.Config.ScheduleFunc(time.Now().UTC())

		s.Logger.DebugContext(ctx, s.Name+": Scheduling first run", slog.Time("next_run_at", nextRunAt))

		timerUntilNextRun := time.NewTimer(time.Until(nextRunAt))
		scheduleNextRun := func() {
			// Advance from the previous scheduled time, not "now", so retries
			// stay aligned with the configured cadence and don't immediately
			// refire after a timer that has already elapsed.
			nextRunAt = s.Config.ScheduleFunc(nextRunAt)
			timerUntilNextRun.Reset(time.Until(nextRunAt))
		}

		for {
			select {
			case <-timerUntilNextRun.C:
				if err := s.repackTable(ctx); err != nil {
					if !errors.Is(err, context.Canceled) {
						s.Logger.ErrorContext(ctx, s.Name+": Error repacking table", slog.String("error", err.Error()))
					}
				}

				s.TestSignals.Repacked.Signal(struct{}{})

				scheduleNextRun()

				s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRanSuccessfully,
					slog.Time("next_run_at", nextRunAt))

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

// detectVacuumFallback checks the Postgres version and returns true if we need
// to fall back to VACUUM FULL (i.e., Postgres < 19).
func (s *TableRepacker) detectVacuumFallback(ctx context.Context) (bool, error) {
	var versionNumStr string
	if err := s.exec.QueryRow(ctx, "SHOW server_version_num").Scan(&versionNumStr); err != nil {
		return false, err
	}

	versionNum, err := strconv.Atoi(versionNumStr)
	if err != nil {
		return false, err
	}

	if versionNum < PostgresVersionRepackMinimum {
		s.Logger.WarnContext(ctx, s.Name+": Postgres version does not support REPACK (CONCURRENTLY); falling back to VACUUM FULL which takes an exclusive lock",
			slog.Int("postgres_version_num", versionNum), slog.Int("minimum_version_for_repack", PostgresVersionRepackMinimum))
		return true, nil
	}

	s.Logger.InfoContext(ctx, s.Name+": Using REPACK (CONCURRENTLY)",
		slog.Int("postgres_version_num", versionNum))

	return false, nil
}

func (s *TableRepacker) repackTable(ctx context.Context) error {
	var cancel func()
	if s.Config.Timeout > -1 {
		ctx, cancel = context.WithTimeout(ctx, s.Config.Timeout)
		defer cancel()
	}

	if err := s.exec.TableRepack(ctx, &riverdriver.TableRepackParams{
		Schema:        s.Config.Schema,
		Table:         "river_job",
		UseVacuumFull: s.useVacuumFull,
	}); err != nil {
		return err
	}

	action := "REPACK (CONCURRENTLY)"
	if s.useVacuumFull {
		action = "VACUUM FULL"
	}

	s.Logger.InfoContext(ctx, s.Name+": Repacked table", slog.String("table", "river_job"), slog.String("action", action))
	return nil
}

// DefaultTableRepackerSchedule is a default schedule for the table repacker
// which runs at 01:00 UTC daily, offset from the reindexer's midnight schedule
// so they don't overlap.
type DefaultTableRepackerSchedule struct{}

// Next returns the next scheduled time for the table repacker.
func (s *DefaultTableRepackerSchedule) Next(t time.Time) time.Time {
	return t.Add(24 * time.Hour).Truncate(24 * time.Hour).Add(1 * time.Hour)
}
