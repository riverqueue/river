package maintenance

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

const QueueStateCounterIntervalDefault = 10 * time.Second

var jobStateAll = rivertype.JobStates() //nolint:gochecknoglobals

// QueueStateCounterTestSignals are internal signals used exclusively in tests.
type QueueStateCounterTestSignals struct {
	CountedOnce testsignal.TestSignal[struct{}] // notifies when a count pass finishes
}

func (ts *QueueStateCounterTestSignals) Init(tb testutil.TestingTB) {
	ts.CountedOnce.Init(tb)
}

type QueueStateCounterConfig struct {
	// HookLookupGlobal provides access to globally registered hooks.
	HookLookupGlobal hooklookup.HookLookupInterface

	// Interval is the amount of time between count runs.
	Interval time.Duration

	// QueueNames is the list of configured queue names. Counts are emitted for
	// all of these queues even if they have no jobs, with zero counts for all
	// states.
	QueueNames []string

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *QueueStateCounterConfig) mustValidate() *QueueStateCounterConfig {
	if c.Interval <= 0 {
		panic("QueueStateCounterConfig.Interval must be above zero")
	}

	return c
}

// QueueStateCounter periodically counts jobs by queue and state, logging the
// results. This provides visibility into queue health without requiring
// external monitoring queries. The maintenance service only runs if there is a
// HookQueueStateCount hook registered that consumes the counts.
type QueueStateCounter struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *QueueStateCounterConfig
	TestSignals QueueStateCounterTestSignals

	exec riverdriver.Executor
}

func NewQueueStateCounter(archetype *baseservice.Archetype, config *QueueStateCounterConfig, exec riverdriver.Executor) *QueueStateCounter {
	return baseservice.Init(archetype, &QueueStateCounter{
		Config: (&QueueStateCounterConfig{
			HookLookupGlobal: config.HookLookupGlobal,
			Interval:         cmp.Or(config.Interval, QueueStateCounterIntervalDefault),
			QueueNames:       config.QueueNames,
			Schema:           config.Schema,
		}).mustValidate(),
		exec: exec,
	})
}

func (s *QueueStateCounter) Start(ctx context.Context) error {
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

		// If no hooks are registered, there's no one to send counts to, so
		// start, but don't do anything.
		if len(s.Config.HookLookupGlobal.ByHookKind(hooklookup.HookKindQueueStateCount)) < 1 {
			<-ctx.Done()
			return
		}

		ticker := timeutil.NewTickerWithInitialTick(ctx, s.Config.Interval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			if err := s.runOnce(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.Logger.ErrorContext(ctx, s.Name+": Error counting queue states", slog.String("error", err.Error()))
				}
			}
		}
	}()

	return nil
}

func (s *QueueStateCounter) runOnce(ctx context.Context) error {
	ctx, cancelFunc := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	defer cancelFunc()

	rawResults, err := s.exec.JobCountByQueueAndState(ctx, &riverdriver.JobCountByQueueAndStateParams{
		QueueNames: s.Config.QueueNames,
		Schema:     s.Config.Schema,
	})
	if err != nil {
		return err
	}

	byQueue := s.buildResults(ctx, rawResults)

	for _, hook := range s.Config.HookLookupGlobal.ByHookKind(hooklookup.HookKindQueueStateCount) {
		hook.(rivertype.HookQueueStateCount).QueueStateCount(ctx, &rivertype.HookQueueStateCountParams{ //nolint:forcetypeassert
			ByQueue: byQueue,
		})
	}

	s.TestSignals.CountedOnce.Signal(struct{}{})

	return nil
}

// buildResults converts raw driver counts into results with all configured
// queues and all job states filled in (zeroed where needed), logging one line
// per queue.
func (s *QueueStateCounter) buildResults(ctx context.Context, rawResults []*riverdriver.JobCountByQueueAndStateResult) map[string]rivertype.HookQueueStateCountResult {
	countsByQueue := make(map[string]rivertype.HookQueueStateCountResult, len(rawResults))

	for _, result := range rawResults {
		stateCounts := result.States

		attrs := make([]slog.Attr, 0, 2+len(jobStateAll))
		attrs = append(attrs, slog.String("queue", result.Queue))
		total := 0

		for _, state := range jobStateAll {
			if _, ok := stateCounts[state]; !ok {
				stateCounts[state] = 0
			}
			total += stateCounts[state]
			attrs = append(attrs, slog.Int(string(state), stateCounts[state]))
		}

		attrs = append(attrs, slog.Int("total", total))
		s.Logger.LogAttrs(ctx, slog.LevelInfo, s.Name+": Queue state counts", attrs...)

		countsByQueue[result.Queue] = rivertype.HookQueueStateCountResult{
			ByState: stateCounts,
			Total:   total,
		}
	}

	return countsByQueue
}
