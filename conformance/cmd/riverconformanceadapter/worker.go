package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/conformance/protocol"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivertype"
)

// barrierRegistry holds named barriers that jobs and claims wait on. A
// barrier exists from its first use, whether a wait or a release.
type barrierRegistry struct {
	mu       sync.Mutex
	barriers map[string]chan struct{}
}

func newBarrierRegistry() *barrierRegistry {
	return &barrierRegistry{barriers: make(map[string]chan struct{})}
}

func (r *barrierRegistry) get(name string) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	barrier, ok := r.barriers[name]
	if !ok {
		barrier = make(chan struct{})
		r.barriers[name] = barrier
	}
	return barrier
}

func (r *barrierRegistry) release(name string) {
	barrier := r.get(name)

	r.mu.Lock()
	defer r.mu.Unlock()

	select {
	case <-barrier:
	default:
		close(barrier)
	}
}

func (r *barrierRegistry) wait(ctx context.Context, name string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.get(name):
		return nil
	}
}

// stats is what a running client observed.
type stats struct {
	mu                sync.Mutex
	cancelledAtStart  int
	errorHandlerCalls int
	events            []string
	periodicStarts    int
}

func (s *stats) consume(events <-chan *river.Event) {
	for event := range events {
		s.mu.Lock()
		s.events = append(s.events, string(event.Kind))
		s.mu.Unlock()
	}
}

func (s *stats) increment(counter *int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*counter++
}

func (s *stats) snapshot() *protocol.StatsResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &protocol.StatsResult{
		CancelledAtStart:  s.cancelledAtStart,
		ErrorHandlerCalls: s.errorHandlerCalls,
		Events:            append([]string{}, s.events...),
		PeriodicStarts:    s.periodicStarts,
	}
}

// worker is the built-in worker, which follows each job's behavior.
type worker struct {
	river.WorkerDefaults[echoArgs]

	barriers *barrierRegistry
	stats    *stats
}

func (w *worker) Work(ctx context.Context, job *river.Job[echoArgs]) error {
	args := job.Args
	switch args.Behavior {
	case protocol.BehaviorBarrierOutput, protocol.BehaviorBarrierWait:
		if err := w.barriers.wait(ctx, args.Message); err != nil {
			return err
		}
		if args.Behavior == protocol.BehaviorBarrierOutput {
			return river.RecordOutput(ctx, map[string]any{"race": "worker"})
		}
		return nil

	case protocol.BehaviorCancel:
		return river.JobCancel(errors.New("cancelled by conformance worker"))

	case protocol.BehaviorComplete:
		return nil

	case protocol.BehaviorCooperativeCancel:
		if ctx.Err() != nil {
			w.stats.increment(&w.stats.cancelledAtStart)
		}
		<-ctx.Done()
		return ctx.Err()

	case protocol.BehaviorError:
		return errors.New(protocol.ErrorRetryable)

	case protocol.BehaviorOutput:
		return river.RecordOutput(ctx, map[string]any{"message": args.Message})

	case protocol.BehaviorResumableCursor:
		river.ResumableStep(ctx, "first", nil, func(ctx context.Context) error {
			return river.MetadataSet(ctx, "first_attempt", job.Attempt)
		})
		river.ResumableStepCursor(ctx, "second", nil, func(ctx context.Context, cursor int) error {
			if job.Attempt == 1 {
				if err := river.ResumableSetCursor(ctx, 7); err != nil {
					return err
				}
				return errors.New("retry with cursor")
			}
			if cursor != 7 {
				return fmt.Errorf("expected cursor 7, got %d", cursor)
			}
			return river.MetadataSet(ctx, "cursor_observed", cursor)
		})
		river.ResumableStep(ctx, "third", nil, func(ctx context.Context) error {
			if job.Attempt == 2 {
				return errors.New("retry after consuming cursor")
			}
			return nil
		})
		return nil

	case protocol.BehaviorSleep:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(args.DurationMS) * time.Millisecond):
			return nil
		}

	case protocol.BehaviorSnoozeOnce:
		var metadata map[string]json.RawMessage
		if err := json.Unmarshal(job.Metadata, &metadata); err != nil {
			return err
		}
		if _, snoozed := metadata["snoozes"]; snoozed {
			return nil
		}
		return river.JobSnooze(time.Duration(max(args.DurationMS, 1)) * time.Millisecond)
	}
	return fmt.Errorf("unknown behavior %q", args.Behavior)
}

// kindWorker works jobs of another kind with the built-in worker.
type kindWorker[T kindArgs] struct {
	river.WorkerDefaults[T]

	inner *worker
}

func (w *kindWorker[T]) Work(ctx context.Context, job *river.Job[T]) error {
	return w.inner.Work(ctx, &river.Job[echoArgs]{JobRow: job.JobRow, Args: job.Args.echo()})
}

// kindArgs are args registered under a kind other than the echo kind.
type kindArgs interface {
	river.JobArgs

	echo() echoArgs
}

type peerArgs struct{ echoArgs }

func (peerArgs) Kind() string { return protocol.KindEchoPeer }

func (a peerArgs) echo() echoArgs { return a.echoArgs }

type renamedArgs struct{ echoArgs }

func (renamedArgs) Kind() string { return protocol.KindEchoRenamed }

func (renamedArgs) KindAliases() []string { return []string{protocol.KindEcho} }

func (a renamedArgs) echo() echoArgs { return a.echoArgs }

// workerConfig is the River configuration of a client `start` starts.
func workerConfig(params *protocol.StartParams, barriers *barrierRegistry, stats *stats, logger *slog.Logger) (*river.Config, error) {
	workers := river.NewWorkers()
	inner := &worker{barriers: barriers, stats: stats}
	kinds := params.WorkerKinds
	if len(kinds) == 0 {
		kinds = []string{protocol.KindEcho}
	}
	for _, kind := range kinds {
		var err error
		switch kind {
		case protocol.KindEcho:
			err = river.AddWorkerSafely(workers, inner)
		case protocol.KindEchoPeer:
			err = river.AddWorkerSafely(workers, &kindWorker[peerArgs]{inner: inner})
		case protocol.KindEchoRenamed:
			err = river.AddWorkerSafely(workers, &kindWorker[renamedArgs]{inner: inner})
		default:
			return nil, invalidParams(fmt.Errorf("unknown worker kind %q", kind))
		}
		if err != nil {
			return nil, err
		}
	}

	queueNames := params.Queues
	if len(queueNames) == 0 {
		queueNames = []string{river.QueueDefault}
	}
	maxWorkers := params.MaxWorkers
	if maxWorkers == 0 {
		maxWorkers = 4
	}

	queues := make(map[string]river.QueueConfig, len(queueNames))
	for _, queue := range queueNames {
		queues[queue] = river.QueueConfig{MaxWorkers: maxWorkers}
	}

	config := &river.Config{
		FetchCooldown:          time.Millisecond,
		FetchOnlyKnownKinds:    params.FetchOnlyKnownKinds,
		FetchPollInterval:      milliseconds(params.FetchPollIntervalMS),
		Hooks:                  []rivertype.Hook{&periodicStartHook{stats: stats}},
		ID:                     params.ClientID,
		JobTimeout:             milliseconds(params.JobTimeoutMS),
		LeaderElectionDisabled: params.LeaderElectionDisabled,
		Logger:                 logger,
		PollOnly:               params.PollOnly,
		Queues:                 queues,
		RescueStuckJobsAfter:   milliseconds(params.RescueAfterMS),
		Schema:                 params.Schema,
		TestOnly:               true,
		Workers:                workers,
	}
	if params.ErrorHandlerCancel {
		config.ErrorHandler = &cancellingErrorHandler{stats: stats}
	}
	if params.RetryDelayMS > 0 {
		config.RetryPolicy = &fixedRetryPolicy{delay: milliseconds(params.RetryDelayMS)}
	}

	if params.PeriodicUnique && !params.PeriodicRunOnStart {
		return nil, invalidParams(errors.New("periodic_unique requires periodic_run_on_start"))
	}
	if params.PeriodicRunOnStart {
		var uniqueOpts river.UniqueOpts
		if params.PeriodicUnique {
			uniqueOpts = river.UniqueOpts{ByArgs: true, ByQueue: true}
		}
		config.PeriodicJobs = append(config.PeriodicJobs, periodicJob(protocol.PeriodicJobID, "periodic run on start", uniqueOpts))
		if params.PeriodicUnique {
			// Configured after the unique job, so its insertion shows the
			// unique job's insertion was attempted.
			config.PeriodicJobs = append(config.PeriodicJobs, periodicJob(protocol.PeriodicMarkerJobID, "periodic marker", river.UniqueOpts{}))
		}
	}
	return config, nil
}

func periodicJob(id, message string, uniqueOpts river.UniqueOpts) *river.PeriodicJob {
	return river.NewPeriodicJob(
		river.PeriodicInterval(time.Hour),
		func() (river.JobArgs, *river.InsertOpts) {
			return echoArgs{Message: message}, &river.InsertOpts{
				Metadata:   []byte(`{"periodic":true}`),
				UniqueOpts: uniqueOpts,
			}
		},
		&river.PeriodicJobOpts{ID: id, RunOnStart: true},
	)
}

func milliseconds(value int64) time.Duration { return time.Duration(value) * time.Millisecond }

// cancellingErrorHandler cancels every job whose attempt fails.
type cancellingErrorHandler struct {
	stats *stats
}

func (h *cancellingErrorHandler) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *river.ErrorHandlerResult {
	h.stats.increment(&h.stats.errorHandlerCalls)
	return &river.ErrorHandlerResult{SetCancelled: true}
}

func (h *cancellingErrorHandler) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *river.ErrorHandlerResult {
	h.stats.increment(&h.stats.errorHandlerCalls)
	return &river.ErrorHandlerResult{SetCancelled: true}
}

// fixedRetryPolicy retries every failed attempt after the same delay.
type fixedRetryPolicy struct {
	delay time.Duration
}

func (p *fixedRetryPolicy) NextRetry(job *rivertype.JobRow) time.Time {
	return time.Now().UTC().Add(p.delay)
}

// periodicStartHook counts starts of the periodic job enqueuer.
type periodicStartHook struct {
	river.HookDefaults

	stats *stats
}

func (h *periodicStartHook) Start(_ context.Context, _ *rivertype.HookPeriodicJobsStartParams) error { //nolint:unparam // River's hook signature
	h.stats.increment(&h.stats.periodicStarts)
	return nil
}

// claimBarrierDriver installs a claimBarrierPilot through the driver plugin
// hook River's client checks for when it's built.
type claimBarrierDriver[TTx any] struct {
	riverdriver.Driver[TTx]

	pilot *claimBarrierPilot
}

func (d *claimBarrierDriver[TTx]) PluginInit(*baseservice.Archetype) {}

func (d *claimBarrierDriver[TTx]) PluginPilot() riverpilot.Pilot { return d.pilot }

// claimBarrierPilot is River's standard pilot, except that its first fetch
// that claims jobs holds them until the named barrier is released. The claim
// has committed, so the jobs are running without an executor while the
// producer keeps handling notifications, such as a cancellation.
type claimBarrierPilot struct {
	riverpilot.StandardPilot

	barriers *barrierRegistry
	name     string
	waited   atomic.Bool
}

func (p *claimBarrierPilot) JobGetAvailable(ctx context.Context, exec riverdriver.Executor, state riverpilot.ProducerState, params *riverdriver.JobGetAvailableParams) (*riverdriver.JobGetAvailableResult, error) {
	result, err := p.StandardPilot.JobGetAvailable(ctx, exec, state, params)
	if err != nil || len(result.Jobs) == 0 || p.waited.Swap(true) {
		return result, err
	}
	// The jobs are claimed either way, so they're returned however the wait
	// ends. Stopping the client releases the barrier.
	_ = p.barriers.wait(ctx, p.name)
	return result, nil
}

// withClaimBarrier returns driver unchanged without a barrier name, and
// otherwise wraps it to install a claimBarrierPilot.
func withClaimBarrier[TTx any](driver riverdriver.Driver[TTx], barriers *barrierRegistry, name string) riverdriver.Driver[TTx] {
	if name == "" {
		return driver
	}
	return &claimBarrierDriver[TTx]{Driver: driver, pilot: &claimBarrierPilot{barriers: barriers, name: name}}
}
