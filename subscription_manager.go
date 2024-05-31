package river

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

type subscriptionManager struct {
	baseservice.BaseService
	startstop.BaseStartStop

	subscribeCh <-chan []jobcompleter.CompleterJobUpdated

	statsMu        sync.Mutex // protects stats fields
	statsAggregate jobstats.JobStatistics
	statsNumJobs   int

	mu               sync.Mutex // protects subscription fields
	subscriptions    map[int]*eventSubscription
	subscriptionsSeq int // used for generating simple IDs
}

func newSubscriptionManager(archetype *baseservice.Archetype, subscribeCh <-chan []jobcompleter.CompleterJobUpdated) *subscriptionManager {
	return baseservice.Init(archetype, &subscriptionManager{
		subscribeCh:   subscribeCh,
		subscriptions: make(map[int]*eventSubscription),
	})
}

// ResetSubscribeChan is used to change the channel that the subscription
// manager listens on. It must only be called when the subscription manager is
// stopped.
func (sm *subscriptionManager) ResetSubscribeChan(subscribeCh <-chan []jobcompleter.CompleterJobUpdated) {
	sm.subscribeCh = subscribeCh
}

func (sm *subscriptionManager) Start(ctx context.Context) error {
	_, shouldStart, stopped := sm.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		for updates := range sm.subscribeCh {
			sm.distributeJobUpdates(updates)
		}
	}()

	return nil
}

func (sm *subscriptionManager) Stop() {
	shouldStop, stopped, finalizeStop := sm.StopInit()
	if !shouldStop {
		return
	}

	<-stopped

	// Remove all subscriptions and close corresponding channels.
	func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()

		for subID, sub := range sm.subscriptions {
			close(sub.Chan)
			delete(sm.subscriptions, subID)
		}
	}()

	finalizeStop(true)
}

func (sm *subscriptionManager) logStats(ctx context.Context, svcName string) {
	sm.statsMu.Lock()
	defer sm.statsMu.Unlock()

	sm.Logger.InfoContext(ctx, svcName+": Job stats (since last stats line)",
		"num_jobs_run", sm.statsNumJobs,
		"average_complete_duration", sm.safeDurationAverage(sm.statsAggregate.CompleteDuration, sm.statsNumJobs),
		"average_queue_wait_duration", sm.safeDurationAverage(sm.statsAggregate.QueueWaitDuration, sm.statsNumJobs),
		"average_run_duration", sm.safeDurationAverage(sm.statsAggregate.RunDuration, sm.statsNumJobs))

	sm.statsAggregate = jobstats.JobStatistics{}
	sm.statsNumJobs = 0
}

// Handles a potential divide by zero.
func (sm *subscriptionManager) safeDurationAverage(d time.Duration, n int) time.Duration {
	if n == 0 {
		return 0
	}
	return d / time.Duration(n)
}

// Receives updates from the completer and prompts the client to update
// statistics and distribute jobs into any listening subscriber channels.
// (Subscriber channels are non-blocking so this should be quite fast.)
func (sm *subscriptionManager) distributeJobUpdates(updates []jobcompleter.CompleterJobUpdated) {
	func() {
		sm.statsMu.Lock()
		defer sm.statsMu.Unlock()

		for _, update := range updates {
			stats := update.JobStats
			sm.statsAggregate.CompleteDuration += stats.CompleteDuration
			sm.statsAggregate.QueueWaitDuration += stats.QueueWaitDuration
			sm.statsAggregate.RunDuration += stats.RunDuration
			sm.statsNumJobs++
		}
	}()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Quick path so we don't need to allocate anything if no one is listening.
	if len(sm.subscriptions) < 1 {
		return
	}

	for _, update := range updates {
		sm.distributeJobEvent(update.Job, jobStatisticsFromInternal(update.JobStats))
	}
}

// Distribute a single event into any listening subscriber channels.
//
// Job events should specify the job and stats, while queue events should only specify
// the queue.
//
// MUST be called with sm.mu already held.
func (sm *subscriptionManager) distributeJobEvent(job *rivertype.JobRow, stats *JobStatistics) {
	var event *Event
	switch job.State {
	case rivertype.JobStateCancelled:
		event = &Event{Kind: EventKindJobCancelled, Job: job, JobStats: stats}
	case rivertype.JobStateCompleted:
		event = &Event{Kind: EventKindJobCompleted, Job: job, JobStats: stats}
	case rivertype.JobStateScheduled:
		event = &Event{Kind: EventKindJobSnoozed, Job: job, JobStats: stats}
	case rivertype.JobStateAvailable, rivertype.JobStateDiscarded, rivertype.JobStateRetryable, rivertype.JobStateRunning:
		event = &Event{Kind: EventKindJobFailed, Job: job, JobStats: stats}
	case rivertype.JobStatePending:
		panic("completion subscriber unexpectedly received job in pending state, river bug")
	default:
		// linter exhaustive rule prevents this from being reached
		panic("unreachable state to distribute, river bug")
	}

	// All subscription channels are non-blocking so this is always fast and
	// there's no risk of falling behind what producers are sending.
	for _, sub := range sm.subscriptions {
		if sub.ListensFor(event.Kind) {
			// TODO: THIS IS UNSAFE AND WILL LEAD TO DROPPED EVENTS.
			//
			// We are allocating subscriber channels with a fixed size of 1000, but
			// potentially processing job events in batches of 5000 (batch completer
			// max batch size). It's probably not possible for the subscriber to keep
			// up with these bursts.
			select {
			case sub.Chan <- event:
			default:
			}
		}
	}
}

func (sm *subscriptionManager) distributeQueueEvent(event *Event) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// All subscription channels are non-blocking so this is always fast and
	// there's no risk of falling behind what producers are sending.
	for _, sub := range sm.subscriptions {
		if sub.ListensFor(event.Kind) {
			select {
			case sub.Chan <- event:
			default:
			}
		}
	}
}

// Special internal variant that lets us inject an overridden size.
func (sm *subscriptionManager) SubscribeConfig(config *SubscribeConfig) (<-chan *Event, func()) {
	if config.ChanSize < 0 {
		panic("SubscribeConfig.ChanSize must be greater or equal to 1")
	}
	if config.ChanSize == 0 {
		config.ChanSize = subscribeChanSizeDefault
	}

	for _, kind := range config.Kinds {
		if _, ok := allKinds[kind]; !ok {
			panic(fmt.Errorf("unknown event kind: %s", kind))
		}
	}

	subChan := make(chan *Event, config.ChanSize)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Just gives us an easy way of removing the subscription again later.
	subID := sm.subscriptionsSeq
	sm.subscriptionsSeq++

	sm.subscriptions[subID] = &eventSubscription{
		Chan:  subChan,
		Kinds: sliceutil.KeyBy(config.Kinds, func(k EventKind) (EventKind, struct{}) { return k, struct{}{} }),
	}

	cancel := func() {
		sm.mu.Lock()
		defer sm.mu.Unlock()

		// May no longer be present in case this was called after a stop.
		sub, ok := sm.subscriptions[subID]
		if !ok {
			return
		}

		close(sub.Chan)

		delete(sm.subscriptions, subID)
	}

	return subChan, cancel
}
