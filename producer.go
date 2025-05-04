package river

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/internal/hooklookup"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/middlewarelookup"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/chanutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	producerReportIntervalDefault = time.Minute
	queuePollIntervalDefault      = 2 * time.Second
	queueReportIntervalDefault    = 10 * time.Minute
)

// Test-only properties.
type producerTestSignals struct {
	DeletedExpiredQueueRecords testsignal.TestSignal[struct{}] // notifies when the producer deletes expired queue records
	MetadataChanged            testsignal.TestSignal[struct{}] // notifies when the producer detects a metadata change
	Paused                     testsignal.TestSignal[struct{}] // notifies when the producer is paused
	PolledQueueConfig          testsignal.TestSignal[struct{}] // notifies when the producer polls for queue settings
	ReportedProducerStatus     testsignal.TestSignal[struct{}] // notifies when the producer reports its own status
	ReportedQueueStatus        testsignal.TestSignal[struct{}] // notifies when the producer reports queue status
	Resumed                    testsignal.TestSignal[struct{}] // notifies when the producer is resumed
	StartedExecutors           testsignal.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *producerTestSignals) Init(tb testutil.TestingTB) {
	ts.DeletedExpiredQueueRecords.Init(tb)
	ts.MetadataChanged.Init(tb)
	ts.Paused.Init(tb)
	ts.PolledQueueConfig.Init(tb)
	ts.ReportedQueueStatus.Init(tb)
	ts.ReportedProducerStatus.Init(tb)
	ts.Resumed.Init(tb)
	ts.StartedExecutors.Init(tb)
}

type producerConfig struct {
	ClientID     string
	Completer    jobcompleter.JobCompleter
	ErrorHandler ErrorHandler

	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then fetches may be delayed as long as
	// FetchPollInterval.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	FetchPollInterval time.Duration

	HookLookupByJob        *hooklookup.JobHookLookup
	HookLookupGlobal       hooklookup.HookLookupInterface
	JobTimeout             time.Duration
	MaxWorkers             int
	MiddlewareLookupGlobal middlewarelookup.MiddlewareLookupInterface

	// Notifier is a notifier for subscribing to new job inserts and job
	// control. If nil, the producer will operate in poll-only mode.
	Notifier *notifier.Notifier
	// ProducerReportInterval is the amount of time between periodic reports
	// of the producer status.
	ProducerReportInterval time.Duration

	Queue string
	// QueueEventCallback gets called when a queue's config changes (such as
	// pausing or resuming) events can be emitted to subscriptions.
	QueueEventCallback func(event *Event)

	// QueuePollInterval is the amount of time between periodic checks for
	// queue setting changes. This is only used in poll-only mode (when no
	// notifier is provided).
	QueuePollInterval time.Duration
	// QueueReportInterval is the amount of time between periodic reports
	// of the queue status.
	QueueReportInterval          time.Duration
	RetryPolicy                  ClientRetryPolicy
	SchedulerInterval            time.Duration
	Schema                       string
	StaleProducerRetentionPeriod time.Duration
	Workers                      *Workers
}

func (c *producerConfig) mustValidate() *producerConfig {
	if c.Completer == nil {
		panic("producerConfig.Completer is required")
	}
	if c.ClientID == "" {
		panic("producerConfig.ClientID is required")
	}
	if c.FetchCooldown <= 0 {
		panic("producerConfig.FetchCooldown must be great than zero")
	}
	if c.FetchPollInterval <= 0 {
		panic("producerConfig.FetchPollInterval must be greater than zero")
	}
	if c.JobTimeout < -1 {
		panic("producerConfig.JobTimeout must be greater or equal to zero")
	}
	if c.MaxWorkers == 0 {
		panic("producerConfig.MaxWorkers is required")
	}
	if c.ProducerReportInterval == 0 {
		c.ProducerReportInterval = producerReportIntervalDefault
	}
	if c.Queue == "" {
		panic("producerConfig.Queue is required")
	}
	if c.QueuePollInterval == 0 {
		c.QueuePollInterval = queuePollIntervalDefault
	}
	if c.QueuePollInterval <= 0 {
		panic("producerConfig.QueueSettingsPollInterval must be greater than zero")
	}
	if c.QueueReportInterval == 0 {
		c.QueueReportInterval = queueReportIntervalDefault
	}
	if c.QueueReportInterval <= 0 {
		panic("producerConfig.QueueSettingsReportInterval must be greater than zero")
	}
	if c.RetryPolicy == nil {
		panic("producerConfig.RetryPolicy is required")
	}
	if c.SchedulerInterval == 0 {
		panic("producerConfig.SchedulerInterval is required")
	}
	if c.StaleProducerRetentionPeriod <= 0 {
		panic("producerConfig.StaleProducerRetentionPeriod must be greater than zero")
	}
	if c.Workers == nil {
		panic("producerConfig.Workers is required")
	}

	return c
}

// producer manages a fleet of Workers up to a maximum size. It periodically fetches jobs
// from the adapter and dispatches them to Workers. It receives completed job results from Workers.
//
// The producer never fetches more jobs than the number of free Worker slots it
// has available. This is not optimal for throughput compared to pre-fetching
// extra jobs, but it is better for smaller job counts or slower jobs where even
// distribution and minimizing execution latency is more important.
type producer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	// Jobs which are currently being worked. Only used by main goroutine.
	activeJobs map[int64]*jobexecutor.JobExecutor

	completer    jobcompleter.JobCompleter
	config       *producerConfig
	id           atomic.Int64 // atomic because it's written at startup and read during shutdown
	exec         riverdriver.Executor
	errorHandler jobexecutor.ErrorHandler
	state        riverpilot.ProducerState
	pilot        riverpilot.Pilot
	workers      *Workers

	// Receives job IDs to cancel. Written by notifier goroutine, only read from
	// main goroutine.
	cancelCh chan int64

	// Set to true when the producer thinks it should trigger another fetch as
	// soon as slots are available. This is written and read by the main
	// goroutine.
	fetchWhenSlotsAreAvailable bool

	// Receives completed jobs from workers. Written by completed workers, only
	// read from main goroutine.
	jobResultCh chan *rivertype.JobRow

	jobTimeout time.Duration

	// An atomic count of the number of jobs actively being worked on. This is
	// written to by the main goroutine, but read by the dispatcher.
	numJobsActive atomic.Int32

	numJobsRan atomic.Uint64
	paused     bool
	// Receives control messages from the notifier goroutine. Written by notifier
	// goroutine, only read from main goroutine.
	queueControlCh chan *controlEventPayload
	retryPolicy    ClientRetryPolicy
	testSignals    producerTestSignals
}

func newProducer(archetype *baseservice.Archetype, exec riverdriver.Executor, pilot riverpilot.Pilot, config *producerConfig) *producer {
	if archetype == nil {
		panic("archetype is required")
	}
	if exec == nil {
		panic("exec is required")
	}

	var errorHandler jobexecutor.ErrorHandler
	if config.ErrorHandler != nil {
		errorHandler = &errorHandlerAdapter{config.ErrorHandler}
	}

	return baseservice.Init(archetype, &producer{
		activeJobs:     make(map[int64]*jobexecutor.JobExecutor),
		cancelCh:       make(chan int64, 1000),
		completer:      config.Completer,
		config:         config.mustValidate(),
		exec:           exec,
		errorHandler:   errorHandler,
		jobResultCh:    make(chan *rivertype.JobRow, config.MaxWorkers),
		jobTimeout:     config.JobTimeout,
		pilot:          pilot,
		queueControlCh: make(chan *controlEventPayload, 100),
		retryPolicy:    config.RetryPolicy,
		workers:        config.Workers,
	})
}

// Start starts the producer. It backgrounds a goroutine which is stopped when
// context is cancelled or Stop is invoked.
//
// This variant uses a single context as fetchCtx and workCtx, and is here to
// implement startstop.Service so that the producer can be stored as a service
// variable and used with various service utilities. StartWorkContext below
// should be preferred for production use.
func (p *producer) Start(ctx context.Context) error {
	return p.StartWorkContext(ctx, ctx)
}

func (p *producer) Stop() {
	p.Logger.Debug(p.Name+": Stopping", slog.String("queue", p.config.Queue), slog.Int64("id", p.id.Load()))
	p.BaseStartStop.Stop()
	p.Logger.Debug(p.Name+": Stop returned", slog.String("queue", p.config.Queue), slog.Int64("id", p.id.Load()))
}

// Start starts the producer. It backgrounds a goroutine which is stopped when
// context is cancelled or Stop is invoked.
//
// When fetchCtx is cancelled, no more jobs will be fetched; however, if a fetch
// is already in progress, It will be allowed to complete and run any fetched
// jobs. When workCtx is cancelled, any in-progress jobs will have their
// contexts cancelled too.
func (p *producer) StartWorkContext(fetchCtx, workCtx context.Context) error {
	fetchCtx, shouldStart, started, stopped := p.StartInit(fetchCtx)
	if !shouldStart {
		return nil
	}

	isExpectedShutdownError := func(err error) bool {
		return errors.Is(err, startstop.ErrStop) || strings.HasSuffix(err.Error(), "conn closed") || fetchCtx.Err() != nil
	}

	fetchedQueue, err := func() (*rivertype.Queue, error) {
		ctx, cancel := context.WithTimeout(fetchCtx, 10*time.Second)
		defer cancel()

		p.Logger.DebugContext(ctx, p.Name+": Fetching initial queue settings", slog.String("queue", p.config.Queue))
		return p.exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
			Metadata: []byte("{}"),
			Name:     p.config.Queue,
			Now:      p.Time.NowUTCOrNil(),
			Schema:   p.config.Schema,
		})
	}()
	if err != nil {
		stopped()
		if isExpectedShutdownError(err) {
			return nil
		}
		p.Logger.ErrorContext(fetchCtx, p.Name+": Error fetching initial queue settings", slog.String("err", err.Error()))
		return err
	}

	initiallyPaused := fetchedQueue != nil && (fetchedQueue.PausedAt != nil)
	initialMetadata := []byte("{}")
	if fetchedQueue != nil {
		initialMetadata = fetchedQueue.Metadata
	}
	p.paused = initiallyPaused

	id := p.id.Load()
	id, p.state, err = p.pilot.ProducerInit(fetchCtx, p.exec, &riverpilot.ProducerInitParams{
		ClientID:      p.config.ClientID,
		ProducerID:    id,
		Queue:         p.config.Queue,
		QueueMetadata: initialMetadata,
		Schema:        p.config.Schema,
	})
	if err != nil {
		stopped()
		if isExpectedShutdownError(err) {
			return nil
		}
		p.Logger.ErrorContext(fetchCtx, p.Name+": Error initializing producer state", slog.String("err", err.Error()))
		return err
	}
	p.id.Store(id)

	// TODO: fetcher should have some jitter in it to avoid stampeding issues.
	fetchLimiter := chanutil.NewDebouncedChan(fetchCtx, p.config.FetchCooldown, true)

	var (
		controlSub *notifier.Subscription
		insertSub  *notifier.Subscription
	)
	if p.config.Notifier != nil {
		var err error

		handleInsertNotification := func(topic notifier.NotificationTopic, payload string) {
			var decoded insertPayload
			if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
				p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal insert notification payload", slog.String("err", err.Error()))
				return
			}
			if decoded.Queue != p.config.Queue {
				return
			}
			p.Logger.DebugContext(workCtx, p.Name+": Received insert notification", slog.String("queue", decoded.Queue))
			fetchLimiter.Call()
		}
		insertSub, err = p.config.Notifier.Listen(fetchCtx, notifier.NotificationTopicInsert, handleInsertNotification)
		if err != nil {
			stopped()
			if strings.HasSuffix(err.Error(), "conn closed") || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		controlSub, err = p.config.Notifier.Listen(fetchCtx, notifier.NotificationTopicControl, p.handleControlNotification(workCtx))
		if err != nil {
			stopped()
			if strings.HasSuffix(err.Error(), "conn closed") || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		p.Logger.DebugContext(fetchCtx, p.Name+": Run loop started", slog.String("queue", p.config.Queue), slog.Bool("paused", p.paused))
		defer func() {
			p.Logger.DebugContext(fetchCtx, p.Name+": Run loop stopped", slog.String("queue", p.config.Queue), slog.Uint64("num_completed_jobs", p.numJobsRan.Load()))
		}()

		if insertSub != nil {
			defer insertSub.Unlisten(fetchCtx)
		}

		if controlSub != nil {
			defer controlSub.Unlisten(fetchCtx)
		}

		var subroutineWG sync.WaitGroup
		subroutineWG.Add(3)
		subroutineCtx, cancelSubroutines := context.WithCancelCause(context.WithoutCancel(fetchCtx))
		go p.heartbeatLogLoop(subroutineCtx, &subroutineWG)
		go p.reportQueueStatusLoop(subroutineCtx, &subroutineWG)
		go p.reportProducerStatusLoop(subroutineCtx, &subroutineWG)

		if p.config.Notifier == nil {
			p.Logger.DebugContext(subroutineCtx, p.Name+": No notifier configured; starting in poll mode", "client_id", p.config.ClientID)

			subroutineWG.Add(1)
			go p.pollForSettingChanges(subroutineCtx, &subroutineWG, initiallyPaused, initialMetadata)
		}

		p.fetchAndRunLoop(fetchCtx, workCtx, fetchLimiter)
		p.Logger.Debug(p.Name+": Entering shutdown loop", slog.String("queue", p.config.Queue), slog.Int64("id", p.id.Load()))
		p.executorShutdownLoop()

		p.Logger.Debug(p.Name+": Shutdown loop exited, awaiting subroutines", slog.String("queue", p.config.Queue), slog.Int64("id", p.id.Load()))
		cancelSubroutines(errors.New("producer stopped"))
		subroutineWG.Wait()
		p.Logger.Debug(p.Name+": Shutdown subroutines completed, finalizing", slog.String("queue", p.config.Queue), slog.Int64("id", p.id.Load()))

		p.finalizeShutdown(context.WithoutCancel(fetchCtx))
	}()

	return nil
}

type controlAction string

const (
	controlActionCancel          controlAction = "cancel"
	controlActionMetadataChanged controlAction = "metadata_changed"
	controlActionPause           controlAction = "pause"
	controlActionResume          controlAction = "resume"
)

type controlEventPayload struct {
	Action   controlAction   `json:"action"`
	JobID    int64           `json:"job_id,omitempty"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
	Queue    string          `json:"queue"`
}

type insertPayload struct {
	Queue string `json:"queue"`
}

func (p *producer) handleControlNotification(workCtx context.Context) func(notifier.NotificationTopic, string) {
	return func(topic notifier.NotificationTopic, payload string) {
		var decoded controlEventPayload
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal job control notification payload", slog.String("err", err.Error()))
			return
		}

		switch decoded.Action {
		case controlActionMetadataChanged, controlActionPause, controlActionResume:
			if decoded.Queue != rivercommon.AllQueuesString && decoded.Queue != p.config.Queue {
				p.Logger.DebugContext(workCtx, p.Name+": Queue control notification for other queue", slog.String("action", string(decoded.Action)))
				return
			}
			select {
			case <-workCtx.Done():
			case p.queueControlCh <- &decoded:
			default:
				p.Logger.WarnContext(workCtx, p.Name+": Queue control notification dropped due to full buffer", slog.String("action", string(decoded.Action)))
			}
		case controlActionCancel:
			if decoded.Queue != p.config.Queue {
				p.Logger.DebugContext(workCtx, p.Name+": Received job cancel notification for other queue",
					slog.String("action", string(decoded.Action)),
					slog.Int64("job_id", decoded.JobID),
					slog.String("queue", decoded.Queue),
				)
				return
			}
			select {
			case <-workCtx.Done():
			case p.cancelCh <- decoded.JobID:
			default:
				p.Logger.WarnContext(workCtx, p.Name+": Job cancel notification dropped due to full buffer", slog.Int64("job_id", decoded.JobID))
			}
		default:
			p.Logger.DebugContext(workCtx, p.Name+": Received job control notification with unknown action",
				slog.String("action", string(decoded.Action)),
				slog.Int64("job_id", decoded.JobID),
				slog.String("queue", decoded.Queue),
			)
		}
	}
}

func (p *producer) fetchAndRunLoop(fetchCtx, workCtx context.Context, fetchLimiter *chanutil.DebouncedChan) {
	// Prime the fetchLimiter so we can make an initial fetch without waiting for
	// an insert notification or a fetch poll.
	fetchLimiter.Call()

	fetchPollTimer := time.NewTimer(p.config.FetchPollInterval)
	go func() {
		for {
			select {
			case <-fetchCtx.Done():
				// Stop fetch timer so no more fetches are triggered.
				if !fetchPollTimer.Stop() {
					<-fetchPollTimer.C
				}
				return
			case <-fetchPollTimer.C:
				fetchLimiter.Call()
				fetchPollTimer.Reset(p.config.FetchPollInterval)
			}
		}
	}()

	fetchResultCh := make(chan producerFetchResult)
	for {
		select {
		case <-fetchCtx.Done():
			return
		case msg := <-p.queueControlCh:
			switch msg.Action {
			case controlActionCancel:
				// Separate this case to make linter happy:
				p.Logger.DebugContext(workCtx, p.Name+": Unhandled queue control action", "action", msg.Action)
			case controlActionMetadataChanged:
				p.Logger.DebugContext(workCtx, p.Name+": Queue metadata changed", slog.String("queue", p.config.Queue), slog.String("queue_in_message", msg.Queue))
				p.testSignals.MetadataChanged.Signal(struct{}{})
				if err := p.pilot.QueueMetadataChanged(workCtx, p.exec, &riverpilot.QueueMetadataChangedParams{
					Metadata: msg.Metadata,
					Schema:   p.config.Schema,
					State:    p.state,
				}); err != nil {
					p.Logger.ErrorContext(workCtx, p.Name+": Error updating queue metadata with pilot", slog.String("queue", p.config.Queue), slog.String("err", err.Error()))
				}
			case controlActionPause:
				if p.paused {
					continue
				}
				p.paused = true
				p.Logger.DebugContext(workCtx, p.Name+": Paused", slog.String("queue", p.config.Queue), slog.String("queue_in_message", msg.Queue))
				p.testSignals.Paused.Signal(struct{}{})
				if p.config.QueueEventCallback != nil {
					p.config.QueueEventCallback(&Event{Kind: EventKindQueuePaused, Queue: &rivertype.Queue{Name: p.config.Queue}})
				}
			case controlActionResume:
				if !p.paused {
					continue
				}
				p.paused = false
				p.Logger.DebugContext(workCtx, p.Name+": Resumed", slog.String("queue", p.config.Queue), slog.String("queue_in_message", msg.Queue))
				fetchLimiter.Call() // try another fetch because more jobs may be available to run which were gated behind the paused queue
				p.testSignals.Resumed.Signal(struct{}{})
				if p.config.QueueEventCallback != nil {
					p.config.QueueEventCallback(&Event{Kind: EventKindQueueResumed, Queue: &rivertype.Queue{Name: p.config.Queue}})
				}
			default:
				p.Logger.DebugContext(workCtx, p.Name+": Unknown queue control action", "action", msg.Action)
			}
		case jobID := <-p.cancelCh:
			p.maybeCancelJob(jobID)
		case <-fetchLimiter.C():
			p.innerFetchLoop(workCtx, fetchResultCh)
			// Ensure we can't start another fetch when fetchCtx is done, even if
			// the fetchLimiter is also ready to fire:
			select {
			case <-fetchCtx.Done():
				return
			default:
			}
		case result := <-p.jobResultCh:
			p.removeActiveJob(result)
			if p.fetchWhenSlotsAreAvailable {
				// If we missed a fetch because all worker slots were full, or if we
				// fetched the maximum number of jobs on the last attempt, get a little
				// more aggressive triggering the fetch limiter now that we have a slot
				// available.
				p.fetchWhenSlotsAreAvailable = false
				fetchLimiter.Call()
			}
		}
	}
}

func (p *producer) innerFetchLoop(workCtx context.Context, fetchResultCh chan producerFetchResult) {
	var limit int
	if p.paused {
		limit = 0
	} else {
		limit = p.maxJobsToFetch()
		if limit <= 0 {
			// We have no slots for new jobs, so don't bother fetching. However, since
			// we knew it was time to fetch, we keep track of what happened so we can
			// trigger another fetch as soon as we have open slots.
			p.fetchWhenSlotsAreAvailable = true
			return
		}
	}

	go p.dispatchWork(workCtx, limit, fetchResultCh)

	for {
		select {
		case result := <-fetchResultCh:
			if result.err != nil {
				p.Logger.ErrorContext(workCtx, p.Name+": Error fetching jobs", slog.String("err", result.err.Error()))
			} else if len(result.jobs) > 0 {
				p.startNewExecutors(workCtx, result.jobs)

				if len(result.jobs) == limit {
					// Fetch returned the maximum number of jobs that were requested,
					// implying there may be more in the queue. Trigger another fetch when
					// slots are available.
					p.fetchWhenSlotsAreAvailable = true
				}
			}
			return
		case result := <-p.jobResultCh:
			p.removeActiveJob(result)
		case jobID := <-p.cancelCh:
			p.maybeCancelJob(jobID)
		}
	}
}

func (p *producer) executorShutdownLoop() {
	// No more jobs will be fetched or executed. However, we must wait for all
	// in-progress jobs to complete.
	for len(p.activeJobs) != 0 {
		result := <-p.jobResultCh
		p.removeActiveJob(result)
	}
}

func (p *producer) finalizeShutdown(ctx context.Context) {
	p.Logger.Debug(p.Name + ": Finalizing shutdown")

	const (
		maxAttempts = 4                      // Maximum number of shutdown attempts
		baseTimeout = 100 * time.Millisecond // Base timeout for the first attempt
	)

	attemptShutdown := func(timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		if err := p.pilot.ProducerShutdown(ctx, p.exec, &riverpilot.ProducerShutdownParams{
			ProducerID: p.id.Load(),
			Schema:     p.config.Schema,
			State:      p.state,
		}); err != nil {
			// Don't retry on these errors:
			// - context.Canceled: parent context is canceled, so retrying with a new timeout won't help
			// - ErrClosedPool: the database connection pool is closed, so retrying won't succeed
			if errors.Is(err, context.Canceled) || errors.Is(err, riverdriver.ErrClosedPool) {
				return nil
			}
			return err
		}
		return nil
	}

	// Progressive retry with increasing timeouts:
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Exponential backoff with base 5
		// Attempt 1: 100ms, Attempt 2: 500ms, Attempt 3: 2.5s, Attempt 4: 12.5s
		timeout := baseTimeout * time.Duration(math.Pow(5, float64(attempt-1)))

		if ctx.Err() != nil {
			return // Don't retry if parent context is already done
		}

		if err := attemptShutdown(timeout); err != nil {
			p.Logger.ErrorContext(ctx, p.Name+": Error shutting down producer with pilot",
				slog.String("err", err.Error()),
				slog.Int("attempt", attempt),
				slog.Duration("timeout", timeout))
			continue
		}
		return
	}

	p.Logger.WarnContext(ctx, p.Name+": Failed to cleanly shutdown producer after all attempts")
}

func (p *producer) addActiveJob(id int64, executor *jobexecutor.JobExecutor) {
	p.numJobsActive.Add(1)
	p.activeJobs[id] = executor
}

func (p *producer) removeActiveJob(job *rivertype.JobRow) {
	delete(p.activeJobs, job.ID)
	p.numJobsActive.Add(-1)
	p.numJobsRan.Add(1)
	p.state.JobFinish(job)
}

func (p *producer) maybeCancelJob(id int64) {
	executor, ok := p.activeJobs[id]
	if !ok {
		return
	}
	executor.Cancel()
}

func (p *producer) dispatchWork(workCtx context.Context, count int, fetchResultCh chan<- producerFetchResult) {
	// This intentionally removes any deadlines or cancellation from the parent
	// context because we don't want it to get cancelled if the producer is asked
	// to shut down. In that situation, we want to finish fetching any jobs we are
	// in the midst of fetching, work them, and then stop. Otherwise we'd have a
	// risk of shutting down when we had already fetched jobs in the database,
	// leaving those jobs stranded. We'd then potentially have to release them
	// back to the queue.
	ctx := context.WithoutCancel(workCtx)

	jobs, err := p.pilot.JobGetAvailable(ctx, p.exec, p.state, &riverdriver.JobGetAvailableParams{
		ClientID:   p.config.ClientID,
		Max:        count,
		Now:        p.Time.NowUTCOrNil(),
		Queue:      p.config.Queue,
		ProducerID: p.id.Load(),
		Schema:     p.config.Schema,
	})
	if err != nil {
		p.Logger.Error(p.Name+": Error fetching jobs", slog.String("err", err.Error()), slog.String("queue", p.config.Queue))
		fetchResultCh <- producerFetchResult{err: err}
		return
	}

	fetchResultCh <- producerFetchResult{jobs: jobs}
}

// Periodically logs an informational log line giving some insight into the
// current state of the producer.
func (p *producer) heartbeatLogLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	type jobCount struct {
		ran    uint64
		active int
	}
	var prevCount jobCount
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curCount := jobCount{ran: p.numJobsRan.Load(), active: int(p.numJobsActive.Load())}
			if curCount != prevCount {
				p.Logger.InfoContext(ctx, p.Name+": Producer job counts",
					slog.Uint64("num_completed_jobs", curCount.ran),
					slog.Int("num_jobs_running", curCount.active),
					slog.String("queue", p.config.Queue),
				)
			}
			prevCount = curCount
		}
	}
}

func (p *producer) startNewExecutors(workCtx context.Context, jobs []*rivertype.JobRow) {
	for _, job := range jobs {
		workInfo, ok := p.workers.workersMap[job.Kind]

		var workUnit workunit.WorkUnit
		if ok {
			workUnit = workInfo.workUnitFactory.MakeUnit(job)
		}

		// jobCancel will always be called by the executor to prevent leaks.
		jobCtx, jobCancel := context.WithCancelCause(workCtx)

		executor := baseservice.Init(&p.Archetype, &jobexecutor.JobExecutor{
			CancelFunc:               jobCancel,
			ClientJobTimeout:         p.jobTimeout,
			ClientRetryPolicy:        p.retryPolicy,
			Completer:                p.completer,
			DefaultClientRetryPolicy: &DefaultClientRetryPolicy{},
			ErrorHandler:             p.errorHandler,
			HookLookupByJob:          p.config.HookLookupByJob,
			HookLookupGlobal:         p.config.HookLookupGlobal,
			MiddlewareLookupGlobal:   p.config.MiddlewareLookupGlobal,
			InformProducerDoneFunc:   p.handleWorkerDone,
			JobRow:                   job,
			SchedulerInterval:        p.config.SchedulerInterval,
			WorkUnit:                 workUnit,
		})
		p.addActiveJob(job.ID, executor)

		go executor.Execute(jobCtx)
	}

	p.Logger.DebugContext(workCtx, p.Name+": Distributed batch of jobs to executors", "num_jobs", len(jobs))

	p.testSignals.StartedExecutors.Signal(struct{}{})
}

func (p *producer) maxJobsToFetch() int {
	return p.config.MaxWorkers - int(p.numJobsActive.Load())
}

func (p *producer) handleWorkerDone(job *rivertype.JobRow) {
	p.jobResultCh <- job
}

func (p *producer) pollForSettingChanges(ctx context.Context, wg *sync.WaitGroup, lastPaused bool, lastMetadata []byte) {
	defer wg.Done()

	ticker := time.NewTicker(p.config.QueuePollInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updatedQueue, err := p.fetchQueueSettings(ctx)
			if err != nil {
				p.Logger.ErrorContext(ctx, p.Name+": Error fetching queue settings", slog.String("err", err.Error()))
				continue
			}

			if updatedQueue == nil {
				p.Logger.ErrorContext(ctx, p.Name+": Queue row not found when polling for setting changes", slog.String("queue", p.config.Queue))
				continue
			}

			// Look for a change in the paused state:
			shouldBePaused := (updatedQueue.PausedAt != nil)
			if lastPaused != shouldBePaused {
				action := controlActionPause
				if !shouldBePaused {
					action = controlActionResume
				}
				payload := &controlEventPayload{
					Action: action,
					Queue:  p.config.Queue,
				}
				p.Logger.DebugContext(ctx, p.Name+": Queue control state changed from polling",
					slog.String("queue", p.config.Queue),
					slog.String("action", string(action)),
					slog.Bool("paused", shouldBePaused),
				)

				select {
				case p.queueControlCh <- payload:
					lastPaused = shouldBePaused
				default:
					p.Logger.WarnContext(ctx, p.Name+": Queue control notification dropped due to full buffer", slog.String("action", string(action)))
				}
			}

			// Look for a change in the queue's metadata:
			if !metadataEqual(lastMetadata, updatedQueue.Metadata) {
				payload := &controlEventPayload{
					Action:   controlActionMetadataChanged,
					Queue:    p.config.Queue,
					Metadata: updatedQueue.Metadata,
				}
				p.Logger.DebugContext(ctx, p.Name+": Queue metadata changed from polling",
					slog.String("queue", p.config.Queue),
				)

				select {
				case p.queueControlCh <- payload:
					lastMetadata = updatedQueue.Metadata
				default:
					p.Logger.WarnContext(ctx, p.Name+": Queue control notification dropped due to full buffer", slog.String("action", string(controlActionMetadataChanged)))
				}
			}

			p.testSignals.PolledQueueConfig.Signal(struct{}{})
		}
	}
}

func (p *producer) fetchQueueSettings(ctx context.Context) (*rivertype.Queue, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return p.exec.QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   p.config.Queue,
		Schema: p.config.Schema,
	})
}

func (p *producer) reportProducerStatusLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	serviceutil.CancellableSleep(ctx, randutil.DurationBetween(0, time.Second))
	reportTicker := timeutil.NewTickerWithInitialTick(ctx, p.config.ProducerReportInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-reportTicker.C:
			p.reportProducerStatusOnce(ctx)
		}
	}
}

func (p *producer) reportProducerStatusOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	p.Logger.DebugContext(ctx, p.Name+": Reporting producer status", slog.Int64("id", p.id.Load()), slog.String("queue", p.config.Queue))
	err := p.pilot.ProducerKeepAlive(ctx, p.exec, &riverdriver.ProducerKeepAliveParams{
		ID:                    p.id.Load(),
		QueueName:             p.config.Queue,
		Schema:                p.config.Schema,
		StaleUpdatedAtHorizon: p.Time.NowUTC().Add(-p.config.StaleProducerRetentionPeriod),
	})
	if err != nil && errors.Is(context.Cause(ctx), startstop.ErrStop) {
		return
	}
	if err != nil {
		p.Logger.ErrorContext(ctx, p.Name+": Producer status update, error updating in database",
			slog.Int64("id", p.id.Load()),
			slog.String("queue", p.config.Queue),
			slog.String("err", err.Error()),
		)
		return
	}
	p.testSignals.ReportedProducerStatus.Signal(struct{}{})
}

func (p *producer) reportQueueStatusLoop(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	serviceutil.CancellableSleep(ctx, randutil.DurationBetween(0, time.Second))
	reportTicker := time.NewTicker(p.config.QueueReportInterval)
	for {
		select {
		case <-ctx.Done():
			reportTicker.Stop()
			return
		case <-reportTicker.C:
			p.reportQueueStatusOnce(ctx)
		}
	}
}

func (p *producer) reportQueueStatusOnce(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	p.Logger.DebugContext(ctx, p.Name+": Reporting queue status", slog.String("queue", p.config.Queue))
	_, err := p.exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
		Metadata: []byte("{}"),
		Name:     p.config.Queue,
		Now:      p.Time.NowUTCOrNil(),
		Schema:   p.config.Schema,
	})
	if err != nil && errors.Is(context.Cause(ctx), startstop.ErrStop) {
		return
	}
	if err != nil {
		p.Logger.ErrorContext(ctx, p.Name+": Queue status update, error updating in database", slog.String("err", err.Error()))
		return
	}
	p.testSignals.ReportedQueueStatus.Signal(struct{}{})
}

type producerFetchResult struct {
	jobs []*rivertype.JobRow
	err  error
}

type errorHandlerAdapter struct {
	errorHandler ErrorHandler
}

func (e *errorHandlerAdapter) HandleError(ctx context.Context, job *rivertype.JobRow, err error) *jobexecutor.ErrorHandlerResult {
	result := e.errorHandler.HandleError(ctx, job, err)
	return (*jobexecutor.ErrorHandlerResult)(result)
}

func (e *errorHandlerAdapter) HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *jobexecutor.ErrorHandlerResult {
	result := e.errorHandler.HandlePanic(ctx, job, panicVal, trace)
	return (*jobexecutor.ErrorHandlerResult)(result)
}

// metadataEqual compares two JSON byte slices for semantic equality by parsing
// them into maps and re-marshaling them. This handles cases where the JSON is
// equivalent but formatted differently (whitespace, field order, etc).
func metadataEqual(a, b []byte) bool {
	var unmarshaledA, unmarshaledB map[string]any
	if err := json.Unmarshal(a, &unmarshaledA); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &unmarshaledB); err != nil {
		return false
	}
	marshaledA, err := json.Marshal(unmarshaledA)
	if err != nil {
		return false
	}
	marshaledB, err := json.Marshal(unmarshaledB)
	if err != nil {
		return false
	}
	return bytes.Equal(marshaledA, marshaledB)
}
