package river

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/startstop"
	"github.com/riverqueue/river/internal/util/chanutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

const (
	queuePollIntervalDefault   = 2 * time.Second
	queueReportIntervalDefault = 10 * time.Minute
)

// Test-only properties.
type producerTestSignals struct {
	DeletedExpiredQueueRecords rivercommon.TestSignal[struct{}] // notifies when the producer deletes expired queue records
	Paused                     rivercommon.TestSignal[struct{}] // notifies when the producer is paused
	PolledQueueConfig          rivercommon.TestSignal[struct{}] // notifies when the producer polls for queue settings
	ReportedQueueStatus        rivercommon.TestSignal[struct{}] // notifies when the producer reports queue status
	Resumed                    rivercommon.TestSignal[struct{}] // notifies when the producer is resumed
	StartedExecutors           rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *producerTestSignals) Init() {
	ts.DeletedExpiredQueueRecords.Init()
	ts.Paused.Init()
	ts.PolledQueueConfig.Init()
	ts.ReportedQueueStatus.Init()
	ts.Resumed.Init()
	ts.StartedExecutors.Init()
}

type producerConfig struct {
	ClientID     string
	Completer    jobcompleter.JobCompleter
	ErrorHandler ErrorHandler

	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then feches may be delayed as long as
	// FetchPollInterval.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	FetchPollInterval time.Duration

	JobTimeout time.Duration
	MaxWorkers int

	// Notifier is a notifier for subscribing to new job inserts and job
	// control. If nil, the producer will operate in poll-only mode.
	Notifier *notifier.Notifier

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
	QueueReportInterval time.Duration
	RetryPolicy         ClientRetryPolicy
	SchedulerInterval   time.Duration
	StatusFunc          producerStatusUpdateFunc
	Workers             *Workers
}

func (c *producerConfig) mustValidate() *producerConfig {
	if c.Completer == nil {
		panic("producerConfig.Completer is required")
	}
	if c.ClientID == "" {
		panic("producerConfig.ClientName is required")
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
	if c.StatusFunc == nil {
		panic("producerConfig.StatusFunc is required")
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
	activeJobs map[int64]*jobExecutor

	completer    jobcompleter.JobCompleter
	config       *producerConfig
	exec         riverdriver.Executor
	errorHandler ErrorHandler
	workers      *Workers

	// Receives job IDs to cancel. Written by notifier goroutine, only read from
	// main goroutine.
	cancelCh chan int64

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
	queueControlCh chan *jobControlPayload
	retryPolicy    ClientRetryPolicy
	testSignals    producerTestSignals
}

func newProducer(archetype *baseservice.Archetype, exec riverdriver.Executor, config *producerConfig) *producer {
	if archetype == nil {
		panic("archetype is required")
	}
	if exec == nil {
		panic("exec is required")
	}

	return baseservice.Init(archetype, &producer{
		activeJobs:     make(map[int64]*jobExecutor),
		cancelCh:       make(chan int64, 1000),
		completer:      config.Completer,
		config:         config.mustValidate(),
		exec:           exec,
		errorHandler:   config.ErrorHandler,
		jobResultCh:    make(chan *rivertype.JobRow, config.MaxWorkers),
		jobTimeout:     config.JobTimeout,
		queueControlCh: make(chan *jobControlPayload, 100),
		retryPolicy:    config.RetryPolicy,
		workers:        config.Workers,
	})
}

type producerStatusUpdateFunc func(queue string, status componentstatus.Status)

// Start starts the producer. It backgrounds a goroutine which is stopped when
// context is cancelled or Stop is invoked.
//
// This variant uses a single context as fetchCtx and workCtx, and is here to
// implement startstop.Service so that the producer can be stored as a service
// variable and used with various serviec utilties. StartWorkContext below
// should be preferred for production use.
func (p *producer) Start(ctx context.Context) error {
	return p.StartWorkContext(ctx, ctx)
}

func (p *producer) Stop() {
	p.Logger.Debug(p.Name + ": Stopping")
	p.BaseStartStop.Stop()
	p.Logger.Debug(p.Name + ": Stop returned")
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

	queue, err := func() (*rivertype.Queue, error) {
		ctx, cancel := context.WithTimeout(fetchCtx, 10*time.Second)
		defer cancel()

		p.Logger.DebugContext(ctx, p.Name+": Fetching initial queue settings", slog.String("queue", p.config.Queue))
		return p.exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
			Metadata: []byte("{}"),
			Name:     p.config.Queue,
		})
	}()
	if err != nil {
		stopped()
		if errors.Is(err, startstop.ErrStop) || strings.HasSuffix(err.Error(), "conn closed") || fetchCtx.Err() != nil {
			return nil //nolint:nilerr
		}
		p.Logger.ErrorContext(fetchCtx, p.Name+": Error fetching initial queue settings", slog.String("err", err.Error()))
		return err
	}

	initiallyPaused := queue != nil && (queue.PausedAt != nil)
	p.paused = initiallyPaused

	// TODO: fetcher should have some jitter in it to avoid stampeding issues.
	fetchLimiter := chanutil.NewDebouncedChan(fetchCtx, p.config.FetchCooldown, true)

	var (
		controlSub *notifier.Subscription
		insertSub  *notifier.Subscription
	)
	if p.config.Notifier == nil {
		p.Logger.InfoContext(fetchCtx, p.Name+": No notifier configured; starting in poll mode", "client_id", p.config.ClientID)

		go p.pollForSettingChanges(fetchCtx, initiallyPaused)
	} else {
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

		// TODO(bgentry): this should probably happen even earlier in this start
		// function, but need to figure out how handle defers and shutdown properly.
		p.config.StatusFunc(p.config.Queue, componentstatus.Initializing)

		go p.heartbeatLogLoop(fetchCtx)
		go p.reportQueueStatusLoop(fetchCtx)
		p.fetchAndRunLoop(fetchCtx, workCtx, fetchLimiter)

		p.config.StatusFunc(p.config.Queue, componentstatus.ShuttingDown)
		p.executorShutdownLoop()
		p.config.StatusFunc(p.config.Queue, componentstatus.Stopped)
	}()

	return nil
}

type controlAction string

const (
	controlActionCancel controlAction = "cancel"
	controlActionPause  controlAction = "pause"
	controlActionResume controlAction = "resume"
)

type jobControlPayload struct {
	Action controlAction `json:"action"`
	JobID  int64         `json:"job_id"`
	Queue  string        `json:"queue"`
}

type insertPayload struct {
	Queue string `json:"queue"`
}

func (p *producer) handleControlNotification(workCtx context.Context) func(notifier.NotificationTopic, string) {
	return func(topic notifier.NotificationTopic, payload string) {
		var decoded jobControlPayload
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal job control notification payload", slog.String("err", err.Error()))
			return
		}

		switch decoded.Action {
		case controlActionPause, controlActionResume:
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

	p.config.StatusFunc(p.config.Queue, componentstatus.Healthy)

	fetchResultCh := make(chan producerFetchResult)
	for {
		select {
		case <-fetchCtx.Done():
			return
		case msg := <-p.queueControlCh:
			switch msg.Action {
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
				p.testSignals.Resumed.Signal(struct{}{})
				if p.config.QueueEventCallback != nil {
					p.config.QueueEventCallback(&Event{Kind: EventKindQueueResumed, Queue: &rivertype.Queue{Name: p.config.Queue}})
				}
			case controlActionCancel:
				// Separate this case to make linter happy:
				p.Logger.DebugContext(workCtx, p.Name+": Unhandled queue control action", "action", msg.Action)
			default:
				p.Logger.DebugContext(workCtx, p.Name+": Unknown queue control action", "action", msg.Action)
			}
		case <-fetchLimiter.C():
			if p.paused {
				continue
			}
			p.innerFetchLoop(workCtx, fetchResultCh)
			// Ensure we can't start another fetch when fetchCtx is done, even if
			// the fetchLimiter is also ready to fire:
			select {
			case <-fetchCtx.Done():
				return
			default:
			}
		case result := <-p.jobResultCh:
			p.removeActiveJob(result.ID)
		}
	}
}

func (p *producer) innerFetchLoop(workCtx context.Context, fetchResultCh chan producerFetchResult) {
	limit := p.maxJobsToFetch()
	go p.dispatchWork(limit, fetchResultCh) //nolint:contextcheck

	for {
		select {
		case result := <-fetchResultCh:
			if result.err != nil {
				p.Logger.ErrorContext(workCtx, p.Name+": Error fetching jobs", slog.String("err", result.err.Error()))
			} else if len(result.jobs) > 0 {
				p.startNewExecutors(workCtx, result.jobs)
			}
			return
		case result := <-p.jobResultCh:
			p.removeActiveJob(result.ID)
		case jobID := <-p.cancelCh:
			p.maybeCancelJob(jobID)
		}
	}
}

func (p *producer) executorShutdownLoop() {
	// No more jobs will be fetched or executed. However, we must wait for all
	// in-progress jobs to complete.
	for {
		if len(p.activeJobs) == 0 {
			break
		}
		result := <-p.jobResultCh
		p.removeActiveJob(result.ID)
	}
}

func (p *producer) addActiveJob(id int64, executor *jobExecutor) {
	p.numJobsActive.Add(1)
	p.activeJobs[id] = executor
}

func (p *producer) removeActiveJob(id int64) {
	delete(p.activeJobs, id)
	p.numJobsActive.Add(-1)
	p.numJobsRan.Add(1)
}

func (p *producer) maybeCancelJob(id int64) {
	executor, ok := p.activeJobs[id]
	if !ok {
		return
	}
	executor.Cancel()
}

func (p *producer) dispatchWork(count int, fetchResultCh chan<- producerFetchResult) {
	// This intentionally uses a background context because we don't want it to
	// get cancelled if the producer is asked to shut down. In that situation, we
	// want to finish fetching any jobs we are in the midst of fetching, work
	// them, and then stop. Otherwise we'd have a risk of shutting down when we
	// had already fetched jobs in the database, leaving those jobs stranded. We'd
	// then potentially have to release them back to the queue.
	jobs, err := p.exec.JobGetAvailable(context.Background(), &riverdriver.JobGetAvailableParams{
		AttemptedBy: p.config.ClientID,
		Max:         count,
		Queue:       p.config.Queue,
	})
	if err != nil {
		fetchResultCh <- producerFetchResult{err: err}
		return
	}
	fetchResultCh <- producerFetchResult{jobs: jobs}
}

// Periodically logs an informational log line giving some insight into the
// current state of the producer.
func (p *producer) heartbeatLogLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Logger.InfoContext(ctx, p.Name+": Heartbeat",
				slog.Uint64("num_completed_jobs", p.numJobsRan.Load()),
				slog.Int("num_jobs_running", int(p.numJobsActive.Load())),
				slog.String("queue", p.config.Queue),
			)
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

		executor := baseservice.Init(&p.Archetype, &jobExecutor{
			CancelFunc:             jobCancel,
			ClientJobTimeout:       p.jobTimeout,
			ClientRetryPolicy:      p.retryPolicy,
			Completer:              p.completer,
			ErrorHandler:           p.errorHandler,
			InformProducerDoneFunc: p.handleWorkerDone,
			JobRow:                 job,
			SchedulerInterval:      p.config.SchedulerInterval,
			WorkUnit:               workUnit,
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

func (p *producer) pollForSettingChanges(ctx context.Context, lastPaused bool) {
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
			shouldBePaused := (updatedQueue.PausedAt != nil)
			if lastPaused != shouldBePaused {
				action := controlActionPause
				if !shouldBePaused {
					action = controlActionResume
				}
				payload := &jobControlPayload{
					Action: action,
					Queue:  p.config.Queue,
				}
				p.Logger.InfoContext(ctx, p.Name+": Queue control state changed from polling",
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
			p.testSignals.PolledQueueConfig.Signal(struct{}{})
		}
	}
}

func (p *producer) fetchQueueSettings(ctx context.Context) (*rivertype.Queue, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	return p.exec.QueueGet(ctx, p.config.Queue)
}

func (p *producer) reportQueueStatusLoop(ctx context.Context) {
	p.CancellableSleepRandomBetween(ctx, 0, time.Second)
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
