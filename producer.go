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
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/chanutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// Test-only properties.
type producerTestSignals struct {
	StartedExecutors rivercommon.TestSignal[struct{}] // notifies when runOnce finishes a pass
}

func (ts *producerTestSignals) Init() {
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
	// control. If nil, the producer will operate in poll only mode.
	Notifier *notifier.Notifier

	Queue             string
	RetryPolicy       ClientRetryPolicy
	SchedulerInterval time.Duration
	StatusFunc        producerStatusUpdateFunc
	Workers           *Workers
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

	numJobsRan  atomic.Uint64
	retryPolicy ClientRetryPolicy
	testSignals producerTestSignals
}

func newProducer(archetype *baseservice.Archetype, exec riverdriver.Executor, config *producerConfig) *producer {
	if archetype == nil {
		panic("archetype is required")
	}
	if exec == nil {
		panic("exec is required")
	}

	return baseservice.Init(archetype, &producer{
		activeJobs:   make(map[int64]*jobExecutor),
		cancelCh:     make(chan int64, 1000),
		completer:    config.Completer,
		config:       config.mustValidate(),
		exec:         exec,
		errorHandler: config.ErrorHandler,
		jobResultCh:  make(chan *rivertype.JobRow, config.MaxWorkers),
		jobTimeout:   config.JobTimeout,
		retryPolicy:  config.RetryPolicy,
		workers:      config.Workers,
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

// Start starts the producer. It backgrounds a goroutine which is stopped when
// context is cancelled or Stop is invoked.
//
// When fetchCtx is cancelled, no more jobs will be fetched; however, if a fetch
// is already in progress, It will be allowed to complete and run any fetched
// jobs. When workCtx is cancelled, any in-progress jobs will have their
// contexts cancelled too.
func (p *producer) StartWorkContext(fetchCtx, workCtx context.Context) error {
	fetchCtx, shouldStart, stopped := p.StartInit(fetchCtx)
	if !shouldStart {
		return nil
	}

	// TODO: fetcher should have some jitter in it to avoid stampeding issues.
	fetchLimiter := chanutil.NewDebouncedChan(fetchCtx, p.config.FetchCooldown, true)

	var (
		insertSub     *notifier.Subscription
		jobControlSub *notifier.Subscription
	)
	if p.config.Notifier == nil {
		p.Logger.Info(p.Name+": No notifier configured; starting in poll mode", "client_id", p.config.ClientID)
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
		// TODO(brandur): Get rid of this retry loop after refactor.
		insertSub, err = notifier.ListenRetryLoop(fetchCtx, &p.BaseService, p.config.Notifier, notifier.NotificationTopicInsert, handleInsertNotification)
		if err != nil {
			close(stopped)
			if strings.HasSuffix(err.Error(), "conn closed") || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		handleJobControlNotification := func(topic notifier.NotificationTopic, payload string) {
			var decoded jobControlPayload
			if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
				p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal job control notification payload", slog.String("err", err.Error()))
				return
			}

			if string(decoded.Action) == string(jobControlActionCancel) && decoded.Queue == p.config.Queue && decoded.JobID > 0 {
				select {
				case p.cancelCh <- decoded.JobID:
				default:
					p.Logger.WarnContext(workCtx, p.Name+": Job cancel notification dropped due to full buffer", slog.Int64("job_id", decoded.JobID))
				}
				return
			}
			p.Logger.DebugContext(workCtx, p.Name+": Received job control notification with unknown action or other queue",
				slog.String("action", string(decoded.Action)),
				slog.Int64("job_id", decoded.JobID),
				slog.String("queue", decoded.Queue),
			)
		}
		// TODO(brandur): Get rid of this retry loop after refactor.
		jobControlSub, err = notifier.ListenRetryLoop(fetchCtx, &p.BaseService, p.config.Notifier, notifier.NotificationTopicJobControl, handleJobControlNotification)
		if err != nil {
			close(stopped)
			if strings.HasSuffix(err.Error(), "conn closed") || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		p.Logger.InfoContext(fetchCtx, p.Name+": Run loop started", slog.String("queue", p.config.Queue))
		defer func() {
			p.Logger.InfoContext(fetchCtx, p.Name+": Run loop stopped", slog.String("queue", p.config.Queue), slog.Uint64("num_completed_jobs", p.numJobsRan.Load()))
		}()

		if insertSub != nil {
			defer insertSub.Unlisten(fetchCtx)
		}

		if jobControlSub != nil {
			defer jobControlSub.Unlisten(fetchCtx)
		}

		go p.heartbeatLogLoop(fetchCtx)

		p.config.StatusFunc(p.config.Queue, componentstatus.Initializing)
		p.fetchAndRunLoop(fetchCtx, workCtx, fetchLimiter)
		p.config.StatusFunc(p.config.Queue, componentstatus.ShuttingDown)
		p.executorShutdownLoop()
		p.config.StatusFunc(p.config.Queue, componentstatus.Stopped)
	}()

	return nil
}

type jobControlAction string

const (
	jobControlActionCancel jobControlAction = "cancel"
)

type jobControlPayload struct {
	Action jobControlAction `json:"action"`
	JobID  int64            `json:"job_id"`
	Queue  string           `json:"queue"`
}

type insertPayload struct {
	Queue string `json:"queue"`
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

type producerFetchResult struct {
	jobs []*rivertype.JobRow
	err  error
}
