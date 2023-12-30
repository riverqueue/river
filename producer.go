package river

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/util/chanutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/rivertype"
)

type producerConfig struct {
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

	JobTimeout        time.Duration
	MaxWorkerCount    uint16
	Notifier          *notifier.Notifier
	QueueName         string
	RetryPolicy       ClientRetryPolicy
	SchedulerInterval time.Duration
	WorkerName        string
	Workers           *Workers
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

	// Jobs which are currently being worked. Only used by main goroutine.
	activeJobs map[int64]*jobExecutor

	adapter      dbadapter.Adapter
	completer    jobcompleter.JobCompleter
	config       *producerConfig
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
}

func newProducer(archetype *baseservice.Archetype, adapter dbadapter.Adapter, completer jobcompleter.JobCompleter, config *producerConfig) (*producer, error) {
	if adapter == nil {
		return nil, errors.New("Adapter is required") //nolint:stylecheck
	}
	if completer == nil {
		return nil, errors.New("Completer is required") //nolint:stylecheck
	}

	if config.FetchCooldown <= 0 {
		return nil, errors.New("FetchCooldown must be great than zero")
	}
	if config.FetchPollInterval <= 0 {
		return nil, errors.New("FetchPollInterval must be greater than zero")
	}
	if config.JobTimeout < -1 {
		return nil, errors.New("JobTimeout must be greater or equal to zero")
	}
	if config.MaxWorkerCount == 0 {
		return nil, errors.New("MaxWorkerCount is required")
	}
	if config.Notifier == nil {
		return nil, errors.New("Notifier is required") //nolint:stylecheck
	}
	if config.QueueName == "" {
		return nil, errors.New("QueueName is required")
	}
	if config.RetryPolicy == nil {
		return nil, errors.New("RetryPolicy is required")
	}
	if config.SchedulerInterval == 0 {
		return nil, errors.New("SchedulerInterval is required")
	}
	if config.WorkerName == "" {
		return nil, errors.New("WorkerName is required")
	}
	if config.Workers == nil {
		return nil, errors.New("Workers is required")
	}

	return baseservice.Init(archetype, &producer{
		activeJobs:   make(map[int64]*jobExecutor),
		adapter:      adapter,
		cancelCh:     make(chan int64, 1000),
		completer:    completer,
		config:       config,
		errorHandler: config.ErrorHandler,
		jobResultCh:  make(chan *rivertype.JobRow, config.MaxWorkerCount),
		jobTimeout:   config.JobTimeout,
		retryPolicy:  config.RetryPolicy,
		workers:      config.Workers,
	}), nil
}

type producerStatusUpdateFunc func(queue string, status componentstatus.Status)

// Run starts the producer. It blocks until the producer has completed
// graceful shutdown.
//
// When fetchCtx is cancelled, no more jobs will be fetched; however, if a fetch
// is already in progress, It will be allowed to complete and run any fetched
// jobs. When workCtx is cancelled, any in-progress jobs will have their
// contexts cancelled too.
func (p *producer) Run(fetchCtx, workCtx context.Context, statusFunc producerStatusUpdateFunc) {
	p.Logger.InfoContext(workCtx, p.Name+": Producer started", slog.String("queue", p.config.QueueName))
	defer func() {
		p.Logger.InfoContext(workCtx, p.Name+": Producer stopped", slog.String("queue", p.config.QueueName), slog.Uint64("num_completed_jobs", p.numJobsRan.Load()))
	}()

	go p.heartbeatLogLoop(fetchCtx)

	statusFunc(p.config.QueueName, componentstatus.Initializing)
	// TODO: fetcher should have some jitter in it to avoid stampeding issues.
	fetchLimiter := chanutil.NewDebouncedChan(fetchCtx, p.config.FetchCooldown)

	handleJobControlNotification := func(topic notifier.NotificationTopic, payload string) {
		var decoded jobControlPayload
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal job control notification payload", slog.String("err", err.Error()))
			return
		}
		if string(decoded.Action) == string(jobControlActionCancel) && decoded.Queue == p.config.QueueName && decoded.JobID > 0 {
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
	sub := p.config.Notifier.Listen(notifier.NotificationTopicJobControl, handleJobControlNotification)
	defer sub.Unlisten()

	p.fetchAndRunLoop(fetchCtx, workCtx, fetchLimiter, statusFunc)
	statusFunc(p.config.QueueName, componentstatus.ShuttingDown)
	p.executorShutdownLoop()
	statusFunc(p.config.QueueName, componentstatus.Stopped)
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

func (p *producer) fetchAndRunLoop(fetchCtx, workCtx context.Context, fetchLimiter *chanutil.DebouncedChan, statusFunc producerStatusUpdateFunc) {
	p.Logger.InfoContext(workCtx, p.Name+": Run loop started")
	defer p.Logger.InfoContext(workCtx, p.Name+": Run loop stopped")

	// Prime the fetchLimiter so we can make an initial fetch without waiting for
	// an insert notification or a fetch poll.
	fetchLimiter.Call()

	handleInsertNotification := func(topic notifier.NotificationTopic, payload string) {
		var decoded insertPayload
		if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
			p.Logger.ErrorContext(workCtx, p.Name+": Failed to unmarshal insert notification payload", slog.String("err", err.Error()))
			return
		}
		if decoded.Queue != p.config.QueueName {
			return
		}
		p.Logger.DebugContext(workCtx, p.Name+": Received insert notification", slog.String("queue", decoded.Queue))
		fetchLimiter.Call()
	}
	sub := p.config.Notifier.Listen(notifier.NotificationTopicInsert, handleInsertNotification)
	defer sub.Unlisten()

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

	statusFunc(p.config.QueueName, componentstatus.Healthy)

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
	count := p.maxJobsToFetch()
	go p.dispatchWork(count, fetchResultCh) //nolint:contextcheck

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

func (p *producer) dispatchWork(count int32, jobsFetchedCh chan<- producerFetchResult) {
	// This intentionally uses a background context because we don't want it to
	// get cancelled if the producer is asked to shut down. In that situation, we
	// want to finish fetching any jobs we are in the midst of fetching, work
	// them, and then stop. Otherwise we'd have a risk of shutting down when we
	// had already fetched jobs in the database, leaving those jobs stranded. We'd
	// then potentially have to release them back to the queue.
	internalJobs, err := p.adapter.JobGetAvailable(context.Background(), p.config.QueueName, count)
	if err != nil {
		jobsFetchedCh <- producerFetchResult{err: err}
		return
	}
	jobs := sliceutil.Map(internalJobs, dbsqlc.JobRowFromInternal)
	jobsFetchedCh <- producerFetchResult{jobs: jobs}
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
				slog.String("queue", p.config.QueueName),
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

		jobCtx, jobCancel := context.WithCancelCause(workCtx)

		executor := baseservice.Init(&p.Archetype, &jobExecutor{
			Adapter:                p.adapter,
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
		// TODO:
		// Errors can be recorded synchronously before the Executor slot is considered
		// available.
		//
		// Successful jobs can be sent to the completer for async acking, IF they
		// aren't already completed by the user. Do we need an internal field +
		// convenience method to make that part work?
	}
}

func (p *producer) maxJobsToFetch() int32 {
	return int32(p.config.MaxWorkerCount) - p.numJobsActive.Load()
}

func (p *producer) handleWorkerDone(job *rivertype.JobRow) {
	p.jobResultCh <- job
}

type producerFetchResult struct {
	jobs []*rivertype.JobRow
	err  error
}
