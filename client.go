package river

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/dbadapter"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

const (
	FetchCooldownDefault = 100 * time.Millisecond
	FetchCooldownMin     = 1 * time.Millisecond

	FetchPollIntervalDefault = 1 * time.Second
	FetchPollIntervalMin     = 1 * time.Millisecond

	JobTimeoutDefault  = 1 * time.Minute
	MaxAttemptsDefault = rivercommon.MaxAttemptsDefault
	PriorityDefault    = rivercommon.PriorityDefault
	QueueDefault       = rivercommon.QueueDefault
	QueueNumWorkersMax = 10_000
)

// Config is the configuration for a Client.
type Config struct {
	// AdvisoryLockPrefix is a configurable 32-bit prefix that River will use
	// when generating any key to acquire a Postgres advisory lock. All advisory
	// locks share the same 64-bit number space, so this allows a calling
	// application to guarantee that a River advisory lock will never conflict
	// with one of its own by cordoning each type to its own prefix.
	//
	// If this value isn't set, River defaults to generating key hashes across
	// the entire 64-bit advisory lock number space, which is large enough that
	// conflicts are exceedingly unlikely. If callers don't strictly need this
	// option then it's recommended to leave it unset because the prefix leaves
	// only 32 bits of number space for advisory lock hashes, so it makes
	// internally conflicting River-generated keys more likely.
	AdvisoryLockPrefix int32

	// CancelledJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	//
	// Defaults to 24 hours.
	CancelledJobRetentionPeriod time.Duration

	// CompletedJobRetentionPeriod is the amount of time to keep completed jobs
	// around before they're removed permanently.
	//
	// Defaults to 24 hours.
	CompletedJobRetentionPeriod time.Duration

	// DiscardedJobRetentionPeriod is the amount of time to keep discarded jobs
	// around before they're removed permanently.
	//
	// Defaults to 7 days.
	DiscardedJobRetentionPeriod time.Duration

	// ErrorHandler can be configured to be invoked in case of an error or panic
	// occurring in a job. This is often useful for logging and exception
	// tracking, but can also be used to customize retry behavior.
	ErrorHandler ErrorHandler

	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then feches may be delayed as long as
	// FetchPollInterval.
	//
	// Throughput is limited by this value.
	//
	// Defaults to 100 ms.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	//
	// Defaults to 1 second.
	FetchPollInterval time.Duration

	// JobTimeout is the maximum amount of time a job is allowed to run before its
	// context is cancelled. A timeout of zero means JobTimeoutDefault will be
	// used, whereas a value of -1 means the job's context will not be cancelled
	// unless the Client is shutting down.
	//
	// Defaults to 1 minute.
	JobTimeout time.Duration

	// Logger is the structured logger to use for logging purposes. If none is
	// specified, logs will be emitted to STDOUT with messages at warn level
	// or higher.
	Logger *slog.Logger

	// PeriodicJobs are a set of periodic jobs to run at the specified intervals
	// in the client.
	PeriodicJobs []*PeriodicJob

	// Queues is a list of queue names for this client to operate on along with
	// configuration for the queue like the maximum number of workers to run for
	// each queue.
	//
	// This field may be omitted for a program that's only queueing jobs rather
	// than working them. If it's specified, then Workers must also be given.
	Queues map[string]QueueConfig

	// ReindexerSchedule is the schedule for running the reindexer. If nil, the
	// reindexer will run at midnight UTC every day.
	ReindexerSchedule PeriodicSchedule

	// RescueStuckJobsAfter is the amount of time a job can be running before it
	// is considered stuck. A stuck job which has not yet reached its max attempts
	// will be scheduled for a retry, while one which has exhausted its attempts
	// will be discarded.  This prevents jobs from being stuck forever if a worker
	// crashes or is killed.
	//
	// Note that this can result in repeat or duplicate execution of a job that is
	// not actually stuck but is still working. The value should be set higher
	// than the maximum duration you expect your jobs to run. Setting a value too
	// low will result in more duplicate executions, whereas too high of a value
	// will result in jobs being stuck for longer than necessary before they are
	// retried.
	//
	// RescueStuckJobsAfter must be greater than JobTimeout. Otherwise, jobs
	// would become eligible for rescue while they're still running.
	//
	// Defaults to 1 hour, or in cases where JobTimeout has been configured and
	// is greater than 1 hour, JobTimeout + 1 hour.
	RescueStuckJobsAfter time.Duration

	// RetryPolicy is a configurable retry policy for the client.
	//
	// Defaults to DefaultRetryPolicy.
	RetryPolicy ClientRetryPolicy

	// Workers is a bundle of registered job workers.
	//
	// This field may be omitted for a program that's only enqueueing jobs
	// rather than working them, but if it is configured the client can validate
	// ahead of time that a worker is properly registered for an inserted job.
	// (i.e.  That it wasn't forgotten by accident.)
	Workers *Workers

	// Test-only property that allows sleep statements to be disable. Only
	// functions in cases where the CancellableSleep helper is used to sleep.
	disableSleep bool

	// Scheduler run interval. Shared between the scheduler and producer/job
	// executors, but not currently exposed for configuration.
	schedulerInterval time.Duration
}

func (c *Config) validate() error {
	if c.CancelledJobRetentionPeriod < 0 {
		return errors.New("CancelledJobRetentionPeriod time cannot be less than zero")
	}
	if c.CompletedJobRetentionPeriod < 0 {
		return errors.New("CompletedJobRetentionPeriod cannot be less than zero")
	}
	if c.DiscardedJobRetentionPeriod < 0 {
		return errors.New("DiscardedJobRetentionPeriod cannot be less than zero")
	}
	if c.FetchCooldown < FetchCooldownMin {
		return fmt.Errorf("FetchCooldown must be at least %s", FetchCooldownMin)
	}
	if c.FetchPollInterval < FetchPollIntervalMin {
		return fmt.Errorf("FetchPollInterval must be at least %s", FetchPollIntervalMin)
	}
	if c.FetchPollInterval < c.FetchCooldown {
		return fmt.Errorf("FetchPollInterval cannot be shorter than FetchCooldown (%s)", c.FetchCooldown)
	}
	if c.JobTimeout < -1 {
		return errors.New("JobTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.RescueStuckJobsAfter < 0 {
		return errors.New("RescueStuckJobsAfter cannot be less than zero")
	}
	if c.RescueStuckJobsAfter < c.JobTimeout {
		return errors.New("RescueStuckJobsAfter cannot be less than JobTimeout")
	}

	for queue, queueConfig := range c.Queues {
		if queueConfig.MaxWorkers < 1 || queueConfig.MaxWorkers > QueueNumWorkersMax {
			return fmt.Errorf("invalid number of workers for queue %q: %d", queue, queueConfig.MaxWorkers)
		}
		if err := validateQueueName(queue); err != nil {
			return err
		}
	}

	if c.Workers == nil && c.Queues != nil {
		return errors.New("Workers must be set if Queues is set")
	}

	return nil
}

// Indicates whether with the given configuration, this client will be expected
// to execute jobs (rather than just being used to enqueue them). Executing jobs
// requires a set of configured queues.
func (c *Config) willExecuteJobs() bool {
	return len(c.Queues) > 0
}

// QueueConfig contains queue-specific configuration.
type QueueConfig struct {
	// MaxWorkers is the maximum number of workers to run for the queue, or put
	// otherwise, the maximum parallelism to run.
	//
	// This is the maximum number of workers within this particular client
	// instance, but note that it doesn't control the total number of workers
	// across parallel processes. Installations will want to calculate their
	// total number by multiplying this number by the number of parallel nodes
	// running River clients configured to the same database and queue.
	//
	// Requires a minimum of 1, and a maximum of 10,000.
	MaxWorkers int
}

// Client is a single isolated instance of River. Your application may use
// multiple instances operating on different databases or Postgres schemas
// within a single database.
type Client[TTx any] struct {
	adapter dbadapter.Adapter

	// BaseService can't be embedded like on other services because its
	// properties would leak to the external API.
	baseService baseservice.BaseService

	completer jobcompleter.JobCompleter
	config    *Config
	driver    riverdriver.Driver[TTx]
	elector   *leadership.Elector

	// fetchNewWorkCancel cancels the context used for fetching new work. This
	// will be used to stop fetching new work whenever stop is initiated, or
	// when the context provided to Run is itself cancelled.
	fetchNewWorkCancel context.CancelFunc

	id                   string
	monitor              *clientMonitor
	notifier             *notifier.Notifier
	producersByQueueName map[string]*producer
	queueMaintainer      *maintenance.QueueMaintainer
	subscriptions        map[int]*eventSubscription
	subscriptionsMu      sync.Mutex
	subscriptionsSeq     int // used for generating simple IDs
	stopComplete         chan struct{}
	statsAggregate       jobstats.JobStatistics
	statsMu              sync.Mutex
	statsNumJobs         int
	testSignals          clientTestSignals
	wg                   sync.WaitGroup

	// workCancel cancels the context used for all work goroutines. Normal Stop
	// does not cancel that context.
	workCancel context.CancelFunc
}

// Test-only signals.
type clientTestSignals struct {
	electedLeader rivercommon.TestSignal[struct{}] // notifies when elected leader

	jobCleaner          *maintenance.JobCleanerTestSignals
	periodicJobEnqueuer *maintenance.PeriodicJobEnqueuerTestSignals
	reindexer           *maintenance.ReindexerTestSignals
	rescuer             *maintenance.RescuerTestSignals
	scheduler           *maintenance.SchedulerTestSignals
}

func (ts *clientTestSignals) Init() {
	ts.electedLeader.Init()

	if ts.jobCleaner != nil {
		ts.jobCleaner.Init()
	}
	if ts.periodicJobEnqueuer != nil {
		ts.periodicJobEnqueuer.Init()
	}
	if ts.reindexer != nil {
		ts.reindexer.Init()
	}
	if ts.rescuer != nil {
		ts.rescuer.Init()
	}
	if ts.scheduler != nil {
		ts.scheduler.Init()
	}
}

var (
	errMissingConfig                 = errors.New("missing config")
	errMissingDatabasePoolWithQueues = errors.New("must have a non-nil database pool to execute jobs (either use a driver with database pool or don't configure Queues)")
	errMissingDriver                 = errors.New("missing database driver (try wrapping a Pgx pool with river/riverdriver/riverpgxv5.New)")
)

// NewClient creates a new Client with the given database driver and
// configuration.
//
// Currently only one driver is supported, which is Pgx v5. See package
// riverpgxv5.
//
// The function takes a generic parameter TTx representing a transaction type,
// but it can be omitted because it'll generally always be inferred from the
// driver. For example:
//
//	import "github.com/riverqueue/river"
//	import "github.com/riverqueue/river/riverdriver/riverpgxv5"
//
//	...
//
//	dbPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	if err != nil {
//		// handle error
//	}
//	defer dbPool.Close()
//
//	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
//		...
//	})
//	if err != nil {
//		// handle error
//	}
func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error) {
	if driver == nil {
		return nil, errMissingDriver
	}
	if config == nil {
		return nil, errMissingConfig
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	retryPolicy := config.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = &DefaultClientRetryPolicy{}
	}

	// For convenience, in case the user's specified a large JobTimeout but no
	// RescueStuckJobsAfter, since RescueStuckJobsAfter must be greater than
	// JobTimeout, set a reasonable default value that's longer thah JobTimeout.
	rescueAfter := maintenance.RescueAfterDefault
	if config.JobTimeout > 0 && config.RescueStuckJobsAfter < 1 && config.JobTimeout > config.RescueStuckJobsAfter {
		rescueAfter = config.JobTimeout + maintenance.RescueAfterDefault
	}

	// Create a new version of config with defaults filled in. This replaces the
	// original object, so everything that we care about must be initialized
	// here, even if it's only carrying over the original value.
	config = &Config{
		AdvisoryLockPrefix:          config.AdvisoryLockPrefix,
		CancelledJobRetentionPeriod: valutil.ValOrDefault(config.CancelledJobRetentionPeriod, maintenance.CancelledJobRetentionPeriodDefault),
		CompletedJobRetentionPeriod: valutil.ValOrDefault(config.CompletedJobRetentionPeriod, maintenance.CompletedJobRetentionPeriodDefault),
		DiscardedJobRetentionPeriod: valutil.ValOrDefault(config.DiscardedJobRetentionPeriod, maintenance.DiscardedJobRetentionPeriodDefault),
		ErrorHandler:                config.ErrorHandler,
		FetchCooldown:               valutil.ValOrDefault(config.FetchCooldown, FetchCooldownDefault),
		FetchPollInterval:           valutil.ValOrDefault(config.FetchPollInterval, FetchPollIntervalDefault),
		JobTimeout:                  valutil.ValOrDefault(config.JobTimeout, JobTimeoutDefault),
		Logger:                      logger,
		PeriodicJobs:                config.PeriodicJobs,
		Queues:                      config.Queues,
		ReindexerSchedule:           config.ReindexerSchedule,
		RescueStuckJobsAfter:        valutil.ValOrDefault(config.RescueStuckJobsAfter, rescueAfter),
		RetryPolicy:                 retryPolicy,
		Workers:                     config.Workers,
		disableSleep:                config.disableSleep,
		schedulerInterval:           valutil.ValOrDefault(config.schedulerInterval, maintenance.SchedulerIntervalDefault),
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	clientID, err := ulid.New(ulid.Now(), rand.Reader)
	if err != nil {
		return nil, err
	}

	archetype := &baseservice.Archetype{
		DisableSleep: config.disableSleep,
		Logger:       config.Logger,
		TimeNowUTC:   func() time.Time { return time.Now().UTC() },
	}

	adapter := dbadapter.NewStandardAdapter(archetype, &dbadapter.StandardAdapterConfig{
		AdvisoryLockPrefix: config.AdvisoryLockPrefix,
		DeadlineTimeout:    5 * time.Second, // not exposed in client configuration for now, but we may want to do so
		Executor:           driver.GetDBPool(),
		WorkerName:         clientID.String(),
	})

	completer := jobcompleter.NewAsyncCompleter(archetype, adapter, 100)

	client := &Client[TTx]{
		adapter:              adapter,
		completer:            completer,
		config:               config,
		driver:               driver,
		id:                   clientID.String(),
		monitor:              newClientMonitor(),
		producersByQueueName: make(map[string]*producer),
		stopComplete:         make(chan struct{}),
		subscriptions:        make(map[int]*eventSubscription),
		testSignals:          clientTestSignals{},
	}

	baseservice.Init(archetype, &client.baseService)
	client.baseService.Name = reflect.TypeOf(Client[TTx]{}).Name() // Have to correct the name because base service isn't embedded like it usually is

	// There are a number of internal components that are only needed/desired if
	// we're actually going to be working jobs (as opposed to just enqueueing
	// them):
	if config.willExecuteJobs() {
		if driver.GetDBPool() == nil {
			return nil, errMissingDatabasePoolWithQueues
		}

		// TODO: for now we only support a single instance per database/schema.
		// If we want to provide isolation within a single database/schema,
		// we'll need to add a config for this.
		instanceName := "default"

		client.notifier = notifier.New(archetype, driver.GetDBPool().Config().ConnConfig, client.monitor.SetNotifierStatus, logger)
		var err error
		client.elector, err = leadership.NewElector(client.adapter, client.notifier, instanceName, client.id, 5*time.Second, logger)
		if err != nil {
			return nil, err
		}

		if err := client.provisionProducers(); err != nil {
			return nil, err
		}

		//
		// Maintenance services
		//

		maintenanceServices := []maintenance.Service{}

		{
			jobCleaner := maintenance.NewJobCleaner(archetype, &maintenance.JobCleanerConfig{
				CancelledJobRetentionPeriod: config.CancelledJobRetentionPeriod,
				CompletedJobRetentionPeriod: config.CompletedJobRetentionPeriod,
				DiscardedJobRetentionPeriod: config.DiscardedJobRetentionPeriod,
			}, driver.GetDBPool())
			maintenanceServices = append(maintenanceServices, jobCleaner)
			client.testSignals.jobCleaner = &jobCleaner.TestSignals
		}

		{
			emptyOpts := PeriodicJobOpts{}
			periodicJobs := make([]*maintenance.PeriodicJob, 0, len(config.PeriodicJobs))
			for _, periodicJob := range config.PeriodicJobs {
				periodicJob := periodicJob // capture range var

				opts := &emptyOpts
				if periodicJob.opts != nil {
					opts = periodicJob.opts
				}

				periodicJobs = append(periodicJobs, &maintenance.PeriodicJob{
					ConstructorFunc: func() (*dbadapter.JobInsertParams, error) {
						return insertParamsFromArgsAndOptions(periodicJob.constructorFunc())
					},
					RunOnStart:   opts.RunOnStart,
					ScheduleFunc: periodicJob.scheduleFunc.Next,
				})
			}

			periodicJobEnqueuer := maintenance.NewPeriodicJobEnqueuer(archetype, &maintenance.PeriodicJobEnqueuerConfig{
				PeriodicJobs: periodicJobs,
			}, adapter)
			maintenanceServices = append(maintenanceServices, periodicJobEnqueuer)
			client.testSignals.periodicJobEnqueuer = &periodicJobEnqueuer.TestSignals
		}

		{
			var scheduleFunc func(time.Time) time.Time
			if config.ReindexerSchedule != nil {
				scheduleFunc = config.ReindexerSchedule.Next
			}

			reindexer := maintenance.NewReindexer(archetype, &maintenance.ReindexerConfig{ScheduleFunc: scheduleFunc}, driver.GetDBPool())
			maintenanceServices = append(maintenanceServices, reindexer)
			client.testSignals.reindexer = &reindexer.TestSignals
		}

		{
			rescuer := maintenance.NewRescuer(archetype, &maintenance.RescuerConfig{
				ClientRetryPolicy: retryPolicy,
				RescueAfter:       config.RescueStuckJobsAfter,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if workerInfo, ok := config.Workers.workersMap[kind]; ok {
						return workerInfo.workUnitFactory
					}
					return nil
				},
			}, driver.GetDBPool())
			maintenanceServices = append(maintenanceServices, rescuer)
			client.testSignals.rescuer = &rescuer.TestSignals
		}

		{
			scheduler := maintenance.NewScheduler(archetype, &maintenance.SchedulerConfig{
				Interval: config.schedulerInterval,
			}, driver.GetDBPool())
			maintenanceServices = append(maintenanceServices, scheduler)
			client.testSignals.scheduler = &scheduler.TestSignals
		}

		client.queueMaintainer = maintenance.NewQueueMaintainer(archetype, maintenanceServices)
	}

	return client, nil
}

// Start starts the client's job fetching and working loops. Once this is called,
// the client will run in a background goroutine until stopped. All jobs are
// run with a context inheriting from the provided context, but with a timeout
// deadline applied based on the job's settings.
//
// A graceful shutdown stops fetching new jobs but allows any previously fetched
// jobs to complete. This can be initiated with the Stop method.
//
// A more abrupt shutdown can be achieved by either cancelling the provided
// context or by calling StopAndCancel. This will not only stop fetching new
// jobs, but will also cancel the context for any currently-running jobs. If
// using StopAndCancel, there's no need to also call Stop.
func (c *Client[TTx]) Start(ctx context.Context) error {
	if !c.config.willExecuteJobs() {
		return errors.New("client Queues and Workers must be configured for a client to start working")
	}
	if c.config.Workers != nil && len(c.config.Workers.workersMap) < 1 {
		return errors.New("at least one Worker must be added to the Workers bundle")
	}

	// We use separate contexts for fetching and working to allow for a graceful
	// stop. However, both inherit from the provided context so if it is
	// cancelled a more aggressive stop will be initiated.
	fetchNewWorkCtx, fetchNewWorkCancel := context.WithCancel(ctx)
	c.fetchNewWorkCancel = fetchNewWorkCancel
	workCtx, workCancel := context.WithCancel(withClient[TTx](ctx, c))
	c.workCancel = workCancel

	// Before doing anything else, make an initial connection to the database to
	// verify that it appears healthy. Many of the subcomponents below start up
	// in a goroutine and in case of initial failure, only produce a log line,
	// so even in the case of a fundamental failure like the database not being
	// available, the client appears to have started even though it's completely
	// non-functional. Here we try to make an initial assessment of health and
	// return quickly in case of an apparent problem.
	_, err := c.driver.GetDBPool().Exec(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("error making initial connection to database: %w", err)
	}

	// Monitor should be the first subprocess to start, and the last to stop.
	// It's not part of the waitgroup because we need to wait for everything else
	// to shut down prior to closing the monitor.
	go c.monitor.Run()

	// Receives job complete notifications from the completer and distributes
	// them to any subscriptions.
	c.completer.Subscribe(c.distributeJobCompleterCallback)

	c.wg.Add(1)
	go func() {
		c.logStatsLoop(fetchNewWorkCtx)
		c.wg.Done()
	}()

	if c.elector != nil {
		go func() {
			sub := c.elector.Listen()
			defer sub.Unlisten()

			for {
				select {
				case <-c.stopComplete: // stop complete
					return

				case <-workCtx.Done(): // stop started
					return

				case notification := <-sub.C():
					c.handleLeadershipChange(ctx, notification)
				}
			}
		}()

		c.wg.Add(2)
		go func() {
			c.notifier.Run(fetchNewWorkCtx)
			c.wg.Done()
		}()
		go func() {
			c.elector.Run(fetchNewWorkCtx)
			c.wg.Done()
		}()
	}

	c.runProducers(fetchNewWorkCtx, workCtx)
	go c.signalStopComplete(workCtx)

	c.baseService.Logger.InfoContext(workCtx, "River client successfully started")

	return nil
}

// ctx is used only for logging, not for lifecycle.
func (c *Client[TTx]) signalStopComplete(ctx context.Context) {
	// Wait for producers, notifier, and elector to exit:
	c.wg.Wait()

	// Once the producers have all finished, we know that completers have at least
	// enqueued any remaining work. Wait for the completer to finish.
	//
	// TODO: there's a risk here that the completer is stuck on a job that won't
	// complete. We probably need a timeout or way to move on in those cases.
	c.completer.Wait()

	c.queueMaintainer.Stop()

	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": All services stopped")

	// As of now, the Adapter doesn't have any async behavior, so we don't need
	// to wait for it to stop.  Once all executors and completers are done, we
	// know that nothing else is happening that's from us.

	// Remove all subscriptions and close corresponding channels.
	func() {
		c.subscriptionsMu.Lock()
		defer c.subscriptionsMu.Unlock()

		for subID, sub := range c.subscriptions {
			close(sub.Chan)
			delete(c.subscriptions, subID)
		}
	}()

	// Shut down the monitor last so it can broadcast final status updates:
	c.monitor.Shutdown()

	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Stop complete")
	close(c.stopComplete)
}

// Stop performs a graceful shutdown of the Client. It signals all producers
// to stop fetching new jobs and waits for any fetched or in-progress jobs to
// complete before exiting. If the provided context is done before shutdown has
// completed, Stop will return immediately with the context's error.
//
// There's no need to call this method if a hard stop has already been initiated
// by cancelling the context passed to Start or by calling StopAndCancel.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	if c.fetchNewWorkCancel == nil {
		return errors.New("client not started")
	}

	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Stop started")
	c.fetchNewWorkCancel()
	return c.awaitStop(ctx)
}

func (c *Client[TTx]) awaitStop(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.stopComplete:
		return nil
	}
}

// StopAndCancel shuts down the client and cancels all work in progress. It is a
// more aggressive stop than Stop because the contexts for any in-progress jobs
// are cancelled. However, it still waits for jobs to complete before returning,
// even though their contexts are cancelled. If the provided context is done
// before shutdown has completed, Stop will return immediately with the
// context's error.
//
// This can also be initiated by cancelling the context passed to Run. There is
// no need to call this method if the context passed to Run is cancelled
// instead.
func (c *Client[TTx]) StopAndCancel(ctx context.Context) error {
	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Hard stop started; cancelling all work")
	c.fetchNewWorkCancel()
	c.workCancel()
	return c.awaitStop(ctx)
}

// Stopped returns a channel that will be closed when the Client has stopped.
// It can be used to wait for a graceful shutdown to complete.
//
// It is not affected by any contexts passed to Stop or StopAndCancel.
func (c *Client[TTx]) Stopped() <-chan struct{} {
	return c.stopComplete
}

// Subscribe subscribes to the provided kinds of events that occur within the
// client, like EventKindJobCompleted for when a job completes.
//
// Returns a channel over which to receive events along with a cancel function
// that can be used to cancel and tear down resources associated with the
// subscription. It's recommended but not necessary to invoke the cancel
// function. Resources will be freed when the client stops in case it's not.
//
// The event channel is buffered and sends on it are non-blocking. Consumers
// must process events in a timely manner or it's possible for events to be
// dropped. Any slow operations performed in a response to a receipt (e.g.
// persisting to a database) should be made asynchronous to avoid event loss.
//
// Callers must specify the kinds of events they're interested in. This allows
// for forward compatibility in case new kinds of events are added in future
// versions. If new event kinds are added, callers will have to explicitly add
// them to their requested list and ensure they can be handled correctly.
func (c *Client[TTx]) Subscribe(kinds ...EventKind) (<-chan *Event, func()) {
	for _, kind := range kinds {
		if _, ok := allKinds[kind]; !ok {
			panic(fmt.Errorf("unknown event kind: %s", kind))
		}
	}

	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	subChan := make(chan *Event, subscribeChanSize)

	// Just gives us an easy way of removing the subscription again later.
	subID := c.subscriptionsSeq
	c.subscriptionsSeq++

	c.subscriptions[subID] = &eventSubscription{
		Chan:  subChan,
		Kinds: sliceutil.KeyBy(kinds, func(k EventKind) (EventKind, struct{}) { return k, struct{}{} }),
	}

	cancel := func() {
		c.subscriptionsMu.Lock()
		defer c.subscriptionsMu.Unlock()

		// May no longer be present in case this was called after a stop.
		sub, ok := c.subscriptions[subID]
		if !ok {
			return
		}

		close(sub.Chan)

		delete(c.subscriptions, subID)
	}

	return subChan, cancel
}

// Distribute a single job into any listening subscriber channels.
func (c *Client[TTx]) distributeJob(job *rivertype.JobRow, stats *JobStatistics) {
	c.subscriptionsMu.Lock()
	defer c.subscriptionsMu.Unlock()

	// Quick path so we don't need to allocate anything if no one is listening.
	if len(c.subscriptions) < 1 {
		return
	}

	var event *Event
	switch job.State {
	case JobStateCancelled:
		event = &Event{Kind: EventKindJobCancelled, Job: job, JobStats: stats}
	case JobStateCompleted:
		event = &Event{Kind: EventKindJobCompleted, Job: job, JobStats: stats}
	case JobStateScheduled:
		event = &Event{Kind: EventKindJobSnoozed, Job: job, JobStats: stats}
	case JobStateAvailable, JobStateDiscarded, JobStateRetryable, JobStateRunning:
		event = &Event{Kind: EventKindJobFailed, Job: job, JobStats: stats}
	default:
		// linter exhaustive rule prevents this from being reached
		panic("unreachable state to distribute, river bug")
	}

	// All subscription channels are non-blocking so this is always fast and
	// there's no risk of falling behind what producers are sending.
	for _, sub := range c.subscriptions {
		if sub.ListensFor(event.Kind) {
			select {
			case sub.Chan <- event:
			default:
			}
		}
	}
}

// Callback invoked by the completer and which prompts the client to update
// statistics and distribute jobs into any listening subscriber channels.
// (Subscriber channels are non-blocking so this should be quite fast.)
func (c *Client[TTx]) distributeJobCompleterCallback(update jobcompleter.CompleterJobUpdated) {
	func() {
		c.statsMu.Lock()
		defer c.statsMu.Unlock()

		stats := update.JobStats
		c.statsAggregate.CompleteDuration += stats.CompleteDuration
		c.statsAggregate.QueueWaitDuration += stats.QueueWaitDuration
		c.statsAggregate.RunDuration += stats.RunDuration
		c.statsNumJobs++
	}()

	c.distributeJob(dbsqlc.JobRowFromInternal(update.Job), jobStatisticsFromInternal(update.JobStats))
}

// Dump aggregate stats from job completions to logs periodically.  These
// numbers don't mean much in themselves, but can give a rough idea of the
// proportions of each compared to each other, and may help flag outlying values
// indicative of a problem.
func (c *Client[TTx]) logStatsLoop(ctx context.Context) {
	// Handles a potential divide by zero.
	safeDurationAverage := func(d time.Duration, n int) time.Duration {
		if n == 0 {
			return 0
		}
		return d / time.Duration(n)
	}

	logStats := func() {
		c.statsMu.Lock()
		defer c.statsMu.Unlock()

		c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Job stats (since last stats line)",
			"num_jobs_run", c.statsNumJobs,
			"average_complete_duration", safeDurationAverage(c.statsAggregate.CompleteDuration, c.statsNumJobs),
			"average_queue_wait_duration", safeDurationAverage(c.statsAggregate.QueueWaitDuration, c.statsNumJobs),
			"average_run_duration", safeDurationAverage(c.statsAggregate.RunDuration, c.statsNumJobs))

		c.statsAggregate = jobstats.JobStatistics{}
		c.statsNumJobs = 0
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			logStats()
		}
	}
}

func (c *Client[TTx]) handleLeadershipChange(ctx context.Context, notification *leadership.Notification) {
	c.baseService.Logger.InfoContext(ctx, "Election change received", slog.Bool("is_leader", notification.IsLeader))

	leaderStatus := componentstatus.ElectorNonLeader
	if notification.IsLeader {
		leaderStatus = componentstatus.ElectorLeader
	}
	c.monitor.SetElectorStatus(leaderStatus)

	switch {
	case notification.IsLeader:
		if err := c.queueMaintainer.Start(ctx); err != nil {
			c.baseService.Logger.ErrorContext(ctx, "Error starting queue maintainer", slog.String("err", err.Error()))
		}

		c.testSignals.electedLeader.Signal(struct{}{})

	default:
		c.queueMaintainer.Stop()
	}
}

func (c *Client[TTx]) provisionProducers() error {
	for queue, queueConfig := range c.config.Queues {
		config := &producerConfig{
			ErrorHandler:      c.config.ErrorHandler,
			FetchCooldown:     c.config.FetchCooldown,
			FetchPollInterval: c.config.FetchPollInterval,
			JobTimeout:        c.config.JobTimeout,
			MaxWorkerCount:    uint16(queueConfig.MaxWorkers),
			Notifier:          c.notifier,
			QueueName:         queue,
			RetryPolicy:       c.config.RetryPolicy,
			SchedulerInterval: c.config.schedulerInterval,
			WorkerName:        c.id,
			Workers:           c.config.Workers,
		}
		producer, err := newProducer(&c.baseService.Archetype, c.adapter, c.completer, config)
		if err != nil {
			return err
		}
		c.producersByQueueName[queue] = producer
		c.monitor.InitializeProducerStatus(queue)
	}
	return nil
}

func (c *Client[TTx]) runProducers(fetchNewWorkCtx, workCtx context.Context) {
	c.wg.Add(len(c.producersByQueueName))
	for _, producer := range c.producersByQueueName {
		producer := producer

		go func() {
			defer c.wg.Done()
			producer.Run(fetchNewWorkCtx, workCtx, c.monitor.SetProducerStatus)
		}()
	}
}

func insertParamsFromArgsAndOptions(args JobArgs, insertOpts *InsertOpts) (*dbadapter.JobInsertParams, error) {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("error marshaling args to JSON: %w", err)
	}

	if insertOpts == nil {
		insertOpts = &InsertOpts{}
	}

	var jobInsertOpts InsertOpts
	if argsWithOpts, ok := args.(JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	maxAttempts := valutil.FirstNonZero(insertOpts.MaxAttempts, jobInsertOpts.MaxAttempts, rivercommon.MaxAttemptsDefault)
	priority := valutil.FirstNonZero(insertOpts.Priority, jobInsertOpts.Priority, rivercommon.PriorityDefault)
	queue := valutil.FirstNonZero(insertOpts.Queue, jobInsertOpts.Queue, rivercommon.QueueDefault)

	tags := insertOpts.Tags
	if insertOpts.Tags == nil {
		tags = jobInsertOpts.Tags
	}

	if priority > 4 {
		return nil, errors.New("priority must be between 1 and 4")
	}

	uniqueOpts := insertOpts.UniqueOpts
	if uniqueOpts.isEmpty() {
		uniqueOpts = jobInsertOpts.UniqueOpts
	}
	if err := uniqueOpts.validate(); err != nil {
		return nil, err
	}

	metadata := insertOpts.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	insertParams := &dbadapter.JobInsertParams{
		EncodedArgs: encodedArgs,
		Kind:        args.Kind(),
		MaxAttempts: maxAttempts,
		Metadata:    metadata,
		Priority:    priority,
		Queue:       queue,
		State:       dbsqlc.JobState(JobStateAvailable),
		Tags:        tags,
	}

	if !uniqueOpts.isEmpty() {
		insertParams.Unique = true
		insertParams.UniqueByArgs = uniqueOpts.ByArgs
		insertParams.UniqueByQueue = uniqueOpts.ByQueue
		insertParams.UniqueByPeriod = uniqueOpts.ByPeriod
		insertParams.UniqueByState = sliceutil.Map(uniqueOpts.ByState, func(s rivertype.JobState) dbsqlc.JobState { return dbsqlc.JobState(s) })
	}

	if !insertOpts.ScheduledAt.IsZero() {
		insertParams.ScheduledAt = insertOpts.ScheduledAt
		insertParams.State = dbsqlc.JobState(JobStateScheduled)
	}

	return insertParams, nil
}

var errInsertNoDriverDBPool = errors.New("driver must have non-nil database pool to use Insert and InsertMany (try InsertTx or InsertManyTx instead")

// Insert inserts a new job with the provided args. Job opts can be used to
// override any defaults that may have been provided by an implementation of
// JobArgsWithInsertOpts.InsertOpts, as well as any global defaults. The
// provided context is used for the underlying Postgres insert and can be used
// to cancel the operation or apply a timeout.
//
//	jobRow, err := client.Insert(insertCtx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*rivertype.JobRow, error) {
	if c.driver.GetDBPool() == nil {
		return nil, errInsertNoDriverDBPool
	}

	if err := c.validateJobArgs(args); err != nil {
		return nil, err
	}

	insertParams, err := insertParamsFromArgsAndOptions(args, opts)
	if err != nil {
		return nil, err
	}

	res, err := c.adapter.JobInsert(ctx, insertParams)
	if err != nil {
		return nil, err
	}

	return dbsqlc.JobRowFromInternal(res.Job), nil
}

// InsertTx inserts a new job with the provided args on the given transaction.
// Job opts can be used to override any defaults that may have been provided by
// an implementation of JobArgsWithInsertOpts.InsertOpts, as well as any global
// defaults. The provided context is used for the underlying Postgres insert and
// can be used to cancel the operation or apply a timeout.
//
//	jobRow, err := client.InsertTx(insertCtx, tx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*rivertype.JobRow, error) {
	if err := c.validateJobArgs(args); err != nil {
		return nil, err
	}

	insertParams, err := insertParamsFromArgsAndOptions(args, opts)
	if err != nil {
		return nil, err
	}

	res, err := c.adapter.JobInsertTx(ctx, c.driver.UnwrapTx(tx), insertParams)
	if err != nil {
		return nil, err
	}

	return dbsqlc.JobRowFromInternal(res.Job), nil
}

// InsertManyParams encapsulates a single job combined with insert options for
// use with batch insertion.
type InsertManyParams struct {
	// Args are the arguments of the job to insert.
	Args JobArgs

	// InsertOpts are insertion options for this job.
	InsertOpts *InsertOpts
}

// InsertMany inserts many jobs at once using Postgres' `COPY FROM` mechanism,
// making the operation quite fast and memory efficient. Each job is inserted as
// an InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertMany(ctx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertManyParams) (int64, error) {
	if c.driver.GetDBPool() == nil {
		return 0, errInsertNoDriverDBPool
	}

	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return 0, err
	}

	return c.adapter.JobInsertMany(ctx, insertParams)
}

// InsertManyTx inserts many jobs at once using Postgres' `COPY FROM` mechanism,
// making the operation quite fast and memory efficient. Each job is inserted as
// an InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertManyParams) (int64, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return 0, err
	}

	return c.adapter.JobInsertManyTx(ctx, c.driver.UnwrapTx(tx), insertParams)
}

// Validates input parameters for an a batch insert operation and generates a
// set of batch insert parameters.
func (c *Client[TTx]) insertManyParams(params []InsertManyParams) ([]*dbadapter.JobInsertParams, error) {
	if len(params) < 1 {
		return nil, errors.New("no jobs to insert")
	}

	insertParams := make([]*dbadapter.JobInsertParams, len(params))
	for i, param := range params {
		if err := c.validateJobArgs(param.Args); err != nil {
			return nil, err
		}

		if param.InsertOpts != nil {
			// UniqueOpts aren't support for batch inserts because they use PG
			// advisory locks to work, and taking many locks simultaneously
			// could easily lead to contention and deadlocks.
			if !param.InsertOpts.UniqueOpts.isEmpty() {
				return nil, errors.New("UniqueOpts are not supported for batch inserts")
			}
		}

		var err error
		insertParams[i], err = insertParamsFromArgsAndOptions(param.Args, param.InsertOpts)
		if err != nil {
			return nil, err
		}
	}

	return insertParams, nil
}

// Validates job args prior to insertion. Currently, verifies that a worker to
// handle the kind is registered in the configured workers bundle. An
// insert-only client doesn't require a workers bundle be configured though, so
// no validation occurs if one wasn't.
func (c *Client[TTx]) validateJobArgs(args JobArgs) error {
	if c.config.Workers == nil {
		return nil
	}

	if _, ok := c.config.Workers.workersMap[args.Kind()]; !ok {
		return &UnknownJobKindError{Kind: args.Kind()}
	}

	return nil
}

var nameRegex = regexp.MustCompile(`^(?:[a-z0-9])+(?:_?[a-z0-9]+)*$`)

func validateQueueName(queueName string) error {
	if queueName == "" {
		return errors.New("queue name cannot be empty")
	}
	if len(queueName) > 64 {
		return errors.New("queue name cannot be longer than 64 characters")
	}
	if !nameRegex.MatchString(queueName) {
		return fmt.Errorf("queue name is invalid, see documentation: %q", queueName)
	}
	return nil
}
