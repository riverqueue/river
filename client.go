package river

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/componentstatus"
	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/maintenance/startstop"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/notifylimiter"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/maputil"
	"github.com/riverqueue/river/internal/util/randutil"
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
//
// Both Queues and Workers are required for a client to work jobs, but an
// insert-only client can be initialized by omitting Queues, and not calling
// Start for the client. Workers can also be omitted, but it's better to include
// it so River can check that inserted job kinds have a worker that can run
// them.
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

	// ID is the unique identifier for this client. If not set, a random
	// identifier will be generated.
	//
	// This is used to identify the client in job attempts and for leader election.
	// This value must be unique across all clients in the same database and
	// schema and there must not be more than one process running with the same
	// ID at the same time.
	//
	// A client ID should differ between different programs and must be unique
	// across all clients in the same database and schema. There must not be more
	// than one process running with the same ID at the same time.  However, the
	// client ID is shared by all executors within any given client. (i.e.
	// different Go processes have different IDs, but IDs are shared within any
	// given process.)
	ID string

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

	// MaxAttempts is the default number of times a job will be retried before
	// being discarded. This value is applied to all jobs by default, and can be
	// overridden on individual job types on the JobArgs or on a per-job basis at
	// insertion time.
	//
	// If not specified, defaults to 25 (MaxAttemptsDefault).
	MaxAttempts int

	// PeriodicJobs are a set of periodic jobs to run at the specified intervals
	// in the client.
	PeriodicJobs []*PeriodicJob

	// PollOnly starts the client in "poll only" mode, which avoids issuing
	// `LISTEN` statements to wait for events like a leadership resignation or
	// new job available. The program instead polls periodically to look for
	// changes (checking for new jobs on the period in FetchPollInterval).
	//
	// The downside of this mode of operation is that events will usually be
	// noticed less quickly. A new job in the queue may have to wait up to
	// FetchPollInterval to be locked for work. When a leader resigns, it will
	// be up to five seconds before a new one elects itself.
	//
	// The upside is that it makes River compatible with systems where
	// listen/notify isn't available. For example, PgBouncer in transaction
	// pooling mode.
	PollOnly bool

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

	// Disables the normal random jittered sleep occurring in queue maintenance
	// services to stagger their startup so they don't all try to work at the
	// same time. Appropriate for use in tests to make sure that the client can
	// always be started and stopped again hastily.
	disableStaggerStart bool

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
	if len(c.ID) > 100 {
		return errors.New("ID cannot be longer than 100 characters")
	}
	if c.JobTimeout < -1 {
		return errors.New("JobTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.MaxAttempts < 0 {
		return errors.New("MaxAttempts cannot be less than zero")
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
	// BaseService and BaseStartStop can't be embedded like on other services
	// because their properties would leak to the external API.
	baseService   baseservice.BaseService
	baseStartStop startstop.BaseStartStop

	completer            jobcompleter.JobCompleter
	completerSubscribeCh chan []jobcompleter.CompleterJobUpdated
	config               *Config
	driver               riverdriver.Driver[TTx]
	elector              *leadership.Elector
	insertNotifyLimiter  *notifylimiter.Limiter
	monitor              *clientMonitor
	notifier             *notifier.Notifier // may be nil in poll-only mode
	periodicJobs         *PeriodicJobBundle
	producersByQueueName map[string]*producer
	queueMaintainer      *maintenance.QueueMaintainer
	services             []startstop.Service
	subscriptionManager  *subscriptionManager
	stopped              chan struct{}
	testSignals          clientTestSignals
	uniqueInserter       *dbunique.UniqueInserter

	// workCancel cancels the context used for all work goroutines. Normal Stop
	// does not cancel that context.
	workCancel context.CancelCauseFunc
}

// Test-only signals.
type clientTestSignals struct {
	electedLeader rivercommon.TestSignal[struct{}] // notifies when elected leader

	jobCleaner          *maintenance.JobCleanerTestSignals
	jobRescuer          *maintenance.JobRescuerTestSignals
	jobScheduler        *maintenance.JobSchedulerTestSignals
	periodicJobEnqueuer *maintenance.PeriodicJobEnqueuerTestSignals
	queueCleaner        *maintenance.QueueCleanerTestSignals
	reindexer           *maintenance.ReindexerTestSignals
}

func (ts *clientTestSignals) Init() {
	ts.electedLeader.Init()

	if ts.jobCleaner != nil {
		ts.jobCleaner.Init()
	}
	if ts.jobRescuer != nil {
		ts.jobRescuer.Init()
	}
	if ts.jobScheduler != nil {
		ts.jobScheduler.Init()
	}
	if ts.periodicJobEnqueuer != nil {
		ts.periodicJobEnqueuer.Init()
	}
	if ts.queueCleaner != nil {
		ts.queueCleaner.Init()
	}
	if ts.reindexer != nil {
		ts.reindexer.Init()
	}
}

var (
	// ErrNotFound is returned when a query by ID does not match any existing
	// rows. For example, attempting to cancel a job that doesn't exist will
	// return this error.
	ErrNotFound = rivertype.ErrNotFound

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
	rescueAfter := maintenance.JobRescuerRescueAfterDefault
	if config.JobTimeout > 0 && config.RescueStuckJobsAfter < 1 && config.JobTimeout > config.RescueStuckJobsAfter {
		rescueAfter = config.JobTimeout + maintenance.JobRescuerRescueAfterDefault
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
		ID:                          valutil.ValOrDefaultFunc(config.ID, func() string { return defaultClientID(time.Now().UTC()) }),
		JobTimeout:                  valutil.ValOrDefault(config.JobTimeout, JobTimeoutDefault),
		Logger:                      logger,
		MaxAttempts:                 valutil.ValOrDefault(config.MaxAttempts, MaxAttemptsDefault),
		PeriodicJobs:                config.PeriodicJobs,
		PollOnly:                    config.PollOnly,
		Queues:                      config.Queues,
		ReindexerSchedule:           config.ReindexerSchedule,
		RescueStuckJobsAfter:        valutil.ValOrDefault(config.RescueStuckJobsAfter, rescueAfter),
		RetryPolicy:                 retryPolicy,
		Workers:                     config.Workers,
		disableStaggerStart:         config.disableStaggerStart,
		schedulerInterval:           valutil.ValOrDefault(config.schedulerInterval, maintenance.JobSchedulerIntervalDefault),
	}

	if err := config.validate(); err != nil {
		return nil, err
	}

	archetype := &baseservice.Archetype{
		Logger:     config.Logger,
		Rand:       randutil.NewCryptoSeededConcurrentSafeRand(),
		TimeNowUTC: func() time.Time { return time.Now().UTC() },
	}

	client := &Client[TTx]{
		config:               config,
		driver:               driver,
		monitor:              newClientMonitor(),
		producersByQueueName: make(map[string]*producer),
		testSignals:          clientTestSignals{},
		uniqueInserter: baseservice.Init(archetype, &dbunique.UniqueInserter{
			AdvisoryLockPrefix: config.AdvisoryLockPrefix,
		}),
	}

	baseservice.Init(archetype, &client.baseService)
	client.baseService.Name = "Client" // Have to correct the name because base service isn't embedded like it usually is
	client.insertNotifyLimiter = notifylimiter.NewLimiter(archetype, config.FetchCooldown)

	// There are a number of internal components that are only needed/desired if
	// we're actually going to be working jobs (as opposed to just enqueueing
	// them):
	if config.willExecuteJobs() {
		if !driver.HasPool() {
			return nil, errMissingDatabasePoolWithQueues
		}

		client.completer = jobcompleter.NewBatchCompleter(archetype, driver.GetExecutor(), nil)
		client.subscriptionManager = newSubscriptionManager(archetype, nil)
		client.services = append(client.services, client.completer, client.subscriptionManager)

		// In poll only mode, we don't try to initialize a notifier that uses
		// listen/notify. Instead, each service polls for changes it's
		// interested in. e.g. Elector polls to see if leader has expired.
		if !config.PollOnly {
			client.notifier = notifier.New(archetype, driver.GetListener(), client.monitor.SetNotifierStatus)
			client.services = append(client.services, client.notifier)
		}

		client.elector = leadership.NewElector(archetype, driver.GetExecutor(), client.notifier, &leadership.Config{
			ClientID: config.ID,
		})
		client.services = append(client.services, client.elector)

		for queue, queueConfig := range config.Queues {
			client.producersByQueueName[queue] = newProducer(archetype, driver.GetExecutor(), &producerConfig{
				ClientID:           config.ID,
				Completer:          client.completer,
				ErrorHandler:       config.ErrorHandler,
				FetchCooldown:      config.FetchCooldown,
				FetchPollInterval:  config.FetchPollInterval,
				JobTimeout:         config.JobTimeout,
				MaxWorkers:         queueConfig.MaxWorkers,
				Notifier:           client.notifier,
				Queue:              queue,
				QueueEventCallback: client.subscriptionManager.distributeQueueEvent,
				RetryPolicy:        config.RetryPolicy,
				SchedulerInterval:  config.schedulerInterval,
				StatusFunc:         client.monitor.SetProducerStatus,
				Workers:            config.Workers,
			})
			client.monitor.InitializeProducerStatus(queue)
		}

		client.services = append(client.services,
			startstop.StartStopFunc(client.logStatsLoop))

		client.services = append(client.services,
			startstop.StartStopFunc(client.handleLeadershipChangeLoop))

		//
		// Maintenance services
		//

		maintenanceServices := []startstop.Service{}

		{
			jobCleaner := maintenance.NewJobCleaner(archetype, &maintenance.JobCleanerConfig{
				CancelledJobRetentionPeriod: config.CancelledJobRetentionPeriod,
				CompletedJobRetentionPeriod: config.CompletedJobRetentionPeriod,
				DiscardedJobRetentionPeriod: config.DiscardedJobRetentionPeriod,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobCleaner)
			client.testSignals.jobCleaner = &jobCleaner.TestSignals
		}

		{
			jobRescuer := maintenance.NewRescuer(archetype, &maintenance.JobRescuerConfig{
				ClientRetryPolicy: retryPolicy,
				RescueAfter:       config.RescueStuckJobsAfter,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if workerInfo, ok := config.Workers.workersMap[kind]; ok {
						return workerInfo.workUnitFactory
					}
					return nil
				},
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobRescuer)
			client.testSignals.jobRescuer = &jobRescuer.TestSignals
		}

		{
			jobScheduler := maintenance.NewJobScheduler(archetype, &maintenance.JobSchedulerConfig{
				Interval:     config.schedulerInterval,
				NotifyInsert: client.maybeNotifyInsertForQueues,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobScheduler)
			client.testSignals.jobScheduler = &jobScheduler.TestSignals
		}

		{
			periodicJobEnqueuer := maintenance.NewPeriodicJobEnqueuer(archetype, &maintenance.PeriodicJobEnqueuerConfig{
				AdvisoryLockPrefix: config.AdvisoryLockPrefix,
				NotifyInsert:       client.maybeNotifyInsertForQueues,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, periodicJobEnqueuer)
			client.testSignals.periodicJobEnqueuer = &periodicJobEnqueuer.TestSignals

			client.periodicJobs = newPeriodicJobBundle(client.config, periodicJobEnqueuer)
			client.periodicJobs.AddMany(config.PeriodicJobs)
		}

		{
			queueCleaner := maintenance.NewQueueCleaner(archetype, &maintenance.QueueCleanerConfig{
				RetentionPeriod: maintenance.QueueRetentionPeriodDefault,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, queueCleaner)
			client.testSignals.queueCleaner = &queueCleaner.TestSignals
		}

		{
			var scheduleFunc func(time.Time) time.Time
			if config.ReindexerSchedule != nil {
				scheduleFunc = config.ReindexerSchedule.Next
			}

			reindexer := maintenance.NewReindexer(archetype, &maintenance.ReindexerConfig{ScheduleFunc: scheduleFunc}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, reindexer)
			client.testSignals.reindexer = &reindexer.TestSignals
		}

		// Not added to the main services list because the queue maintainer is
		// started conditionally based on whether the client is the leader.
		client.queueMaintainer = maintenance.NewQueueMaintainer(archetype, maintenanceServices)

		if config.disableStaggerStart {
			client.queueMaintainer.StaggerStartupDisable(true)
		}
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
	fetchCtx, shouldStart, stopped := c.baseStartStop.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	c.stopped = stopped

	stopProducers := func() {
		startstop.StopAllParallel(sliceutil.Map(
			maputil.Values(c.producersByQueueName),
			func(p *producer) startstop.Service { return p }),
		)
	}

	var workCtx context.Context

	// Startup code. Wrapped in a closure so it doesn't have to remember to
	// close the stopped channel if returning with an error.
	if err := func() error {
		if !c.config.willExecuteJobs() {
			return errors.New("client Queues and Workers must be configured for a client to start working")
		}
		if c.config.Workers != nil && len(c.config.Workers.workersMap) < 1 {
			return errors.New("at least one Worker must be added to the Workers bundle")
		}

		// Before doing anything else, make an initial connection to the database to
		// verify that it appears healthy. Many of the subcomponents below start up
		// in a goroutine and in case of initial failure, only produce a log line,
		// so even in the case of a fundamental failure like the database not being
		// available, the client appears to have started even though it's completely
		// non-functional. Here we try to make an initial assessment of health and
		// return quickly in case of an apparent problem.
		_, err := c.driver.GetExecutor().Exec(fetchCtx, "SELECT 1")
		if err != nil {
			return fmt.Errorf("error making initial connection to database: %w", err)
		}

		// Each time we start, we need a fresh completer subscribe channel to
		// send job completion events on, because the completer will close it
		// each time it shuts down.
		c.completerSubscribeCh = make(chan []jobcompleter.CompleterJobUpdated, 10)
		c.completer.ResetSubscribeChan(c.completerSubscribeCh)
		c.subscriptionManager.ResetSubscribeChan(c.completerSubscribeCh)

		// In case of error, stop any services that might have started. This
		// is safe because even services that were never started will still
		// tolerate being stopped.
		stopServicesOnError := func() {
			startstop.StopAllParallel(c.services)
			c.monitor.Stop()
		}

		// Monitor should be the first subprocess to start, and the last to stop.
		// It's not part of the waitgroup because we need to wait for everything else
		// to shut down prior to closing the monitor.
		//
		// Unlike other services, it's given a background context so that it doesn't
		// cancel on normal stops.
		if err := c.monitor.Start(context.Background()); err != nil { //nolint:contextcheck
			return err
		}

		// The completer is part of the services list below, but although it can
		// stop gracefully along with all the other services, it needs to be
		// started with a context that's _not_ fetchCtx. This ensures that even
		// when fetch is cancelled on shutdown, the completer is still given a
		// separate opportunity to start stopping only after the producers have
		// finished up and returned.
		if err := c.completer.Start(ctx); err != nil {
			stopServicesOnError()
			return err
		}

		// We use separate contexts for fetching and working to allow for a graceful
		// stop. Both inherit from the provided context, so if it's cancelled, a
		// more aggressive stop will be initiated.
		workCtx, c.workCancel = context.WithCancelCause(withClient[TTx](ctx, c))

		for _, service := range c.services {
			if err := service.Start(fetchCtx); err != nil {
				stopServicesOnError()
				return err
			}
		}

		for _, producer := range c.producersByQueueName {
			producer := producer

			if err := producer.StartWorkContext(fetchCtx, workCtx); err != nil {
				stopProducers()
				stopServicesOnError()
				return err
			}
		}

		return nil
	}(); err != nil {
		defer close(stopped)
		if errors.Is(context.Cause(fetchCtx), startstop.ErrStop) {
			return rivercommon.ErrShutdown
		}
		return err
	}

	go func() {
		defer close(stopped)

		c.baseService.Logger.InfoContext(ctx, "River client started", slog.String("client_id", c.ID()))
		defer c.baseService.Logger.InfoContext(ctx, "River client stopped", slog.String("client_id", c.ID()))

		// The call to Stop cancels this context. Block here until shutdown.
		<-fetchCtx.Done()

		// On stop, have the producers stop fetching first of all.
		stopProducers()

		// Stop all mainline services where stop order isn't important.
		startstop.StopAllParallel(append(
			// This list of services contains the completer, which should always
			// stop after the producers so that any remaining work that was enqueued
			// will have a chance to have its state completed as it finishes.
			//
			// TODO: there's a risk here that the completer is stuck on a job that
			// won't complete. We probably need a timeout or way to move on in those
			// cases.
			c.services,

			// Will only be started if this client was leader, but can tolerate a
			// stop without having been started.
			c.queueMaintainer,
		))

		// Shut down the monitor last so it can broadcast final status updates:
		c.monitor.Stop()
	}()

	return nil
}

// Stop performs a graceful shutdown of the Client. It signals all producers
// to stop fetching new jobs and waits for any fetched or in-progress jobs to
// complete before exiting. If the provided context is done before shutdown has
// completed, Stop will return immediately with the context's error.
//
// There's no need to call this method if a hard stop has already been initiated
// by cancelling the context passed to Start or by calling StopAndCancel.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
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
	c.workCancel(rivercommon.ErrShutdown)

	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
		return nil
	}
}

// Stopped returns a channel that will be closed when the Client has stopped.
// It can be used to wait for a graceful shutdown to complete.
//
// It is not affected by any contexts passed to Stop or StopAndCancel.
func (c *Client[TTx]) Stopped() <-chan struct{} {
	return c.stopped
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
	return c.SubscribeConfig(&SubscribeConfig{Kinds: kinds})
}

// The default maximum size of the subscribe channel. Events that would overflow
// it will be dropped.
const subscribeChanSizeDefault = 1_000

// SubscribeConfig is more thorough subscription configuration used for
// Client.SubscribeConfig.
type SubscribeConfig struct {
	// ChanSize is the size of the buffered channel that will be created for the
	// subscription. Incoming events that overall this number because a listener
	// isn't reading from the channel in a timely manner will be dropped.
	//
	// Defaults to 1000.
	ChanSize int

	// Kinds are the kinds of events that the subscription will receive.
	// Requiring that kinds are specified explicitly allows for forward
	// compatibility in case new kinds of events are added in future versions.
	// If new event kinds are added, callers will have to explicitly add them to
	// their requested list and esnure they can be handled correctly.
	Kinds []EventKind
}

// Special internal variant that lets us inject an overridden size.
func (c *Client[TTx]) SubscribeConfig(config *SubscribeConfig) (<-chan *Event, func()) {
	return c.subscriptionManager.SubscribeConfig(config)
}

// Dump aggregate stats from job completions to logs periodically.  These
// numbers don't mean much in themselves, but can give a rough idea of the
// proportions of each compared to each other, and may help flag outlying values
// indicative of a problem.
func (c *Client[TTx]) logStatsLoop(ctx context.Context, shouldStart bool, stopped chan struct{}) error {
	if !shouldStart {
		return nil
	}

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				c.subscriptionManager.logStats(ctx, c.baseService.Name)
			}
		}
	}()

	return nil
}

func (c *Client[TTx]) handleLeadershipChangeLoop(ctx context.Context, shouldStart bool, stopped chan struct{}) error {
	handleLeadershipChange := func(ctx context.Context, notification *leadership.Notification) {
		c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Election change received",
			slog.String("client_id", c.config.ID), slog.Bool("is_leader", notification.IsLeader))

		leaderStatus := componentstatus.ElectorNonLeader
		if notification.IsLeader {
			leaderStatus = componentstatus.ElectorLeader
		}
		c.monitor.SetElectorStatus(leaderStatus)

		switch {
		case notification.IsLeader:
			// Starting the queue maintainer can take a little time so send to
			// this test signal _first_ so tests waiting on it can finish,
			// cancel the queue maintainer start, and overall run much faster.
			c.testSignals.electedLeader.Signal(struct{}{})

			if err := c.queueMaintainer.Start(ctx); err != nil {
				c.baseService.Logger.ErrorContext(ctx, "Error starting queue maintainer", slog.String("err", err.Error()))
			}

		default:
			c.queueMaintainer.Stop()
		}
	}

	if !shouldStart {
		return nil
	}

	go func() {
		// This defer should come first so that it's last out,
		// thereby avoiding races.
		defer close(stopped)

		sub := c.elector.Listen()
		defer sub.Unlisten()

		for {
			select {
			case <-ctx.Done():
				return

			case notification := <-sub.C():
				handleLeadershipChange(ctx, notification)
			}
		}
	}()

	return nil
}

// JobCancel cancels the job with the given ID. If possible, the job is
// cancelled immediately and will not be retried. The provided context is used
// for the underlying Postgres update and can be used to cancel the operation or
// apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified (via LISTEN/NOTIFY) to cancel the running job's context. Although
// the job's context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancel(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	return c.jobCancel(ctx, c.driver.GetExecutor(), jobID)
}

// JobCancelTx cancels the job with the given ID within the specified
// transaction. This variant lets a caller cancel a job atomically alongside
// other database changes. An cancelled job doesn't take effect until the
// transaction commits, and if the transaction rolls back, so too is the
// cancelled job.
//
// If possible, the job is cancelled immediately and will not be retried. The
// provided context is used for the underlying Postgres update and can be used
// to cancel the operation or apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified (via LISTEN/NOTIFY) to cancel the running job's context. Although
// the job's context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancelTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	return c.jobCancel(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) jobCancel(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobRow, error) {
	return exec.JobCancel(ctx, &riverdriver.JobCancelParams{
		ID:                jobID,
		CancelAttemptedAt: c.baseService.TimeNowUTC(),
		ControlTopic:      string(notifier.NotificationTopicControl),
	})
}

// JobGet fetches a single job by its ID. Returns the up-to-date JobRow for the
// specified jobID if it exists. Returns ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobGet(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.driver.GetExecutor().JobGetByID(ctx, id)
}

// JobGetTx fetches a single job by its ID, within a transaction. Returns the
// up-to-date JobRow for the specified jobID if it exists. Returns ErrNotFound
// if the job doesn't exist.
func (c *Client[TTx]) JobGetTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.driver.UnwrapExecutor(tx).JobGetByID(ctx, id)
}

// JobRetry updates the job with the given ID to make it immediately available
// to be retried. Jobs in the running state are not touched, while jobs in any
// other state are made available. To prevent jobs already waiting in the queue
// from being set back in line, the job's scheduled_at field is set to the
// current time only if it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts.
func (c *Client[TTx]) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.driver.GetExecutor().JobRetry(ctx, id)
}

// JobRetryTx updates the job with the given ID to make it immediately available
// to be retried, within the specified transaction. This variant lets a caller
// retry a job atomically alongside other database changes. A retried job isn't
// visible to be worked until the transaction commits, and if the transaction
// rolls back, so too is the retried job.
//
// Jobs in the running state are not touched, while jobs in any other state are
// made available. To prevent jobs already waiting in the queue from being set
// back in line, the job's scheduled_at field is set to the current time only if
// it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts.
func (c *Client[TTx]) JobRetryTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.driver.UnwrapExecutor(tx).JobRetry(ctx, id)
}

// ID returns the unique ID of this client as set in its config or
// auto-generated if not specified.
func (c *Client[TTx]) ID() string {
	return c.config.ID
}

func insertParamsFromConfigArgsAndOptions(config *Config, args JobArgs, insertOpts *InsertOpts) (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error) {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshaling args to JSON: %w", err)
	}

	if insertOpts == nil {
		insertOpts = &InsertOpts{}
	}

	var jobInsertOpts InsertOpts
	if argsWithOpts, ok := args.(JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	maxAttempts := valutil.FirstNonZero(insertOpts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts)
	priority := valutil.FirstNonZero(insertOpts.Priority, jobInsertOpts.Priority, rivercommon.PriorityDefault)
	queue := valutil.FirstNonZero(insertOpts.Queue, jobInsertOpts.Queue, rivercommon.QueueDefault)

	if err := validateQueueName(queue); err != nil {
		return nil, nil, err
	}

	tags := insertOpts.Tags
	if insertOpts.Tags == nil {
		tags = jobInsertOpts.Tags
	}
	if tags == nil {
		tags = []string{}
	}

	if priority > 4 {
		return nil, nil, errors.New("priority must be between 1 and 4")
	}

	uniqueOpts := insertOpts.UniqueOpts
	if uniqueOpts.isEmpty() {
		uniqueOpts = jobInsertOpts.UniqueOpts
	}
	if err := uniqueOpts.validate(); err != nil {
		return nil, nil, err
	}

	metadata := insertOpts.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	insertParams := &riverdriver.JobInsertFastParams{
		EncodedArgs: encodedArgs,
		Kind:        args.Kind(),
		MaxAttempts: maxAttempts,
		Metadata:    metadata,
		Priority:    priority,
		Queue:       queue,
		State:       rivertype.JobStateAvailable,
		Tags:        tags,
	}

	if !insertOpts.ScheduledAt.IsZero() {
		insertParams.ScheduledAt = &insertOpts.ScheduledAt
		insertParams.State = rivertype.JobStateScheduled
	}

	if insertOpts.Pending {
		insertParams.State = rivertype.JobStatePending
	}

	return insertParams, (*dbunique.UniqueOpts)(&uniqueOpts), nil
}

var errNoDriverDBPool = errors.New("driver must have non-nil database pool to use non-transactional methods like Insert and InsertMany (try InsertTx or InsertManyTx instead")

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
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	if !c.driver.HasPool() {
		return nil, errNoDriverDBPool
	}

	return c.insert(ctx, c.driver.GetExecutor(), args, opts)
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
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	return c.insert(ctx, c.driver.UnwrapExecutor(tx), args, opts)
}

func (c *Client[TTx]) insert(ctx context.Context, exec riverdriver.Executor, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	if err := c.validateJobArgs(args); err != nil {
		return nil, err
	}

	params, uniqueOpts, err := insertParamsFromConfigArgsAndOptions(c.config, args, opts)
	if err != nil {
		return nil, err
	}

	tx, err := exec.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	jobInsertRes, err := c.uniqueInserter.JobInsert(ctx, tx, params, uniqueOpts)
	if err != nil {
		return nil, err
	}

	if err := c.maybeNotifyInsert(ctx, tx, params.State, params.Queue); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return jobInsertRes, nil
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
//
// Job uniqueness is not respected when using InsertMany due to unique inserts
// using an internal transaction and advisory lock that might lead to
// significant lock contention. Insert unique jobs using Insert instead.
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertManyParams) (int, error) {
	if !c.driver.HasPool() {
		return 0, errNoDriverDBPool
	}

	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return 0, err
	}

	// Wrap in a transaction in case we need to notify about inserts.
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	inserted, err := c.insertFastMany(ctx, tx, insertParams)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return inserted, nil
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
// Job uniqueness is not respected when using InsertManyTx due to unique inserts
// using an internal transaction and advisory lock that might lead to
// significant lock contention. Insert unique jobs using InsertTx instead.
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertManyParams) (int, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return 0, err
	}

	exec := c.driver.UnwrapExecutor(tx)
	return c.insertFastMany(ctx, exec, insertParams)
}

func (c *Client[TTx]) insertFastMany(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*riverdriver.JobInsertFastParams) (int, error) {
	inserted, err := tx.JobInsertFastMany(ctx, insertParams)
	if err != nil {
		return inserted, err
	}

	queues := make([]string, 0, 10)
	for _, params := range insertParams {
		if params.State == rivertype.JobStateAvailable {
			queues = append(queues, params.Queue)
		}
	}
	if err := c.maybeNotifyInsertForQueues(ctx, tx, queues); err != nil {
		return 0, err
	}
	return inserted, nil
}

// Validates input parameters for an a batch insert operation and generates a
// set of batch insert parameters.
func (c *Client[TTx]) insertManyParams(params []InsertManyParams) ([]*riverdriver.JobInsertFastParams, error) {
	if len(params) < 1 {
		return nil, errors.New("no jobs to insert")
	}

	insertParams := make([]*riverdriver.JobInsertFastParams, len(params))
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
		insertParams[i], _, err = insertParamsFromConfigArgsAndOptions(c.config, param.Args, param.InsertOpts)
		if err != nil {
			return nil, err
		}
	}

	return insertParams, nil
}

func (c *Client[TTx]) maybeNotifyInsert(ctx context.Context, tx riverdriver.ExecutorTx, state rivertype.JobState, queue string) error {
	if state != rivertype.JobStateAvailable {
		return nil
	}
	return c.maybeNotifyInsertForQueues(ctx, tx, []string{queue})
}

// Notify the given queues that new jobs are available. The queues list will be
// deduplicated and each will be checked to see if it is due for an insert
// notification from this client.
func (c *Client[TTx]) maybeNotifyInsertForQueues(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error {
	if len(queues) < 1 {
		return nil
	}

	queueMap := make(map[string]struct{})
	queuesDeduped := make([]string, 0, len(queues))
	payloads := make([]string, 0, len(queues))

	for _, queue := range queues {
		if _, ok := queueMap[queue]; ok {
			continue
		}

		queueMap[queue] = struct{}{}
		if c.insertNotifyLimiter.ShouldTrigger(queue) {
			payloads = append(payloads, fmt.Sprintf("{\"queue\": %q}", queue))
			queuesDeduped = append(queuesDeduped, queue)
		}
	}

	if len(payloads) < 1 {
		return nil
	}

	err := tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Topic:   string(notifier.NotificationTopicInsert),
		Payload: payloads,
	})
	if err != nil {
		c.baseService.Logger.ErrorContext(
			ctx,
			c.baseService.Name+": Failed to send job insert notification",
			slog.String("queues", strings.Join(queuesDeduped, ",")),
			slog.String("err", err.Error()),
		)
		return err
	}
	return nil
}

// emit a notification about a queue being paused or resumed.
func (c *Client[TTx]) notifyQueuePauseOrResume(ctx context.Context, tx riverdriver.ExecutorTx, action controlAction, queue string, opts *QueuePauseOpts) error {
	c.baseService.Logger.DebugContext(ctx,
		c.baseService.Name+": Notifying about queue state change",
		slog.String("action", string(action)),
		slog.String("queue", queue),
		slog.String("opts", fmt.Sprintf("%+v", opts)),
	)

	payload, err := json.Marshal(jobControlPayload{Action: action, Queue: queue})
	if err != nil {
		return err
	}

	err = tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{string(payload)},
		Topic:   string(notifier.NotificationTopicControl),
	})
	if err != nil {
		c.baseService.Logger.ErrorContext(
			ctx,
			c.baseService.Name+": Failed to send queue state change notification",
			slog.String("err", err.Error()),
		)
		return err
	}
	return nil
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

var nameRegex = regexp.MustCompile(`^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$`)

func validateQueueName(queueName string) error {
	if queueName == "" {
		return errors.New("queue name cannot be empty")
	}
	if len(queueName) > 64 {
		return errors.New("queue name cannot be longer than 64 characters")
	}
	if !nameRegex.MatchString(queueName) {
		return fmt.Errorf("queue name is invalid, expected letters and numbers separated by underscores or hyphens: %q", queueName)
	}
	return nil
}

// JobListResult is the result of a job list operation. It contains a list of
// jobs and a cursor for fetching the next page of results.
type JobListResult struct {
	// Jobs is a slice of job returned as part of the list operation.
	Jobs []*rivertype.JobRow

	// LastCursor is a cursor that can be used to list the next page of jobs.
	LastCursor *JobListCursor
}

// JobList returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().WithLimit(10).State(rivertype.JobStateCompleted)
//	jobRows, err := client.JobList(ctx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobList(ctx context.Context, params *JobListParams) (*JobListResult, error) {
	if !c.driver.HasPool() {
		return nil, errNoDriverDBPool
	}

	if params == nil {
		params = NewJobListParams()
	}
	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	jobs, err := dblist.JobList(ctx, c.driver.GetExecutor(), dbParams)
	if err != nil {
		return nil, err
	}
	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// JobListTx returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().First(10).State(river.JobStateCompleted)
//	jobRows, err := client.JobListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobListTx(ctx context.Context, tx TTx, params *JobListParams) (*JobListResult, error) {
	if params == nil {
		params = NewJobListParams()
	}

	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	jobs, err := dblist.JobList(ctx, c.driver.UnwrapExecutor(tx), dbParams)
	if err != nil {
		return nil, err
	}
	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// PeriodicJobs returns the currently configured set of periodic jobs for the
// client, and can be used to add new ones or remove existing ones.
func (c *Client[TTx]) PeriodicJobs() *PeriodicJobBundle { return c.periodicJobs }

// QueueGet returns the queue with the given name. If the queue has not recently
// been active or does not exist, returns ErrNotFound.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
func (c *Client[TTx]) QueueGet(ctx context.Context, name string) (*rivertype.Queue, error) {
	return c.driver.GetExecutor().QueueGet(ctx, name)
}

// QueueListResult is the result of a job list operation. It contains a list of
// jobs and leaves room for future cursor functionality.
type QueueListResult struct {
	// Queues is a slice of queues returned as part of the list operation.
	Queues []*rivertype.Queue
}

// QueueList returns a list of all queues that are currently active or were
// recently active. Limit and offset can be used to paginate the results.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
//
//	params := river.NewQueueListParams().First(10)
//	queueRows, err := client.QueueListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) QueueList(ctx context.Context, params *QueueListParams) (*QueueListResult, error) {
	if params == nil {
		params = NewQueueListParams()
	}

	queues, err := c.driver.GetExecutor().QueueList(ctx, int(params.paginationCount))
	if err != nil {
		return nil, err
	}

	listRes := &QueueListResult{Queues: queues}
	return listRes, nil
}

// QueuePause pauses the queue with the given name. When a queue is paused,
// clients will not fetch any more jobs for that particular queue. To pause all
// queues at once, use the special queue name "*".
//
// Clients with a configured notifier should receive a notification about the
// paused queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will pause after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueuePause(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueuePause(ctx, name); err != nil {
		return err
	}

	if err := c.notifyQueuePauseOrResume(ctx, tx, controlActionPause, name, opts); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// QueueResume resumes the queue with the given name. If the queue was
// previously paused, any clients configured to work that queue will resume
// fetching additional jobs. To resume all queues at once, use the special queue
// name "*".
//
// Clients with a configured notifier should receive a notification about the
// resumed queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will resume after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueueResume(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := tx.QueueResume(ctx, name); err != nil {
		return err
	}

	if err := c.notifyQueuePauseOrResume(ctx, tx, controlActionResume, name, opts); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Generates a default client ID using the current hostname and time.
func defaultClientID(startedAt time.Time) string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown_host"
	}

	return defaultClientIDWithHost(startedAt, host)
}

// Same as the above, but allows host injection for testability.
func defaultClientIDWithHost(startedAt time.Time, host string) string {
	const maxHostLength = 60

	// Truncate degenerately long host names.
	host = strings.ReplaceAll(host, ".", "_")
	if len(host) > maxHostLength {
		host = host[0:maxHostLength]
	}

	// Dots, hyphens, and colons aren't particularly friendly for double click
	// to select (depends on application and configuration), so avoid them all
	// in favor of underscores.
	const rfc3339Compact = "2006_01_02T15_04_05"

	return host + "_" + startedAt.Format(rfc3339Compact)
}
