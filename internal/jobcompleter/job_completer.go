package jobcompleter

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// JobCompleter is an interface to a service that "completes" jobs by marking
// them with an appropriate state and any other necessary metadata in the
// database. It's a generic interface to let us experiment with the speed of a
// number of implementations, although River will likely always prefer our most
// optimized one.
type JobCompleter interface {
	startstop.Service

	// JobSetState sets a new state for the given job, as long as it's
	// still running (i.e. its state has not changed to something else already).
	JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error

	// ResetSubscribeChan resets the subscription channel for the completer. It
	// must only be called when the completer is stopped.
	ResetSubscribeChan(subscribeCh SubscribeChan)
}

type SubscribeChan chan<- []CompleterJobUpdated

// SubscribeFunc will be invoked whenever a job is updated.
type SubscribeFunc func(update CompleterJobUpdated)

type CompleterJobUpdated struct {
	Job      *rivertype.JobRow
	JobStats *jobstats.JobStatistics
}

type InlineCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	disableSleep bool // disable sleep in testing
	exec         riverdriver.Executor
	subscribeCh  SubscribeChan

	// A waitgroup is not actually needed for the inline completer because as
	// long as the caller is waiting on each function call, completion is
	// guaranteed to be done by the time Wait is called. However, we use a
	// generic test helper for all completers that starts goroutines, so this
	// left in for now for the benefit of the test suite.
	wg sync.WaitGroup
}

func NewInlineCompleter(archetype *baseservice.Archetype, exec riverdriver.Executor, subscribeCh SubscribeChan) *InlineCompleter {
	return baseservice.Init(archetype, &InlineCompleter{
		exec:        exec,
		subscribeCh: subscribeCh,
	})
}

func (c *InlineCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	c.wg.Add(1)
	defer c.wg.Done()

	start := c.Time.NowUTC()

	job, err := withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) (*rivertype.JobRow, error) {
		return c.exec.JobSetStateIfRunning(ctx, params)
	})
	if err != nil {
		return err
	}

	stats.CompleteDuration = c.Time.NowUTC().Sub(start)
	c.subscribeCh <- []CompleterJobUpdated{{Job: job, JobStats: stats}}

	return nil
}

func (c *InlineCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

func (c *InlineCompleter) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	go func() {
		started()
		defer stopped()
		defer close(c.subscribeCh)

		<-ctx.Done()

		c.wg.Wait()
	}()

	return nil
}

// A default concurrency of 100 seems to perform better a much smaller number
// like 10, but it's quite dependent on environment (10 and 100 bench almost
// identically on MBA when it's on battery power). This number should represent
// our best known default for most use cases, but don't consider its choice to
// be particularly well informed at this point.
const asyncCompleterDefaultConcurrency = 100

type AsyncCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	concurrency  int
	disableSleep bool // disable sleep in testing
	errGroup     *errgroup.Group
	exec         riverdriver.Executor
	subscribeCh  SubscribeChan
}

func NewAsyncCompleter(archetype *baseservice.Archetype, exec riverdriver.Executor, subscribeCh SubscribeChan) *AsyncCompleter {
	return newAsyncCompleterWithConcurrency(archetype, exec, asyncCompleterDefaultConcurrency, subscribeCh)
}

func newAsyncCompleterWithConcurrency(archetype *baseservice.Archetype, exec riverdriver.Executor, concurrency int, subscribeCh SubscribeChan) *AsyncCompleter {
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(concurrency)

	return baseservice.Init(archetype, &AsyncCompleter{
		exec:        exec,
		concurrency: concurrency,
		errGroup:    errGroup,
		subscribeCh: subscribeCh,
	})
}

func (c *AsyncCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	// Start clock outside of goroutine so that the time spent blocking waiting
	// for an errgroup slot is accurately measured.
	start := c.Time.NowUTC()

	c.errGroup.Go(func() error {
		job, err := withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) (*rivertype.JobRow, error) {
			return c.exec.JobSetStateIfRunning(ctx, params)
		})
		if err != nil {
			return err
		}

		stats.CompleteDuration = c.Time.NowUTC().Sub(start)
		c.subscribeCh <- []CompleterJobUpdated{{Job: job, JobStats: stats}}

		return nil
	})
	return nil
}

func (c *AsyncCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

func (c *AsyncCompleter) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's first out
		defer close(c.subscribeCh)

		<-ctx.Done()

		if err := c.errGroup.Wait(); err != nil {
			c.Logger.Error("Error waiting on async completer", "err", err)
		}
	}()

	return nil
}

type batchCompleterSetState struct {
	Params *riverdriver.JobSetStateIfRunningParams
	Stats  *jobstats.JobStatistics
}

// BatchCompleter accumulates incoming completions, and instead of completing
// them immediately, every so often complete many of them as a single efficient
// batch. To minimize the amount of driver surface area we need, the batching is
// only performed for jobs being changed to a `completed` state, which we expect
// to the vast common case under normal operation. The completer embeds an
// AsyncCompleter to perform other non-`completed` state completions.
type BatchCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	completionMaxSize    int  // configurable for testing purposes; max jobs to complete in single database operation
	disableSleep         bool // disable sleep in testing
	maxBacklog           int  // configurable for testing purposes; max backlog allowed before no more completions accepted
	exec                 riverdriver.Executor
	pilot                riverpilot.Pilot
	setStateParams       map[int64]*batchCompleterSetState
	setStateParamsMu     sync.RWMutex
	setStateStartTimes   map[int64]time.Time
	subscribeCh          SubscribeChan
	waitOnBacklogChan    chan struct{}
	waitOnBacklogWaiting bool
}

func NewBatchCompleter(archetype *baseservice.Archetype, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *BatchCompleter {
	const (
		completionMaxSize = 5_000
		maxBacklog        = 20_000
	)

	return baseservice.Init(archetype, &BatchCompleter{
		completionMaxSize:  completionMaxSize,
		exec:               exec,
		maxBacklog:         maxBacklog,
		pilot:              pilot,
		setStateParams:     make(map[int64]*batchCompleterSetState),
		setStateStartTimes: make(map[int64]time.Time),
		subscribeCh:        subscribeCh,
	})
}

func (c *BatchCompleter) ResetSubscribeChan(subscribeCh SubscribeChan) {
	c.subscribeCh = subscribeCh
}

func (c *BatchCompleter) Start(ctx context.Context) error {
	stopCtx, shouldStart, started, stopped := c.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	if c.subscribeCh == nil {
		panic("subscribeCh must be non-nil")
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's first out
		defer close(c.subscribeCh)

		c.Logger.DebugContext(ctx, c.Name+": Run loop started")
		defer c.Logger.DebugContext(ctx, c.Name+": Run loop stopped")

		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		backlogSize := func() int {
			c.setStateParamsMu.RLock()
			defer c.setStateParamsMu.RUnlock()
			return len(c.setStateParams)
		}

		for numTicks := 0; ; numTicks++ {
			select {
			case <-stopCtx.Done():
				// Try to insert last batch before leaving. Note we use the
				// original context so operations aren't immediately cancelled.
				if err := c.handleBatch(ctx); err != nil {
					c.Logger.Error(c.Name+": Error completing batch", "err", err)
				}
				return

			case <-ticker.C:
			}

			// The ticker fires quite often to make sure that given a huge glut
			// of jobs, we don't accidentally build up too much of a backlog by
			// waiting too long. However, don't start a complete operation until
			// we reach a minimum threshold unless we're on a tick that's a
			// multiple of 5. So, jobs will be completed every 250ms even if the
			// threshold hasn't been met.
			const batchCompleterStartThreshold = 100
			if backlogSize() < min(c.maxBacklog, batchCompleterStartThreshold) && numTicks != 0 && numTicks%5 != 0 {
				continue
			}

			for {
				if err := c.handleBatch(ctx); err != nil {
					c.Logger.Error(c.Name+": Error completing batch", "err", err)
				}

				// New jobs to complete may have come in while working the batch
				// above. If enough have to bring us above the minimum complete
				// threshold, loop again and do another batch. Otherwise, break
				// and listen for a new tick.
				if backlogSize() < batchCompleterStartThreshold {
					break
				}
			}
		}
	}()

	return nil
}

func (c *BatchCompleter) handleBatch(ctx context.Context) error {
	var (
		setStateBatch      map[int64]*batchCompleterSetState
		setStateStartTimes map[int64]time.Time
	)
	func() {
		c.setStateParamsMu.Lock()
		defer c.setStateParamsMu.Unlock()

		setStateBatch = c.setStateParams
		setStateStartTimes = c.setStateStartTimes

		// Don't bother resetting the map if there's nothing to process,
		// allowing the completer to idle efficiently.
		if len(setStateBatch) > 0 {
			c.setStateParams = make(map[int64]*batchCompleterSetState)
			c.setStateStartTimes = make(map[int64]time.Time)
		} else {
			// Set nil to avoid a data race below in case the map is set as a
			// new job comes in.
			setStateBatch = nil
		}
	}()

	if len(setStateBatch) < 1 {
		return nil
	}

	// Complete a sub-batch with retries. Also helps reduce visual noise and
	// increase readability of loop below.
	completeSubBatch := func(batchParams *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		start := time.Now()
		defer func() {
			c.Logger.DebugContext(ctx, c.Name+": Completed sub-batch of job(s)", "duration", time.Since(start), "num_jobs", len(batchParams.ID))
		}()

		return withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
			tx, err := c.exec.Begin(ctx)
			if err != nil {
				return nil, err
			}
			defer tx.Rollback(ctx)

			rows, err := c.pilot.JobSetStateIfRunningMany(ctx, tx, batchParams)
			if err != nil {
				return nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}

			return rows, nil
		})
	}

	// This could be written more simply using multiple `sliceutil.Map`s, but
	// it's done this way to allocate as few new slices as necessary.
	mapBatch := func(setStateBatch map[int64]*batchCompleterSetState) *riverdriver.JobSetStateIfRunningManyParams {
		params := &riverdriver.JobSetStateIfRunningManyParams{
			ID:          make([]int64, len(setStateBatch)),
			ErrData:     make([][]byte, len(setStateBatch)),
			FinalizedAt: make([]*time.Time, len(setStateBatch)),
			MaxAttempts: make([]*int, len(setStateBatch)),
			ScheduledAt: make([]*time.Time, len(setStateBatch)),
			State:       make([]rivertype.JobState, len(setStateBatch)),
		}
		var i int
		for _, setState := range setStateBatch {
			params.ID[i] = setState.Params.ID
			params.ErrData[i] = setState.Params.ErrData
			params.FinalizedAt[i] = setState.Params.FinalizedAt
			params.MaxAttempts[i] = setState.Params.MaxAttempts
			params.ScheduledAt[i] = setState.Params.ScheduledAt
			params.State[i] = setState.Params.State
			i++
		}
		return params
	}

	// Tease apart enormous batches into sub-batches.
	//
	// All the code below is concerned with doing that, with a fast loop that
	// doesn't allocate any additional memory in case the entire batch is
	// smaller than the sub-batch maximum size (which will be the common case).
	var (
		params  = mapBatch(setStateBatch)
		jobRows []*rivertype.JobRow
	)
	c.Logger.DebugContext(ctx, c.Name+": Completing batch of job(s)", "num_jobs", len(setStateBatch))
	if len(setStateBatch) > c.completionMaxSize {
		jobRows = make([]*rivertype.JobRow, 0, len(setStateBatch))
		for i := 0; i < len(setStateBatch); i += c.completionMaxSize {
			endIndex := min(i+c.completionMaxSize, len(params.ID)) // beginning of next sub-batch or end of slice
			subBatch := &riverdriver.JobSetStateIfRunningManyParams{
				ID:          params.ID[i:endIndex],
				ErrData:     params.ErrData[i:endIndex],
				FinalizedAt: params.FinalizedAt[i:endIndex],
				MaxAttempts: params.MaxAttempts[i:endIndex],
				ScheduledAt: params.ScheduledAt[i:endIndex],
				State:       params.State[i:endIndex],
			}
			jobRowsSubBatch, err := completeSubBatch(subBatch)
			if err != nil {
				return err
			}
			jobRows = append(jobRows, jobRowsSubBatch...)
		}
	} else {
		var err error
		jobRows, err = completeSubBatch(params)
		if err != nil {
			return err
		}
	}

	events := sliceutil.Map(jobRows, func(jobRow *rivertype.JobRow) CompleterJobUpdated {
		setState := setStateBatch[jobRow.ID]
		startTime := setStateStartTimes[jobRow.ID]
		setState.Stats.CompleteDuration = c.Time.NowUTC().Sub(startTime)
		return CompleterJobUpdated{Job: jobRow, JobStats: setState.Stats}
	})

	c.subscribeCh <- events

	func() {
		c.setStateParamsMu.Lock()
		defer c.setStateParamsMu.Unlock()

		if c.waitOnBacklogWaiting && len(c.setStateParams) < c.maxBacklog {
			c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
			close(c.waitOnBacklogChan)
			c.waitOnBacklogWaiting = false
		}
	}()

	return nil
}

func (c *BatchCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	now := c.Time.NowUTC()
	// If we've built up too much of a backlog because the completer's fallen
	// behind, block completions until the complete loop's had a chance to catch
	// up.
	c.waitOrInitBacklogChannel(ctx)

	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	c.setStateParams[params.ID] = &batchCompleterSetState{params, stats}
	c.setStateStartTimes[params.ID] = now

	return nil
}

func (c *BatchCompleter) waitOrInitBacklogChannel(ctx context.Context) {
	c.setStateParamsMu.RLock()
	var (
		backlogSize = len(c.setStateParams)
		waitChan    = c.waitOnBacklogChan
		waiting     = c.waitOnBacklogWaiting
	)
	c.setStateParamsMu.RUnlock()

	if waiting {
		<-waitChan
		return
	}

	// Not at max backlog. A little raciness is allowed here: multiple
	// goroutines may have acquired the read lock above and seen a size under
	// limit, but with all allowed to continue it could put the backlog over its
	// maximum. The backlog will only be nominally over because generally max
	// backlog >> max workers, so consider this okay.
	if backlogSize < c.maxBacklog {
		return
	}

	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	// Check once more if another process has already started waiting (it's
	// possible for multiple to race between the acquiring the lock above). If
	// so, we fall through and allow this insertion to happen, even though it
	// might bring the batch slightly over limit, because arranging the locks
	// otherwise would get complicated.
	if c.waitOnBacklogWaiting {
		return
	}

	// Tell all future insertions to start waiting. This one is allowed to fall
	// through and succeed even though it may bring the batch a little over
	// limit.
	c.waitOnBacklogChan = make(chan struct{})
	c.waitOnBacklogWaiting = true
	c.Logger.WarnContext(ctx, c.Name+": Hit maximum backlog; completions will wait until below threshold", "max_backlog", c.maxBacklog)
}

// As configued, total time asleep from initial attempt is ~7 seconds (1 + 2 +
// 4) (not including jitter). However, if each attempt times out, that's up to
// ~37 seconds (7 seconds + 3 * 10 seconds).
const numRetries = 3

func withRetries[T any](logCtx context.Context, baseService *baseservice.BaseService, disableSleep bool, retryFunc func(ctx context.Context) (T, error)) (T, error) {
	uncancelledCtx := context.WithoutCancel(logCtx)

	var (
		defaultVal T
		lastErr    error
	)

	for attempt := 1; attempt <= numRetries; attempt++ {
		const timeout = 10 * time.Second

		// I've found that we want at least ten seconds for a large batch,
		// although it usually doesn't need that long.
		ctx, cancel := context.WithTimeout(uncancelledCtx, timeout)
		defer cancel()

		retVal, err := retryFunc(ctx)
		if err != nil {
			// A cancelled context will never succeed, return immediately.
			if errors.Is(err, context.Canceled) {
				return defaultVal, err
			}

			// A closed pool will never succeed, return immediately.
			if errors.Is(err, riverdriver.ErrClosedPool) {
				return defaultVal, err
			}

			lastErr = err
			sleepDuration := serviceutil.ExponentialBackoff(baseService.Rand, attempt, serviceutil.MaxAttemptsBeforeResetDefault)
			baseService.Logger.ErrorContext(logCtx, baseService.Name+": Completer error (will retry after sleep)",
				"attempt", attempt, "err", err, "sleep_duration", sleepDuration, "timeout", timeout)
			if !disableSleep {
				serviceutil.CancellableSleep(logCtx, sleepDuration)
			}
			continue
		}

		return retVal, nil
	}

	baseService.Logger.ErrorContext(logCtx, baseService.Name+": Too many errors; giving up")

	return defaultVal, lastErr
}
