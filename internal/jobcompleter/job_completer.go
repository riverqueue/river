package jobcompleter

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
	"github.com/riverqueue/river/rivershared/util/timeoututil"
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
	Snoozed  bool
}

type InlineCompleter struct {
	baseservice.BaseService
	startstop.BaseStartStop

	disableSleep bool // disable sleep in testing
	exec         riverdriver.Executor
	pilot        riverpilot.Pilot
	schema       string
	subscribeCh  SubscribeChan

	// A waitgroup is not actually needed for the inline completer because as
	// long as the caller is waiting on each function call, completion is
	// guaranteed to be done by the time Wait is called. However, we use a
	// generic test helper for all completers that starts goroutines, so this
	// left in for now for the benefit of the test suite.
	wg sync.WaitGroup
}

func NewInlineCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *InlineCompleter {
	return baseservice.Init(archetype, &InlineCompleter{
		exec:        exec,
		pilot:       pilot,
		schema:      schema,
		subscribeCh: subscribeCh,
	})
}

func (c *InlineCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	c.wg.Add(1)
	defer c.wg.Done()

	start := c.Time.Now()

	jobs, err := withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
		jobs, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, setStateParamsToMany(c.Time.NowOrNil(), c.schema, params))
		if err != nil {
			return nil, err
		}

		return jobs, nil
	})
	if err != nil {
		return err
	}

	// The driver intentionally returns 0 rows when a job is deleted while the
	// completer is finalizing it (see UnknownJobIgnored shared driver test).
	// Guard against an index-out-of-range panic in that case.
	if len(jobs) < 1 {
		return nil
	}

	stats.CompleteDuration = c.Time.Now().Sub(start)
	c.subscribeCh <- []CompleterJobUpdated{{
		Job:      jobs[0],
		JobStats: stats,
		Snoozed:  params.Snoozed,
	}}

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

func setStateParamsToMany(now *time.Time, schema string, params *riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
	return &riverdriver.JobSetStateIfRunningManyParams{
		Attempt:         []*int{params.Attempt},
		ErrData:         [][]byte{params.ErrData},
		FinalizedAt:     []*time.Time{params.FinalizedAt},
		ID:              []int64{params.ID},
		MetadataDoMerge: []bool{params.MetadataDoMerge},
		MetadataUpdates: [][]byte{params.MetadataUpdates},
		Now:             now,
		ScheduledAt:     []*time.Time{params.ScheduledAt},
		Schema:          schema,
		State:           []rivertype.JobState{params.State},
	}
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
	pilot        riverpilot.Pilot
	schema       string
	subscribeCh  SubscribeChan
}

func NewAsyncCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *AsyncCompleter {
	return newAsyncCompleterWithConcurrency(archetype, schema, exec, pilot, asyncCompleterDefaultConcurrency, subscribeCh)
}

func newAsyncCompleterWithConcurrency(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, concurrency int, subscribeCh SubscribeChan) *AsyncCompleter {
	errGroup := &errgroup.Group{}
	errGroup.SetLimit(concurrency)

	return baseservice.Init(archetype, &AsyncCompleter{
		concurrency: concurrency,
		errGroup:    errGroup,
		exec:        exec,
		pilot:       pilot,
		schema:      schema,
		subscribeCh: subscribeCh,
	})
}

func (c *AsyncCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	// Start clock outside of goroutine so that the time spent blocking waiting
	// for an errgroup slot is accurately measured.
	start := c.Time.Now()

	c.errGroup.Go(func() error {
		jobs, err := withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
			rows, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, setStateParamsToMany(c.Time.NowOrNil(), c.schema, params))
			if err != nil {
				return nil, err
			}

			return rows, nil
		})
		if err != nil {
			return err
		}

		// The driver intentionally returns 0 rows when a job is deleted while the
		// completer is finalizing it (see UnknownJobIgnored shared driver test).
		// Guard against an index-out-of-range panic in that case.
		if len(jobs) < 1 {
			return nil
		}

		stats.CompleteDuration = c.Time.Now().Sub(start)
		c.subscribeCh <- []CompleterJobUpdated{{
			Job:      jobs[0],
			JobStats: stats,
			Snoozed:  params.Snoozed,
		}}

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
			c.Logger.ErrorContext(ctx, "Error waiting on async completer", "err", err)
		}
	}()

	return nil
}

type batchCompleterSetState struct {
	Params    *riverdriver.JobSetStateIfRunningParams
	StartTime time.Time
	Stats     *jobstats.JobStatistics
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

	backlogWaitThreshold int // configurable for testing purposes; backlog at which completions start waiting for the completer to catch up
	batchReadyChan       chan struct{}
	completionMaxSize    int  // configurable for testing purposes; max jobs to complete in single database operation
	disableSleep         bool // disable sleep in testing
	maxBacklog           int  // configurable for testing purposes; emergency backlog threshold where a warning is logged
	exec                 riverdriver.Executor
	pilot                riverpilot.Pilot
	schema               string
	setStateParams       map[int64]batchCompleterSetState
	setStateParamsMu     sync.RWMutex
	subscribeCh          SubscribeChan
	waitOnBacklogChan    chan struct{}
	waitOnBacklogWaiting bool
}

func NewBatchCompleter(archetype *baseservice.Archetype, schema string, exec riverdriver.Executor, pilot riverpilot.Pilot, subscribeCh SubscribeChan) *BatchCompleter {
	const (
		completionMaxSize    = 5_000
		backlogWaitThreshold = completionMaxSize * 2
		maxBacklog           = 20_000
	)

	return baseservice.Init(archetype, &BatchCompleter{
		backlogWaitThreshold: backlogWaitThreshold,
		batchReadyChan:       make(chan struct{}, 1),
		completionMaxSize:    completionMaxSize,
		exec:                 exec,
		maxBacklog:           maxBacklog,
		pilot:                pilot,
		schema:               schema,
		setStateParams:       make(map[int64]batchCompleterSetState),
		subscribeCh:          subscribeCh,
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
					c.Logger.ErrorContext(ctx, c.Name+": Error completing batch", "err", err)
				}
				return

			case <-c.batchReadyChan:
			case <-ticker.C:
			}

			// The ticker fires quite often to make sure that given a huge glut
			// of jobs, we don't accidentally build up too much of a backlog by
			// waiting too long. However, don't start a complete operation until
			// we reach a minimum threshold unless we're on a tick that's a
			// multiple of 5. So, jobs will be completed every 250ms even if the
			// threshold hasn't been met.
			const batchCompleterStartThreshold = 100
			if backlogSize() < min(c.backlogWaitThresholdEffective(), batchCompleterStartThreshold) && numTicks != 0 && numTicks%5 != 0 {
				continue
			}

			for {
				if err := c.handleBatch(ctx); err != nil {
					c.Logger.ErrorContext(ctx, c.Name+": Error completing batch", "err", err)
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
	var setStateBatch map[int64]batchCompleterSetState
	func() {
		c.setStateParamsMu.Lock()
		defer c.setStateParamsMu.Unlock()

		setStateBatch = c.setStateParams

		// Don't bother resetting the map if there's nothing to process,
		// allowing the completer to idle efficiently.
		if len(setStateBatch) > 0 {
			c.setStateParams = make(map[int64]batchCompleterSetState)
		} else {
			// Set nil to avoid a data race below in case the map is set as a
			// new job comes in.
			setStateBatch = nil
		}
	}()

	if len(setStateBatch) < 1 {
		return nil
	}

	handleBatchError := func(err error) error {
		if isNonRetryableCompleterError(err) {
			c.releaseBacklogWaitIfReady(ctx)
			return err
		}

		c.requeueBatch(ctx, setStateBatch)
		return err
	}

	// Complete a sub-batch with retries. Also helps reduce visual noise and
	// increase readability of loop below.
	completeSubBatch := func(batchParams *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		start := time.Now()
		defer func() {
			c.Logger.DebugContext(ctx, c.Name+": Completed sub-batch of job(s)", "duration", time.Since(start), "num_jobs", len(batchParams.ID))
		}()

		return withRetries(ctx, &c.BaseService, c.disableSleep, func(ctx context.Context) ([]*rivertype.JobRow, error) {
			rows, err := c.pilot.JobSetStateIfRunningMany(ctx, c.exec, batchParams)
			if err != nil {
				return nil, err
			}

			return rows, nil
		})
	}

	// This could be written more simply using multiple map helpers, but it's
	// done this way to allocate as few new slices as necessary.
	mapBatch := func(setStateBatch map[int64]batchCompleterSetState) *riverdriver.JobSetStateIfRunningManyParams {
		params := &riverdriver.JobSetStateIfRunningManyParams{
			ID:              make([]int64, len(setStateBatch)),
			Attempt:         make([]*int, len(setStateBatch)),
			ErrData:         make([][]byte, len(setStateBatch)),
			FinalizedAt:     make([]*time.Time, len(setStateBatch)),
			MetadataDoMerge: make([]bool, len(setStateBatch)),
			MetadataUpdates: make([][]byte, len(setStateBatch)),
			ScheduledAt:     make([]*time.Time, len(setStateBatch)),
			State:           make([]rivertype.JobState, len(setStateBatch)),
		}
		var i int
		for _, setState := range setStateBatch {
			params.ID[i] = setState.Params.ID
			params.Attempt[i] = setState.Params.Attempt
			params.ErrData[i] = setState.Params.ErrData
			params.FinalizedAt[i] = setState.Params.FinalizedAt
			params.MetadataDoMerge[i] = setState.Params.MetadataDoMerge
			params.MetadataUpdates[i] = setState.Params.MetadataUpdates
			params.ScheduledAt[i] = setState.Params.ScheduledAt
			params.State[i] = setState.Params.State
			i++
		}
		params.Schema = c.schema
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
				ID:              params.ID[i:endIndex],
				Attempt:         params.Attempt[i:endIndex],
				ErrData:         params.ErrData[i:endIndex],
				FinalizedAt:     params.FinalizedAt[i:endIndex],
				MetadataDoMerge: params.MetadataDoMerge[i:endIndex],
				MetadataUpdates: params.MetadataUpdates[i:endIndex],
				ScheduledAt:     params.ScheduledAt[i:endIndex],
				Schema:          params.Schema,
				State:           params.State[i:endIndex],
			}
			jobRowsSubBatch, err := completeSubBatch(subBatch)
			if err != nil {
				return handleBatchError(err)
			}
			jobRows = append(jobRows, jobRowsSubBatch...)
		}
	} else {
		var err error
		jobRows, err = completeSubBatch(params)
		if err != nil {
			return handleBatchError(err)
		}
	}

	var (
		completeTime = c.Time.Now()
		events       = make([]CompleterJobUpdated, len(jobRows))
	)
	for i, jobRow := range jobRows {
		setState := setStateBatch[jobRow.ID]
		setState.Stats.CompleteDuration = completeTime.Sub(setState.StartTime)
		events[i] = CompleterJobUpdated{
			Job:      jobRow,
			JobStats: setState.Stats,
			Snoozed:  setState.Params.Snoozed,
		}
	}

	c.subscribeCh <- events

	func() {
		c.setStateParamsMu.Lock()
		defer c.setStateParamsMu.Unlock()

		if c.waitOnBacklogWaiting && len(c.setStateParams) < c.backlogResumeThreshold() {
			c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
			close(c.waitOnBacklogChan)
			c.waitOnBacklogWaiting = false
		}
	}()

	return nil
}

func (c *BatchCompleter) releaseBacklogWaitIfReady(ctx context.Context) {
	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	if c.waitOnBacklogWaiting && len(c.setStateParams) < c.backlogResumeThreshold() {
		c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
		close(c.waitOnBacklogChan)
		c.waitOnBacklogWaiting = false
	}
}

func (c *BatchCompleter) requeueBatch(ctx context.Context, setStateBatch map[int64]batchCompleterSetState) {
	c.setStateParamsMu.Lock()
	for id, setState := range setStateBatch {
		if _, exists := c.setStateParams[id]; exists {
			continue
		}
		c.setStateParams[id] = setState
	}
	backlogSize := len(c.setStateParams)
	if c.waitOnBacklogWaiting && backlogSize < c.backlogResumeThreshold() {
		c.Logger.DebugContext(ctx, c.Name+": Disabling waitOnBacklog; ready to complete more jobs")
		close(c.waitOnBacklogChan)
		c.waitOnBacklogWaiting = false
	}
	c.setStateParamsMu.Unlock()

	if backlogSize >= c.batchReadyThreshold() {
		c.signalBatchReady()
	}

	c.Logger.DebugContext(ctx, c.Name+": Requeued failed batch of job(s)", "num_jobs", len(setStateBatch))
}

func (c *BatchCompleter) JobSetStateIfRunning(ctx context.Context, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	now := c.Time.Now()

	var backlogSize int
	for {
		// Keep the common enqueue path to one lock acquisition. If the
		// completer is behind, wait for the current backlog gate to open and
		// retry so the threshold is checked against fresh state.
		var waitChan <-chan struct{}
		backlogSize, waitChan = c.tryEnqueueSetState(ctx, now, stats, params)
		if waitChan != nil {
			<-waitChan
			continue
		}
		break
	}

	if backlogSize >= c.batchReadyThreshold() {
		c.signalBatchReady()
	}

	return nil
}

func (c *BatchCompleter) tryEnqueueSetState(ctx context.Context, now time.Time, stats *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) (int, <-chan struct{}) {
	c.setStateParamsMu.Lock()
	defer c.setStateParamsMu.Unlock()

	if c.waitOnBacklogWaiting {
		return 0, c.waitOnBacklogChan
	}

	var (
		backlogSize = len(c.setStateParams)
		waitAt      = c.backlogWaitThresholdEffective()
	)
	if backlogSize >= waitAt {
		c.initBacklogWaitLocked(ctx, backlogSize, waitAt)
	}

	statsSnapshot := *stats
	c.setStateParams[params.ID] = batchCompleterSetState{Params: params, StartTime: now, Stats: &statsSnapshot}

	return len(c.setStateParams), nil
}

// backlogResumeThreshold returns the low-water mark below which waiting
// completers are released. Keeping this below the wait threshold avoids rapidly
// cycling between waiting and not waiting when the completer is near capacity.
func (c *BatchCompleter) backlogResumeThreshold() int {
	return max(c.backlogWaitThresholdEffective()/2, 1)
}

// backlogWaitThresholdEffective returns the backlog size at which new
// completions should wait for the batch completer to catch up. It's capped at
// maxBacklog so tests and future configuration can't set a normal wait
// threshold beyond the emergency warning threshold.
func (c *BatchCompleter) backlogWaitThresholdEffective() int {
	if c.backlogWaitThreshold <= 0 {
		return c.maxBacklog
	}
	return min(c.backlogWaitThreshold, c.maxBacklog)
}

// batchReadyThreshold returns the backlog size at which the run loop should be
// nudged to process a batch immediately instead of waiting for its next ticker.
// It aims for a full database batch while still respecting low test thresholds.
func (c *BatchCompleter) batchReadyThreshold() int {
	return min(c.completionMaxSize, c.backlogWaitThresholdEffective())
}

func (c *BatchCompleter) signalBatchReady() {
	select {
	case c.batchReadyChan <- struct{}{}:
	default:
	}
}

// initBacklogWaitLocked starts a backlog wait gate and must be called with
// setStateParamsMu held.
func (c *BatchCompleter) initBacklogWaitLocked(ctx context.Context, backlogSize, waitAt int) chan struct{} {
	c.waitOnBacklogChan = make(chan struct{})
	c.waitOnBacklogWaiting = true
	if backlogSize >= c.maxBacklog {
		c.Logger.WarnContext(ctx, c.Name+": Hit maximum backlog; completions will wait until below threshold",
			"backlog_size", backlogSize,
			"backlog_wait_threshold", waitAt,
			"max_backlog", c.maxBacklog,
		)
	} else {
		c.Logger.DebugContext(ctx, c.Name+": Applying completion backlog pressure",
			"backlog_resume_threshold", c.backlogResumeThreshold(),
			"backlog_size", backlogSize,
			"backlog_wait_threshold", waitAt,
		)
	}
	return c.waitOnBacklogChan
}

func isNonRetryableCompleterError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, riverdriver.ErrClosedPool)
}

// As configured, total time asleep from initial attempt is ~7 seconds (1 + 2 +
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
		// I've found that we want at least ten seconds for a large batch,
		// although it usually doesn't need that long.
		retVal, err := timeoututil.WithTimeoutV(uncancelledCtx, rivercommon.HotOperationTimeout, baseService.Name+".withRetries", retryFunc)
		if err != nil {
			// A cancelled context or a closed pool will never succeed.
			if isNonRetryableCompleterError(err) {
				return defaultVal, err
			}

			lastErr = err
			sleepDuration := serviceutil.ExponentialBackoff(attempt, serviceutil.MaxAttemptsBeforeResetDefault)
			baseService.Logger.ErrorContext(logCtx, baseService.Name+": Completer error (will retry after sleep)",
				slog.Int("attempt", attempt),
				slog.String("err", err.Error()),
				slog.String("sleep_duration", sleepDuration.String()),
				slog.String("timeout", rivercommon.HotOperationTimeout.String()),
			)
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
