package jobcompleter

import (
	"context"
	"math"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"weavelab.xyz/river/internal/baseservice"
	"weavelab.xyz/river/internal/dbadapter"
	"weavelab.xyz/river/internal/dbsqlc"
	"weavelab.xyz/river/internal/jobstats"
	"weavelab.xyz/river/internal/util/timeutil"
)

type JobCompleter interface {
	// JobSetState sets a new state for the given job, as long as it's
	// still running (i.e. its state has not changed to something else already).
	JobSetStateIfRunning(stats *jobstats.JobStatistics, params *dbadapter.JobSetStateIfRunningParams) error

	// Subscribe injects a callback which will be invoked whenever a job is
	// updated.
	Subscribe(subscribeFunc func(update CompleterJobUpdated))

	// Wait waits for all ongoing completions to finish, enabling graceful
	// shutdown.
	Wait()
}

type CompleterJobUpdated struct {
	Job      *dbsqlc.RiverJob
	JobStats *jobstats.JobStatistics
}

type InlineJobCompleter struct {
	baseservice.BaseService

	adapter         dbadapter.Adapter
	subscribeFunc   func(update CompleterJobUpdated)
	subscribeFuncMu sync.Mutex

	// A waitgroup is not actually needed for the inline completer because as
	// long as the caller is waiting on each function call, completion is
	// guaranteed to be done by the time Wait is called. However, we use a
	// generic test helper for all completers that starts goroutines, so this
	// left in for now for the benefit of the test suite.
	wg sync.WaitGroup
}

func NewInlineCompleter(archetype *baseservice.Archetype, adapter dbadapter.Adapter) *InlineJobCompleter {
	return baseservice.Init(archetype, &InlineJobCompleter{
		adapter: adapter,
	})
}

func (c *InlineJobCompleter) JobSetStateIfRunning(stats *jobstats.JobStatistics, params *dbadapter.JobSetStateIfRunningParams) error {
	return c.doOperation(stats, func(ctx context.Context) (*dbsqlc.RiverJob, error) {
		return c.adapter.JobSetStateIfRunning(ctx, params)
	})
}

func (c *InlineJobCompleter) Subscribe(subscribeFunc func(update CompleterJobUpdated)) {
	c.subscribeFuncMu.Lock()
	defer c.subscribeFuncMu.Unlock()

	c.subscribeFunc = subscribeFunc
}

func (c *InlineJobCompleter) Wait() {
	c.wg.Wait()
}

func (c *InlineJobCompleter) doOperation(stats *jobstats.JobStatistics, f func(ctx context.Context) (*dbsqlc.RiverJob, error)) error {
	c.wg.Add(1)
	defer c.wg.Done()

	start := c.TimeNowUTC()

	job, err := withRetries(&c.BaseService, f)
	if err != nil {
		return err
	}

	stats.CompleteDuration = c.TimeNowUTC().Sub(start)

	func() {
		c.subscribeFuncMu.Lock()
		defer c.subscribeFuncMu.Unlock()

		if c.subscribeFunc != nil {
			c.subscribeFunc(CompleterJobUpdated{Job: job, JobStats: stats})
		}
	}()

	return nil
}

type AsyncJobCompleter struct {
	baseservice.BaseService

	adapter         dbadapter.Adapter
	concurrency     uint32
	eg              *errgroup.Group
	subscribeFunc   func(update CompleterJobUpdated)
	subscribeFuncMu sync.Mutex
}

func NewAsyncCompleter(archetype *baseservice.Archetype, adapter dbadapter.Adapter, concurrency uint32) *AsyncJobCompleter {
	eg := &errgroup.Group{}
	// TODO: int concurrency may feel more natural than uint32
	eg.SetLimit(int(concurrency))

	return baseservice.Init(archetype, &AsyncJobCompleter{
		adapter:     adapter,
		concurrency: concurrency,
		eg:          eg,
	})
}

func (c *AsyncJobCompleter) JobSetStateIfRunning(stats *jobstats.JobStatistics, params *dbadapter.JobSetStateIfRunningParams) error {
	return c.doOperation(stats, func(ctx context.Context) (*dbsqlc.RiverJob, error) {
		return c.adapter.JobSetStateIfRunning(ctx, params)
	})
}

func (c *AsyncJobCompleter) doOperation(stats *jobstats.JobStatistics, f func(ctx context.Context) (*dbsqlc.RiverJob, error)) error {
	c.eg.Go(func() error {
		start := c.TimeNowUTC()

		job, err := withRetries(&c.BaseService, f)
		if err != nil {
			return err
		}

		stats.CompleteDuration = c.TimeNowUTC().Sub(start)

		func() {
			c.subscribeFuncMu.Lock()
			defer c.subscribeFuncMu.Unlock()

			if c.subscribeFunc != nil {
				c.subscribeFunc(CompleterJobUpdated{Job: job, JobStats: stats})
			}
		}()

		return nil
	})
	return nil
}

func (c *AsyncJobCompleter) Subscribe(subscribeFunc func(update CompleterJobUpdated)) {
	c.subscribeFuncMu.Lock()
	defer c.subscribeFuncMu.Unlock()

	c.subscribeFunc = subscribeFunc
}

func (c *AsyncJobCompleter) Wait() {
	// TODO: handle error?
	_ = c.eg.Wait()
}

// As configued, total time from initial attempt is ~7 seconds (1 + 2 + 4) (not
// including jitter). I put in a basic retry algorithm to hold us over, but we
// may want to rethink these numbers and strategy.
const numRetries = 3

func withRetries(c *baseservice.BaseService, f func(ctx context.Context) (*dbsqlc.RiverJob, error)) (*dbsqlc.RiverJob, error) { //nolint:varnamelen
	retrySecondsWithoutJitter := func(attempt int) float64 {
		// Uses a different algorithm (2 ** N) compared to retry policies (4 **
		// N) so we can get more retries sooner: 1, 2, 4, 8
		return math.Pow(2, float64(attempt))
	}

	retrySeconds := func(attempt int) float64 {
		retrySeconds := retrySecondsWithoutJitter(attempt)

		// Jitter number of seconds +/- 10%.
		retrySeconds += retrySeconds * (c.Rand.Float64()*0.2 - 0.1)

		return retrySeconds
	}

	tryOnce := func() (*dbsqlc.RiverJob, error) {
		ctx := context.Background()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		return f(ctx)
	}

	var lastErr error

	// TODO: Added a basic retry algorithm based on the top-level retry policies
	// for now, but we may want to reevaluate this somewhat.
	for attempt := 1; attempt < numRetries+1; attempt++ {
		job, err := tryOnce()
		if err != nil {
			lastErr = err
			sleepDuration := timeutil.SecondsAsDuration(retrySeconds(attempt))
			// TODO: this logger doesn't use the user-provided context because it's
			// not currently available here. It should.
			c.Logger.Error(c.Name+": Completer error (will retry)", "attempt", attempt, "err", err, "sleep_duration", sleepDuration)
			c.CancellableSleep(context.Background(), sleepDuration)
			continue
		}

		return job, nil
	}

	// TODO: this logger doesn't use the user-provided context because it's
	// not currently available here. It should.
	c.Logger.Error(c.Name + ": Too many errors; giving up")
	return nil, lastErr
}
