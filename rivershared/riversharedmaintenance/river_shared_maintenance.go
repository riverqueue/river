package riversharedmaintenance

import (
	"cmp"
	"context"
	"time"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/circuitbreaker"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
)

const (
	// Maintainers will sleep a brief period of time between batches to give the
	// database some breathing room.
	BatchBackoffMax = 1 * time.Second
	BatchBackoffMin = 50 * time.Millisecond

	LogPrefixRanSuccessfully = ": Ran successfully"
	LogPrefixRunLoopStarted  = ": Run loop started"
	LogPrefixRunLoopStopped  = ": Run loop stopped"

	// TimeoutDefault is a reasonable timeout for any large maintenance-related
	// queries. Some maintainers may opt to switch to their own timeout, but
	// this one should generally be used unless there's a good reason to have a
	// specific version.
	TimeoutDefault = 30 * time.Second
)

// Constants related to JobCleaner.
const (
	CancelledJobRetentionPeriodDefault = 24 * time.Hour
	CompletedJobRetentionPeriodDefault = 24 * time.Hour
	DiscardedJobRetentionPeriodDefault = 7 * 24 * time.Hour

	JobCleanerIntervalDefault = 30 * time.Second
	JobCleanerTimeoutDefault  = 30 * time.Second
)

const (
	// BatchSizeDefault is the default batch size of most maintenance services.
	//
	// Bulk maintenance tasks like job removal operate in batches so that even
	// in the event of an enormous backlog of work to do, transactions stay
	// relatively short and aren't at risk of cancellation. This number is the
	// batch size, or the number of rows that are handled at a time.
	//
	// The specific value is somewhat arbitrary as large enough to make good
	// progress, but not so large as to make the operation overstay its welcome.
	// For now it's not configurable because we can likely pick a number that's
	// suitable for almost everyone.
	//
	// In case database degradation is detected, most maintenance services will
	// back off to use the smaller batch size BatchSizeReduced.
	BatchSizeDefault = 10_000

	// BatchSizeReduced is the reduced batch size of most maintenance services.
	//
	// Services start out with a batch size of BatchSizeDefault, but as they
	// detect a degraded database may switch to BatchSizeReduced instead so that
	// they're trying to do less work per operation.
	BatchSizeReduced = 1_000
)

// BatchSizes containing batch size information for maintenance services. It's
// mean to be embedded on each service's configuration struct so as to provide a
// common way of organizing and initializing batch sizes for improved
// succinctness.
type BatchSizes struct {
	// Default is the maximum number of jobs to transition at once from
	// "scheduled" to "available" during periodic scheduling checks.
	Default int

	// Reduced is a considerably smaller batch size that the service
	// uses after encountering 3 consecutive timeouts in a row. The idea behind
	// this is that if it appears the database is degraded, then we start doing
	// less work in the hope that it can better succeed.
	Reduced int
}

// MustValidate validates the struct and panics in case a value is invalid.
func (b BatchSizes) MustValidate() BatchSizes {
	if b.Default <= 0 {
		panic("BatchSizes.Default must be above zero")
	}
	if b.Reduced <= 0 {
		panic("BatchSizes.Reduced must be above zero")
	}
	return b
}

// WithDefaults returns the struct with any configuration overrides that were
// already set, but sets defaults for any that were zero values.
func (b BatchSizes) WithDefaults() BatchSizes {
	return BatchSizes{
		Default: cmp.Or(b.Default, BatchSizeDefault),
		Reduced: cmp.Or(b.Reduced, BatchSizeReduced),
	}
}

// ReducedBatchSizeBreaker returns a reduced batch circuit breaker suitable for
// use in most maintenance services. After being tripped three consecutive times
// inside a ten minute window it switches to a reduced batch size. A success at
// any point between failures will reset it, but after the circuit breaker has
// tripped, it stays tripped for the life time of the program.
func ReducedBatchSizeBreaker(batchSizes BatchSizes) *circuitbreaker.CircuitBreaker {
	return circuitbreaker.NewCircuitBreaker(&circuitbreaker.CircuitBreakerOptions{
		Limit:  3,
		Window: 10 * time.Minute,
	})
}

// QueueMaintainerServiceBase is a struct that should be embedded on all queue
// maintainer services. Its main use is to provide a StaggerStart function that
// should be called on service start to avoid thundering herd problems.
type QueueMaintainerServiceBase struct {
	baseservice.BaseService

	staggerStartupDisabled bool
}

// StaggerStart is called when queue maintainer services start. It jitters by
// sleeping for a short random period so services don't all perform their first
// run at exactly the same time.
func (s *QueueMaintainerServiceBase) StaggerStart(ctx context.Context) {
	if s.staggerStartupDisabled {
		return
	}

	serviceutil.CancellableSleep(ctx, randutil.DurationBetween(0*time.Second, 1*time.Second))
}

// StaggerStartupDisable sets whether the short staggered sleep on start up
// is disabled. This is useful in tests where the extra sleep involved in a
// staggered start up is not helpful for test run time.
func (s *QueueMaintainerServiceBase) StaggerStartupDisable(disabled bool) {
	s.staggerStartupDisabled = disabled
}

func (s *QueueMaintainerServiceBase) StaggerStartupIsDisabled() bool {
	return s.staggerStartupDisabled
}
