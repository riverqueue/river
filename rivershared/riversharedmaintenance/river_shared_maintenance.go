package riversharedmaintenance

import (
	"context"
	"time"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/serviceutil"
)

const (
	// Maintainers will sleep a brief period of time between batches to give the
	// database some breathing room.
	BatchBackoffMax = 1 * time.Second
	BatchBackoffMin = 50 * time.Millisecond

	// Bulk maintenance tasks like job removal operate in batches so that even
	// in the event of an enormous backlog of work to do, transactions stay
	// relatively short and aren't at risk of cancellation. This number is the
	// batch size, or the number of rows that are handled at a time.
	//
	// The specific value is somewhat arbitrary as large enough to make good
	// progress, but not so large as to make the operation overstay its welcome.
	// For now it's not configurable because we can likely pick a number that's
	// suitable for almost everyone.
	BatchSizeDefault = 1_000

	LogPrefixRanSuccessfully = ": Ran successfully"
	LogPrefixRunLoopStarted  = ": Run loop started"
	LogPrefixRunLoopStopped  = ": Run loop stopped"
)

// Constants related to JobCleaner.
const (
	CancelledJobRetentionPeriodDefault = 24 * time.Hour
	CompletedJobRetentionPeriodDefault = 24 * time.Hour
	DiscardedJobRetentionPeriodDefault = 7 * 24 * time.Hour

	JobCleanerIntervalDefault = 30 * time.Second
	JobCleanerTimeoutDefault  = 30 * time.Second
)

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
