package maintenance

import (
	"context"
	"reflect"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/startstop"
	"github.com/riverqueue/river/internal/util/maputil"
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

	logPrefixRanSuccessfully = ": Ran successfully"
	logPrefixRunLoopStarted  = ": Run loop started"
	logPrefixRunLoopStopped  = ": Run loop stopped"
)

// QueueMaintainer runs regular maintenance operations against job queues, like
// pruning completed jobs. It runs only on the client which has been elected
// leader at any given time.
//
// Its methods are not safe for concurrent usage.
type QueueMaintainer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	servicesByName map[string]startstop.Service
}

func NewQueueMaintainer(archetype *baseservice.Archetype, services []startstop.Service) *QueueMaintainer {
	servicesByName := make(map[string]startstop.Service, len(services))
	for _, service := range services {
		servicesByName[reflect.TypeOf(service).Elem().Name()] = service
	}
	return baseservice.Init(archetype, &QueueMaintainer{
		servicesByName: servicesByName,
	})
}

// StaggerStartupDisable sets whether the short staggered sleep on start up
// is disabled. This is useful in tests where the extra sleep involved in a
// staggered start up is not helpful for test run time.
func (m *QueueMaintainer) StaggerStartupDisable(disabled bool) {
	for _, svc := range m.servicesByName {
		if svcWithDisable, ok := svc.(withStaggerStartupDisable); ok {
			svcWithDisable.StaggerStartupDisable(disabled)
		}
	}
}

func (m *QueueMaintainer) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := m.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	for _, service := range m.servicesByName {
		if err := service.Start(ctx); err != nil {
			return err
		}
	}

	go func() {
		// Wait for all subservices to start up before signaling our own start.
		startstop.WaitAllStarted(maputil.Values(m.servicesByName)...)

		started()
		defer stopped() // this defer should come first so it's last out

		<-ctx.Done()

		startstop.StopAllParallel(maputil.Values(m.servicesByName))
	}()

	return nil
}

// GetService is a convenience method for getting a service by name and casting
// it to the desired type. It should only be used in tests due to its use of
// reflection and potential for panics.
func GetService[T startstop.Service](maintainer *QueueMaintainer) T {
	var kindPtr T
	return maintainer.servicesByName[reflect.TypeOf(kindPtr).Elem().Name()].(T) //nolint:forcetypeassert
}

// queueMaintainerServiceBase is a struct that should be embedded on all queue
// maintainer services. Its main use is to provide a StaggerStart function that
// should be called on service start to avoid thundering herd problems.
type queueMaintainerServiceBase struct {
	baseservice.BaseService
	staggerStartupDisabled bool
}

// StaggerStart is called when queue maintainer services start. It jitters by
// sleeping for a short random period so services don't all perform their first
// run at exactly the same time.
func (s *queueMaintainerServiceBase) StaggerStart(ctx context.Context) {
	if s.staggerStartupDisabled {
		return
	}

	s.CancellableSleepRandomBetween(ctx, 0*time.Second, 1*time.Second)
}

// StaggerStartupDisable sets whether the short staggered sleep on start up
// is disabled. This is useful in tests where the extra sleep involved in a
// staggered start up is not helpful for test run time.
func (s *queueMaintainerServiceBase) StaggerStartupDisable(disabled bool) {
	s.staggerStartupDisabled = disabled
}

func (s *queueMaintainerServiceBase) StaggerStartupIsDisabled() bool {
	return s.staggerStartupDisabled
}

// withStaggerStartupDisable is an interface to a service whose stagger startup
// sleep can be disable.
type withStaggerStartupDisable interface {
	// StaggerStartupDisable sets whether the short staggered sleep on start up
	// is disabled. This is useful in tests where the extra sleep involved in a
	// staggered start up is not helpful for test run time.
	StaggerStartupDisable(disabled bool)
}
