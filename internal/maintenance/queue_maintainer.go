package maintenance

import (
	"context"
	"reflect"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/maintenance/startstop"
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
	DefaultBatchSize = 1_000

	JitterMin = 0 * time.Second
	JitterMax = 1 * time.Second
)

// QueueMaintainer runs regular maintenance operations against job queues, like
// pruning completed jobs. It runs only on the client which has been elected
// leader at any given time.
//
// Its methods are not safe for concurrent usage.
type QueueMaintainer struct {
	baseservice.BaseService
	startstop.BaseStartStop

	servicesByName map[string]Service
}

func NewQueueMaintainer(archetype *baseservice.Archetype, services []Service) *QueueMaintainer {
	servicesByName := make(map[string]Service, len(services))
	for _, service := range services {
		servicesByName[reflect.TypeOf(service).Elem().Name()] = service
	}
	return baseservice.Init(archetype, &QueueMaintainer{
		servicesByName: servicesByName,
	})
}

func (m *QueueMaintainer) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := m.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	for _, service := range m.servicesByName {
		if err := service.Start(ctx); err != nil {
			return err
		}
	}

	go func() {
		// This defer should come first so that it's last out, thereby avoiding
		// races.
		defer close(stopped)

		<-ctx.Done()

		for _, service := range m.servicesByName {
			service.Stop()
		}
	}()

	return nil
}

// GetService is a convenience method for getting a service by name and casting
// it to the desired type. It should only be used in tests due to its use of
// reflection and potential for panics.
func GetService[T Service](maintainer *QueueMaintainer) T {
	var kindPtr T
	return maintainer.servicesByName[reflect.TypeOf(kindPtr).Elem().Name()].(T) //nolint:forcetypeassert
}

type Service interface {
	// Start starts a service. Services are responsible for backgrounding
	// themselves, so this function should be invoked synchronously. Services
	// may return an error if they have trouble starting up, so the caller
	// should wait and respond to the error if necessary.
	Start(ctx context.Context) error

	// Stop stops a service. Services are responsible for making sure their stop
	// is complete before returning so a caller can wait on this invocation
	// synchronously and be guaranteed the service is fully stopped. Services
	// are expected to be able to tolerate (1) being stopped without having been
	// started, and (2) being double-stopped.
	Stop()
}
