package maintenance

import (
	"context"
	"reflect"

	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/maputil"
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
		servicesByName[serviceName(service)] = service
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

		startstop.StopAllParallel(maputil.Values(m.servicesByName)...)
	}()

	return nil
}

// GetService is a convenience method for getting a service by name and casting
// it to the desired type. It should only be used in tests due to its use of
// reflection and potential for panics.
func GetService[T startstop.Service](maintainer *QueueMaintainer) T {
	var kindPtr T
	return maintainer.servicesByName[serviceName(kindPtr)].(T) //nolint:forcetypeassert
}

func serviceName(service startstop.Service) string {
	elem := reflect.TypeOf(service).Elem()
	return elem.PkgPath() + "." + elem.Name()
}

// withStaggerStartupDisable is an interface to a service whose stagger startup
// sleep can be disabled.
type withStaggerStartupDisable interface {
	// StaggerStartupDisable sets whether the short staggered sleep on start up
	// is disabled. This is useful in tests where the extra sleep involved in a
	// staggered start up is not helpful for test run time.
	StaggerStartupDisable(disabled bool)
}
