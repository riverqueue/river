package startstop

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/riverinternaltest"
)

type sampleService struct {
	baseservice.BaseService
	BaseStartStop

	// Some simple state in the service which a started service taints. The
	// purpose of this variable is to allow us to detect a data race allowed by
	// BaseStartStop.
	state bool
}

func (s *sampleService) Start(ctx context.Context) error {
	ctx, shouldStart, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	go func() {
		defer close(stopped)
		s.state = true
		<-ctx.Done()
	}()

	return nil
}

func testService(t *testing.T, newService func(t *testing.T) serviceWithStopped) {
	t.Helper()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (serviceWithStopped, *testBundle) {
		t.Helper()

		return newService(t), &testBundle{}
	}

	t.Run("DoubleStop", func(t *testing.T) {
		t.Parallel()

		service, _ := setup(t)

		require.NoError(t, service.Start(ctx))
		service.Stop()
		service.Stop()
	})

	t.Run("StopWithoutStart", func(t *testing.T) {
		t.Parallel()

		service, _ := setup(t)

		service.Stop()
	})

	t.Run("StoppedChannel", func(t *testing.T) {
		t.Parallel()

		service, _ := setup(t)

		require.NoError(t, service.Start(ctx))

		// A reference to stopped must be procured _before_ stopping the service
		// because the stopped channel is deinitialized as part of the stop
		// procedure.
		stopped := service.Stopped()
		service.Stop()
		riverinternaltest.WaitOrTimeout(t, stopped)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		service, _ := setup(t)

		var wg sync.WaitGroup

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				for j := 0; j < 50; j++ {
					require.NoError(t, service.Start(ctx))
					service.Stop()
				}
				wg.Done()
			}()
		}

		wg.Wait()
	})
}

func TestBaseStartStop(t *testing.T) {
	t.Parallel()

	testService(t, func(t *testing.T) serviceWithStopped { t.Helper(); return &sampleService{} })
}

func TestBaseStartStopFunc(t *testing.T) {
	t.Parallel()

	makeFunc := func(t *testing.T) serviceWithStopped {
		t.Helper()

		// Some simple state in the service which a started service taints. The
		// purpose of this variable is to allow us to detect a data race allowed by
		// BaseStartStop.
		var state bool

		return StartStopFunc(func(ctx context.Context, shouldStart bool, stopped chan struct{}) error {
			if !shouldStart {
				return nil
			}

			go func() {
				defer close(stopped)
				state = true
				t.Logf("State: %t", state) // here so variable doesn't register as unused
				<-ctx.Done()
			}()

			return nil
		})
	}

	testService(t, makeFunc)
}

func TestStopAllParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Started", func(t *testing.T) {
		t.Parallel()

		var (
			service1 = &sampleService{}
			service2 = &sampleService{}
			service3 = &sampleService{}
		)

		require.NoError(t, service1.Start(ctx))
		require.NoError(t, service2.Start(ctx))
		require.NoError(t, service3.Start(ctx))

		var (
			stopped1 = service1.Stopped()
			stopped2 = service2.Stopped()
			stopped3 = service3.Stopped()
		)

		StopAllParallel([]Service{
			service1,
			service2,
			service3,
		})

		riverinternaltest.WaitOrTimeout(t, stopped1)
		riverinternaltest.WaitOrTimeout(t, stopped2)
		riverinternaltest.WaitOrTimeout(t, stopped3)
	})

	// We can't use the stopped channels in this case because they're only
	// initiated when a service is started.
	t.Run("NotStarted", func(t *testing.T) {
		t.Parallel()

		var (
			service1 = &sampleService{}
			service2 = &sampleService{}
			service3 = &sampleService{}
		)

		StopAllParallel([]Service{
			service1,
			service2,
			service3,
		})
	})
}
