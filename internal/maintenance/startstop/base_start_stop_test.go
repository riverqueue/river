package startstop

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"weavelab.xyz/river/internal/baseservice"
	"weavelab.xyz/river/internal/riverinternaltest"
)

type testService struct {
	baseservice.BaseService
	BaseStartStop

	// Some simple state in the service which a started service taints. The
	// purpose of this variable is to allow us to detect a data race allowed by
	// BaseStartStop.
	state bool
}

func (s *testService) Start(ctx context.Context) error {
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

func TestBaseStartStop_doubleStop(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		testSvc = &testService{}
	)

	require.NoError(t, testSvc.Start(ctx))
	testSvc.Stop()
	testSvc.Stop()
}

func TestBaseStartStop_stopWithoutStart(t *testing.T) {
	t.Parallel()

	testSvc := &testService{}
	testSvc.Stop()
}

func TestBaseStartStop_stopped(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		testSvc = &testService{}
	)

	require.NoError(t, testSvc.Start(ctx))

	// A reference to stopped must be procured _before_ stopping the service
	// because the stopped channel is deinitialized as part of the stop
	// procedure.
	stopped := testSvc.Stopped()
	testSvc.Stop()
	riverinternaltest.WaitOrTimeout(t, stopped)
}

func TestBaseStartStop_stress(t *testing.T) {
	t.Parallel()

	var (
		ctx     = context.Background()
		testSvc = &testService{}
		wg      sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 50; j++ {
				require.NoError(t, testSvc.Start(ctx))
				testSvc.Stop()
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
