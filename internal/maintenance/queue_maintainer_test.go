package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

type testService struct {
	queueMaintainerServiceBase
	startstop.BaseStartStop

	testSignals testServiceTestSignals
}

func newTestService(tb testing.TB) *testService {
	tb.Helper()

	testSvc := baseservice.Init(riversharedtest.BaseServiceArchetype(tb), &testService{})
	testSvc.testSignals.Init()

	return testSvc
}

func (s *testService) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped()

		s.testSignals.started.Signal(struct{}{})
		<-ctx.Done()
		s.testSignals.returning.Signal(struct{}{})
	}()

	return nil
}

type testServiceTestSignals struct {
	returning testsignal.TestSignal[struct{}]
	started   testsignal.TestSignal[struct{}]
}

func (ts *testServiceTestSignals) Init() {
	ts.returning.Init()
	ts.started.Init()
}

func TestQueueMaintainer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T, services []startstop.Service) *QueueMaintainer {
		t.Helper()

		maintainer := NewQueueMaintainer(riversharedtest.BaseServiceArchetype(t), services)
		maintainer.StaggerStartupDisable(true)

		return maintainer
	}

	t.Run("StartStop", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []startstop.Service{testSvc})

		require.NoError(t, maintainer.Start(ctx))
		testSvc.testSignals.started.WaitOrTimeout()
		maintainer.Stop()
		testSvc.testSignals.returning.WaitOrTimeout()
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		tx := riverinternaltest.TestTx(ctx, t)
		sharedTx := sharedtx.NewSharedTx(tx)

		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress

		driver := riverpgxv5.New(nil).UnwrapExecutor(sharedTx)

		// Use realistic services in this one so we can verify stress not only
		// on the queue maintainer, but it and all its subservices together.
		maintainer := setup(t, []startstop.Service{
			NewJobCleaner(archetype, &JobCleanerConfig{}, driver),
			NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
				PeriodicJobs: []*PeriodicJob{
					{
						ConstructorFunc: func() (*rivertype.JobInsertParams, error) {
							return nil, ErrNoJobToInsert
						},
						ScheduleFunc: cron.Every(15 * time.Minute).Next,
					},
				},
			}, driver),
			NewQueueCleaner(archetype, &QueueCleanerConfig{}, driver),
			NewJobScheduler(archetype, &JobSchedulerConfig{}, driver),
		})
		maintainer.Logger = riversharedtest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		startstoptest.Stress(ctx, t, maintainer)
	})

	t.Run("StopWithoutHavingBeenStarted", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []startstop.Service{testSvc})

		// Tolerate being stopped without having been started, without blocking:
		maintainer.Stop()

		require.NoError(t, maintainer.Start(ctx))
		testSvc.testSignals.started.WaitOrTimeout()
		maintainer.Stop()
	})

	t.Run("MultipleStartStop", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []startstop.Service{testSvc})

		runOnce := func() {
			require.NoError(t, maintainer.Start(ctx))
			testSvc.testSignals.started.WaitOrTimeout()
			maintainer.Stop()
			testSvc.testSignals.returning.WaitOrTimeout()
		}

		for i := 0; i < 3; i++ {
			runOnce()
		}
	})

	t.Run("RespectsContextCancellation", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []startstop.Service{testSvc})

		ctx, cancelFunc := context.WithCancel(ctx)

		require.NoError(t, maintainer.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := maintainer.Stopped()
		cancelFunc()
		riversharedtest.WaitOrTimeout(t, stopped)
	})

	t.Run("GetService", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)

		maintainer := setup(t, []startstop.Service{testSvc})

		require.NoError(t, maintainer.Start(ctx))
		testSvc.testSignals.started.WaitOrTimeout()

		svc := GetService[*testService](maintainer)
		if svc == nil {
			t.Fatal("GetService returned nil")
		}

		maintainer.Stop()
		testSvc.testSignals.returning.WaitOrTimeout()
	})
}
