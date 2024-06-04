package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/internal/riverinternaltest/startstoptest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
)

func runMaintenanceService(ctx context.Context, t *testing.T, svc rivertype.MaintenanceService) {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		svc.Run(ctx)
	}()

	t.Cleanup(func() {
		riverinternaltest.WaitOrTimeout(t, stopped)
	})
	t.Cleanup(cancel)
}

type testService struct {
	baseservice.BaseService

	testSignals testServiceTestSignals
}

func newTestService(tb testing.TB) *testService {
	tb.Helper()

	testSvc := baseservice.Init(riverinternaltest.BaseServiceArchetype(tb), &testService{})
	testSvc.testSignals.Init()

	return testSvc
}

func (s *testService) Run(ctx context.Context) {
	s.testSignals.started.Signal(struct{}{})
	<-ctx.Done()
	s.testSignals.returning.Signal(struct{}{})
}

type testServiceTestSignals struct {
	returning rivercommon.TestSignal[struct{}]
	started   rivercommon.TestSignal[struct{}]
}

func (ts *testServiceTestSignals) Init() {
	ts.returning.Init()
	ts.started.Init()
}

func TestQueueMaintainer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T, services []rivertype.MaintenanceService) *QueueMaintainer {
		t.Helper()

		maintainer := NewQueueMaintainer(riverinternaltest.BaseServiceArchetype(t), services)
		maintainer.StaggerStartupDisable(true)

		return maintainer
	}

	t.Run("StartStop", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []rivertype.MaintenanceService{testSvc})

		require.NoError(t, maintainer.Start(ctx))
		testSvc.testSignals.started.WaitOrTimeout()
		maintainer.Stop()
		testSvc.testSignals.returning.WaitOrTimeout()
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		tx := riverinternaltest.TestTx(ctx, t)
		sharedTx := sharedtx.NewSharedTx(tx)

		archetype := riverinternaltest.BaseServiceArchetype(t)
		archetype.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress

		driver := riverpgxv5.New(nil).UnwrapExecutor(sharedTx)

		// Use realistic services in this one so we can verify stress not only
		// on the queue maintainer, but it and all its subservices together.
		maintainer := setup(t, []rivertype.MaintenanceService{
			NewJobCleaner(archetype, &JobCleanerConfig{}, driver),
			NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
				PeriodicJobs: []*PeriodicJob{
					{
						ConstructorFunc: func() (*riverdriver.JobInsertFastParams, *dbunique.UniqueOpts, error) {
							return nil, nil, ErrNoJobToInsert
						},
						ScheduleFunc: cron.Every(15 * time.Minute).Next,
					},
				},
			}, driver),
			NewQueueCleaner(archetype, &QueueCleanerConfig{}, driver),
			NewJobScheduler(archetype, &JobSchedulerConfig{}, driver),
		})
		maintainer.Logger = riverinternaltest.LoggerWarn(t) // loop started/stop log is very noisy; suppress
		startstoptest.Stress(ctx, t, maintainer)
	})

	t.Run("StopWithoutHavingBeenStarted", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []rivertype.MaintenanceService{testSvc})

		// Tolerate being stopped without having been started, without blocking:
		maintainer.Stop()

		require.NoError(t, maintainer.Start(ctx))
		testSvc.testSignals.started.WaitOrTimeout()
		maintainer.Stop()
	})

	t.Run("MultipleStartStop", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := setup(t, []rivertype.MaintenanceService{testSvc})

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
		maintainer := setup(t, []rivertype.MaintenanceService{testSvc})

		ctx, cancelFunc := context.WithCancel(ctx)

		require.NoError(t, maintainer.Start(ctx))

		// To avoid a potential race, make sure to get a reference to the
		// service's stopped channel _before_ cancellation as it's technically
		// possible for the cancel to "win" and remove the stopped channel
		// before we can start waiting on it.
		stopped := maintainer.Stopped()
		cancelFunc()
		riverinternaltest.WaitOrTimeout(t, stopped)
	})

	t.Run("GetService", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)

		maintainer := setup(t, []rivertype.MaintenanceService{testSvc})

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

func MaintenanceServiceStopsImmediately(ctx context.Context, t *testing.T, svc rivertype.MaintenanceService) {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)
	stopCh := make(chan struct{})
	go func() {
		defer close(stopCh)
		svc.Run(ctx)
	}()
	cancel()
	riverinternaltest.WaitOrTimeout(t, stopCh)
}
