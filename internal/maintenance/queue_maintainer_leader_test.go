package maintenance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
)

func TestQueueMaintainerLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T, maintainer *QueueMaintainer) *QueueMaintainerLeader {
		t.Helper()

		var (
			dbPool    = riversharedtest.DBPool(ctx, t)
			driver    = riverpgxv5.New(dbPool)
			schema    = riverdbtest.TestSchema(ctx, t, driver, nil)
			archetype = riversharedtest.BaseServiceArchetype(t)
		)

		elector := leadership.NewElector(archetype, driver.GetExecutor(), nil, &leadership.Config{
			ClientID: "test_client_id",
			Schema:   schema,
		})
		require.NoError(t, elector.Start(ctx))
		t.Cleanup(elector.Stop)

		leader := NewQueueMaintainerLeader(archetype, &QueueMaintainerLeaderConfig{
			ClientID:        "test_client_id",
			Elector:         elector,
			QueueMaintainer: maintainer,
			RequestResignFunc: func(ctx context.Context) error {
				return nil
			},
		})
		leader.TestSignals.Init(t)

		return leader
	}

	t.Run("RetriesAndResignsOnStartFailure", func(t *testing.T) {
		t.Parallel()

		var startAttempts atomic.Int64
		failingSvc := &failingStartService{startAttempts: &startAttempts}

		maintainer := NewQueueMaintainer(riversharedtest.BaseServiceArchetype(t), []startstop.Service{failingSvc})
		maintainer.StaggerStartupDisable(true)

		resignCalled := make(chan struct{})
		archetype := riversharedtest.BaseServiceArchetype(t)

		var (
			dbPool = riversharedtest.DBPool(ctx, t)
			driver = riverpgxv5.New(dbPool)
			schema = riverdbtest.TestSchema(ctx, t, driver, nil)
		)

		elector := leadership.NewElector(archetype, driver.GetExecutor(), nil, &leadership.Config{
			ClientID: "test_client_id",
			Schema:   schema,
		})
		require.NoError(t, elector.Start(ctx))
		t.Cleanup(elector.Stop)

		leader := NewQueueMaintainerLeader(archetype, &QueueMaintainerLeaderConfig{
			ClientID:        "test_client_id",
			Elector:         elector,
			QueueMaintainer: maintainer,
			RequestResignFunc: func(ctx context.Context) error {
				close(resignCalled)
				return nil
			},
		})
		leader.TestSignals.Init(t)

		require.NoError(t, leader.Start(ctx))
		t.Cleanup(leader.Stop)

		leader.TestSignals.ElectedLeader.WaitOrTimeout()

		for range queueMaintainerMaxStartAttempts {
			err := leader.TestSignals.StartError.WaitOrTimeout()
			require.EqualError(t, err, "start error")
		}

		leader.TestSignals.StartRetriesExhausted.WaitOrTimeout()
		riversharedtest.WaitOrTimeout(t, resignCalled)
		require.Equal(t, int64(queueMaintainerMaxStartAttempts), startAttempts.Load())
	})

	t.Run("StartsMaintainerOnLeadershipGain", func(t *testing.T) {
		t.Parallel()

		testSvc := newTestService(t)
		maintainer := NewQueueMaintainer(riversharedtest.BaseServiceArchetype(t), []startstop.Service{testSvc})
		maintainer.StaggerStartupDisable(true)

		leader := setup(t, maintainer)

		require.NoError(t, leader.Start(ctx))
		t.Cleanup(leader.Stop)

		leader.TestSignals.ElectedLeader.WaitOrTimeout()
		testSvc.testSignals.started.WaitOrTimeout()

		leader.Stop()
		testSvc.testSignals.returning.WaitOrTimeout()
	})
}

// failingStartService is a service whose Start always returns an error.
type failingStartService struct {
	startstop.BaseStartStop

	startAttempts *atomic.Int64
}

func (s *failingStartService) Start(ctx context.Context) error {
	s.startAttempts.Add(1)
	return errors.New("start error")
}
