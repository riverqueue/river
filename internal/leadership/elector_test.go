package leadership

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/riverinternaltest/sharedtx"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstoptest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

type leaderAttemptScriptExecutorMock struct {
	riverdriver.Executor

	LeaderAttemptElectFunc func(ctx context.Context, execTx riverdriver.ExecutorTx, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error)
}

func (m *leaderAttemptScriptExecutorMock) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := m.Executor.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &leaderAttemptScriptExecutorTxMock{
		ExecutorTx: tx,
		mock:       m,
	}, nil
}

type leaderAttemptScriptExecutorTxMock struct {
	riverdriver.ExecutorTx

	mock *leaderAttemptScriptExecutorMock
}

func (m *leaderAttemptScriptExecutorTxMock) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
	if m.mock.LeaderAttemptElectFunc == nil {
		return m.ExecutorTx.LeaderAttemptElect(ctx, params)
	}

	return m.mock.LeaderAttemptElectFunc(ctx, m.ExecutorTx, params)
}

type leaderReelectExecutorMock struct {
	riverdriver.Executor

	LeaderAttemptReelectFunc func(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error)
}

func (m *leaderReelectExecutorMock) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
	if m.LeaderAttemptReelectFunc == nil {
		return m.Executor.LeaderAttemptReelect(ctx, params)
	}

	return m.LeaderAttemptReelectFunc(ctx, params)
}

type localNowTimeGeneratorStub struct {
	now time.Time
}

func (g *localNowTimeGeneratorStub) Now() time.Time {
	return g.now
}

func (g *localNowTimeGeneratorStub) NowOrNil() *time.Time {
	return nil
}

func (g *localNowTimeGeneratorStub) StubNow(now time.Time) time.Time {
	g.now = now
	return now
}

func TestElector_PollOnly(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		driver = riverpgxv5.New(nil)
	)

	type electorBundle struct {
		tx pgx.Tx
	}

	testElector(ctx, t,
		func(ctx context.Context, t *testing.T, stress bool) *electorBundle {
			t.Helper()

			tx, _ := riverdbtest.TestTxPgxDriver(ctx, t, riverpgxv5.New(riversharedtest.DBPool(ctx, t)), &riverdbtest.TestTxOpts{
				DisableSchemaSharing: true,
			})

			tx = sharedtx.NewSharedTx(tx)

			return &electorBundle{tx: tx}
		},
		func(t *testing.T, electorBundle *electorBundle) *Elector {
			t.Helper()

			return NewElector(
				riversharedtest.BaseServiceArchetype(t),
				driver.UnwrapExecutor(electorBundle.tx),
				nil,
				&Config{ClientID: "test_client_id"},
			)
		},
	)
}

func TestElectorHandleLeadershipNotification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T) (*Elector, *testBundle) {
		t.Helper()

		tx := riverdbtest.TestTxPgx(ctx, t)
		driver := riverpgxv5.New(nil)

		elector := NewElector(
			riversharedtest.BaseServiceArchetype(t),
			driver.UnwrapExecutor(tx),
			nil,
			&Config{ClientID: "test_client_id"},
		)
		elector.wakeupChan = make(chan struct{}, 1)

		return elector, &testBundle{
			driver: driver,
			tx:     tx,
		}
	}

	mustMarshalJSON := func(t *testing.T, val any) []byte {
		t.Helper()

		data, err := json.Marshal(val)
		require.NoError(t, err)
		return data
	}

	validLeadershipChange := func() *DBNotification {
		t.Helper()

		return &DBNotification{
			Action:   DBNotificationKindResigned,
			LeaderID: "other-client-id",
		}
	}

	t.Run("IgnoresNonResignedAction", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		change := validLeadershipChange()
		change.Action = "not_resigned"

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, change)))

		require.Empty(t, elector.wakeupChan)
	})

	t.Run("IgnoresSameClientID", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		change := validLeadershipChange()
		change.LeaderID = elector.config.ClientID

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, change)))

		require.Empty(t, elector.wakeupChan)
	})

	t.Run("SignalsLeadershipChange", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, validLeadershipChange())))

		riversharedtest.WaitOrTimeout(t, elector.wakeupChan)
	})

	t.Run("SignalsLeadershipChangeDoesNotBlockOnFullWakeup", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)
		elector.wakeupChan <- struct{}{}

		done := make(chan struct{})

		go func() {
			defer close(done)
			elector.handleLeadershipNotification(context.Background(), notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, validLeadershipChange())))
		}()

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			require.Fail(t, "expected leadership notification to coalesce the wakeup instead of blocking")
		}

		require.Len(t, elector.wakeupChan, 1)
	})

	t.Run("StopsOnContextDone", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t)

		ctx, cancel := context.WithCancel(ctx)
		cancel()

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(mustMarshalJSON(t, validLeadershipChange())))

		require.Empty(t, elector.wakeupChan)
	})
}

func TestElectorRunLeaderState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	setup := func(t *testing.T) (*Elector, riverdriver.Executor) {
		t.Helper()

		driver := riverpgxv5.New(nil)
		exec := driver.UnwrapExecutor(riverdbtest.TestTxPgx(ctx, t))

		elector := NewElector(
			riversharedtest.BaseServiceArchetype(t),
			exec,
			nil,
			&Config{ClientID: "test_client_id"},
		)
		elector.config.ElectInterval = 10 * time.Millisecond
		elector.config.ElectIntervalJitter = time.Millisecond
		elector.testSignals.Init(t)

		return elector, exec
	}

	t.Run("ResignsCurrentTermAfterReelectErrorsExhaustTrust", func(t *testing.T) {
		t.Parallel()

		elector, exec := setup(t)
		initialNow := elector.Time.StubNow(time.Now().UTC())
		elector.publishLeadershipState(true)

		leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			ElectedAt: ptrutil.Ptr(initialNow),
			ExpiresAt: ptrutil.Ptr(initialNow.Add(elector.leaderTTL())),
			LeaderID:  ptrutil.Ptr(elector.config.ClientID),
		})

		elector.exec = &leaderReelectExecutorMock{
			Executor: exec,
			LeaderAttemptReelectFunc: func(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
				elector.Time.StubNow(initialNow.Add(elector.leaderTTL()))
				return nil, errors.New("reelection error")
			},
		}

		runCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- elector.runLeaderState(runCtx, newLeadershipTerm(elector.config.ClientID, leader.ElectedAt, initialNow, elector.leaderTTL()))
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			require.Fail(t, "timed out waiting for leader state to exit")
		}

		elector.testSignals.LostLeadership.WaitOrTimeout()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("SlowSuccessfulReelectDoesNotExtendTrustWindow", func(t *testing.T) {
		t.Parallel()

		elector, exec := setup(t)
		initialNow := elector.Time.StubNow(time.Now().UTC())
		elector.publishLeadershipState(true)

		leader := testfactory.Leader(ctx, t, exec, &testfactory.LeaderOpts{
			ElectedAt: ptrutil.Ptr(initialNow),
			ExpiresAt: ptrutil.Ptr(initialNow.Add(elector.leaderTTL())),
			LeaderID:  ptrutil.Ptr(elector.config.ClientID),
		})

		var numAttempts int
		elector.exec = &leaderReelectExecutorMock{
			Executor: exec,
			LeaderAttemptReelectFunc: func(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
				numAttempts++

				switch numAttempts {
				case 1:
					elector.Time.StubNow(initialNow.Add(elector.leaderTTL()))
					return exec.LeaderAttemptReelect(ctx, params)
				default:
					return nil, errors.New("unexpected reelection attempt after trust window elapsed")
				}
			},
		}

		runCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- elector.runLeaderState(runCtx, newLeadershipTerm(elector.config.ClientID, leader.ElectedAt, initialNow, elector.leaderTTL()))
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			require.Fail(t, "timed out waiting for leader state to exit")
		}

		require.Equal(t, 1, numAttempts)
		elector.testSignals.LostLeadership.WaitOrTimeout()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err := exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("UsesNowForLocalDeadlineChecks", func(t *testing.T) {
		t.Parallel()

		driver := riverpgxv5.New(nil)
		exec := driver.UnwrapExecutor(riverdbtest.TestTxPgx(ctx, t))

		timeGenerator := &localNowTimeGeneratorStub{now: time.Now()}
		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Time = timeGenerator

		elector := NewElector(
			archetype,
			&leaderReelectExecutorMock{
				Executor: exec,
				LeaderAttemptReelectFunc: func(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
					require.Fail(t, "unexpected reelection attempt")
					panic("unreachable")
				},
			},
			nil,
			&Config{ClientID: "test_client_id"},
		)
		elector.config.ElectInterval = 10 * time.Millisecond
		elector.testSignals.Init(t)

		elector.publishLeadershipState(true)

		runCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		err := elector.runLeaderState(runCtx, leadershipTerm{
			clientID:     elector.config.ClientID,
			electedAt:    time.Now(),
			trustedUntil: timeGenerator.now.Add(-time.Millisecond),
		})
		require.NoError(t, err)

		elector.testSignals.LostLeadership.WaitOrTimeout()
	})
}

func TestElectorSubscriptions(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *Elector {
		t.Helper()

		return NewElector(
			riversharedtest.BaseServiceArchetype(t),
			nil,
			nil,
			&Config{ClientID: "test_client_id"},
		)
	}

	t.Run("SlowSubscribersDoNotBlockAndReceiveTransitionsInOrder", func(t *testing.T) {
		t.Parallel()

		elector := setup(t)
		sub := elector.Listen()
		t.Cleanup(sub.Unlisten)

		done := make(chan struct{})
		go func() {
			defer close(done)
			elector.publishLeadershipState(true)
			elector.publishLeadershipState(false)
		}()

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			require.Fail(t, "expected leadership publication to queue without blocking")
		}

		notification := riversharedtest.WaitOrTimeout(t, sub.C())
		require.False(t, notification.IsLeader)

		notification = riversharedtest.WaitOrTimeout(t, sub.C())
		require.True(t, notification.IsLeader)

		notification = riversharedtest.WaitOrTimeout(t, sub.C())
		require.False(t, notification.IsLeader)
	})
}

func TestElector_WithNotifier(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type electorBundle struct {
		archetype *baseservice.Archetype
		exec      riverdriver.Executor
		notifier  *notifier.Notifier
		schema    string
	}

	testElector(ctx, t,
		func(ctx context.Context, t *testing.T, stress bool) *electorBundle {
			t.Helper()

			dbPool := riversharedtest.DBPoolClone(ctx, t)

			var (
				driver    = riverpgxv5.New(dbPool)
				schema    = riverdbtest.TestSchema(ctx, t, driver, nil)
				archetype = riversharedtest.BaseServiceArchetype(t)
			)

			notifierSvc := notifier.New(archetype, driver.GetListener(&riverdriver.GetListenenerParams{Schema: schema}))
			require.NoError(t, notifierSvc.Start(ctx))
			t.Cleanup(notifierSvc.Stop)

			return &electorBundle{
				archetype: archetype,
				exec:      driver.GetExecutor(),
				notifier:  notifierSvc,
				schema:    schema,
			}
		},
		func(t *testing.T, electorBundle *electorBundle) *Elector {
			t.Helper()

			return NewElector(
				electorBundle.archetype,
				electorBundle.exec,
				electorBundle.notifier,
				&Config{
					ClientID: "test_client_id",
					Schema:   electorBundle.schema,
				},
			)
		},
	)
}

func testElector[TElectorBundle any](
	ctx context.Context,
	t *testing.T,
	makeElectorBundle func(ctx context.Context, t *testing.T, stress bool) TElectorBundle,
	makeElector func(t *testing.T, bundle TElectorBundle) *Elector,
) {
	t.Helper()

	type testBundle struct {
		electorBundle TElectorBundle
		exec          riverdriver.Executor
	}

	type testOpts struct {
		stress bool
	}

	setup := func(t *testing.T, opts *testOpts) (*Elector, *testBundle) {
		t.Helper()

		if opts == nil {
			opts = &testOpts{}
		}

		electorBundle := makeElectorBundle(ctx, t, opts.stress)
		elector := makeElector(t, electorBundle)
		elector.testSignals.Init(t)

		return elector, &testBundle{
			electorBundle: electorBundle,
			exec:          elector.exec,
		}
	}

	startElector := func(ctx context.Context, t *testing.T, elector *Elector) {
		t.Helper()

		require.NoError(t, elector.Start(ctx))
		t.Cleanup(elector.Stop)
	}

	signalLeaderResigned := func(ctx context.Context, t *testing.T, elector *Elector, leaderID string) {
		t.Helper()

		payload, err := json.Marshal(DBNotification{
			Action:   DBNotificationKindResigned,
			LeaderID: leaderID,
		})
		require.NoError(t, err)

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(payload))
	}

	signalRequestResign := func(ctx context.Context, t *testing.T, elector *Elector) {
		t.Helper()

		payload, err := json.Marshal(DBNotification{Action: DBNotificationKindRequestResign})
		require.NoError(t, err)

		elector.handleLeadershipNotification(ctx, notifier.NotificationTopicLeadership, string(payload))
	}

	t.Run("CoalescesResignedWakeups", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, nil)
		elector.config.ElectInterval = 100 * time.Millisecond
		elector.config.ElectIntervalJitter = 10 * time.Millisecond
		elector.wakeupChan = make(chan struct{}, 1)

		var (
			attempt      int
			attemptTimes []time.Time
		)

		elector.exec = &leaderAttemptScriptExecutorMock{
			Executor: elector.exec,
			LeaderAttemptElectFunc: func(ctx context.Context, execTx riverdriver.ExecutorTx, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
				attempt++
				attemptTimes = append(attemptTimes, time.Now())

				switch attempt {
				case 1, 2:
					return nil, rivertype.ErrNotFound
				case 3:
					return execTx.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
						LeaderID: params.LeaderID,
						Now:      params.Now,
						Schema:   params.Schema,
						TTL:      params.TTL,
					})
				default:
					require.FailNowf(t, "unexpected election attempt", "attempt %d", attempt)
					panic("unreachable")
				}
			},
		}

		signalLeaderResigned(ctx, t, elector, "leader-1")

		secondNotificationDone := make(chan struct{})
		go func() {
			defer close(secondNotificationDone)
			signalLeaderResigned(ctx, t, elector, "leader-2")
		}()

		select {
		case <-secondNotificationDone:
		case <-time.After(50 * time.Millisecond):
			require.Fail(t, "expected second resignation notification to be coalesced instead of blocking")
		}

		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		start := time.Now()
		term, err := elector.runFollowerState(attemptCtx)
		require.NoError(t, err)
		require.Equal(t, elector.config.ClientID, term.clientID)
		require.False(t, term.electedAt.IsZero())
		require.GreaterOrEqual(t, time.Since(start), 75*time.Millisecond)

		require.Equal(t, 3, attempt)
		require.Len(t, attemptTimes, 3)
		require.GreaterOrEqual(t, attemptTimes[2].Sub(attemptTimes[1]), 75*time.Millisecond)
	})

	t.Run("CompetingElectors", func(t *testing.T) {
		t.Parallel()

		elector1, bundle := setup(t, nil)
		elector1.config.ClientID = "elector1"

		startElector(ctx, t, elector1)
		elector1.testSignals.GainedLeadership.WaitOrTimeout()

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector1.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, elector1.config.ClientID, leader.LeaderID)

		elector2 := makeElector(t, bundle.electorBundle)
		elector2.config.ClientID = "elector2"
		elector2.config.ElectInterval = 10 * time.Millisecond
		elector2.config.ElectIntervalJitter = time.Millisecond
		elector2.exec = elector1.exec
		elector2.testSignals.Init(t)

		startElector(ctx, t, elector2)

		elector2.testSignals.DeniedLeadership.WaitOrTimeout()

		elector1.Stop()
		elector1.testSignals.ResignedLeadership.WaitOrTimeout()

		elector2.testSignals.GainedLeadership.WaitOrTimeout()

		elector2.Stop()
		elector2.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err = bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector1.config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("IndependentBundlesAreIsolated", func(t *testing.T) {
		t.Parallel()

		elector1, bundle1 := setup(t, nil)
		elector1.config.ClientID = "elector1"

		elector2, bundle2 := setup(t, nil)
		elector2.config.ClientID = "elector2"

		startElector(ctx, t, elector1)
		startElector(ctx, t, elector2)

		elector1.testSignals.GainedLeadership.WaitOrTimeout()
		elector2.testSignals.GainedLeadership.WaitOrTimeout()

		leader1, err := bundle1.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector1.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, elector1.config.ClientID, leader1.LeaderID)

		leader2, err := bundle2.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector2.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, elector2.config.ClientID, leader2.LeaderID)
	})

	t.Run("LosesLeadershipWhenSameLeaderIDTermIsReplaced", func(t *testing.T) {
		t.Parallel()

		elector, bundle := setup(t, nil)
		elector.config.ElectInterval = 25 * time.Millisecond
		elector.config.ElectIntervalJitter = time.Millisecond

		sub := elector.Listen()
		t.Cleanup(sub.Unlisten)

		require.False(t, riversharedtest.WaitOrTimeout(t, sub.C()).IsLeader)

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()
		require.True(t, riversharedtest.WaitOrTimeout(t, sub.C()).IsLeader)

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.NoError(t, err)

		newElectedAt := leader.ElectedAt.Add(time.Second)
		newExpiresAt := newElectedAt.Add(elector.leaderTTL())

		require.NoError(t, dbutil.WithTx(ctx, bundle.exec, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
			resigned, err := execTx.LeaderResign(ctx, &riverdriver.LeaderResignParams{
				ElectedAt:       leader.ElectedAt,
				LeaderID:        leader.LeaderID,
				LeadershipTopic: string(notifier.NotificationTopicLeadership),
				Schema:          elector.config.Schema,
			})
			if err != nil {
				return err
			}
			if !resigned {
				return errors.New("expected leader replacement to resign current term")
			}

			_, err = execTx.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
				ElectedAt: &newElectedAt,
				ExpiresAt: &newExpiresAt,
				LeaderID:  leader.LeaderID,
				Schema:    elector.config.Schema,
				TTL:       elector.leaderTTL(),
			})
			return err
		}))

		elector.testSignals.LostLeadership.WaitOrTimeout()
		require.False(t, riversharedtest.WaitOrTimeout(t, sub.C()).IsLeader)
		elector.testSignals.DeniedLeadership.WaitOrTimeout()

		leaderFromDB, err := bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, leader.LeaderID, leaderFromDB.LeaderID)
		require.Equal(t, newElectedAt, leaderFromDB.ElectedAt)
	})

	t.Run("NotifiesSubscribers", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, nil)

		sub := elector.Listen()
		t.Cleanup(sub.Unlisten)

		notification := riversharedtest.WaitOrTimeout(t, sub.C())
		require.False(t, notification.IsLeader)

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		notification = riversharedtest.WaitOrTimeout(t, sub.C())
		require.True(t, notification.IsLeader)

		elector.Stop()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		notification = riversharedtest.WaitOrTimeout(t, sub.C())
		require.False(t, notification.IsLeader)
	})

	t.Run("RequestResignImmediatelyAfterElection", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, nil)

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		signalRequestResign(ctx, t, elector)

		elector.testSignals.ResignedLeadership.WaitOrTimeout()
		elector.testSignals.GainedLeadership.WaitOrTimeout()
	})

	t.Run("RequestResignStress", func(t *testing.T) {
		t.Parallel()

		elector, bundle := setup(t, nil)

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, elector.config.ClientID, leader.LeaderID)

		for range 5 {
			signalRequestResign(ctx, t, elector)
		}
		elector.testSignals.ResignedLeadership.WaitOrTimeout()
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		elector.Stop()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err = bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("RequestResignWhileLeader", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, nil)
		elector.config.ElectInterval = 10 * time.Millisecond
		elector.config.ElectIntervalJitter = time.Millisecond

		startElector(ctx, t, elector)

		elector.testSignals.GainedLeadership.WaitOrTimeout()
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()

		signalRequestResign(ctx, t, elector)

		elector.testSignals.ResignedLeadership.WaitOrTimeout()
		elector.testSignals.GainedLeadership.WaitOrTimeout()
	})

	t.Run("StartsGainsLeadershipAndStops", func(t *testing.T) {
		t.Parallel()

		elector, bundle := setup(t, nil)

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		leader, err := bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.NoError(t, err)
		require.Equal(t, elector.config.ClientID, leader.LeaderID)

		elector.Stop()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()

		_, err = bundle.exec.LeaderGetElectedLeader(ctx, &riverdriver.LeaderGetElectedLeaderParams{
			Schema: elector.config.Schema,
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("StartStopStress", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, &testOpts{stress: true})
		elector.Logger = riversharedtest.LoggerWarn(t)
		elector.testSignals = electorTestSignals{}

		startstoptest.Stress(ctx, t, elector)
	})

	t.Run("SustainsLeadership", func(t *testing.T) {
		t.Parallel()

		elector, _ := setup(t, nil)
		elector.config.ElectInterval = 10 * time.Millisecond
		elector.config.ElectIntervalJitter = time.Millisecond

		startElector(ctx, t, elector)
		elector.testSignals.GainedLeadership.WaitOrTimeout()

		elector.testSignals.MaintainedLeadership.WaitOrTimeout()
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()
		elector.testSignals.MaintainedLeadership.WaitOrTimeout()

		elector.Stop()
		elector.testSignals.ResignedLeadership.WaitOrTimeout()
	})
}
