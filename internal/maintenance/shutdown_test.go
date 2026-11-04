package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

// Inject the different error shapes drivers can return when cancellation
// interrupts a query, independently of a database's cancellation timing.
type maintenanceShutdownExecutor struct {
	riverdriver.Executor

	operationFunc func(context.Context, string) error
}

func (e *maintenanceShutdownExecutor) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	return map[string]bool{"index": true}, e.operationFunc(ctx, "IndexesExist")
}

func (e *maintenanceShutdownExecutor) IndexReindex(ctx context.Context, _ *riverdriver.IndexReindexParams) error {
	return e.operationFunc(ctx, "IndexReindex")
}

func (*maintenanceShutdownExecutor) IndexReindexArtifacts(context.Context, *riverdriver.IndexReindexArtifactsParams) ([]string, error) {
	return nil, nil
}

func (e *maintenanceShutdownExecutor) JobDeleteBefore(ctx context.Context, _ *riverdriver.JobDeleteBeforeParams) (int, error) {
	return 0, e.operationFunc(ctx, "JobDeleteBefore")
}

func (e *maintenanceShutdownExecutor) JobGetStuck(ctx context.Context, _ *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	return []*rivertype.JobRow{{ID: 1, Kind: "unknown", Metadata: []byte(`{}`)}}, e.operationFunc(ctx, "JobGetStuck")
}

func (e *maintenanceShutdownExecutor) JobRescueMany(ctx context.Context, _ *riverdriver.JobRescueManyParams) (*struct{}, error) {
	return nil, e.operationFunc(ctx, "JobRescueMany")
}

func (e *maintenanceShutdownExecutor) NotificationDeleteBefore(ctx context.Context, _ *riverdriver.NotificationDeleteBeforeParams) (int, error) {
	return 0, e.operationFunc(ctx, "NotificationDeleteBefore")
}

func (e *maintenanceShutdownExecutor) QueueDeleteExpired(ctx context.Context, _ *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	return nil, e.operationFunc(ctx, "QueueDeleteExpired")
}

func TestMaintenance_ShutdownErrorLogging(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		logOutput bytes.Buffer
		queried   testsignal.TestSignal[struct{}]
		service   startstop.Service
	}

	setup := func(t *testing.T, operation string, queryFunc func(context.Context) error) *testBundle {
		t.Helper()

		bundle := &testBundle{}
		bundle.queried.Init(t)
		archetype := riversharedtest.BaseServiceArchetype(t)
		archetype.Logger = slog.New(slog.NewTextHandler(&bundle.logOutput, &slog.HandlerOptions{Level: slog.LevelError}))
		exec := &maintenanceShutdownExecutor{operationFunc: func(ctx context.Context, name string) error {
			if name != operation {
				return nil
			}
			bundle.queried.Signal(struct{}{})
			return queryFunc(ctx)
		}}
		switch operation {
		case "IndexesExist", "IndexReindex":
			bundle.service = NewReindexer(archetype, &ReindexerConfig{
				IndexNames:   []string{"index"},
				ScheduleFunc: func(now time.Time) time.Time { return now.Add(time.Second) },
			}, exec)
		case "JobDeleteBefore":
			bundle.service = NewJobCleaner(archetype, &JobCleanerConfig{}, exec)
		case "JobGetStuck", "JobRescueMany":
			bundle.service = NewRescuer(archetype, &JobRescuerConfig{
				ClientRetryPolicy: &SimpleClientRetryPolicy{},
				WorkUnitFactoryFunc: func(string) workunit.WorkUnitFactory {
					return &callbackWorkUnitFactory{Callback: func(context.Context, *rivertype.JobRow) error { return nil }}
				},
			}, exec)
		case "NotificationDeleteBefore":
			bundle.service = NewSQLiteNotificationCleaner(archetype, &SQLiteNotificationCleanerConfig{}, exec)
		case "PeriodicJobGetAll", "PeriodicJobKeepAliveAndReap":
			pilot := NewPilotPeriodicJobMock()
			pilot.PeriodicJobGetAllMock = func(ctx context.Context, _ riverdriver.Executor, _ *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
				return nil, exec.operationFunc(ctx, "PeriodicJobGetAll")
			}
			pilot.PeriodicJobKeepAliveAndReapMock = func(ctx context.Context, _ riverdriver.Executor, _ *riverpilot.PeriodicJobKeepAliveAndReapParams) ([]*riverpilot.PeriodicJob, error) {
				return nil, exec.operationFunc(ctx, "PeriodicJobKeepAliveAndReap")
			}
			periodicExec := &periodicShutdownExecutor{
				beginFunc: func(context.Context) error { return nil },
				tx: &periodicShutdownTx{
					commitFunc:   func(context.Context) error { return nil },
					rollbackFunc: func(context.Context) error { return nil },
				},
			}
			enqueuer, err := NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
				PeriodicJobs: []*PeriodicJob{{
					ID:              "periodic",
					ConstructorFunc: func() (*rivertype.JobInsertParams, error) { return nil, ErrNoJobToInsert },
					ScheduleFunc:    func(now time.Time) time.Time { return now.Add(time.Hour) },
				}},
				Pilot: pilot,
			}, periodicExec)
			require.NoError(t, err)
			if operation == "PeriodicJobGetAll" {
				enqueuer.StaggerStartupDisable(true)
				bundle.service = maintenanceStartupService(archetype, enqueuer)
			} else {
				bundle.service = startstop.StartStopFunc(enqueuer.periodicJobKeepAliveAndReapPeriodically)
			}
		case "QueueDeleteExpired":
			bundle.service = NewQueueCleaner(archetype, &QueueCleanerConfig{}, exec)
		default:
			t.Fatalf("unknown operation: %s", operation)
		}
		if svc, ok := bundle.service.(withStaggerStartupDisable); ok {
			svc.StaggerStartupDisable(true)
		}
		t.Cleanup(bundle.service.Stop)
		return bundle
	}

	for _, operation := range []string{"IndexReindex", "IndexesExist", "JobDeleteBefore", "JobGetStuck", "JobRescueMany", "NotificationDeleteBefore", "PeriodicJobGetAll", "PeriodicJobKeepAliveAndReap", "QueueDeleteExpired"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			for _, outcome := range []string{"ContextCanceled", "DatabaseError", "DeadlineExceeded", "Stop"} {
				t.Run(outcome, func(t *testing.T) {
					t.Parallel()

					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						writeErr := fmt.Errorf("write failed: %w", os.ErrDeadlineExceeded)
						bundle := setup(t, operation, func(ctx context.Context) error {
							if outcome == "DatabaseError" {
								return writeErr
							}
							if outcome == "DeadlineExceeded" {
								return context.DeadlineExceeded
							}
							<-ctx.Done()
							return writeErr
						})
						require.NoError(t, bundle.service.Start(ctx))
						bundle.queried.WaitOrTimeout()
						synctest.Wait()
						if outcome == "ContextCanceled" {
							cancel()
						}
						bundle.service.Stop()
						switch outcome {
						case "DatabaseError":
							require.Contains(t, bundle.logOutput.String(), writeErr.Error())
						case "DeadlineExceeded":
							require.Contains(t, bundle.logOutput.String(), context.DeadlineExceeded.Error())
						default:
							require.Empty(t, bundle.logOutput.String())
						}
					})
				})
			}
		})
	}
}

// Exercise synchronous startup failures through the leader's real retry loop.
func maintenanceStartupService(archetype *baseservice.Archetype, service startstop.Service) startstop.Service {
	leader := NewQueueMaintainerLeader(archetype, &QueueMaintainerLeaderConfig{
		QueueMaintainer: NewQueueMaintainer(archetype, []startstop.Service{service}),
	})
	return startstop.StartStopFunc(func(ctx context.Context, shouldStart bool, started, stopped func()) error {
		if !shouldStart {
			return nil
		}
		go func() {
			started()
			defer stopped()
			leader.tryStart(ctx, 0, 0)
		}()
		return nil
	})
}
