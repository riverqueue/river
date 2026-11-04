package maintenance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

type periodicShutdownExecutor struct {
	riverdriver.Executor

	beginFunc func(context.Context) error
	tx        *periodicShutdownTx
}

func (e *periodicShutdownExecutor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if err := e.beginFunc(ctx); err != nil {
		return nil, err
	}
	return e.tx, nil
}

type periodicShutdownTx struct {
	riverdriver.ExecutorTx

	commitFunc   func(context.Context) error
	rollbackFunc func(context.Context) error
}

func (tx *periodicShutdownTx) Commit(ctx context.Context) error { return tx.commitFunc(ctx) }

func (tx *periodicShutdownTx) Rollback(ctx context.Context) error { return tx.rollbackFunc(ctx) }

func TestPeriodicJobEnqueuer_ShutdownDuringStagger(t *testing.T) {
	t.Parallel()

	for _, stopService := range []bool{false, true} {
		t.Run(fmt.Sprintf("StopService=%t", stopService), func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				var logOutput bytes.Buffer
				archetype := riversharedtest.BaseServiceArchetype(t)
				archetype.Logger = slog.New(slog.NewTextHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelError}))
				var beginCalls, constructorCalls, getAllCalls int
				pilot := NewPilotPeriodicJobMock()
				pilot.PeriodicJobGetAllMock = func(context.Context, riverdriver.Executor, *riverpilot.PeriodicJobGetAllParams) ([]*riverpilot.PeriodicJob, error) {
					getAllCalls++
					return nil, nil
				}
				svc, err := NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
					PeriodicJobs: []*PeriodicJob{{
						ConstructorFunc: func() (*rivertype.JobInsertParams, error) {
							constructorCalls++
							return &rivertype.JobInsertParams{}, nil
						},
						RunOnStart:   true,
						ScheduleFunc: func(now time.Time) time.Time { return now.Add(time.Hour) },
					}},
					Pilot: pilot,
				}, &periodicShutdownExecutor{beginFunc: func(ctx context.Context) error {
					beginCalls++
					return ctx.Err()
				}})
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var startResult testsignal.TestSignal[error]
				startResult.Init(t)
				go func() { startResult.Signal(svc.Start(ctx)) }()
				// Wait until Start is sleeping in the real stagger, without
				// depending on its randomly selected duration.
				synctest.Wait()
				if stopService {
					svc.Stop()
				} else {
					cancel()
				}
				err = startResult.WaitOrTimeout()
				svc.Stop()
				require.Empty(t, logOutput.String())
				require.NoError(t, err)
				require.Zero(t, beginCalls)
				require.Zero(t, constructorCalls)
				require.Zero(t, getAllCalls)
			})
		})
	}
}

func TestPeriodicJobEnqueuer_TransactionErrorLogging(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"Begin", "Commit", "Insert", "Upsert"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()

			for _, outcome := range []string{"Canceled", "DatabaseError", "DriverErrorOnCancellation", "LocalTimeout", "ParentDeadline"} {
				t.Run(outcome, func(t *testing.T) {
					t.Parallel()

					synctest.Test(t, func(t *testing.T) {
						var logOutput bytes.Buffer
						archetype := riversharedtest.BaseServiceArchetype(t)
						archetype.Logger = slog.New(slog.NewTextHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelError}))
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						if outcome == "ParentDeadline" {
							var cancelDeadline context.CancelFunc
							ctx, cancelDeadline = context.WithTimeout(ctx, time.Second)
							defer cancelDeadline()
						}
						dbErr := errors.New("database failed")
						operationFunc := func(queryCtx context.Context, name string) error {
							if name != operation {
								return nil
							}
							switch outcome {
							case "Canceled":
								cancel()
								return fmt.Errorf("query interrupted: %w", queryCtx.Err())
							case "DriverErrorOnCancellation":
								cancel()
								return fmt.Errorf("write failed: %w", os.ErrDeadlineExceeded)
							case "LocalTimeout", "ParentDeadline":
								<-queryCtx.Done()
								return queryCtx.Err()
							default:
								return dbErr
							}
						}
						rolledBack := false
						exec := &periodicShutdownExecutor{
							beginFunc: func(ctx context.Context) error { return operationFunc(ctx, "Begin") },
							tx: &periodicShutdownTx{
								commitFunc: func(ctx context.Context) error { return operationFunc(ctx, "Commit") },
								rollbackFunc: func(ctx context.Context) error {
									rolledBack = true
									require.NoError(t, ctx.Err(), "rollback must survive query cancellation")
									return nil
								},
							},
						}
						pilot := NewPilotPeriodicJobMock()
						pilot.PeriodicJobUpsertManyMock = func(ctx context.Context, _ riverdriver.Executor, _ *riverpilot.PeriodicJobUpsertManyParams) ([]*riverpilot.PeriodicJob, error) {
							return nil, operationFunc(ctx, "Upsert")
						}
						svc, err := NewPeriodicJobEnqueuer(archetype, &PeriodicJobEnqueuerConfig{
							Insert: func(ctx context.Context, _ riverdriver.ExecutorTx, _ []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
								return nil, operationFunc(ctx, "Insert")
							},
							Pilot: pilot,
						}, exec)
						require.NoError(t, err)
						svc.TestSignals.Init(t)
						svc.insertBatch(ctx, []*rivertype.JobInsertParams{{}}, &riverpilot.PeriodicJobUpsertManyParams{
							Jobs: []*riverpilot.PeriodicJobUpsertParams{{ID: "periodic"}},
						}, nil)
						switch outcome {
						case "DatabaseError":
							require.Contains(t, logOutput.String(), dbErr.Error())
						case "LocalTimeout":
							require.Contains(t, logOutput.String(), context.DeadlineExceeded.Error())
						default:
							require.Empty(t, logOutput.String())
						}
						require.Equal(t, operation != "Begin", rolledBack)
						svc.TestSignals.InsertedJobs.RequireEmpty()
						svc.TestSignals.PeriodicJobUpserted.RequireEmpty()
					})
				})
			}
		})
	}
}
