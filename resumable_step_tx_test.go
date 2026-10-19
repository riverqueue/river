package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/execution"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

func TestResumableAttemptMetadata(t *testing.T) {
	t.Parallel()

	type args struct {
		testutil.JobArgsReflectKind[args]
	}
	for _, outcome := range []string{"Error", "Rollback", "Shutdown", "Snooze"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			pool := riversharedtest.DBPool(ctx, t)
			schema := riverdbtest.TestSchema(ctx, t, riverpgxv5.New(pool), nil)
			config := newTestConfig(t, schema)
			var entered testsignal.TestSignal[struct{}]
			entered.Init(t)
			AddWorker(config.Workers, WorkFunc(func(ctx context.Context, job *Job[args]) error {
				if outcome == "Rollback" {
					ResumableStep(ctx, "first", nil, func(context.Context) error { return nil })
				}
				ResumableStepCursor(ctx, "loop", nil, func(ctx context.Context, _ int) error {
					if outcome == "Rollback" {
						return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
							_, err := ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, tx, job, 1)
							if err != nil {
								return err
							}
							return errors.New("rollback")
						})
					}
					if err := ResumableSetCursor(ctx, 7); err != nil {
						return err
					}
					switch outcome {
					case "Shutdown":
						entered.Signal(struct{}{})
						<-ctx.Done()
						return ctx.Err()
					case "Snooze":
						return JobSnooze(time.Minute)
					default:
						return errors.New("retry")
					}
				})
				return nil
			}))
			client := newTestClient(t, pool, config)
			events := subscribe(t, client)
			startClient(ctx, t, client)
			inserted, err := client.Insert(ctx, args{}, &InsertOpts{MaxAttempts: 1})
			require.NoError(t, err)
			if outcome == "Shutdown" {
				entered.WaitOrTimeout()
				require.NoError(t, client.StopAndCancel(ctx))
			}
			event := riversharedtest.WaitOrTimeout(t, events)
			require.Equal(t, inserted.Job.ID, event.Job.ID)
			if outcome == "Rollback" {
				require.Equal(t, EventKindJobFailed, event.Kind)
				require.JSONEq(t, `{}`, string(event.Job.Metadata))
			} else {
				expectedKind := EventKindJobFailed
				if outcome == "Shutdown" {
					expectedKind = EventKindJobInterrupted
				}
				if outcome == "Snooze" {
					expectedKind = EventKindJobSnoozed
				}
				require.Equal(t, expectedKind, event.Kind)
				var metadata map[string]any
				require.NoError(t, json.Unmarshal(event.Job.Metadata, &metadata))
				require.NotContains(t, metadata, rivercommon.MetadataKeyResumableStep)
				require.Equal(t, map[string]any{"loop": float64(7)}, metadata[rivercommon.MetadataKeyResumableCursor])
			}
		})
	}
}

func TestResumableSetStepTx(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type JobArgs struct {
		testutil.JobArgsReflectKind[JobArgs]
	}

	type testBundle struct {
		client *Client[pgx.Tx]
		exec   riverdriver.Executor
		tx     pgx.Tx
	}

	setup := func(ctx context.Context, t *testing.T, stepName string) (context.Context, *testBundle) {
		t.Helper()

		tx := riverdbtest.TestTxPgx(ctx, t)
		client, err := NewClient(riverpgxv5.New(nil), &Config{
			Logger: riversharedtest.Logger(t),
		})
		require.NoError(t, err)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, make(map[string]any))
		ctx = context.WithValue(ctx, riverplugin.ResumableContextKey{}, &riverplugin.ResumableState{
			Cursors:  make(map[string]json.RawMessage),
			StepName: stepName,
		})

		return ctx, &testBundle{
			client: client,
			exec:   riverpgxv5.New(nil).UnwrapExecutor(tx),
			tx:     tx,
		}
	}

	for _, mode := range []string{"Commit", "Rollback", "WriteFailure"} {
		t.Run("Checkpoint"+mode+"ThenError", func(t *testing.T) {
			t.Parallel()

			ctx, bundle := setup(ctx, t, "")
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})
			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStep(ctx, "first", nil, func(context.Context) error { return nil })
				ResumableStepCursor(ctx, "loop", nil, func(ctx context.Context, _ int) error {
					err := pgx.BeginFunc(ctx, bundle.tx, func(tx pgx.Tx) error {
						checkpointJob := &Job[JobArgs]{JobRow: job}
						if mode == "WriteFailure" {
							checkpointJob.ID = -1
						}
						_, err := ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, tx, checkpointJob, 1)
						if mode == "WriteFailure" {
							require.ErrorIs(t, err, rivertype.ErrNotFound)
							return err
						}
						require.NoError(t, err)
						if mode == "Rollback" {
							return errors.New("rollback")
						}
						return nil
					})
					if err != nil {
						return err
					}
					return errors.New("after commit")
				})
				return errors.New("attempt failed")
			})
			require.Error(t, err)
			updates, ok := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
			require.True(t, ok)
			if mode == "WriteFailure" {
				require.Equal(t, "first", updates[rivercommon.MetadataKeyResumableStep])
			} else {
				require.NotContains(t, updates, rivercommon.MetadataKeyResumableStep)
			}
			require.NotContains(t, updates, rivercommon.MetadataKeyResumableCursor)
		})
	}

	t.Run("SetsStep", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "step1")

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateRunning),
		})

		updatedJob, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		reloadedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(reloadedJob.Metadata, &metadata))
		require.Equal(t, "step1", metadata[rivercommon.MetadataKeyResumableStep])
	})

	t.Run("SetsStepAndCursor", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "step2")

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateRunning),
		})

		type Cursor struct {
			ID int `json:"id"`
		}

		updatedJob, err := ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job}, Cursor{ID: 123})
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, updatedJob.State)

		reloadedJob, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
		require.NoError(t, err)

		var metadata map[string]any
		require.NoError(t, json.Unmarshal(reloadedJob.Metadata, &metadata))
		require.Equal(t, "step2", metadata[rivercommon.MetadataKeyResumableStep])
		require.Equal(t, map[string]any{"step2": map[string]any{"id": float64(123)}}, metadata[rivercommon.MetadataKeyResumableCursor])

		metadataUpdates, ok := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
		require.True(t, ok)
		require.Empty(t, metadataUpdates)
	})

	t.Run("ErrorIfNotRunning", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "step1")

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{})

		_, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.EqualError(t, err, "job must be running")
	})

	t.Run("ErrorIfNotInStep", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "") // empty step name

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateRunning),
		})

		_, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.EqualError(t, err, "not inside a resumable step; must be called from within ResumableStep or ResumableStepCursor")
	})

	t.Run("ErrorIfJobDoesntExist", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "step1")

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{
			State: new(rivertype.JobStateAvailable),
		})
		_, err := bundle.exec.JobDelete(ctx, &riverdriver.JobDeleteParams{ID: job.ID})
		require.NoError(t, err)

		job.State = rivertype.JobStateRunning
		_, err = ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
	})

	t.Run("PanicsIfCalledInTestWorkerWithoutInsertingJob", func(t *testing.T) {
		t.Parallel()

		ctx, bundle := setup(ctx, t, "step1")
		ctx = context.WithValue(ctx, execution.ContextKeyInsideTestWorker{}, true)

		job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateAvailable)})
		_, err := bundle.client.JobDeleteTx(ctx, bundle.tx, job.ID)
		require.NoError(t, err)
		job.State = rivertype.JobStateRunning

		require.PanicsWithValue(t, "to use ResumableSetStepTx or ResumableSetStepCursorTx in a rivertest.Worker, the job must be inserted into the database first", func() {
			_, err := ResumableSetStepTx[*riverpgxv5.Driver](ctx, bundle.tx, &Job[JobArgs]{JobRow: job})
			require.NoError(t, err)
		})
	})
}
