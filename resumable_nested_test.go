package river

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

// These draft regressions describe correct behavior if nested steps remain
// supported. They intentionally expose unresolved failures in the current code.
func TestResumableNestedCheckpointTx(t *testing.T) {
	t.Parallel()

	type args struct {
		testutil.JobArgsReflectKind[args]
	}
	type testBundle struct {
		exec    riverdriver.Executor
		tx      pgx.Tx
		updates map[string]any
	}
	setup := func(t *testing.T) (*testBundle, context.Context) {
		t.Helper()

		ctx := context.Background()
		tx := riverdbtest.TestTxPgx(ctx, t)
		client, err := NewClient(riverpgxv5.New(nil), &Config{Logger: riversharedtest.Logger(t)})
		require.NoError(t, err)
		updates := make(map[string]any)
		ctx = context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
		ctx = context.WithValue(ctx, jobexecutor.ContextKeyMetadataUpdates, updates)
		return &testBundle{exec: riverpgxv5.New(nil).UnwrapExecutor(tx), tx: tx, updates: updates}, ctx
	}

	for _, commit := range []bool{true, false} {
		name := "RolledBackParentCursorIsNotReplayed"
		if commit {
			name = "CommittedParentCursorIsPreserved"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bundle, ctx := setup(t)
			job := testfactory.Job(ctx, t, bundle.exec, &testfactory.JobOpts{State: new(rivertype.JobStateRunning)})
			interrupted := errors.New("child interrupted")
			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStepCursor(ctx, "parent", nil, func(ctx context.Context, _ int) error {
					tx, err := bundle.tx.Begin(ctx)
					require.NoError(t, err)
					defer tx.Rollback(ctx)
					_, err = ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, tx, &Job[args]{JobRow: job}, 1)
					require.NoError(t, err)
					ResumableStepCursor(ctx, "child", nil, func(ctx context.Context, _ int) error {
						require.NoError(t, ResumableSetCursor(ctx, 7))
						return interrupted
					})
					if commit {
						require.NoError(t, tx.Commit(ctx))
					} else {
						require.NoError(t, tx.Rollback(ctx))
					}
					return interrupted
				})
				return nil
			})
			require.ErrorIs(t, err, interrupted)

			beforeResult, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			if !commit {
				require.JSONEq(t, `{}`, string(beforeResult.Metadata))
			}
			updates, err := json.Marshal(bundle.updates)
			require.NoError(t, err)
			// Use the real metadata merge so a fix that writes only the child's
			// cursor must also preserve a parent checkpoint that committed.
			result, err := bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{ID: job.ID, Metadata: updates, MetadataDoMerge: true})
			require.NoError(t, err)
			if commit {
				require.JSONEq(t, `{"river:resumable_step":"parent","river:resumable_cursor":{"parent":1,"child":7}}`, string(result.Metadata))
			} else {
				require.JSONEq(t, `{"river:resumable_cursor":{"child":7}}`, string(result.Metadata))
			}
		})
	}
}

func TestResumableNestedStepResume(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, metadata []byte) (context.Context, map[string]any, *rivertype.JobRow) {
		t.Helper()

		updates := make(map[string]any)
		ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, updates)
		return ctx, updates, &rivertype.JobRow{Metadata: metadata}
	}

	for _, cursorChild := range []bool{true, false} {
		name := "PlainChild"
		if cursorChild {
			name = "CursorChild"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, updates, job := setup(t, []byte(`{}`))
			parentRuns, childRuns := 0, 0
			runParent := func(ctx context.Context, fail bool) {
				ResumableStep(ctx, "parent", nil, func(ctx context.Context) error {
					parentRuns++
					if cursorChild {
						ResumableStepCursor(ctx, "child", nil, func(context.Context, int) error { childRuns++; return nil })
					} else {
						ResumableStep(ctx, "child", nil, func(context.Context) error { childRuns++; return nil })
					}
					if fail {
						return errors.New("parent interrupted after child completed")
					}
					return nil
				})
			}
			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error { runParent(ctx, true); return nil })
			require.EqualError(t, err, "parent interrupted after child completed")
			metadata, err := json.Marshal(updates)
			require.NoError(t, err)
			ctx, _, job = setup(t, metadata)
			err = (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error { runParent(ctx, false); return nil })
			require.NoError(t, err)
			require.Equal(t, 2, parentRuns)
			require.Equal(t, 1, childRuns)
		})
	}
}
