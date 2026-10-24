package river

import (
	"context"
	"encoding/json"
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

func TestResumableNestedStep(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name         string
		childCursor  bool
		parentCursor bool
		parentName   string
	}{
		{name: "CursorInCursor", childCursor: true, parentCursor: true, parentName: "parent"},
		{name: "CursorInPlain", childCursor: true, parentName: "parent"},
		{name: "EmptyParentName"},
		{name: "PlainInCursor", parentCursor: true, parentName: "parent"},
		{name: "PlainInPlain", parentName: "parent"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			updates := make(map[string]any)
			ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, updates)
			job := &rivertype.JobRow{Metadata: []byte(`{}`)}

			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStep(ctx, "before", nil, func(context.Context) error { return nil })
				parentFunc := func(ctx context.Context) {
					if testCase.childCursor {
						ResumableStepCursor(ctx, "child", nil, func(context.Context, int) error {
							t.Fatal("nested child must not run")
							return nil
						})
					} else {
						ResumableStep(ctx, "child", nil, func(context.Context) error {
							t.Fatal("nested child must not run")
							return nil
						})
					}
				}
				if testCase.parentCursor {
					ResumableStepCursor(ctx, testCase.parentName, nil, func(ctx context.Context, _ int) error {
						require.NoError(t, ResumableSetCursor(ctx, 1))
						parentFunc(ctx)
						return nil
					})
				} else {
					ResumableStep(ctx, testCase.parentName, nil, func(ctx context.Context) error {
						parentFunc(ctx)
						return nil
					})
				}
				ResumableStep(ctx, "after", nil, func(context.Context) error {
					t.Fatal("steps after the nesting error must not run")
					return nil
				})
				return nil
			})
			require.EqualError(t, err, "river: resumable steps cannot be nested")
			metadata, err := json.Marshal(updates)
			require.NoError(t, err)
			if testCase.parentCursor {
				require.JSONEq(t, `{"river:resumable_step":"before","river:resumable_cursor":{"parent":1}}`, string(metadata))
			} else {
				require.JSONEq(t, `{"river:resumable_step":"before"}`, string(metadata))
			}
		})
	}
}

func TestResumableNestedStepCheckpointTx(t *testing.T) {
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
			err := (&riverplugin.ResumableMiddleware{}).Work(ctx, job, func(ctx context.Context) error {
				ResumableStepCursor(ctx, "parent", nil, func(ctx context.Context, _ int) error {
					tx, err := bundle.tx.Begin(ctx)
					require.NoError(t, err)
					defer tx.Rollback(ctx)
					_, err = ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, tx, &Job[args]{JobRow: job}, 1)
					require.NoError(t, err)
					ResumableStepCursor(ctx, "child", nil, func(ctx context.Context, _ int) error {
						t.Fatal("nested child must not run")
						return nil
					})
					if commit {
						require.NoError(t, tx.Commit(ctx))
					} else {
						require.NoError(t, tx.Rollback(ctx))
					}
					return nil
				})
				return nil
			})
			require.EqualError(t, err, "river: resumable steps cannot be nested")

			beforeResult, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			if !commit {
				require.JSONEq(t, `{}`, string(beforeResult.Metadata))
			}
			updates, err := json.Marshal(bundle.updates)
			require.NoError(t, err)
			// Rejecting the child must preserve a committed parent checkpoint
			// without replaying one that rolled back.
			result, err := bundle.exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{ID: job.ID, Metadata: updates, MetadataDoMerge: true})
			require.NoError(t, err)
			if commit {
				require.JSONEq(t, `{"river:resumable_step":"parent","river:resumable_cursor":{"parent":1}}`, string(result.Metadata))
			} else {
				require.JSONEq(t, `{}`, string(result.Metadata))
			}
		})
	}
}
