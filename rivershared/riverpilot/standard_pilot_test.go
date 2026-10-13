package riverpilot

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

type standardPilotExecutorMock struct {
	riverdriver.Executor

	jobDeleteBeforeFunc func(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error)
	jobDeleteFunc       func(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error)
	jobDeleteManyFunc   func(ctx context.Context, params *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error)
	jobGetAvailableFunc func(ctx context.Context, params *riverdriver.JobGetAvailableParams) (*riverdriver.JobGetAvailableResult, error)
}

func (m *standardPilotExecutorMock) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	return m.jobDeleteFunc(ctx, params)
}

func (m *standardPilotExecutorMock) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	return m.jobDeleteBeforeFunc(ctx, params)
}

func (m *standardPilotExecutorMock) JobDeleteMany(ctx context.Context, params *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error) {
	return m.jobDeleteManyFunc(ctx, params)
}

func (m *standardPilotExecutorMock) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) (*riverdriver.JobGetAvailableResult, error) {
	return m.jobGetAvailableFunc(ctx, params)
}

type standardPilotExecutorTxMock struct {
	riverdriver.ExecutorTx

	beginCalls    int
	commitCalls   int
	insertFunc    func(context.Context, *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error)
	rollbackCalls int
}

func (m *standardPilotExecutorTxMock) Begin(context.Context) (riverdriver.ExecutorTx, error) {
	m.beginCalls++
	return m, nil
}

func (m *standardPilotExecutorTxMock) Commit(context.Context) error {
	m.commitCalls++
	return nil
}

func (m *standardPilotExecutorTxMock) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	return m.insertFunc(ctx, params)
}

func (m *standardPilotExecutorTxMock) Rollback(context.Context) error {
	m.rollbackCalls++
	return nil
}

func TestStandardPilot_JobDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("JobDelete", func(t *testing.T) {
		t.Parallel()

		params := &riverdriver.JobDeleteParams{ID: 123}
		job := &rivertype.JobRow{ID: 123}
		exec := &standardPilotExecutorMock{
			jobDeleteFunc: func(ctx context.Context, innerParams *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
				require.Same(t, params, innerParams)
				return job, nil
			},
		}

		deletedJob, err := (&StandardPilot{}).JobDelete(ctx, exec, params)
		require.NoError(t, err)
		require.Same(t, job, deletedJob)
	})

	t.Run("JobDeleteBefore", func(t *testing.T) {
		t.Parallel()

		params := &riverdriver.JobDeleteBeforeParams{Max: 10}
		exec := &standardPilotExecutorMock{
			jobDeleteBeforeFunc: func(ctx context.Context, innerParams *riverdriver.JobDeleteBeforeParams) (int, error) {
				require.Same(t, params, innerParams)
				return 7, nil
			},
		}

		numDeleted, err := (&StandardPilot{}).JobDeleteBefore(ctx, exec, params)
		require.NoError(t, err)
		require.Equal(t, 7, numDeleted)
	})

	t.Run("JobDeleteMany", func(t *testing.T) {
		t.Parallel()

		params := &riverdriver.JobDeleteManyParams{Max: 10}
		jobs := []*rivertype.JobRow{{ID: 123}}
		exec := &standardPilotExecutorMock{
			jobDeleteManyFunc: func(ctx context.Context, innerParams *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error) {
				require.Same(t, params, innerParams)
				return jobs, nil
			},
		}

		deletedJobs, err := (&StandardPilot{}).JobDeleteMany(ctx, exec, params)
		require.NoError(t, err)
		require.Equal(t, jobs, deletedJobs)
	})
}

func TestStandardPilot_JobGetAvailable(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		exec  *standardPilotExecutorMock
		pilot *StandardPilot
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		return &testBundle{
			exec:  &standardPilotExecutorMock{},
			pilot: &StandardPilot{},
		}
	}

	t.Run("ReturnsEmptyWhenMaxToLockIsZero", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		res, err := bundle.pilot.JobGetAvailable(context.Background(), bundle.exec, nil, &riverdriver.JobGetAvailableParams{})
		require.NoError(t, err)
		require.Equal(t, &riverdriver.JobGetAvailableResult{}, res)
	})

	t.Run("PreservesParentCancellation", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		parentErr := errors.New("parent cancelled")
		parentCtx, cancel := context.WithCancelCause(context.Background())
		cancel(parentErr)

		bundle.exec.jobGetAvailableFunc = func(ctx context.Context, params *riverdriver.JobGetAvailableParams) (*riverdriver.JobGetAvailableResult, error) {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}

		_, err := bundle.pilot.JobGetAvailable(parentCtx, bundle.exec, nil, &riverdriver.JobGetAvailableParams{
			MaxToLock: 1,
		})
		require.ErrorIs(t, err, parentErr)
	})
}

func TestStandardPilot_JobInsertMany(t *testing.T) {
	t.Parallel()

	for _, fail := range []bool{true, false} {
		name := "Success"
		if fail {
			name = "Error"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			params := &riverdriver.JobInsertFastManyParams{}
			result := []*riverdriver.JobInsertFastResult{{}}
			insertErr := errors.New("insert failure")
			calls := 0
			execTx := &standardPilotExecutorTxMock{
				insertFunc: func(innerCtx context.Context, innerParams *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
					calls++
					require.Equal(t, ctx, innerCtx)
					require.Same(t, params, innerParams)
					if fail {
						return nil, insertErr
					}
					return result, nil
				},
			}

			rows, err := (&StandardPilot{}).JobInsertMany(ctx, execTx, params)
			if fail {
				require.ErrorIs(t, err, insertErr)
				require.Nil(t, rows)
			} else {
				require.NoError(t, err)
				require.Equal(t, result, rows)
			}
			require.Equal(t, 1, calls)
			require.Zero(t, execTx.beginCalls, "the pilot uses the supplied transaction")
			require.Zero(t, execTx.commitCalls, "the caller owns the commit")
			require.Zero(t, execTx.rollbackCalls, "the caller owns error recovery")
		})
	}
}
