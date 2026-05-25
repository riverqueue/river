package riverpilot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

type standardPilotExecutorMock struct {
	riverdriver.Executor

	jobGetAvailableFunc func(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error)
}

func (m *standardPilotExecutorMock) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	return m.jobGetAvailableFunc(ctx, params)
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
			exec: &standardPilotExecutorMock{},
			pilot: &StandardPilot{
				jobGetAvailableTimeout: 5 * time.Millisecond,
			},
		}
	}

	t.Run("ReturnsNilWhenMaxToLockIsZero", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		res, err := bundle.pilot.JobGetAvailable(context.Background(), bundle.exec, nil, &riverdriver.JobGetAvailableParams{})
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("TimesOutHungFetch", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		timeoutSeen := make(chan error, 1)

		bundle.exec.jobGetAvailableFunc = func(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
			<-ctx.Done()
			timeoutSeen <- ctx.Err()
			return nil, ctx.Err()
		}

		start := time.Now()
		_, err := bundle.pilot.JobGetAvailable(context.Background(), bundle.exec, nil, &riverdriver.JobGetAvailableParams{
			MaxToLock: 1,
		})
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.WithinDuration(t, time.Now(), start.Add(bundle.pilot.jobGetAvailableTimeout), 25*time.Millisecond)
		require.ErrorIs(t, <-timeoutSeen, context.DeadlineExceeded)
	})

	t.Run("PreservesParentCancellation", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		parentErr := errors.New("parent cancelled")
		parentCtx, cancel := context.WithCancelCause(context.Background())
		cancel(parentErr)

		bundle.exec.jobGetAvailableFunc = func(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
			<-ctx.Done()
			return nil, context.Cause(ctx)
		}

		_, err := bundle.pilot.JobGetAvailable(parentCtx, bundle.exec, nil, &riverdriver.JobGetAvailableParams{
			MaxToLock: 1,
		})
		require.ErrorIs(t, err, parentErr)
	})
}
