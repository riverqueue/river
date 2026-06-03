package timeoututil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	t.Run("NestedTimeoutsReturnLocalCause", func(t *testing.T) {
		t.Parallel()

		err := WithTimeout(context.Background(), time.Hour, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Outer", func(ctx context.Context) error {
			return WithTimeout(ctx, time.Nanosecond, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Inner", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
		})
		require.EqualError(t, err, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Inner timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)

		err = WithTimeout(context.Background(), time.Nanosecond, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Outer", func(ctx context.Context) error {
			return WithTimeout(ctx, time.Hour, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Inner", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			})
		})
		require.EqualError(t, err, "TestWithTimeout.NestedTimeoutsReturnLocalCause.Outer timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("PreservesLocalTimeoutCause", func(t *testing.T) {
		t.Parallel()

		err := WithTimeout(context.Background(), time.Nanosecond, "TestWithTimeout.PreservesLocalTimeoutCause", func(ctx context.Context) error {
			<-ctx.Done()
			return context.Cause(ctx)
		})
		require.EqualError(t, err, "TestWithTimeout.PreservesLocalTimeoutCause timed out after 1ns: timeoututil.WithTimeout: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("PreservesWrappedDeadlineExceededFromLocalTimeout", func(t *testing.T) {
		t.Parallel()

		innerErr := errors.New("inner error")

		err := WithTimeout(context.Background(), time.Nanosecond, "TestWithTimeout.PreservesWrappedDeadlineExceededFromLocalTimeout", func(ctx context.Context) error {
			<-ctx.Done()
			return fmt.Errorf("%w: %w", innerErr, ctx.Err())
		})
		require.EqualError(t, err, "TestWithTimeout.PreservesWrappedDeadlineExceededFromLocalTimeout timed out after 1ns: inner error: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, innerErr)
	})

	t.Run("ReturnsInnerError", func(t *testing.T) {
		t.Parallel()

		innerErr := errors.New("inner error")

		err := WithTimeout(context.Background(), time.Hour, "TestWithTimeout.ReturnsInnerError", func(ctx context.Context) error {
			return innerErr
		})
		require.ErrorIs(t, err, innerErr)
	})

	t.Run("ReturnsLocalTimeoutCause", func(t *testing.T) {
		t.Parallel()

		err := WithTimeout(context.Background(), time.Nanosecond, "TestWithTimeout.ReturnsLocalTimeoutCause", func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		require.EqualError(t, err, "TestWithTimeout.ReturnsLocalTimeoutCause timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestWithTimeoutV(t *testing.T) {
	t.Parallel()

	t.Run("NestedTimeoutsReturnLocalCause", func(t *testing.T) {
		t.Parallel()

		ret, err := WithTimeoutV(context.Background(), time.Hour, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Outer", func(ctx context.Context) (int, error) {
			return WithTimeoutV(ctx, time.Nanosecond, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Inner", func(ctx context.Context) (int, error) {
				<-ctx.Done()
				return 9, ctx.Err()
			})
		})
		require.Zero(t, ret)
		require.EqualError(t, err, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Inner timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)

		ret, err = WithTimeoutV(context.Background(), time.Nanosecond, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Outer", func(ctx context.Context) (int, error) {
			return WithTimeoutV(ctx, time.Hour, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Inner", func(ctx context.Context) (int, error) {
				<-ctx.Done()
				return 9, ctx.Err()
			})
		})
		require.Zero(t, ret)
		require.EqualError(t, err, "TestWithTimeoutV.NestedTimeoutsReturnLocalCause.Outer timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("PreservesLocalTimeoutCause", func(t *testing.T) {
		t.Parallel()

		ret, err := WithTimeoutV(context.Background(), time.Nanosecond, "TestWithTimeoutV.PreservesLocalTimeoutCause", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 9, context.Cause(ctx)
		})
		require.Zero(t, ret)
		require.EqualError(t, err, "TestWithTimeoutV.PreservesLocalTimeoutCause timed out after 1ns: timeoututil.WithTimeout: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("PreservesParentCancellation", func(t *testing.T) {
		t.Parallel()

		parentErr := errors.New("parent cancelled")
		parentCtx, cancel := context.WithCancelCause(context.Background())
		cancel(parentErr)

		ret, err := WithTimeoutV(parentCtx, time.Hour, "TestWithTimeoutV.PreservesParentCancellation", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, context.Cause(ctx)
		})
		require.Zero(t, ret)
		require.ErrorIs(t, err, parentErr)
	})

	t.Run("PreservesWrappedDeadlineExceededFromLocalTimeout", func(t *testing.T) {
		t.Parallel()

		innerErr := errors.New("inner error")

		ret, err := WithTimeoutV(context.Background(), time.Nanosecond, "TestWithTimeoutV.PreservesWrappedDeadlineExceededFromLocalTimeout", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 9, fmt.Errorf("%w: %w", innerErr, ctx.Err())
		})
		require.Zero(t, ret)
		require.EqualError(t, err, "TestWithTimeoutV.PreservesWrappedDeadlineExceededFromLocalTimeout timed out after 1ns: inner error: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, innerErr)
	})

	t.Run("ReturnsInnerValue", func(t *testing.T) {
		t.Parallel()

		ret, err := WithTimeoutV(context.Background(), time.Hour, "TestWithTimeoutV.ReturnsInnerValue", func(ctx context.Context) (int, error) {
			return 7, nil
		})
		require.NoError(t, err)
		require.Equal(t, 7, ret)
	})

	t.Run("WrapsDeadlineExceededFromLocalTimeout", func(t *testing.T) {
		t.Parallel()

		ret, err := WithTimeoutV(context.Background(), time.Nanosecond, "TestWithTimeoutV.WrapsDeadlineExceededFromLocalTimeout", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 9, ctx.Err()
		})
		require.Zero(t, ret)
		require.EqualError(t, err, "TestWithTimeoutV.WrapsDeadlineExceededFromLocalTimeout timed out after 1ns: context deadline exceeded")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
