package timeoututil

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WithTimeout runs innerFunc with a timeout.
//
// If innerFunc returns context.DeadlineExceeded because this helper's local
// timeout fired, WithTimeout returns an error that includes operation and wraps
// context.DeadlineExceeded. This makes timeout errors easier to trace back to
// the specific River operation that introduced the timeout instead of surfacing
// only the generic "context deadline exceeded" message.
func WithTimeout(ctx context.Context, timeout time.Duration, operation string, innerFunc func(ctx context.Context) error) error {
	_, err := WithTimeoutV(ctx, timeout, operation, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, innerFunc(ctx)
	})
	return err
}

// WithTimeoutV runs innerFunc with a timeout and returns its value.
//
// If innerFunc returns context.DeadlineExceeded because this helper's local
// timeout fired, WithTimeoutV returns an error that includes operation and
// wraps context.DeadlineExceeded. This makes timeout errors easier to trace
// back to the specific River operation that introduced the timeout instead of
// surfacing only the generic "context deadline exceeded" message.
func WithTimeoutV[T any](ctx context.Context, timeout time.Duration, operation string, innerFunc func(ctx context.Context) (T, error)) (T, error) {
	// need a specific, local error that we can recognize in case multiple
	// levels of these helpers are wrapped within one another
	timeoutErr := fmt.Errorf("timeoututil.WithTimeout: %w", context.DeadlineExceeded)

	ctx, cancel := context.WithTimeoutCause(ctx, timeout, timeoutErr)
	defer cancel()

	ret, err := innerFunc(ctx)
	if err != nil && errors.Is(err, context.DeadlineExceeded) && errors.Is(context.Cause(ctx), timeoutErr) {
		var zero T
		return zero, fmt.Errorf("%s timed out after %s: %w", operation, timeout, err)
	}
	return ret, err
}
