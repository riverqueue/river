// Package river_test is an external test package for river. It is separated
// from the internal package to ensure that no internal packages are relied on.
package river_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
)

func TestUnknownJobKindError_As(t *testing.T) {
	// This test isn't really necessary because we didn't have to write any code
	// to make it pass, but it does demonstrate that we can successfully use
	// errors.As with this custom error type.
	t.Parallel()

	t.Run("ReturnsTrueForAnotherUnregisteredKindError", func(t *testing.T) {
		t.Parallel()

		err1 := &river.UnknownJobKindError{Kind: "MyJobArgs"}
		var err2 *river.UnknownJobKindError
		require.ErrorAs(t, err1, &err2)
		require.Equal(t, err1, err2)
		require.Equal(t, err1.Kind, err2.Kind)
	})

	t.Run("ReturnsFalseForADifferentError", func(t *testing.T) {
		t.Parallel()

		var err *river.UnknownJobKindError
		require.NotErrorAs(t, errors.New("some other error"), &err)
	})
}

func TestUnknownJobKindError_Is(t *testing.T) {
	t.Parallel()

	t.Run("ReturnsTrueForAnotherUnregisteredKindError", func(t *testing.T) {
		t.Parallel()

		err1 := &river.UnknownJobKindError{Kind: "MyJobArgs"}
		require.ErrorIs(t, err1, &river.UnknownJobKindError{})
	})

	t.Run("ReturnsFalseForADifferentError", func(t *testing.T) {
		t.Parallel()

		err1 := &river.UnknownJobKindError{Kind: "MyJobArgs"}
		require.NotErrorIs(t, err1, errors.New("some other error"))
	})
}

func TestJobCancel(t *testing.T) {
	t.Parallel()

	t.Run("ErrorsIsReturnsTrueForAnotherErrorOfSameType", func(t *testing.T) {
		t.Parallel()
		err1 := river.JobCancel(errors.New("some message"))
		require.ErrorIs(t, err1, river.JobCancel(errors.New("another message")))
	})

	t.Run("ErrorsIsReturnsFalseForADifferentErrorType", func(t *testing.T) {
		t.Parallel()
		err1 := river.JobCancel(errors.New("some message"))
		require.NotErrorIs(t, err1, &river.UnknownJobKindError{Kind: "MyJobArgs"})
	})
}

func TestJobSnooze(t *testing.T) {
	t.Parallel()

	t.Run("PanicsForNegativeDuration", func(t *testing.T) {
		t.Parallel()

		for _, duration := range []time.Duration{
			-time.Nanosecond,
			-time.Second,
		} {
			require.PanicsWithValue(t, "JobSnooze: duration must be >= 0", func() {
				river.JobSnooze(duration)
			})
		}
	})

	t.Run("AcceptsNonNegativeDuration", func(t *testing.T) {
		t.Parallel()

		for _, duration := range []time.Duration{
			0,
			time.Second,
			5 * time.Minute,
		} {
			err := river.JobSnooze(duration)

			var snoozeErr *river.JobSnoozeError
			require.ErrorAs(t, err, &snoozeErr)
			require.Equal(t, duration, snoozeErr.Duration)
		}
	})
}
