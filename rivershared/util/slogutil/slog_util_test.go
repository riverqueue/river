package slogutil

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoveReplaceAttrFunc(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		buf    *bytes.Buffer
		logger *slog.Logger
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		var buf bytes.Buffer

		return &testBundle{
			buf: &buf,
			logger: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				ReplaceAttr: removeReplaceAttrFunc("removed1", "removed2", "time"),
			})),
		}
	}

	t.Run("RemovesAttrs", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		bundle.logger.Info("A log message", slog.String("not_removed", "val"), slog.String("removed1", "val"), slog.String("removed1", "val"))

		require.Equal(t,
			`level=INFO msg="A log message" not_removed=val`+"\n",
			bundle.buf.String(),
		)
	})
}

func TestSliceInt64(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		t.Parallel()

		logger, buf := plainLoggerAndBuffer()
		logger.Info("log_entry", slog.Any("values", SliceInt64(nil)))

		require.Equal(t, `msg=log_entry values=""`+"\n", buf.String())
	})

	t.Run("Values", func(t *testing.T) {
		t.Parallel()

		logger, buf := plainLoggerAndBuffer()
		logger.Info("log_entry", slog.Any("values", SliceInt64([]int64{1, 2, 3})))

		require.Equal(t, "msg=log_entry values=1,2,3\n", buf.String())
	})
}

func TestSliceString(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		t.Parallel()

		logger, buf := plainLoggerAndBuffer()
		logger.Info("log_entry", slog.Any("values", SliceString(nil)))

		require.Equal(t, `msg=log_entry values=""`+"\n", buf.String())
	})

	t.Run("Values", func(t *testing.T) {
		t.Parallel()

		logger, buf := plainLoggerAndBuffer()
		logger.Info("log_entry", slog.Any("values", SliceString([]string{"foo", "bar"})))

		require.Equal(t, "msg=log_entry values=foo,bar\n", buf.String())
	})
}

func plainLoggerAndBuffer() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		ReplaceAttr: NoLevelTime,
	})), &buf
}
