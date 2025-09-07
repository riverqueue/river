package slogutil

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		// Removes the `level` and `time` keys so that we have clean and stable
		// output to match against in assertions.
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) < 1 {
				switch attr.Key {
				case slog.LevelKey, slog.TimeKey:
					return slog.Attr{}
				}
			}
			return attr
		},
	})), &buf
}
