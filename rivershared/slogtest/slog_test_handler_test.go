package slogtest

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

// This test doesn't assert anything due to the inherent difficulty of testing
// this test helper, but it can be run with `-test.v` to observe that it's
// working correctly.
func TestSlogTestHandler_levels(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	testCases := []struct {
		desc  string
		level slog.Level
	}{
		{desc: "Debug", level: slog.LevelDebug},
		{desc: "Info", level: slog.LevelInfo},
		{desc: "Warn", level: slog.LevelWarn},
		{desc: "Error", level: slog.LevelError},
	}
	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			logger := NewLogger(t, &slog.HandlerOptions{Level: tt.level})

			logger.DebugContext(ctx, "debug message")
			logger.InfoContext(ctx, "info message")
			logger.WarnContext(ctx, "warn message")
			logger.ErrorContext(ctx, "error message")
		})
	}
}

func TestSlogTestHandler_stress(t *testing.T) {
	t.Parallel()

	var (
		ctx    = context.Background()
		logger = NewLogger(t, nil)
		wg     sync.WaitGroup
	)

	for range 10 {
		wg.Go(func() {
			for range 100 {
				logger.InfoContext(ctx, "message", "key", "value")
			}
		})
	}

	wg.Wait()
}
