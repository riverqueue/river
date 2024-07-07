package slogtest

import (
	"log/slog"
	"sync"
	"testing"
)

// This test doesn't assert anything due to the inherent difficulty of testing
// this test helper, but it can be run with `-test.v` to observe that it's
// working correctly.
func TestSlogTestHandler_levels(t *testing.T) {
	t.Parallel()

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
		tt := tt
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			logger := NewLogger(t, &slog.HandlerOptions{Level: tt.level})

			logger.Debug("debug message")
			logger.Info("info message")
			logger.Warn("warn message")
			logger.Error("error message")
		})
	}
}

func TestSlogTestHandler_stress(t *testing.T) {
	t.Parallel()

	var (
		logger = NewLogger(t, nil)
		wg     sync.WaitGroup
	)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 100; j++ {
				logger.Info("message", "key", "value")
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
