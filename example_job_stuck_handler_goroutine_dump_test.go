package river_test

import (
	"context"
	"log/slog"
	"runtime"

	"github.com/riverqueue/river"
)

// Example_jobStuckHandlerGoroutineDump demonstrates how to capture and log a
// goroutine dump from a JobStuckHandler.
func Example_jobStuckHandlerGoroutineDump() {
	logger := slog.Default()

	config := &river.Config{
		JobStuckHandler: func(ctx context.Context, params river.JobStuckHandlerParams) river.JobStuckHandlerResult {
			if logger.Enabled(ctx, slog.LevelWarn) {
				logger.WarnContext(ctx, "River job stuck",
					slog.Int64("job_id", params.ID),
					slog.String("kind", params.Kind),
					slog.String("queue", params.Queue),
					slog.Int("total_stuck_jobs", params.TotalStuckJobs),
					slog.String("goroutine_dump", captureGoroutineDump()),
				)
			}

			return river.JobStuckHandlerResult{}
		},
	}

	_ = config

	// Output:
}

func captureGoroutineDump() string {
	buf := make([]byte, 64*1024)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}

		buf = make([]byte, 2*len(buf))
	}
}
