package riverlog

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/rivertype"
)

func BenchmarkMiddlewareWorkAppend(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark in short mode")
	}

	middleware := NewMiddleware(func(w io.Writer) slog.Handler {
		return slog.NewTextHandler(w, nil)
	}, nil)

	cases := []struct {
		name string
		size int
	}{
		{name: "metadata_256kb", size: 256 * 1024},
		{name: "metadata_2mb", size: 2 * 1024 * 1024},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			metadata := makeMetadataWithLogSize(tc.size)
			appendLogLine := strings.Repeat("x", 2048)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				metadataUpdates := map[string]any{}
				ctx := context.WithValue(context.Background(), jobexecutor.ContextKeyMetadataUpdates, metadataUpdates)
				job := &rivertype.JobRow{
					Attempt:  1,
					Metadata: metadata,
				}

				if err := middleware.Work(ctx, job, func(ctx context.Context) error {
					Logger(ctx).InfoContext(ctx, appendLogLine)
					return nil
				}); err != nil {
					b.Fatalf("work returned error: %v", err)
				}

				if _, ok := metadataUpdates[metadataKey]; !ok {
					b.Fatal("missing river:log metadata update")
				}
			}
		})
	}
}

func makeMetadataWithLogSize(targetBytes int) []byte {
	if targetBytes <= 0 {
		return []byte(`{}`)
	}

	const perEntryLogSize = 1024
	payload := strings.Repeat("x", perEntryLogSize)
	numEntries := max(1, targetBytes/perEntryLogSize)

	logs := make([]logAttempt, numEntries)
	for i := range numEntries {
		logs[i] = logAttempt{
			Attempt: i + 1,
			Log:     payload,
		}
	}

	metadataBytes, err := json.Marshal(metadataWithLog{RiverLog: logs})
	if err != nil {
		panic(err)
	}

	return metadataBytes
}
