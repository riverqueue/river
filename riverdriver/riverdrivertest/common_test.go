package riverdrivertest_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// initTestConfig initializes properties on a given River config with defaults
// suitable for example tests, including an isolated test schema and
// example-friendly logger. It's meant to keep the main example code more terse
// and its use can be removed when copy/pasting from examples.
//
// This helper is intentionally duplicated across example packages so setup
// stays discoverable in each package's docs without introducing an internal
// example-only helper package.
//
// Most workflow examples call this helper to keep setup terse while leaving
// `pgxpool.New` and `river.NewClient` wiring visible in each example.
func initTestConfig(ctx context.Context, dbPool *pgxpool.Pool, config *river.Config) *river.Config {
	if config.Logger == nil {
		// Keep internal client logs off stdout so doctest output stays stable.
		config.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level:       slog.LevelWarn,
			ReplaceAttr: slogutil.NoLevelTime,
		}))
	}

	if dbPool != nil {
		config.Schema = riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)
	}

	config.TestOnly = true

	return config
}
