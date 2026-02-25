package riverlog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

func TestLogger(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(ctx, contextKey{}, riversharedtest.Logger(t))

		Logger(ctx).InfoContext(ctx, "Hello from logger")
	})

	t.Run("PanicIfNotSet", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "no logger in context; do you have riverlog.Middleware configured?", func() {
			Logger(ctx).InfoContext(ctx, "This will panic")
		})
	})
}

func TestAppendLogDataWithCap(t *testing.T) {
	t.Parallel()

	marshalLog := func(tb testing.TB, attempt int, log string) []byte {
		tb.Helper()
		b, err := json.Marshal(logAttempt{Attempt: attempt, Log: log})
		require.NoError(tb, err)
		return b
	}

	marshalMetadataWithLogs := func(tb testing.TB, logs []logAttempt) []byte {
		tb.Helper()
		b, err := json.Marshal(map[string]any{
			metadataKey: logs,
		})
		require.NoError(tb, err)
		return b
	}

	unmarshalLogs := func(tb testing.TB, rawArray []byte) []logAttempt {
		tb.Helper()
		var logs []logAttempt
		require.NoError(tb, json.Unmarshal(rawArray, &logs))
		return logs
	}

	t.Run("MissingKeyStartsFromEmptyArray", func(t *testing.T) {
		t.Parallel()

		newEntry := marshalLog(t, 1, "new")
		result, dropped, err := appendLogDataWithCap([]byte(`{"other":"value"}`), newEntry, maxTotalBytes)
		require.NoError(t, err)
		require.Zero(t, dropped)
		require.Equal(t, []logAttempt{{Attempt: 1, Log: "new"}}, unmarshalLogs(t, result))
	})

	t.Run("NonArrayLogValueReturnsError", func(t *testing.T) {
		t.Parallel()

		newEntry := marshalLog(t, 1, "new")
		_, _, err := appendLogDataWithCap([]byte(`{"river:log":{"not":"array"}}`), newEntry, maxTotalBytes)
		require.EqualError(t, err, `"river:log" value is not an array`)
	})

	t.Run("PrunesOldestEntriesOnlyAsNeeded", func(t *testing.T) {
		t.Parallel()

		existing := []logAttempt{
			{Attempt: 1, Log: "a"},
			{Attempt: 2, Log: "b"},
			{Attempt: 3, Log: "c"},
		}
		newEntry := marshalLog(t, 4, "d")

		target, err := json.Marshal([]logAttempt{
			{Attempt: 2, Log: "b"},
			{Attempt: 3, Log: "c"},
			{Attempt: 4, Log: "d"},
		})
		require.NoError(t, err)

		result, dropped, err := appendLogDataWithCap(marshalMetadataWithLogs(t, existing), newEntry, len(target))
		require.NoError(t, err)
		require.Equal(t, 1, dropped)
		require.Equal(t, []logAttempt{
			{Attempt: 2, Log: "b"},
			{Attempt: 3, Log: "c"},
			{Attempt: 4, Log: "d"},
		}, unmarshalLogs(t, result))
	})

	t.Run("KeepsNewestEntryEvenIfOverCap", func(t *testing.T) {
		t.Parallel()

		existing := []logAttempt{
			{Attempt: 1, Log: "a"},
			{Attempt: 2, Log: "b"},
		}
		newEntry := marshalLog(t, 3, strings.Repeat("x", 64))

		result, dropped, err := appendLogDataWithCap(marshalMetadataWithLogs(t, existing), newEntry, 8)
		require.NoError(t, err)
		require.Equal(t, len(existing), dropped)
		require.Equal(t, []logAttempt{
			{Attempt: 3, Log: strings.Repeat("x", 64)},
		}, unmarshalLogs(t, result))
	})

	t.Run("NoCapKeepsEverything", func(t *testing.T) {
		t.Parallel()

		existing := []logAttempt{
			{Attempt: 1, Log: "a"},
		}
		newEntry := marshalLog(t, 2, "b")

		result, dropped, err := appendLogDataWithCap(marshalMetadataWithLogs(t, existing), newEntry, 0)
		require.NoError(t, err)
		require.Zero(t, dropped)
		require.Equal(t, []logAttempt{
			{Attempt: 1, Log: "a"},
			{Attempt: 2, Log: "b"},
		}, unmarshalLogs(t, result))
	})

	t.Run("LargeExistingPayloadPrunesToCap", func(t *testing.T) {
		t.Parallel()

		// Simulate externally-written oversized payloads in metadata.
		existing := []logAttempt{
			{Attempt: 1, Log: strings.Repeat("x", 4*1024*1024)},
		}
		newEntry := marshalLog(t, 2, "new")

		// Cap is small enough that only the new entry can remain.
		const maxTotalBytes = 256

		result, dropped, err := appendLogDataWithCap(marshalMetadataWithLogs(t, existing), newEntry, maxTotalBytes)
		require.NoError(t, err)
		require.Equal(t, 1, dropped)
		require.LessOrEqual(t, len(result), maxTotalBytes)
		require.Equal(t, []logAttempt{
			{Attempt: 2, Log: "new"},
		}, unmarshalLogs(t, result))
	})

	t.Run("WhitespaceSeparatorsCountTowardsCap", func(t *testing.T) {
		t.Parallel()

		// jsonb canonicalization in Postgres usually removes separator
		// whitespace, so this shape is unlikely for production rows loaded from
		// the database. We still keep this test because the cap logic must remain
		// correct for any valid JSON input. If separators include spaces/newlines,
		// those bytes still count toward MaxTotalBytes and must be included in the
		// retention calculation.
		existingMetadata := []byte(`{"river:log":[{"attempt":1,"log":"a"},            {"attempt":2,"log":"b"},            {"attempt":3,"log":"c"}]}`)
		newEntry := marshalLog(t, 4, "d")

		// This cap is large enough to fit [2,3,4] only if separator whitespace is
		// ignored. Correct behavior must count all bytes in the final JSON.
		const maxTotalBytes = 73

		result, dropped, err := appendLogDataWithCap(existingMetadata, newEntry, maxTotalBytes)
		require.NoError(t, err)
		require.LessOrEqual(t, len(result), maxTotalBytes)
		require.Equal(t, 2, dropped)
		require.Equal(t, []logAttempt{
			{Attempt: 3, Log: "c"},
			{Attempt: 4, Log: "d"},
		}, unmarshalLogs(t, result))
	})
}

func TestLoggerSafely(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		ctx := context.WithValue(ctx, contextKey{}, riversharedtest.Logger(t))

		logger, ok := LoggerSafely(ctx)
		require.True(t, ok)
		logger.InfoContext(ctx, "Hello from logger")
	})

	t.Run("PanicIfNotSet", func(t *testing.T) {
		t.Parallel()

		_, ok := LoggerSafely(ctx)
		require.False(t, ok)
	})
}

var _ rivertype.WorkerMiddleware = &Middleware{}

type loggingArgs struct {
	DoError bool   `json:"do_error"`
	DoPanic bool   `json:"do_panic"`
	Message string `json:"message"`
}

func (loggingArgs) Kind() string { return "logging" }

type loggingWorker struct {
	river.WorkerDefaults[loggingArgs]
}

func (w *loggingWorker) Work(ctx context.Context, job *river.Job[loggingArgs]) error {
	if len(job.Args.Message) > 0 {
		Logger(ctx).InfoContext(ctx, job.Args.Message)
	}

	if job.Args.DoError {
		return errors.New("error from worker")
	}

	if job.Args.DoPanic {
		panic("panic from worker")
	}

	return nil
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		clientConfig *river.Config
		driver       *riverpgxv5.Driver
		middleware   *Middleware
		tx           pgx.Tx
	}

	setup := func(t *testing.T, config *MiddlewareConfig) (*rivertest.Worker[loggingArgs, pgx.Tx], *testBundle) {
		t.Helper()

		var (
			driver     = riverpgxv5.New(nil)
			middleware = NewMiddleware(func(w io.Writer) slog.Handler {
				return slog.NewTextHandler(w, &slog.HandlerOptions{ReplaceAttr: slogutil.NoLevelTime})
			}, config)
			clientConfig = &river.Config{
				Middleware: []rivertype.Middleware{middleware},
			}
			tx     = riverdbtest.TestTxPgx(ctx, t)
			worker = &loggingWorker{}
		)

		return rivertest.NewWorker(t, driver, clientConfig, worker), &testBundle{
			clientConfig: clientConfig,
			driver:       driver,
			middleware:   middleware,
			tx:           tx,
		}
	}

	t.Run("Success", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, nil)

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     `msg="Logged from worker"` + "\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("MultipleAttempts", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, nil)

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		// Set state back to available and unfinalize the job to make it runnable again.
		workRes.Job, err = bundle.driver.UnwrapExecutor(bundle.tx).JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:                  workRes.Job.ID,
			FinalizedAtDoUpdate: true,
			FinalizedAt:         nil,
			StateDoUpdate:       true,
			State:               rivertype.JobStateAvailable,
		})
		require.NoError(t, err)

		// Work the job again.
		workRes, err = testWorker.WorkJob(ctx, t, bundle.tx, workRes.Job)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     `msg="Logged from worker"` + "\n",
			},
			{
				Attempt: 2,
				Log:     `msg="Logged from worker"` + "\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("OnError", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, nil)

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{DoError: true, Message: "Logged from worker"}, nil)
		require.EqualError(t, err, "error from worker")

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     `msg="Logged from worker"` + "\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("OnPanic", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, nil)

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{DoPanic: true, Message: "Logged from worker"}, nil)
		var panicErr *rivertest.PanicError
		require.ErrorAs(t, err, &panicErr)
		require.Equal(t, "panic from worker", panicErr.Cause)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     `msg="Logged from worker"` + "\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("EmptyLogsStoreNoMetadata", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, nil)

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: ""}, nil)
		require.NoError(t, err)

		require.JSONEq(t, "{}", string(workRes.Job.Metadata))
	})

	t.Run("TruncatedAtMaxSizeBytes", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, &MiddlewareConfig{
			MaxSizeBytes: 16,
		})

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     `msg="Logged from`,
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("PrunesOldestEntriesAtMaxTotalBytes", func(t *testing.T) {
		t.Parallel()

		maxTotalBytes, err := json.Marshal([]logAttempt{
			{Attempt: 1, Log: `msg="Logged from worker"` + "\n"},
			{Attempt: 2, Log: `msg="Logged from worker"` + "\n"},
		})
		require.NoError(t, err)

		testWorker, bundle := setup(t, &MiddlewareConfig{
			// Keep two entries, force pruning once a third is appended.
			MaxTotalBytes: len(maxTotalBytes) + 16,
		})

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		// Set state back to available and unfinalize the job to make it runnable again.
		workRes.Job, err = bundle.driver.UnwrapExecutor(bundle.tx).JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:                  workRes.Job.ID,
			FinalizedAtDoUpdate: true,
			FinalizedAt:         nil,
			StateDoUpdate:       true,
			State:               rivertype.JobStateAvailable,
		})
		require.NoError(t, err)

		workRes, err = testWorker.WorkJob(ctx, t, bundle.tx, workRes.Job)
		require.NoError(t, err)

		// Set state back to available and unfinalize the job to make it runnable again.
		workRes.Job, err = bundle.driver.UnwrapExecutor(bundle.tx).JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:                  workRes.Job.ID,
			FinalizedAtDoUpdate: true,
			FinalizedAt:         nil,
			StateDoUpdate:       true,
			State:               rivertype.JobStateAvailable,
		})
		require.NoError(t, err)

		workRes, err = testWorker.WorkJob(ctx, t, bundle.tx, workRes.Job)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{Attempt: 2, Log: `msg="Logged from worker"` + "\n"},
			{Attempt: 3, Log: `msg="Logged from worker"` + "\n"},
		}, metadataWithLog.RiverLog)
	})

	t.Run("RetainsLatestEntryWhenTotalLimitTiny", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, &MiddlewareConfig{
			MaxTotalBytes: 1,
		})

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		// Set state back to available and unfinalize the job to make it runnable again.
		workRes.Job, err = bundle.driver.UnwrapExecutor(bundle.tx).JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
			ID:                  workRes.Job.ID,
			FinalizedAtDoUpdate: true,
			FinalizedAt:         nil,
			StateDoUpdate:       true,
			State:               rivertype.JobStateAvailable,
		})
		require.NoError(t, err)

		workRes, err = testWorker.WorkJob(ctx, t, bundle.tx, workRes.Job)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{Attempt: 2, Log: `msg="Logged from worker"` + "\n"},
		}, metadataWithLog.RiverLog)
	})

	t.Run("DefaultsMaxTotalBytes", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t, nil)

		require.Equal(t, maxTotalBytes, bundle.middleware.config.MaxTotalBytes)
	})

	t.Run("ClampsMaxTotalBytes", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t, &MiddlewareConfig{
			MaxTotalBytes: maxTotalBytesMax + 1,
		})

		require.Equal(t, maxTotalBytesMax, bundle.middleware.config.MaxTotalBytes)
	})

	t.Run("RawMiddleware", func(t *testing.T) {
		t.Parallel()

		_, bundle := setup(t, nil)

		type writerContextKey struct{}

		bundle.middleware.newCustomContext = func(ctx context.Context, w io.Writer) context.Context {
			logger := log.New(w, "", 0)
			return context.WithValue(ctx, writerContextKey{}, logger)
		}
		bundle.middleware.newSlogHandler = nil

		testWorker := rivertest.NewWorker(t, bundle.driver, bundle.clientConfig, river.WorkFunc(func(ctx context.Context, job *river.Job[loggingArgs]) error {
			logger := ctx.Value(writerContextKey{}).(*log.Logger) //nolint:forcetypeassert
			logger.Println(job.Args.Message)
			return nil
		}))

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Raw log from worker"}, nil)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     "Raw log from worker\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})
}
