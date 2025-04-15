package riverlog

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

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
	Logger(ctx).InfoContext(ctx, job.Args.Message)

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
		driver *riverpgxv5.Driver
		tx     pgx.Tx
	}

	setup := func(t *testing.T, config *MiddlewareConfig) (*rivertest.Worker[loggingArgs, pgx.Tx], *testBundle) {
		t.Helper()

		var (
			driver     = riverpgxv5.New(nil)
			middleware = NewMiddleware(func(w io.Writer) slog.Handler {
				return &slogutil.SlogMessageOnlyHandler{Out: w}
			}, config)
			clientConfig = &river.Config{
				Middleware: []rivertype.Middleware{middleware},
			}
			tx     = riversharedtest.TestTx(ctx, t)
			worker = &loggingWorker{}
		)

		return rivertest.NewWorker(t, driver, clientConfig, worker), &testBundle{
			driver: driver,
			tx:     tx,
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
				Log:     "Logged from worker\n",
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
		workRes.Job, err = bundle.driver.UnwrapExecutor(bundle.tx).JobUpdate(ctx, &riverdriver.JobUpdateParams{
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
				Log:     "Logged from worker\n",
			},
			{
				Attempt: 2,
				Log:     "Logged from worker\n",
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
				Log:     "Logged from worker\n",
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
				Log:     "Logged from worker\n",
			},
		},
			metadataWithLog.RiverLog,
		)
	})

	t.Run("TruncatedAtMaxSizeBytes", func(t *testing.T) {
		t.Parallel()

		testWorker, bundle := setup(t, &MiddlewareConfig{
			MaxSizeBytes: 11,
		})

		workRes, err := testWorker.Work(ctx, t, bundle.tx, loggingArgs{Message: "Logged from worker"}, nil)
		require.NoError(t, err)

		var metadataWithLog metadataWithLog
		require.NoError(t, json.Unmarshal(workRes.Job.Metadata, &metadataWithLog))

		require.Equal(t, []logAttempt{
			{
				Attempt: 1,
				Log:     "Logged from",
			},
		},
			metadataWithLog.RiverLog,
		)
	})
}
