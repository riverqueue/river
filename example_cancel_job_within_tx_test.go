package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// UploadCancelArgs represents work on a user-initiated upload that the
// worker may discover to be unrecoverable. A separate "uploads" row is
// created by the web app in the same transaction that enqueues this job;
// the job keeps the external status in sync with River's view of
// the job's outcome.
type UploadCancelArgs struct {
	UploadID int64 `json:"upload_id"`
	// Valid is used to simulate validation outcomes. When false, the
	// worker treats the upload as permanently unprocessable and cancels
	// the job rather than retrying.
	Valid bool `json:"valid"`
}

func (UploadCancelArgs) Kind() string { return "example_upload_cancel_worker" }

// UploadCancelWorker processes an upload. If the upload is permanently
// invalid, the worker uses JobCancelTx to record the failure: the job will
// not be retried regardless of MaxAttempts. The external "uploads" row's
// status is promoted to "cancelled" in the same transaction so the web
// app sees a consistent final state.
type UploadCancelWorker struct {
	river.WorkerDefaults[UploadCancelArgs]

	dbPool *pgxpool.Pool
	schema string
}

func (w *UploadCancelWorker) Work(ctx context.Context, job *river.Job[UploadCancelArgs]) error {
	tx, err := w.dbPool.Begin(ctx)
	if err != nil {
		// return err #1: could not begin a tx. Nothing has been done yet;
		// River will retry per its policy.
		return err
	}
	defer tx.Rollback(ctx)

	// Increment the external "attempts" counter so the web app can see
	// that the worker ran.
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %q.uploads SET attempts = attempts + 1 WHERE id = $1`, w.schema),
		job.Args.UploadID,
	); err != nil {
		// return err #2: external UPDATE failed; tx rolls back. River retries.
		return err
	}

	// Perform validation. If the upload is unrecoverably invalid there's
	// no point retrying - cancel the job instead.
	if !job.Args.Valid {
		validationErr := errors.New("upload rejected: invalid content")

		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`UPDATE %q.uploads SET status = 'cancelled', last_error = $1 WHERE id = $2`, w.schema),
			validationErr.Error(), job.Args.UploadID,
		); err != nil {
			// return err #3: the external "mark cancelled" UPDATE failed.
			// Tx rolls back with everything in it. River will retry the
			// job normally - which on the next run will hit the same
			// validation error and (if we then reach JobCancelTx
			// successfully) cancel for real.
			return err
		}

		if _, err := river.JobCancelTx[*riverpgxv5.Driver](ctx, tx, job, validationErr); err != nil {
			// return err #4: JobCancelTx itself errored. Tx rolls back.
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			// return err #5: commit failed. Nothing persisted. River retries.
			return err
		}

		// return validationErr #6: the cancellation is recorded and the
		// external row is in sync. Return the error up so that any
		// registered HookWorkEnd hooks (and ErrorHandler) still see it
		// and can log/trace it or add metadata via river.RecordOutput.
		// Metadata merges on the executor's subsequent UPDATE are *not*
		// guarded by state, so those land asynchronously after our tx
		// commits. Hooks and handlers cannot steer the job's outcome at
		// this point: the state change and any new AttemptError are
		// guarded by state = 'running' in the SQL and won't apply now
		// that JobCancelTx has moved the row out of that state.
		return validationErr
	}

	// Normal success path omitted for brevity; a real worker would do its
	// work here, optionally call JobCompleteTx to finalize the job within
	// this same tx, then commit.
	return errors.New("success path not shown in this example")
}

// Example_cancelJobWithinTx demonstrates how to transactionally cancel a
// job when the worker determines that further retries are pointless,
// while keeping an external status row in sync.
func Example_cancelJobWithinTx() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	schema := riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)

	if _, err := dbPool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %q.uploads (
			id         bigint PRIMARY KEY,
			status     text   NOT NULL,
			attempts   int    NOT NULL DEFAULT 0,
			last_error text
		)`, schema)); err != nil {
		panic(err)
	}
	if _, err := dbPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q.uploads (id, status) VALUES (99, 'working')`, schema)); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &UploadCancelWorker{dbPool: dbPool, schema: schema})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Schema:   schema,
		TestOnly: true,
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Subscribe to the cancelled-event stream so the example can wait for
	// the job to be fully processed.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobCancelled)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// MaxAttempts=5 to make clear the job stops on the first run because
	// of the cancel - attempts remaining doesn't matter.
	if _, err := riverClient.Insert(ctx, UploadCancelArgs{UploadID: 99, Valid: false}, &river.InsertOpts{MaxAttempts: 5}); err != nil {
		panic(err)
	}

	riversharedtest.WaitOrTimeoutN(testutil.PanicTB(), subscribeChan, 1)

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	var (
		status    string
		attempts  int
		lastError *string
	)
	if err := dbPool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, attempts, last_error FROM %q.uploads WHERE id = 99`, schema),
	).Scan(&status, &attempts, &lastError); err != nil {
		panic(err)
	}
	lastErrTxt := ""
	if lastError != nil {
		lastErrTxt = *lastError
	}
	fmt.Printf("upload 99: status=%q attempts=%d last_error=%q\n", status, attempts, lastErrTxt)

	// Output:
	// upload 99: status="cancelled" attempts=1 last_error="upload rejected: invalid content"
}
