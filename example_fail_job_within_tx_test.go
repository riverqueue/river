package river_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/slogutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

// UploadFailArgs represents work on a user-initiated upload. A separate
// "uploads" row is created by the web app in the same transaction that
// enqueues this job, and its "status" column is what the web app polls to
// show progress to the user. The job is responsible for keeping that
// external status in sync with River's view of the job's final outcome.
type UploadFailArgs struct {
	UploadID int64 `json:"upload_id"`
}

func (UploadFailArgs) Kind() string { return "example_upload_fail_worker" }

// UploadFailWorker simulates a worker processing an upload. The work always
// fails (to make the example deterministic). Each attempt increments the
// external "uploads.attempts" counter so the web app can show progress.
// On the final attempt, the external "uploads.status" is promoted to
// "failed" so the user sees a permanent failure; on non-final attempts,
// status stays "working".
type UploadFailWorker struct {
	river.WorkerDefaults[UploadFailArgs]

	dbPool *pgxpool.Pool
	schema string
}

// NextRetry retries immediately so the example finishes quickly.
func (w *UploadFailWorker) NextRetry(*river.Job[UploadFailArgs]) time.Time {
	return time.Now()
}

// Work uses a named return value (err) plus a single deferred failure
// handler. The "real" body below just returns any error it encounters -
// persistence is handled in one place by the defer. The attempts counter
// is incremented up front (outside the defer) so it happens on every run.
func (w *UploadFailWorker) Work(ctx context.Context, job *river.Job[UploadFailArgs]) (err error) {
	tx, err := w.dbPool.Begin(ctx)
	if err != nil {
		// return err #1: could not begin a tx. Nothing has been done;
		// River will retry per its policy.
		return err
	}
	// Rolls back if nothing else commits. No-op after a successful commit.
	defer tx.Rollback(ctx)

	// Always increment the external "attempts" counter at the start of a
	// run so the web app can display progress across retries.
	if _, err = tx.Exec(ctx,
		fmt.Sprintf(`UPDATE %q.uploads SET attempts = attempts + 1 WHERE id = $1`, w.schema),
		job.Args.UploadID,
	); err != nil {
		// return err #2: the external UPDATE failed. Tx rolls back; River retries.
		return err
	}

	// One deferred handler for both success and failure. On success
	// (err == nil) it just commits the tx. On failure it records the
	// error via JobFailTx, synchronizes the external status with
	// River's decision (retry vs final), commits, and clears err so
	// River's executor doesn't record a redundant AttemptError.
	defer func() {
		if err == nil {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				err = commitErr
			}
			return
		}

		// Record the failure atomically with the attempts increment.
		// JobFailTx picks retryable, available, or discarded based on
		// how many attempts remain, using the same retry-policy logic
		// the executor's own error path uses.
		jobAfter, failErr := river.JobFailTx[*riverpgxv5.Driver](ctx, tx, job, err)
		if failErr != nil {
			// JobFailTx itself failed (rare - e.g. JSON marshal of
			// metadata updates, or a DB error). Tx rolls back and the
			// executor's own error path will record an AttemptError for
			// the original err. Surface both via errors.Join.
			err = errors.Join(err, failErr)
			return
		}

		// Only promote the external status to "failed" on the final
		// attempt. On non-final attempts the status stays "working"
		// (only the attempts counter changed) and the user sees the
		// job as still in progress.
		if jobAfter.State == rivertype.JobStateDiscarded {
			if _, syncErr := tx.Exec(ctx,
				fmt.Sprintf(`UPDATE %q.uploads SET status = 'failed', last_error = $1 WHERE id = $2`, w.schema),
				err.Error(), job.Args.UploadID,
			); syncErr != nil {
				err = errors.Join(err, syncErr)
				return
			}
		}

		if commitErr := tx.Commit(ctx); commitErr != nil {
			err = errors.Join(err, commitErr)
			return
		}

		// Leave err set and let it propagate back to River. The failure
		// is already recorded on the job and the external row is in sync,
		// so this isn't about persisting the error - it's about giving
		// any registered HookWorkEnd hooks (and ErrorHandler) a chance
		// to still observe the error, log/trace it, and merge additional
		// metadata (e.g. via river.RecordOutput). Metadata merges on the
		// executor's subsequent UPDATE are *not* guarded by state, so
		// those land asynchronously after our tx commits. Hooks cannot
		// change the job's final state or append a second AttemptError
		// because those columns in that UPDATE are guarded by state =
		// 'running', which no longer holds.
	}()

	// A real worker does its actual work here. On success it would call
	// river.JobCompleteTx and then `return nil`; the deferred handler
	// commits the tx. Any error returned is handed to the defer. For
	// this deterministic example we simulate a transient failure:
	return errors.New("transient upstream failure")
}

// Example_failJobWithinTx demonstrates how to transactionally record a job
// failure while keeping an external "uploads" row in sync. The example runs
// the job with MaxAttempts=2 so you can see both paths: after the first
// failure the external status stays "working" (and attempts=1); after the
// final failure the status flips to "failed" (and attempts=2).
func Example_failJobWithinTx() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	// Isolated schema for this example (only necessary for the example test).
	schema := riverdbtest.TestSchema(ctx, testutil.PanicTB(), riverpgxv5.New(dbPool), nil)

	// Stand in for the web app's own "uploads" table. In a real application
	// this lives alongside your domain tables.
	if _, err := dbPool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %q.uploads (
			id         bigint PRIMARY KEY,
			status     text   NOT NULL,
			attempts   int    NOT NULL DEFAULT 0,
			last_error text
		)`, schema)); err != nil {
		panic(err)
	}
	// The web app inserts this row in the same tx as the River job (not
	// shown here); here we just create it first.
	if _, err := dbPool.Exec(ctx, fmt.Sprintf(`INSERT INTO %q.uploads (id, status) VALUES (42, 'working')`, schema)); err != nil {
		panic(err)
	}

	workers := river.NewWorkers()
	river.AddWorker(workers, &UploadFailWorker{dbPool: dbPool, schema: schema})

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn, ReplaceAttr: slogutil.NoLevelTime})),
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Schema:   schema,
		TestOnly: true, // suitable only for use in tests; remove for live environments
		Workers:  workers,
	})
	if err != nil {
		panic(err)
	}

	// Subscribe to the failed-event stream so the example can wait for the
	// job to be fully processed.
	subscribeChan, subscribeCancel := riverClient.Subscribe(river.EventKindJobFailed)
	defer subscribeCancel()

	if err := riverClient.Start(ctx); err != nil {
		panic(err)
	}

	// MaxAttempts: 2 so the example exercises both the "retry remaining"
	// path (status stays working) and the "final attempt" path (status
	// becomes failed).
	if _, err := riverClient.Insert(ctx, UploadFailArgs{UploadID: 42}, &river.InsertOpts{MaxAttempts: 2}); err != nil {
		panic(err)
	}

	// Wait for the job to reach a terminal Discarded state. Use a generous
	// timeout so the retry loop (attempt 1 -> Available -> fetcher picks it
	// up -> attempt 2) completes reliably even under the race detector or
	// heavy CI parallelism.
	deadline := time.After(30 * time.Second)
	for {
		select {
		case event := <-subscribeChan:
			if event.Job.State == rivertype.JobStateDiscarded {
				goto done
			}
		case <-deadline:
			panic("timed out waiting for job to be discarded")
		}
	}
done:

	if err := riverClient.Stop(ctx); err != nil {
		panic(err)
	}

	var (
		status    string
		attempts  int
		lastError *string
	)
	if err := dbPool.QueryRow(ctx,
		fmt.Sprintf(`SELECT status, attempts, last_error FROM %q.uploads WHERE id = 42`, schema),
	).Scan(&status, &attempts, &lastError); err != nil {
		panic(err)
	}
	lastErrTxt := ""
	if lastError != nil {
		lastErrTxt = *lastError
	}
	fmt.Printf("upload 42: status=%q attempts=%d last_error=%q\n", status, attempts, lastErrTxt)

	// Output:
	// upload 42: status="failed" attempts=2 last_error="transient upstream failure"
}
