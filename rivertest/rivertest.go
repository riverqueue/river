// Package rivertest contains test assertions that can be used in a project's
// tests to verify that certain actions occurred from the main river package.
package rivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
)

// dbtx is a database-like executor which is implemented by all of pgxpool.Pool,
// pgx.Conn, and pgx.Tx. It's used to let package functions share code with a
// common implementation that takes one of these.
type dbtx interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// testingT is an interface wrapper around *testing.T that's implemented by all
// of *testing.T, *testing.F, and *testing.B.
//
// It's used internally to verify that River's test assertions are working as
// expected.
type testingT interface {
	Errorf(format string, args ...any)
	FailNow()
	Helper()
	Log(args ...any)
	Logf(format string, args ...any)
}

// Options for RequireInserted or RequireManyInserted including expectations for
// various queuing properties that stem from InsertOpts.
type RequireInsertedOpts struct {
	// MaxAttempts is the expected maximum number of total attempts for the
	// inserted job.
	//
	// No assertion is made if left the zero value.
	MaxAttempts int

	// Priority is the expected priority for the inserted job.
	//
	// No assertion is made if left the zero value.
	Priority int

	// Queue is the expected queue name of the inserted job.
	//
	// No assertion is made if left the zero value.
	Queue string

	// ScheduledAt is the expected scheduled at time of the inserted job. Times
	// are truncated to the microsecond level for comparison to account for the
	// difference between Go storing times to nanoseconds and Postgres storing
	// only to microsecond precision.
	//
	// No assertion is made if left the zero value.
	ScheduledAt time.Time

	// State is the expected state of the inserted job.
	//
	// No assertion is made if left the zero value.
	State river.JobState

	// Tags are the expected tags of the inserted job.
	//
	// No assertion is made if left the zero value.
	Tags []string
}

// RequireInserted is a test helper that verifies that a job of the given kind
// was inserted for work, failing the test if it wasn't. If found, the inserted
// job is returned so that further assertions can be made against it.
//
//	job := RequireInserted(ctx, t, riverpgxv5.New(dbPool), &Job1Args{}, nil)
//
// This variant takes a driver that wraps a database pool. See also
// RequireManyInsertedTx which takes a transaction.
//
// A RequireInsertedOpts struct can be provided as the last argument, and if it is,
// its properties (e.g. max attempts, priority, queue name) will act as required
// assertions in the inserted job row. UniqueOpts is ignored.
//
// The assertion will fail if more than one job of the given kind was found
// because at that point the job to return is ambiguous. Use RequireManyInserted
// to cover that case instead.
func RequireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	tb.Helper()
	return requireInserted(ctx, tb, driver, expectedJob, opts)
}

func requireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	actualArgs, err := requireInsertedErr[TDriver](ctx, t, driver.GetDBPool(), expectedJob, opts)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

// RequireInsertedTx is a test helper that verifies that a job of the given kind
// was inserted for work, failing the test if it wasn't. If found, the inserted
// job is returned so that further assertions can be made against it.
//
//	job := RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &Job1Args{}, nil)
//
// This variant takes a transaction. See also RequireInserted which takes a
// driver that wraps a database pool.
//
// A RequireInsertedOpts struct can be provided as the last argument, and if it is,
// its properties (e.g. max attempts, priority, queue name) will act as required
// assertions in the inserted job row. UniqueOpts is ignored.
//
// The assertion will fail if more than one job of the given kind was found
// because at that point the job to return is ambiguous. Use RequireManyInserted
// to cover that case instead.
func RequireInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	tb.Helper()
	return requireInsertedTx[TDriver](ctx, tb, tx, expectedJob, opts)
}

// Internal function used by the tests so that the exported version can take
// `testing.TB` instead of `testing.T`.
func requireInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	var driver TDriver
	actualArgs, err := requireInsertedErr[TDriver](ctx, t, driver.UnwrapTx(tx), expectedJob, opts)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

func requireInsertedErr[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, db dbtx, expectedJob TArgs, opts *RequireInsertedOpts) (*river.Job[TArgs], error) {
	queries := dbsqlc.New()

	// Returned ordered by ID.
	dbJobs, err := queries.JobGetByKind(ctx, db, expectedJob.Kind())
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}

	if len(dbJobs) < 1 {
		failure(t, "No jobs found with kind: %s", expectedJob.Kind())
		return nil, nil //nolint:nilnil
	}

	if len(dbJobs) > 1 {
		failure(t, "More than one job found with kind: %s (you might want RequireManyInserted instead)", expectedJob.Kind())
		return nil, nil //nolint:nilnil
	}

	jobRow := jobRowFromInternal(dbJobs[0])

	var actualArgs TArgs
	if err := json.Unmarshal(jobRow.EncodedArgs, &actualArgs); err != nil {
		return nil, fmt.Errorf("error unmarshaling job args: %w", err)
	}

	if opts != nil {
		if !compareJobToInsertOpts(t, jobRow, *opts, -1) {
			return nil, nil //nolint:nilnil
		}
	}

	return &river.Job[TArgs]{JobRow: jobRow, Args: actualArgs}, nil
}

// ExpectedJob is a single job to expect encapsulating job args and possible
// insertion options.
type ExpectedJob struct {
	// Args are job arguments to expect.
	Args river.JobArgs

	// Opts are options for the specific required job including insertion
	// options to assert against.
	Opts *RequireInsertedOpts
}

// RequireManyInserted is a test helper that verifies that jobs of the given
// kinds were inserted for work, failing the test if they weren't, or were
// inserted in the wrong order. If found, the inserted jobs are returned so that
// further assertions can be made against them.
//
//	job := RequireManyInserted(ctx, t, riverpgxv5.New(dbPool), []river.JobArgs{
//		&Job1Args{},
//	})
//
// This variant takes a driver that wraps a database pool. See also
// RequireManyInsertedTx which takes a transaction.
//
// A RequireInsertedOpts struct can be provided for each expected job, and if it is,
// its properties (e.g. max attempts, priority, queue name) will act as required
// assertions for the corresponding inserted job row. UniqueOpts is ignored.
//
// The assertion expects emitted jobs to have occurred exactly in the order and
// the number specified, and will fail in case this expectation isn't met. So if
// a job of a certain kind is emitted multiple times, it must be expected
// multiple times.
func RequireManyInserted[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, driver TDriver, expectedJobs []ExpectedJob) []*river.JobRow {
	tb.Helper()
	return requireManyInserted(ctx, tb, driver, expectedJobs)
}

func requireManyInserted[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, driver TDriver, expectedJobs []ExpectedJob) []*river.JobRow {
	actualArgs, err := requireManyInsertedErr[TDriver](ctx, t, driver.GetDBPool(), expectedJobs)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

// RequireManyInsertedTx is a test helper that verifies that jobs of the given
// kinds were inserted for work, failing the test if they weren't, or were
// inserted in the wrong order. If found, the inserted jobs are returned so that
// further assertions can be made against them.
//
//	job := RequireManyInsertedTx[*riverpgxv5.Driver](ctx, t, tx, []river.JobArgs{
//		&Job1Args{},
//	})
//
// This variant takes a transaction. See also RequireManyInserted which takes a
// driver that wraps a database pool.
//
// A RequireInsertedOpts struct can be provided for each expected job, and if it is,
// its properties (e.g. max attempts, priority, queue name) will act as required
// assertions for the corresponding inserted job row. UniqueOpts is ignored.
//
// The assertion expects emitted jobs to have occurred exactly in the order and
// the number specified, and will fail in case this expectation isn't met. So if
// a job of a certain kind is emitted multiple times, it must be expected
// multiple times.
func RequireManyInsertedTx[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, tx TTx, expectedJobs []ExpectedJob) []*river.JobRow {
	tb.Helper()
	return requireManyInsertedTx[TDriver](ctx, tb, tx, expectedJobs)
}

// Internal function used by the tests so that the exported version can take
// `testing.TB` instead of `testing.T`.
func requireManyInsertedTx[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, tx TTx, expectedJobs []ExpectedJob) []*river.JobRow {
	var driver TDriver
	actualArgs, err := requireManyInsertedErr[TDriver](ctx, t, driver.UnwrapTx(tx), expectedJobs)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

func requireManyInsertedErr[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, db dbtx, expectedJobs []ExpectedJob) ([]*river.JobRow, error) {
	queries := dbsqlc.New()

	expectedArgsKinds := sliceutil.Map(expectedJobs, func(j ExpectedJob) string { return j.Args.Kind() })

	// Returned ordered by ID.
	dbJobs, err := queries.JobGetByKindMany(ctx, db, expectedArgsKinds)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}

	actualArgsKinds := sliceutil.Map(dbJobs, func(j *dbsqlc.RiverJob) string { return j.Kind })

	if !slices.Equal(expectedArgsKinds, actualArgsKinds) {
		failure(t, "Inserted jobs didn't match expectation; expected: %+v, actual: %+v",
			expectedArgsKinds, actualArgsKinds)
		return nil, nil
	}

	jobRows := sliceutil.Map(dbJobs, jobRowFromInternal)

	for i, jobRow := range jobRows {
		if expectedJobs[i].Opts != nil {
			if !compareJobToInsertOpts(t, jobRow, *expectedJobs[i].Opts, i) {
				return nil, nil
			}
		}
	}

	return jobRows, nil
}

const rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"

func compareJobToInsertOpts(t testingT, jobRow *river.JobRow, expectedOpts RequireInsertedOpts, index int) bool {
	// Adds an index position for the case of multiple expected jobs. Wrapped in
	// a function so that the string is only marshaled if needed.
	positionStr := func() string {
		if index == -1 {
			return ""
		}
		return fmt.Sprintf(" (expected job slice index %d)", index)
	}

	if expectedOpts.MaxAttempts != 0 && jobRow.MaxAttempts != expectedOpts.MaxAttempts {
		failure(t, "Job with kind '%s'%s max attempts %d not equal to expected %d",
			jobRow.Kind, positionStr(), jobRow.MaxAttempts, expectedOpts.MaxAttempts)
		return false
	}

	if expectedOpts.Queue != "" && jobRow.Queue != expectedOpts.Queue {
		failure(t, "Job with kind '%s'%s queue '%s' not equal to expected '%s'",
			jobRow.Kind, positionStr(), jobRow.Queue, expectedOpts.Queue)
		return false
	}

	if expectedOpts.Priority != 0 && jobRow.Priority != expectedOpts.Priority {
		failure(t, "Job with kind '%s'%s priority %d not equal to expected %d",
			jobRow.Kind, positionStr(), jobRow.Priority, expectedOpts.Priority)
		return false
	}

	// We have to be more careful when comparing times because Postgres only
	// stores them to microsecond-level precision and the given time is likely
	// to still have nanos.
	var (
		actualScheduledAt   = jobRow.ScheduledAt.Truncate(time.Microsecond)
		expectedScheduledAt = expectedOpts.ScheduledAt.Truncate(time.Microsecond)
	)
	if expectedOpts.ScheduledAt != (time.Time{}) && !actualScheduledAt.Equal(expectedScheduledAt) {
		failure(t, "Job with kind '%s'%s scheduled at %s not equal to expected %s",
			jobRow.Kind, positionStr(), actualScheduledAt.Format(rfc3339Micro), expectedScheduledAt.Format(rfc3339Micro))
		return false
	}

	if expectedOpts.State != "" && jobRow.State != expectedOpts.State {
		failure(t, "Job with kind '%s'%s state '%s' not equal to expected '%s'",
			jobRow.Kind, positionStr(), jobRow.State, expectedOpts.State)
		return false
	}

	if len(expectedOpts.Tags) > 0 && !slices.Equal(jobRow.Tags, expectedOpts.Tags) {
		failure(t, "Job with kind '%s'%s tags attempts %+v not equal to expected %+v",
			jobRow.Kind, positionStr(), jobRow.Tags, expectedOpts.Tags)
		return false
	}

	return true
}

// failure takes a printf-style directive and is a shortcut for failing an
// assertion.
func failure(t testingT, format string, a ...any) {
	t.Log(failureString(format, a...))
	t.FailNow()
}

// failureString wraps a printf-style formatting directive with a River header
// and footer common to all failure messages.
func failureString(format string, a ...any) string {
	return "\n    River assertion failure:\n    " + fmt.Sprintf(format, a...) + "\n"
}

// WARNING!!!!!
//
// !!! When updating this function, the equivalent in `./job.go` must also be
// updated!!!
//
// This is obviously not ideal, but since JobRow is at the top-level package,
// there's no way to put a helper in a shared package that can produce one,
// which is why we have this copy/pasta. There are some potential alternatives
// to this, but none of them are great.
func jobRowFromInternal(internal *dbsqlc.RiverJob) *river.JobRow {
	tags := internal.Tags
	if tags == nil {
		tags = []string{}
	}
	return &river.JobRow{
		ID:          internal.ID,
		Attempt:     max(int(internal.Attempt), 0),
		AttemptedAt: internal.AttemptedAt,
		AttemptedBy: internal.AttemptedBy,
		CreatedAt:   internal.CreatedAt,
		EncodedArgs: internal.Args,
		Errors:      sliceutil.Map(internal.Errors, func(e dbsqlc.AttemptError) river.AttemptError { return attemptErrorFromInternal(&e) }),
		FinalizedAt: internal.FinalizedAt,
		Kind:        internal.Kind,
		MaxAttempts: max(int(internal.MaxAttempts), 0),
		Priority:    max(int(internal.Priority), 0),
		Queue:       internal.Queue,
		ScheduledAt: internal.ScheduledAt.UTC(), // TODO(brandur): Very weird this is the only place a UTC conversion happens.
		State:       river.JobState(internal.State),
		Tags:        tags,

		// metadata: internal.Metadata,
	}
}

func attemptErrorFromInternal(e *dbsqlc.AttemptError) river.AttemptError {
	return river.AttemptError{
		At:    e.At,
		Error: e.Error,
		Num:   int(e.Num),
		Trace: e.Trace,
	}
}
