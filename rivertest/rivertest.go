// Package rivertest contains test assertions that can be used in a project's
// tests to verify that certain actions occurred from the main river package.
package rivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

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

// Options for RequireInserted functions including expectations for various
// queuing properties that stem from InsertOpts.
//
// Multiple properties set on this struct increase the specificity on a job to
// match, acting like an AND condition on each.
//
// In the case of RequireInserted or RequireInsertedMany, if multiple properties
// are set, a job must match all of them to be considered a successful match.
//
// In the case of RequireNotInserted, if multiple properties are set, a test
// failure is triggered only if all match. If any one of them was different, an
// inserted job isn't considered a match, and RequireNotInserted succeeds.
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

	// Schema is a non-standard Schema where River tables are located. All table
	// references in assertion queries will use this value as a prefix.
	//
	// Defaults to empty, which causes the client to look for tables using the
	// setting of Postgres `search_path`.
	Schema string

	// State is the expected state of the inserted job.
	//
	// No assertion is made if left the zero value.
	State rivertype.JobState

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
// assertions in the inserted job row.
//
// The assertion will fail if more than one job of the given kind was found
// because at that point the job to return is ambiguous. Use RequireManyInserted
// to cover that case instead.
func RequireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	tb.Helper()
	return requireInserted(ctx, tb, driver, expectedJob, opts)
}

func requireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	t.Helper()
	actualArgs, err := requireInsertedErr[TDriver](ctx, t, driver.GetExecutor(), expectedJob, opts)
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
// assertions in the inserted job row.
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
//
// Also takes a schema for testing purposes, which I haven't quite figured out
// how to get into the public API yet.
func requireInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	t.Helper()
	var driver TDriver
	actualArgs, err := requireInsertedErr[TDriver](ctx, t, driver.UnwrapExecutor(tx), expectedJob, opts)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

func requireInsertedErr[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, exec riverdriver.Executor, expectedJob TArgs, opts *RequireInsertedOpts) (*river.Job[TArgs], error) {
	t.Helper()

	var schema string
	if opts != nil {
		schema = opts.Schema
	}

	// Returned ordered by ID.
	jobRows, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
		Kind:   []string{expectedJob.Kind()},
		Schema: schema,
	})
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}

	if len(jobRows) < 1 {
		failure(t, "No jobs found with kind: %s", expectedJob.Kind())
		return nil, nil //nolint:nilnil
	}

	if len(jobRows) > 1 {
		failure(t, "More than one job found with kind: %s (you might want RequireManyInserted instead)", expectedJob.Kind())
		return nil, nil //nolint:nilnil
	}

	jobRow := jobRows[0]

	var actualArgs TArgs
	if err := json.Unmarshal(jobRow.EncodedArgs, &actualArgs); err != nil {
		return nil, fmt.Errorf("error unmarshaling job args: %w", err)
	}

	if opts != nil {
		if !compareJobToInsertOpts(t, jobRow, opts, -1, false) {
			return nil, nil //nolint:nilnil
		}
	}

	return &river.Job[TArgs]{JobRow: jobRow, Args: actualArgs}, nil
}

// RequireNotInserted is a test helper that verifies that a job of the given
// kind was not inserted for work, failing the test if one was.
//
//	job := RequireNotInserted(ctx, t, riverpgxv5.New(dbPool), &Job1Args{}, nil)
//
// This variant takes a driver that wraps a database pool. See also
// RequireNotInsertedTx which takes a transaction.
//
// A RequireInsertedOpts struct can be provided as the last argument, and if it
// is, its properties (e.g. max attempts, priority, queue name) will act as
// requirements on a found row. If any fields are set, then the test will fail
// if a job is found that matches all of them. If any property doesn't match a
// found row, the row isn't considered a match, and the assertion doesn't fail.
//
// If more rows than one were found, the assertion fails if any of them match
// the given opts.
func RequireNotInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) {
	tb.Helper()
	requireNotInserted(ctx, tb, driver, expectedJob, opts)
}

func requireNotInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) {
	t.Helper()
	err := requireNotInsertedErr[TDriver](ctx, t, driver.GetExecutor(), expectedJob, opts)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
}

// RequireNotInsertedTx is a test helper that verifies that a job of the given
// kind was not inserted for work, failing the test if one was.
//
//	job := RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &Job1Args{}, nil)
//
// This variant takes a transaction. See also RequireNotInserted which takes a
// driver that wraps a database pool.
//
// A RequireInsertedOpts struct can be provided as the last argument, and if it
// is, its properties (e.g. max attempts, priority, queue name) will act as
// requirements on a found row. If any fields are set, then the test will fail
// if a job is found that matches all of them. If any property doesn't match a
// found row, the row isn't considered a match, and the assertion doesn't fail.
//
// If more rows than one were found, the assertion fails if any of them match
// the given opts.
func RequireNotInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) {
	tb.Helper()
	requireNotInsertedTx[TDriver](ctx, tb, tx, expectedJob, opts)
}

// Internal function used by the tests so that the exported version can take
// `testing.TB` instead of `testing.T`.
//
// Also takes a schema for testing purposes, which I haven't quite figured out
// how to get into the public API yet.
func requireNotInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) {
	t.Helper()
	var driver TDriver
	err := requireNotInsertedErr[TDriver](ctx, t, driver.UnwrapExecutor(tx), expectedJob, opts)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
}

func requireNotInsertedErr[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, exec riverdriver.Executor, expectedJob TArgs, opts *RequireInsertedOpts) error {
	t.Helper()

	var schema string
	if opts != nil {
		schema = opts.Schema
	}

	// Returned ordered by ID.
	jobRows, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
		Kind:   []string{expectedJob.Kind()},
		Schema: schema,
	})
	if err != nil {
		return fmt.Errorf("error querying jobs: %w", err)
	}

	if len(jobRows) < 1 {
		return nil
	}

	if len(jobRows) > 0 && opts == nil {
		failure(t, "%d jobs found with kind, but expected to find none: %s", len(jobRows), expectedJob.Kind())
		return nil
	}

	// If any of these job rows failed assertions against opts, then the test
	// fails, but if they all succeed, then we consider no matching jobs to have
	// been inserted, and the test succeeds.
	for _, jobRow := range jobRows {
		var actualArgs TArgs
		if err := json.Unmarshal(jobRow.EncodedArgs, &actualArgs); err != nil {
			return fmt.Errorf("error unmarshaling job args: %w", err)
		}

		if opts != nil {
			if !compareJobToInsertOpts(t, jobRow, opts, -1, true) {
				return nil
			}
		}
	}

	return nil
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
// assertions for the corresponding inserted job row.
//
// The assertion expects emitted jobs to have occurred exactly in the order and
// the number specified, and will fail in case this expectation isn't met. So if
// a job of a certain kind is emitted multiple times, it must be expected
// multiple times.
//
// If RequireInsertedOpts.Schema is used, it may be set only in the first
// expectation's options (and all expectations will use that schema), or the
// same schema may be set in every expectation. Setting a non-empty schema after
// the first expectation if the first's was empty is not allowed, and neither is
// mixing and matching schemas between options.
func RequireManyInserted[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, driver TDriver, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	tb.Helper()
	return requireManyInserted(ctx, tb, driver, expectedJobs)
}

func requireManyInserted[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, driver TDriver, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	t.Helper()
	actualArgs, err := requireManyInsertedErr[TDriver](ctx, t, driver.GetExecutor(), expectedJobs)
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
// assertions for the corresponding inserted job row.
//
// The assertion expects emitted jobs to have occurred exactly in the order and
// the number specified, and will fail in case this expectation isn't met. So if
// a job of a certain kind is emitted multiple times, it must be expected
// multiple times.
//
// If RequireInsertedOpts.Schema is used, it may be set only in the first
// expectation's options (and all expectations will use that schema), or the
// same schema may be set in every expectation. Setting a non-empty schema after
// the first expectation if the first's was empty is not allowed, and neither is
// mixing and matching schemas between options.
func RequireManyInsertedTx[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, tx TTx, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	tb.Helper()
	return requireManyInsertedTx[TDriver](ctx, tb, tx, expectedJobs)
}

// Internal function used by the tests so that the exported version can take
// `testing.TB` instead of `testing.T`.
//
// Also takes a schema for testing purposes, which I haven't quite figured out
// how to get into the public API yet.
func requireManyInsertedTx[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, tx TTx, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	t.Helper()
	var driver TDriver
	actualArgs, err := requireManyInsertedErr[TDriver](ctx, t, driver.UnwrapExecutor(tx), expectedJobs)
	if err != nil {
		failure(t, "Internal failure: %s", err)
	}
	return actualArgs
}

func requireManyInsertedErr[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, t testingT, exec riverdriver.Executor, expectedJobs []ExpectedJob) ([]*rivertype.JobRow, error) {
	t.Helper()

	expectedArgsKinds := sliceutil.Map(expectedJobs, func(j ExpectedJob) string { return j.Args.Kind() })

	var schema string
	if len(expectedJobs) > 0 && expectedJobs[0].Opts != nil {
		schema = expectedJobs[0].Opts.Schema
	}

	// For simplicity (and because I can't think of any reason anyone would need
	// to do otherwise), require that if an explicit schema is being set that
	// it's the same explicit schema for all options. Callers may specify the
	// schema only once in the first expectation's options, or specify the same
	// schema for all expectations' options, but they're not allowed to set a
	// schema after the first expectation's options if it wasn't set in the
	// first, and not allowed to mix and match schemas between options.
	for i, expectedJob := range expectedJobs {
		if opts := expectedJob.Opts; opts != nil {
			if schema == "" && opts.Schema != "" ||
				schema != "" && opts.Schema != "" && schema != opts.Schema {
				return nil, fmt.Errorf(
					"when setting RequireInsertedOpts.Schema with RequireMany schema should be set only at index 0 or the same schema set for all options; "+
						"expectedJobs[0].Opts.Schema = %q, expectedJobs[%d].Opts.Schema = %q",
					schema, i, opts.Schema)
			}
		}
	}

	// Returned ordered by ID.
	jobRows, err := exec.JobGetByKindMany(ctx, &riverdriver.JobGetByKindManyParams{
		Kind:   expectedArgsKinds,
		Schema: schema,
	})
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}

	actualArgsKinds := sliceutil.Map(jobRows, func(j *rivertype.JobRow) string { return j.Kind })

	if !slices.Equal(expectedArgsKinds, actualArgsKinds) {
		failure(t, "Inserted jobs didn't match expectation; expected: %+v, actual: %+v",
			expectedArgsKinds, actualArgsKinds)
		return nil, nil
	}

	for i, jobRow := range jobRows {
		if expectedJobs[i].Opts != nil {
			if !compareJobToInsertOpts(t, jobRow, expectedJobs[i].Opts, i, false) {
				return nil, nil
			}
		}
	}

	return jobRows, nil
}

const rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"

// The last boolean indicates whether the function's being invoked for
// RequireInserted versus RequireNotInserted. Each need to perform similar
// equality checks (thereby using a single helper), but their semantics for
// succeeds versus failure are orthogonal.
//
// RequireInserted only succeeds if every property is equal. In case any is not
// equal, a set of failures is built up, and a final failure message of them all
// combined emitted at the end.
//
// RequireNotInserted succeeds if any property is not equal. In case of any
// inequality, it returns early and passes the calling test. If case of any
// equality, a set of failures is built up, and a final failure message of them
// all combined emitted at the end.
func compareJobToInsertOpts(t testingT, jobRow *rivertype.JobRow, expectedOpts *RequireInsertedOpts, index int, requireNotInserted bool) bool {
	t.Helper()

	// Adds an index position for the case of multiple expected jobs. Wrapped in
	// a function so that the string is only marshaled if needed.
	positionStr := func() string {
		if index == -1 {
			return ""
		}
		return fmt.Sprintf(" (expected job slice index %d)", index)
	}

	var failures []string

	if expectedOpts.MaxAttempts != 0 {
		if jobRow.MaxAttempts == expectedOpts.MaxAttempts {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("max attempts equal to excluded %d", expectedOpts.MaxAttempts))
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("max attempts %d not equal to expected %d", jobRow.MaxAttempts, expectedOpts.MaxAttempts))
			}
		}
	}

	if expectedOpts.Priority != 0 {
		if jobRow.Priority == expectedOpts.Priority {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("priority equal to excluded %d", expectedOpts.Priority))
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("priority %d not equal to expected %d", jobRow.Priority, expectedOpts.Priority))
			}
		}
	}

	if expectedOpts.Queue != "" {
		if jobRow.Queue == expectedOpts.Queue {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("queue equal to excluded '%s'", expectedOpts.Queue))
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("queue '%s' not equal to expected '%s'", jobRow.Queue, expectedOpts.Queue))
			}
		}
	}

	// We have to be more careful when comparing times because Postgres only
	// stores them to microsecond-level precision and the given time is likely
	// to still have nanos.
	var (
		actualScheduledAt   = jobRow.ScheduledAt.Truncate(time.Microsecond)
		expectedScheduledAt = expectedOpts.ScheduledAt.Truncate(time.Microsecond)
	)
	if expectedOpts.ScheduledAt != (time.Time{}) {
		if actualScheduledAt.Equal(expectedScheduledAt) {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("scheduled at equal to excluded %s", expectedScheduledAt.Format(rfc3339Micro))) //nolint:perfsprint
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("scheduled at %s not equal to expected %s", actualScheduledAt.Format(rfc3339Micro), expectedScheduledAt.Format(rfc3339Micro)))
			}
		}
	}

	if expectedOpts.State != "" {
		if jobRow.State == expectedOpts.State {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("state equal to excluded '%s'", expectedOpts.State))
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("state '%s' not equal to expected '%s'", jobRow.State, expectedOpts.State))
			}
		}
	}

	if len(expectedOpts.Tags) > 0 {
		if slices.Equal(jobRow.Tags, expectedOpts.Tags) {
			if requireNotInserted {
				failures = append(failures, fmt.Sprintf("tags equal to excluded %+v", expectedOpts.Tags))
			}
		} else {
			if requireNotInserted {
				return true // any one property doesn't match; assertion passes
			} else {
				failures = append(failures, fmt.Sprintf("tags %+v not equal to expected %+v", jobRow.Tags, expectedOpts.Tags))
			}
		}
	}

	if len(failures) < 1 {
		return true
	}

	// In the case of RequireInserted, we'll have built up failures for all
	// properties that failed, and are ready to emit a final failure message.
	//
	// In the case of RequireNotInserted, we'll have returned early already if
	// any property did not match (meaning a job was inserted but it overall did
	// not match all requested conditions, so the RequireNotInserted will not
	// fail). If all properties matched, then like with RequireInserted, we'll
	// have built up failures and are ready to emit a final failure message.
	failure(t, "Job with kind '%s'%s %s", jobRow.Kind, positionStr(), strings.Join(failures, ", "))
	return false
}

// failure takes a printf-style directive and is a shortcut for failing an
// assertion.
func failure(t testingT, format string, a ...any) {
	t.Helper()
	t.Log(failureString(format, a...))
	t.FailNow()
}

// failureString wraps a printf-style formatting directive with a River header
// and footer common to all failure messages.
func failureString(format string, a ...any) string {
	return "\n    River assertion failure:\n    " + fmt.Sprintf(format, a...) + "\n"
}

// WorkContext returns a realistic context that can be used to test JobArgs.Work
// implementations.
//
// In particular, adds a client to the context so that river.ClientFromContext is
// usable in the test suite.
func WorkContext[TTx any](ctx context.Context, client *river.Client[TTx]) context.Context {
	return context.WithValue(ctx, rivercommon.ContextKeyClient{}, client)
}
