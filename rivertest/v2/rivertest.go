// Package rivertest provides v2 semantics for River test assertions.
//
// The v2 package keeps familiar `RequireX` method names, but changes
// `RequireInserted` and `RequireNotInserted` to existential matching
// assertions:
//
//   - `RequireInserted` succeeds if any row matches the given criteria.
//   - `RequireNotInserted` fails only if any row matches the given criteria.
//
// Import path:
//
//	github.com/riverqueue/river/rivertest/v2
package rivertest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/riverdriver"
	rivertestv1 "github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

// RequireInsertedOpts are options for `RequireInserted` and
// `RequireNotInserted` in v2.
//
// Options are interpreted as an AND across all non-zero fields. Candidates are
// filtered at query time where practical, with additional matching in Go where
// driver-agnostic SQL filtering is not practical.
type RequireInsertedOpts struct {
	// MaxAttempts is the expected maximum number of attempts.
	MaxAttempts int

	// Priority is the expected job priority.
	Priority int

	// Queue is the expected queue.
	Queue string

	// ScheduledAt is the expected scheduled-at time.
	ScheduledAt time.Time

	// Schema is the schema where River tables are located.
	Schema string

	// State is the expected job state.
	State rivertype.JobState

	// Tags are the expected tags.
	Tags []string

	// EncodedArgs matches a job only if its serialized args bytes are exactly
	// equal.
	EncodedArgs []byte

	// MatchArgs compares expectedJob against each candidate's serialized args.
	MatchArgs bool

	// Metadata is a subset of metadata that must match.
	Metadata map[string]any
}

// ExpectedJob is used by RequireManyInserted and RequireManyInsertedTx.
type ExpectedJob = rivertestv1.ExpectedJob

// WorkResult is returned from Worker methods.
type WorkResult = rivertestv1.WorkResult

// PanicError is returned from Worker methods when panic recovery occurred.
type PanicError = rivertestv1.PanicError

// Worker provides worker testing helpers.
type Worker[T river.JobArgs, TTx any] = rivertestv1.Worker[T, TTx]

// NewWorker creates a new test Worker for testing the provided worker.
func NewWorker[T river.JobArgs, TTx any](tb testing.TB, driver riverdriver.Driver[TTx], config *river.Config, worker river.Worker[T]) *Worker[T, TTx] {
	return rivertestv1.NewWorker(tb, driver, config, worker)
}

// RequireInserted verifies that at least one job of the given kind was
// inserted and matches the provided criteria. If found, the first matching row
// by ID is returned.
func RequireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	tb.Helper()
	return requireInserted(ctx, tb, driver, expectedJob, opts)
}

func requireInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	t.Helper()
	actualJob, err := requireInsertedErr(ctx, t, driver, driver.GetExecutor(), expectedJob, opts)
	if err != nil {
		failuref(t, "Internal failure: %s", err)
	}
	return actualJob
}

// RequireInsertedTx is a transaction variant of RequireInserted.
func RequireInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	tb.Helper()
	return requireInsertedTx[TDriver](ctx, tb, tx, expectedJob, opts)
}

func requireInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) *river.Job[TArgs] {
	t.Helper()
	var driver TDriver
	actualJob, err := requireInsertedErr(ctx, t, driver, driver.UnwrapExecutor(tx), expectedJob, opts)
	if err != nil {
		failuref(t, "Internal failure: %s", err)
	}
	return actualJob
}

func requireInsertedErr[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, exec riverdriver.Executor, expectedJob TArgs, opts *RequireInsertedOpts) (*river.Job[TArgs], error) {
	t.Helper()

	opts = requireInsertedOptsOrDefault(opts)

	jobRows, err := matchingJobRows(ctx, driver, exec, expectedJob.Kind(), opts)
	if err != nil {
		return nil, err
	}

	var expectedArgsNormalized any
	if opts.MatchArgs {
		expectedArgsNormalized, err = normalizeJSONValue(expectedJob)
		if err != nil {
			return nil, fmt.Errorf("error normalizing expected job args: %w", err)
		}
	}

	for _, jobRow := range jobRows {
		matches, err := jobRowMatchesOpts(jobRow, opts, expectedArgsNormalized)
		if err != nil {
			return nil, err
		}
		if !matches {
			continue
		}

		var actualArgs TArgs
		if err := json.Unmarshal(jobRow.EncodedArgs, &actualArgs); err != nil {
			return nil, fmt.Errorf("error unmarshaling job args: %w", err)
		}

		return &river.Job[TArgs]{JobRow: jobRow, Args: actualArgs}, nil
	}

	failuref(t, "No matching jobs found with kind: %s", expectedJob.Kind())
	return nil, nil //nolint:nilnil
}

// RequireNotInserted verifies that no job of the given kind exists matching
// the provided criteria.
func RequireNotInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) {
	tb.Helper()
	requireNotInserted(ctx, tb, driver, expectedJob, opts)
}

func requireNotInserted[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, expectedJob TArgs, opts *RequireInsertedOpts) {
	t.Helper()
	err := requireNotInsertedErr(ctx, t, driver, driver.GetExecutor(), expectedJob, opts)
	if err != nil {
		failuref(t, "Internal failure: %s", err)
	}
}

// RequireNotInsertedTx is a transaction variant of RequireNotInserted.
func RequireNotInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, tb testing.TB, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) {
	tb.Helper()
	requireNotInsertedTx[TDriver](ctx, tb, tx, expectedJob, opts)
}

func requireNotInsertedTx[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, tx TTx, expectedJob TArgs, opts *RequireInsertedOpts) {
	t.Helper()
	var driver TDriver
	err := requireNotInsertedErr(ctx, t, driver, driver.UnwrapExecutor(tx), expectedJob, opts)
	if err != nil {
		failuref(t, "Internal failure: %s", err)
	}
}

func requireNotInsertedErr[TDriver riverdriver.Driver[TTx], TTx any, TArgs river.JobArgs](ctx context.Context, t testingT, driver TDriver, exec riverdriver.Executor, expectedJob TArgs, opts *RequireInsertedOpts) error {
	t.Helper()

	opts = requireInsertedOptsOrDefault(opts)

	jobRows, err := matchingJobRows(ctx, driver, exec, expectedJob.Kind(), opts)
	if err != nil {
		return err
	}

	var expectedArgsNormalized any
	if opts.MatchArgs {
		expectedArgsNormalized, err = normalizeJSONValue(expectedJob)
		if err != nil {
			return fmt.Errorf("error normalizing expected job args: %w", err)
		}
	}

	for _, jobRow := range jobRows {
		matches, err := jobRowMatchesOpts(jobRow, opts, expectedArgsNormalized)
		if err != nil {
			return err
		}
		if matches {
			failuref(t, "Job with kind '%s' and ID %d matched excluded conditions", expectedJob.Kind(), jobRow.ID)
			return nil
		}
	}

	return nil
}

// RequireManyInserted verifies strict ordered insertion of the given expected
// jobs.
func RequireManyInserted[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, driver TDriver, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	tb.Helper()
	return rivertestv1.RequireManyInserted(ctx, tb, driver, expectedJobs)
}

// RequireManyInsertedTx is a transaction variant of RequireManyInserted.
func RequireManyInsertedTx[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, tb testing.TB, tx TTx, expectedJobs []ExpectedJob) []*rivertype.JobRow {
	tb.Helper()
	return rivertestv1.RequireManyInsertedTx[TDriver](ctx, tb, tx, expectedJobs)
}

func requireInsertedOptsOrDefault(opts *RequireInsertedOpts) *RequireInsertedOpts {
	if opts == nil {
		return &RequireInsertedOpts{}
	}
	return opts
}

func matchingJobRows[TDriver riverdriver.Driver[TTx], TTx any](ctx context.Context, driver TDriver, exec riverdriver.Executor, kind string, opts *RequireInsertedOpts) ([]*rivertype.JobRow, error) {
	listParams, err := dblist.JobMakeDriverParams(ctx, matchingJobListParams(kind, opts), driver.SQLFragmentColumnIn)
	if err != nil {
		return nil, fmt.Errorf("error building job list query params: %w", err)
	}

	jobRows, err := exec.JobList(ctx, listParams)
	if err != nil {
		return nil, fmt.Errorf("error querying jobs: %w", err)
	}

	return jobRows, nil
}

func matchingJobListParams(kind string, opts *RequireInsertedOpts) *dblist.JobListParams {
	params := &dblist.JobListParams{
		Kinds:      []string{kind},
		LimitCount: math.MaxInt32,
		OrderBy:    []dblist.JobListOrderBy{{Expr: "id", Order: dblist.SortOrderAsc}},
		Schema:     opts.Schema,
	}

	if opts.Priority != 0 {
		params.Where = append(params.Where, dblist.WherePredicate{
			NamedArgs: map[string]any{"priority": opts.Priority},
			SQL:       "priority = @priority",
		})
	}

	if opts.MaxAttempts != 0 {
		params.Where = append(params.Where, dblist.WherePredicate{
			NamedArgs: map[string]any{"max_attempts": opts.MaxAttempts},
			SQL:       "max_attempts = @max_attempts",
		})
	}

	if opts.Queue != "" {
		params.Queues = []string{opts.Queue}
	}

	if opts.ScheduledAt != (time.Time{}) {
		params.Where = append(params.Where, dblist.WherePredicate{
			NamedArgs: map[string]any{"scheduled_at": opts.ScheduledAt.Truncate(time.Microsecond)},
			SQL:       "scheduled_at = @scheduled_at",
		})
	}

	if opts.State != "" {
		params.States = []rivertype.JobState{opts.State}
	}

	return params
}

func jobRowMatchesOpts(jobRow *rivertype.JobRow, opts *RequireInsertedOpts, expectedArgsNormalized any) (bool, error) {
	if !jobRowMatchesInsertOpts(jobRow, opts) {
		return false, nil
	}

	if len(opts.EncodedArgs) > 0 && !bytes.Equal(jobRow.EncodedArgs, opts.EncodedArgs) {
		return false, nil
	}

	if opts.MatchArgs {
		var actualArgsNormalized any
		if err := json.Unmarshal(jobRow.EncodedArgs, &actualArgsNormalized); err != nil {
			return false, fmt.Errorf("error unmarshaling job args: %w", err)
		}
		if !reflect.DeepEqual(actualArgsNormalized, expectedArgsNormalized) {
			return false, nil
		}
	}

	if len(opts.Metadata) > 0 {
		metadataMatches, err := metadataContainsSubset(jobRow.Metadata, opts.Metadata)
		if err != nil {
			return false, err
		}
		if !metadataMatches {
			return false, nil
		}
	}

	return true, nil
}

func jobRowMatchesInsertOpts(jobRow *rivertype.JobRow, opts *RequireInsertedOpts) bool {
	if opts.MaxAttempts != 0 && jobRow.MaxAttempts != opts.MaxAttempts {
		return false
	}

	if opts.Priority != 0 && jobRow.Priority != opts.Priority {
		return false
	}

	if opts.Queue != "" && jobRow.Queue != opts.Queue {
		return false
	}

	if opts.ScheduledAt != (time.Time{}) {
		if !jobRow.ScheduledAt.Truncate(time.Microsecond).Equal(opts.ScheduledAt.Truncate(time.Microsecond)) {
			return false
		}
	}

	if opts.State != "" && jobRow.State != opts.State {
		return false
	}

	if len(opts.Tags) > 0 && !slices.Equal(jobRow.Tags, opts.Tags) {
		return false
	}

	return true
}

func metadataContainsSubset(jobMetadata []byte, expectedMetadata map[string]any) (bool, error) {
	actualMetadata := map[string]any{}
	if len(jobMetadata) > 0 {
		if err := json.Unmarshal(jobMetadata, &actualMetadata); err != nil {
			return false, fmt.Errorf("error unmarshaling job metadata: %w", err)
		}
	}

	for key, expectedValue := range expectedMetadata {
		actualValue, ok := actualMetadata[key]
		if !ok {
			return false, nil
		}

		normalizedExpectedValue, err := normalizeJSONValue(expectedValue)
		if err != nil {
			return false, fmt.Errorf("error normalizing metadata value for key %q: %w", key, err)
		}

		if !reflect.DeepEqual(actualValue, normalizedExpectedValue) {
			return false, nil
		}
	}

	return true, nil
}

func normalizeJSONValue(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	var normalized any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, err
	}

	return normalized, nil
}

// testingT is an interface wrapper around *testing.T that's implemented by all
// of *testing.T, *testing.F, and *testing.B.
type testingT interface {
	FailNow()
	Helper()
	Log(args ...any)
}

func failuref(t testingT, format string, a ...any) {
	t.Helper()
	t.Log(failureString(format, a...))
	t.FailNow()
}

func failureString(format string, a ...any) string {
	return "\n    River assertion failure:\n    " + fmt.Sprintf(format, a...) + "\n"
}

// WorkContext returns a realistic context that can be used to test
// JobArgs.Work implementations.
func WorkContext[TTx any](ctx context.Context, client *river.Client[TTx]) context.Context {
	return rivertestv1.WorkContext(ctx, client)
}
