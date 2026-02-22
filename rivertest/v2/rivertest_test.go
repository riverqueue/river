package rivertest

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivertype"
)

var testTime = time.Date(2023, 10, 30, 10, 45, 23, 123456, time.UTC) //nolint:gochecknoglobals

type Job1Args struct {
	String string `json:"string"`
}

func (Job1Args) Kind() string { return "job1" }

type Job2Args struct {
	Int int `json:"int"`
}

func (Job2Args) Kind() string { return "job2" }

func TestRequireInsertedTx_MatchesOneOfMany(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	tx := riverdbtest.TestTxPgx(ctx, t)

	_, err = riverClient.InsertTx(ctx, tx, Job1Args{String: "first"}, &river.InsertOpts{Queue: "queue_a"})
	require.NoError(t, err)
	_, err = riverClient.InsertTx(ctx, tx, Job1Args{String: "second"}, &river.InsertOpts{Queue: "queue_b"})
	require.NoError(t, err)

	opts := &RequireInsertedOpts{Queue: "queue_b"}
	job := RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &Job1Args{}, opts)
	require.Equal(t, "second", job.Args.String)
}

func TestRequireInsertedTx_FailsWhenNoMatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	bundle := struct {
		mockT *testutil.MockT
		tx    pgx.Tx
	}{
		mockT: testutil.NewMockT(t),
		tx:    riverdbtest.TestTxPgx(ctx, t),
	}

	_, err = riverClient.InsertTx(ctx, bundle.tx, Job1Args{String: "first"}, &river.InsertOpts{Queue: "queue_a"})
	require.NoError(t, err)

	_ = requireInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job1Args{}, &RequireInsertedOpts{Queue: "queue_b"})
	require.True(t, bundle.mockT.Failed)
	require.Equal(t, failureString("No matching jobs found with kind: job1")+"\n", bundle.mockT.LogOutput())
}

func TestRequireInsertedTx_MatchArgsAndMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	tx := riverdbtest.TestTxPgx(ctx, t)

	_, err = riverClient.InsertTx(ctx, tx, Job1Args{String: "first"}, &river.InsertOpts{
		Metadata: []byte(`{"tenant":"a","nested":{"enabled":true}}`),
	})
	require.NoError(t, err)
	_, err = riverClient.InsertTx(ctx, tx, Job1Args{String: "second"}, &river.InsertOpts{
		Metadata: []byte(`{"tenant":"b","nested":{"enabled":false}}`),
	})
	require.NoError(t, err)

	job := RequireInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &Job1Args{String: "first"}, &RequireInsertedOpts{
		MatchArgs: true,
		Metadata: map[string]any{
			"tenant": "a",
			"nested": map[string]any{"enabled": true},
		},
	})
	require.Equal(t, "first", job.Args.String)
}

func TestRequireNotInsertedTx_IgnoresNonMatching(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	tx := riverdbtest.TestTxPgx(ctx, t)

	_, err = riverClient.InsertTx(ctx, tx, Job1Args{String: "first"}, &river.InsertOpts{Queue: "queue_a"})
	require.NoError(t, err)

	RequireNotInsertedTx[*riverpgxv5.Driver](ctx, t, tx, &Job1Args{}, &RequireInsertedOpts{Queue: "queue_b"})
}

func TestRequireNotInsertedTx_FailsOnMatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	bundle := struct {
		mockT *testutil.MockT
		tx    pgx.Tx
	}{
		mockT: testutil.NewMockT(t),
		tx:    riverdbtest.TestTxPgx(ctx, t),
	}

	insertRes, err := riverClient.InsertTx(ctx, bundle.tx, Job2Args{Int: 123}, &river.InsertOpts{Queue: "queue_a"})
	require.NoError(t, err)

	requireNotInsertedTx[*riverpgxv5.Driver](ctx, bundle.mockT, bundle.tx, &Job2Args{}, &RequireInsertedOpts{Queue: "queue_a"})
	require.True(t, bundle.mockT.Failed)
	require.Equal(t,
		failureString("Job with kind 'job2' and ID %d matched excluded conditions", insertRes.Job.ID)+"\n",
		bundle.mockT.LogOutput())
}

func TestRequireManyInsertedTx_StillStrict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	riverClient, err := river.NewClient(riverpgxv5.New(nil), &river.Config{})
	require.NoError(t, err)

	tx := riverdbtest.TestTxPgx(ctx, t)

	_, err = riverClient.InsertManyTx(ctx, tx, []river.InsertManyParams{
		{Args: Job1Args{String: "first"}},
		{Args: Job1Args{String: "second"}},
	})
	require.NoError(t, err)

	rows := RequireManyInsertedTx[*riverpgxv5.Driver](ctx, t, tx, []ExpectedJob{
		{Args: &Job1Args{}},
		{Args: &Job1Args{}},
	})
	require.Len(t, rows, 2)
}

func TestMatchingJobListParams(t *testing.T) {
	t.Parallel()

	params := matchingJobListParams("job1", &RequireInsertedOpts{
		MaxAttempts: 7,
		Priority:    3,
		Queue:       "queue_b",
		ScheduledAt: testTime,
		Schema:      "custom_schema",
		State:       rivertype.JobStateScheduled,
		Tags:        []string{"tag1"},
		MatchArgs:   true,
		Metadata:    map[string]any{"tenant": "a"},
	})

	require.Equal(t, []string{"job1"}, params.Kinds)
	require.Equal(t, int32(math.MaxInt32), params.LimitCount)
	require.Equal(t, []dblist.JobListOrderBy{{Expr: "id", Order: dblist.SortOrderAsc}}, params.OrderBy)
	require.Equal(t, []string{"queue_b"}, params.Queues)
	require.Equal(t, []rivertype.JobState{rivertype.JobStateScheduled}, params.States)
	require.Equal(t, "custom_schema", params.Schema)

	require.Len(t, params.Where, 3)
	require.Equal(t, "priority = @priority", params.Where[0].SQL)
	require.Equal(t, map[string]any{"priority": 3}, params.Where[0].NamedArgs)
	require.Equal(t, "max_attempts = @max_attempts", params.Where[1].SQL)
	require.Equal(t, map[string]any{"max_attempts": 7}, params.Where[1].NamedArgs)
	require.Equal(t, "scheduled_at = @scheduled_at", params.Where[2].SQL)
	require.Equal(t, map[string]any{"scheduled_at": testTime.Truncate(time.Microsecond)}, params.Where[2].NamedArgs)
}
