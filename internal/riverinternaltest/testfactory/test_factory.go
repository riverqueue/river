// Package testfactory provides low level helpers for inserting records directly
// into the database.
package testfactory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

type JobOpts struct {
	Attempt     *int
	AttemptedAt *time.Time
	CreatedAt   *time.Time
	EncodedArgs []byte
	Errors      [][]byte
	FinalizedAt *time.Time
	Kind        *string
	MaxAttempts *int
	Metadata    json.RawMessage
	Priority    *int
	Queue       *string
	ScheduledAt *time.Time
	State       *rivertype.JobState
	Tags        []string
}

func Job(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *JobOpts) *rivertype.JobRow {
	tb.Helper()

	job, err := exec.JobInsertFull(ctx, Job_Build(tb, opts))
	require.NoError(tb, err)
	return job
}

func Job_Build(tb testing.TB, opts *JobOpts) *riverdriver.JobInsertFullParams { //nolint:stylecheck
	tb.Helper()

	encodedArgs := opts.EncodedArgs
	if opts.EncodedArgs == nil {
		encodedArgs = []byte("{}")
	}

	metadata := opts.Metadata
	if opts.Metadata == nil {
		metadata = []byte("{}")
	}

	tags := opts.Tags
	if tags == nil {
		tags = []string{}
	}

	return &riverdriver.JobInsertFullParams{
		Attempt:     ptrutil.ValOrDefault(opts.Attempt, 0),
		AttemptedAt: opts.AttemptedAt,
		CreatedAt:   opts.CreatedAt,
		EncodedArgs: encodedArgs,
		Errors:      opts.Errors,
		FinalizedAt: opts.FinalizedAt,
		Kind:        ptrutil.ValOrDefault(opts.Kind, "fake_job"),
		MaxAttempts: ptrutil.ValOrDefault(opts.MaxAttempts, rivercommon.MaxAttemptsDefault),
		Metadata:    metadata,
		Priority:    ptrutil.ValOrDefault(opts.Priority, rivercommon.PriorityDefault),
		Queue:       ptrutil.ValOrDefault(opts.Queue, rivercommon.QueueDefault),
		ScheduledAt: opts.ScheduledAt,
		State:       ptrutil.ValOrDefault(opts.State, rivertype.JobStateAvailable),
		Tags:        tags,
	}
}

type LeaderOpts struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  *string
}

func Leader(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *LeaderOpts) *riverdriver.Leader {
	tb.Helper()

	leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
		ElectedAt: opts.ElectedAt,
		ExpiresAt: opts.ExpiresAt,
		LeaderID:  ptrutil.ValOrDefault(opts.LeaderID, "test-client-id"),
		TTL:       10 * time.Second,
	})
	require.NoError(tb, err)
	return leader
}

type MigrationOpts struct {
	Version *int
}

func Migration(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *MigrationOpts) *riverdriver.Migration {
	tb.Helper()

	migration, err := exec.MigrationInsertMany(ctx, []int{
		ptrutil.ValOrDefaultFunc(opts.Version, nextSeq),
	})
	require.NoError(tb, err)
	return migration[0]
}

var seq int64 = 1 //nolint:gochecknoglobals

func nextSeq() int {
	return int(atomic.AddInt64(&seq, 1))
}

type QueueOpts struct {
	Metadata  []byte
	Name      *string
	PausedAt  *time.Time
	UpdatedAt *time.Time
}

func Queue(ctx context.Context, tb testing.TB, exec riverdriver.Executor, opts *QueueOpts) *rivertype.Queue {
	tb.Helper()

	if opts == nil {
		opts = &QueueOpts{}
	}

	metadata := opts.Metadata
	if len(opts.Metadata) == 0 {
		metadata = []byte("{}")
	}

	queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
		Metadata:  metadata,
		Name:      ptrutil.ValOrDefaultFunc(opts.Name, func() string { return fmt.Sprintf("queue_%05d", nextSeq()) }),
		PausedAt:  opts.PausedAt,
		UpdatedAt: opts.UpdatedAt,
	})
	require.NoError(tb, err)
	return queue
}
