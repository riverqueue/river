// Package testfactory provides low level helpers for inserting records directly
// into the database.
package testfactory

import (
	"context"
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
	Metadata    []byte
	Priority    *int
	Queue       *string
	ScheduledAt *time.Time
	State       *rivertype.JobState
	Tags        []string
}

func Job(ctx context.Context, t *testing.T, exec riverdriver.Executor, opts *JobOpts) *rivertype.JobRow {
	t.Helper()

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

	job, err := exec.JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
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
	})
	require.NoError(t, err)
	return job
}

type LeaderOpts struct {
	ElectedAt *time.Time
	ExpiresAt *time.Time
	LeaderID  *string
	Name      *string
}

func Leader(ctx context.Context, t *testing.T, exec riverdriver.Executor, opts *LeaderOpts) *riverdriver.Leader {
	t.Helper()

	leader, err := exec.LeaderInsert(ctx, &riverdriver.LeaderInsertParams{
		ElectedAt: opts.ElectedAt,
		ExpiresAt: opts.ExpiresAt,
		LeaderID:  ptrutil.ValOrDefault(opts.LeaderID, "test-client-id"),
		Name:      ptrutil.ValOrDefault(opts.Name, "default"),
		TTL:       10 * time.Second,
	})
	require.NoError(t, err)
	return leader
}

type MigrationOpts struct {
	Version *int
}

func Migration(ctx context.Context, t *testing.T, exec riverdriver.Executor, opts *MigrationOpts) *riverdriver.Migration {
	t.Helper()

	migration, err := exec.MigrationInsertMany(ctx, []int{
		ptrutil.ValOrDefaultFunc(opts.Version, nextSeq),
	})
	require.NoError(t, err)
	return migration[0]
}

var seq int64 = 1 //nolint:gochecknoglobals

func nextSeq() int {
	return int(atomic.AddInt64(&seq, 1))
}
