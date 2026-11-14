package harness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestBatch(t *testing.T) {
	t.Parallel()

	// A batch fails atomically: an invalid job, or a unique key repeated
	// among jobs whose state it covers, inserts nothing.
	t.Run("Atomicity", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, actor, observer *Adapter) {
			RequireErrorCode(t, actor.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{}}, nil), protocol.CodeRejected)

			err := actor.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{
				withOpts(echo("must roll back", protocol.BehaviorComplete), protocol.InsertOpts{Tags: []string{"invalid_batch"}}),
				withOpts(echo("invalid priority", protocol.BehaviorComplete), protocol.InsertOpts{Priority: 99}),
			}}, nil)
			RequireErrorCode(t, err, protocol.CodeRejected)
			require.Empty(t, observer.List(t, protocol.ListParams{TagsAll: []string{"invalid_batch"}}).Jobs)

			// Postgres and SQLite fail a repeated key differently, so only
			// the failure and its atomicity are compared.
			repeated := withOpts(echo("repeated unique key", protocol.BehaviorComplete), protocol.InsertOpts{
				Tags: []string{"repeated_key_batch"}, Unique: &protocol.UniqueOpts{ByArgs: true},
			})
			require.Error(t, actor.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{repeated, repeated}}, nil),
				"%s inserted a batch repeating a unique key", actor.Label)
			require.Empty(t, observer.List(t, protocol.ListParams{TagsAll: []string{"repeated_key_batch"}}).Jobs)

			// Excluding the kind needs arguments, queue, or period in the key.
			err = actor.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{
				withOpts(echo("unique without kind", protocol.BehaviorComplete), protocol.InsertOpts{Unique: &protocol.UniqueOpts{ExcludeKind: true}}),
			}}, nil)
			RequireErrorCode(t, err, protocol.CodeRejected)
		})
	})

	// A large batch returns its results in input order.
	t.Run("LargeBatchOrder", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			const batchSize = 6_000
			for _, actor := range []*Adapter{env.Reference, env.Candidate} {
				jobs := make([]protocol.InsertJob, batchSize)
				for i := range jobs {
					jobs[i] = withOpts(echo(fmt.Sprintf("large batch %s %d", actor.Label, i), protocol.BehaviorComplete),
						protocol.InsertOpts{Metadata: metadata(t, map[string]any{"batch_index": i})})
				}
				results := actor.Insert(t, protocol.InsertParams{Jobs: jobs})
				for i, result := range results {
					require.InDelta(t, i, result.Job.Metadata["batch_index"], 0, "%s result %d is out of input order", actor.Label, i)
				}
			}
		})
	})

	// A batch's results come back in input order, a duplicate of another
	// implementation's unique job is reported as such, and the other
	// implementation reads every inserted job alike.
	t.Run("Results", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, actor, observer *Adapter) {
			unique := withOpts(echo("batch duplicate", protocol.BehaviorComplete), protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByArgs: true}})
			existing := observer.InsertJob(t, unique)

			results := actor.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{
				withOpts(echo("batch first", protocol.BehaviorComplete), protocol.InsertOpts{
					Metadata: metadata(t, map[string]any{"batch_index": 0}), Priority: 2, Tags: []string{"typed_batch"},
				}),
				unique,
				withOpts(echo("batch pending", protocol.BehaviorComplete), protocol.InsertOpts{Pending: true, Tags: []string{"typed_batch"}}),
			}})
			for _, result := range results {
				require.NotNil(t, result.Job.Errors)
				require.Empty(t, result.Job.Errors)
			}
			require.False(t, results[0].UniqueSkippedAsDuplicate)
			require.InDelta(t, 0, results[0].Job.Metadata["batch_index"], 0)
			require.Equal(t, 2, results[0].Job.Priority)
			require.True(t, results[1].UniqueSkippedAsDuplicate)
			require.Equal(t, existing, &results[1].Job)
			require.False(t, results[2].UniqueSkippedAsDuplicate)
			require.Equal(t, "pending", results[2].Job.State)
			for _, result := range results {
				require.Equal(t, &result.Job, listOne(t, observer, result.Job.ID))
			}
		})
	})
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestTransactions(t *testing.T) {
	t.Parallel()

	// A job cancelled in one implementation's transaction stays available to
	// the other until the transaction commits.
	t.Run("Cancel", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, canceller, inserter *Adapter) {
			job := inserter.InsertJob(t, echo("transactional cancellation", protocol.BehaviorComplete))
			canceller.TxBegin(t, "cancel")
			cancelled := canceller.Cancel(t, protocol.JobParams{ID: job.ID, Tx: "cancel"})
			require.Equal(t, "cancelled", cancelled.State)
			require.NotNil(t, cancelled.FinalizedAt)
			require.Equal(t, "available", listOne(t, inserter, job.ID).State)

			canceller.TxEnd(t, "cancel", true)
			committed := listOne(t, inserter, job.ID)
			require.Equal(t, "cancelled", committed.State)
			require.NotNil(t, committed.FinalizedAt)
		})
	})

	// Jobs inserted, cancelled, and retried in one implementation's
	// transaction are visible inside it, invisible to the other until
	// commit, and never visible after rollback.
	t.Run("Visibility", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, actor, observer *Adapter) {
			// An invalid batch inside a transaction fails without partially
			// inserting. It aborts a Postgres transaction, which can then
			// only roll back, while SQLite's survives to commit.
			actor.TxBegin(t, "invalid")
			err := actor.Call(protocol.MethodInsert, &protocol.InsertParams{Tx: "invalid", Jobs: []protocol.InsertJob{
				withOpts(echo("must not partially commit", protocol.BehaviorComplete), protocol.InsertOpts{Tags: []string{"invalid"}}),
				withOpts(echo("invalid", protocol.BehaviorComplete), protocol.InsertOpts{Priority: 99}),
			}}, nil)
			RequireErrorCode(t, err, protocol.CodeRejected)
			actor.TxEnd(t, "invalid", env.Driver == DriverSQLite)
			require.Empty(t, observer.List(t, protocol.ListParams{TagsAll: []string{"invalid"}}).Jobs)

			actor.TxBegin(t, "empty")
			RequireErrorCode(t, actor.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{}, Tx: "empty"}, nil), protocol.CodeRejected)
			actor.TxEnd(t, "empty", env.Driver == DriverSQLite)

			for _, commit := range []bool{false, true} {
				tx := fmt.Sprintf("visibility_%t", commit)
				actor.TxBegin(t, tx)
				inserted := actor.Insert(t, protocol.InsertParams{Tx: tx, Jobs: []protocol.InsertJob{
					withOpts(echo(tx+" single", protocol.BehaviorComplete), protocol.InsertOpts{Tags: []string{tx}}),
				}})[0].Job
				batch := actor.Insert(t, protocol.InsertParams{Tx: tx, Jobs: []protocol.InsertJob{
					withOpts(echo(tx+" first", protocol.BehaviorComplete), protocol.InsertOpts{Metadata: metadata(t, map[string]any{"batch_index": 0}), Priority: 2, Tags: []string{tx}}),
					withOpts(echo(tx+" second", protocol.BehaviorComplete), protocol.InsertOpts{Metadata: metadata(t, map[string]any{"batch_index": 1}), Priority: 3, Tags: []string{tx}}),
				}})
				require.InDelta(t, 0, batch[0].Job.Metadata["batch_index"], 0)
				require.InDelta(t, 1, batch[1].Job.Metadata["batch_index"], 0)

				inTx := actor.List(t, protocol.ListParams{IDs: []int64{inserted.ID}, Tx: tx}).Jobs
				require.Equal(t, []protocol.Job{inserted}, inTx)
				cancelled := actor.Cancel(t, protocol.JobParams{ID: inserted.ID, Tx: tx})
				require.Equal(t, "cancelled", cancelled.State)
				retried := actor.Retry(t, protocol.JobParams{ID: inserted.ID, Tx: tx})
				require.Equal(t, "available", retried.State)
				require.Equal(t, []protocol.Job{*retried}, actor.List(t, protocol.ListParams{IDs: []int64{inserted.ID}, Tx: tx}).Jobs)
				require.Empty(t, observer.List(t, protocol.ListParams{TagsAll: []string{tx}}).Jobs)

				actor.TxEnd(t, tx, commit)
				listed := observer.List(t, protocol.ListParams{OrderBy: "id", TagsAll: []string{tx}}).Jobs
				if !commit {
					require.Empty(t, listed)
					continue
				}
				require.Equal(t, []int64{inserted.ID, batch[0].Job.ID, batch[1].Job.ID}, listedIDs(listed))
				require.Equal(t, *retried, listed[0])
				require.Equal(t, []int{2, 3}, []int{listed[1].Priority, listed[2].Priority})
			}
		})
	})
}
