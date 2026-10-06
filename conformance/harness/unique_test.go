package harness

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// uniqueCases are unique options for which every implementation must store
// the same key and state mask. The period cases schedule the job at a fixed
// time, so the period, derived from the scheduled time, doesn't depend on
// when the scenario runs.
func uniqueCases() []struct {
	name string
	opts protocol.InsertOpts
} {
	scheduledAt := time.Date(2031, 2, 3, 4, 5, 6, 789_000_000, time.UTC)
	return []struct {
		name string
		opts protocol.InsertOpts
	}{
		{name: "by_args", opts: protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByArgs: true}}},
		{name: "by_args_exclude_kind", opts: protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByArgs: true, ExcludeKind: true}}},
		{name: "by_period", opts: protocol.InsertOpts{ScheduledAt: &scheduledAt, Unique: &protocol.UniqueOpts{ByPeriodMS: time.Hour.Milliseconds()}}},
		{name: "by_queue", opts: protocol.InsertOpts{Queue: "unique_queue", Unique: &protocol.UniqueOpts{ByQueue: true}}},
		{name: "by_state", opts: protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByState: []string{"available", "pending", "running", "scheduled"}}}},
		{name: "combined", opts: protocol.InsertOpts{
			Queue:       "unique_queue",
			ScheduledAt: &scheduledAt,
			Unique:      &protocol.UniqueOpts{ByArgs: true, ByPeriodMS: (24 * time.Hour).Milliseconds(), ByQueue: true, ByState: allStates},
		}},
	}
}

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestUnique(t *testing.T) {
	t.Parallel()

	// Each implementation stores the same unique key and state mask, byte for
	// byte and with the same SQLite storage types, and so the other's insert
	// of the same job is skipped as a duplicate. A job without unique options
	// stores neither.
	t.Run("Columns", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, writer, duplicator *Adapter) {
			uniqueColumns := func(id int64) map[string]any {
				row := env.DB.StoredRow(t, writer.Label, id)
				columns := map[string]any{}
				for _, column := range []string{"unique_key", "unique_key_type", "unique_states", "unique_states_type"} {
					columns[column] = row[column]
				}
				return columns
			}

			expected := map[string]map[string]any{}
			for _, testCase := range uniqueCases() {
				job := withOpts(echo("unique "+testCase.name, protocol.BehaviorComplete), testCase.opts)
				inserted := env.Reference.InsertJob(t, job)
				require.NotNil(t, inserted.UniqueKey, testCase.name)
				expected[testCase.name] = uniqueColumns(inserted.ID)
				env.DB.Exec(t, "DELETE FROM river_job WHERE id = $1", inserted.ID)

				inserted = writer.InsertJob(t, job)
				require.Equal(t, expected[testCase.name], uniqueColumns(inserted.ID), "%s: %s stored different unique columns", testCase.name, writer.Label)
				results := duplicator.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{job}})
				require.True(t, results[0].UniqueSkippedAsDuplicate, "%s: %s inserted a duplicate of %s's job", testCase.name, duplicator.Label, writer.Label)
				require.Equal(t, inserted, &results[0].Job, testCase.name)
			}

			notUnique := writer.InsertJob(t, echo("not unique", protocol.BehaviorComplete))
			require.Nil(t, notUnique.UniqueKey)
			require.Nil(t, notUnique.UniqueStates)
			row := env.DB.StoredRow(t, writer.Label, notUnique.ID)
			require.Nil(t, row["unique_key"])
			require.Nil(t, row["unique_states"])
		})
	})

	// A unique insert blocks on another implementation's uncommitted
	// conflicting insert and then returns the committed winner. The loser's
	// statement is observed waiting on a lock before the winner commits, so
	// a slow response can't pass for a blocked one.
	t.Run("ConcurrentConflict", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env, winner, loser *Adapter) {
			fixedScheduledAt := time.Now().Add(-time.Minute).UTC()
			for _, testCase := range []struct {
				name string
				opts protocol.InsertOpts
			}{
				{name: "by_args", opts: protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByArgs: true}}},
				{name: "by_period", opts: protocol.InsertOpts{ScheduledAt: &fixedScheduledAt, Unique: &protocol.UniqueOpts{ByPeriodMS: time.Minute.Milliseconds()}}},
				{name: "by_queue", opts: protocol.InsertOpts{Queue: "unique_queue", Unique: &protocol.UniqueOpts{ByQueue: true}}},
				{name: "by_state", opts: protocol.InsertOpts{Unique: &protocol.UniqueOpts{ByState: allStates}}},
			} {
				job := withOpts(echo("concurrent unique "+testCase.name, protocol.BehaviorComplete), testCase.opts)
				winner.TxBegin(t, testCase.name)
				won := winner.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{job}, Tx: testCase.name})[0].Job

				loser.TxBegin(t, testCase.name)
				lost := make(chan protocol.InsertResult, 1)
				lostErr := make(chan error, 1)
				go func() {
					var result protocol.InsertResult
					lostErr <- loser.Call(protocol.MethodInsert, &protocol.InsertParams{Jobs: []protocol.InsertJob{job}, Tx: testCase.name}, &result)
					lost <- result
				}()
				env.DB.WaitLockWait(t, loser)
				select {
				case err := <-lostErr:
					require.FailNowf(t, "returned early", "%s's insert returned while %s's conflict was uncommitted (%s): %v", loser.Label, winner.Label, testCase.name, err)
				default:
				}
				winner.TxEnd(t, testCase.name, true)
				select {
				case err := <-lostErr:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					require.FailNowf(t, "still blocked", "%s's insert stayed blocked after %s committed (%s)", loser.Label, winner.Label, testCase.name)
				}
				result := <-lost
				loser.TxEnd(t, testCase.name, true)
				require.True(t, result.Results[0].UniqueSkippedAsDuplicate, testCase.name)
				require.Equal(t, won, result.Results[0].Job, testCase.name)
				require.Equal(t, []*protocol.Job{&won}, env.DB.Jobs(t, "TRUE"), testCase.name)
				env.DB.Exec(t, "DELETE FROM river_job")
			}
		})
	})

	// A job inserted unique by args without its kind keeps its key when its
	// kind changes out of band. The other implementation's insertions of the
	// same args, single and batched, are skipped as duplicates and must
	// return the existing job unchanged rather than rewrite its kind to their
	// own, which would hand it to the wrong worker.
	t.Run("SkipKeepsExistingKind", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, first, skipper *Adapter) {
			job := withOpts(echo("unique skip keeps kind", protocol.BehaviorComplete), protocol.InsertOpts{
				Unique: &protocol.UniqueOpts{ByArgs: true, ExcludeKind: true},
			})
			existing := first.InsertJob(t, job)
			env.DB.SetKind(t, existing.ID, "conformance_unique_other_kind")
			existing = env.DB.MustJob(t, existing.ID)

			require.Equal(t, existing, skipper.InsertJob(t, job))
			results := skipper.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{job}})
			require.True(t, results[0].UniqueSkippedAsDuplicate)
			require.Equal(t, existing, &results[0].Job)
			require.Equal(t, []*protocol.Job{existing}, env.DB.Jobs(t, "TRUE"))
		})
	})
}
