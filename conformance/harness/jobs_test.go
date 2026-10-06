package harness

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

// jobRowText is written into every JSON column the row scenarios compare. It
// holds characters JSON encoders escape differently (Go escapes `<`, `>`,
// `&`, U+2028, and U+2029), which is fine as long as every writer stores the
// same string.
const jobRowText = "a<b>&c d é"

//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestJobs(t *testing.T) {
	t.Parallel()

	// Retrying another implementation's finalized jobs: a retry makes the job
	// available again, and when it has used every attempt raises
	// max_attempts by one so it gets another.
	t.Run("ExhaustedRetry", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, finisher, retrier *Adapter) {
			for _, testCase := range []struct {
				expectedMaxAttempts int
				finalState          string
				job                 protocol.InsertJob
			}{
				{
					expectedMaxAttempts: 2,
					finalState:          "discarded",
					job:                 withOpts(echo("exhausted", protocol.BehaviorError), protocol.InsertOpts{MaxAttempts: 1}),
				},
				{
					expectedMaxAttempts: 3,
					finalState:          "cancelled",
					job:                 withOpts(echo("attempts left", protocol.BehaviorCancel), protocol.InsertOpts{MaxAttempts: 3}),
				},
			} {
				inserted := finisher.InsertJob(t, testCase.job)
				finished := workOne(t, env, finisher, "exhausted-retry", inserted.ID)
				require.Equal(t, testCase.finalState, finished.State)
				require.Equal(t, 1, finished.Attempt)

				retried := retrier.Retry(t, protocol.JobParams{ID: inserted.ID})
				require.Equal(t, env.DB.MustJob(t, inserted.ID), retried)
				require.Equal(t, listOne(t, finisher, inserted.ID), retried)
				require.Equal(t, "available", retried.State, testCase.finalState)
				require.Equal(t, 1, retried.Attempt, testCase.finalState)
				require.Len(t, retried.Errors, 1, testCase.finalState)
				require.Nil(t, retried.FinalizedAt, testCase.finalState)
				require.Equal(t, testCase.expectedMaxAttempts, retried.MaxAttempts, testCase.finalState)
				require.True(t, retried.ScheduledAt.After(*finished.FinalizedAt), "%s job retried without rescheduling", testCase.finalState)
			}
		})
	})

	// A worker's completion never overwrites a terminal state written while
	// it ran, and the output it records merges into the external metadata.
	t.Run("ExternalCompletionRace", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, worker, inserter *Adapter) {
			worker.Start(t, protocol.StartParams{ClientID: "completion-race", MaxWorkers: 1})
			for i, testCase := range []struct {
				behavior      string
				externalState string
			}{
				{behavior: protocol.BehaviorBarrierOutput, externalState: "completed"},
				{behavior: protocol.BehaviorBarrierOutput, externalState: "discarded"},
				{behavior: protocol.BehaviorBarrierWait, externalState: "completed"},
			} {
				barrier := fmt.Sprintf("completion-race-%d", i)
				inserted := inserter.InsertJob(t, echo(barrier, testCase.behavior))
				env.DB.WaitJob(t, inserted.ID, workWait, "running")

				// Recent, so the leader's job cleaner doesn't delete the job.
				finalizedAt := time.Now().UTC().Truncate(time.Millisecond)
				errorsJSON := `[]`
				if testCase.externalState == "discarded" {
					errorsJSON = fmt.Sprintf(`[{"at":%q,"attempt":1,"error":"external discard","trace":"external trace"}]`, finalizedAt.Format(time.RFC3339Nano))
				}
				externalMetadata := fmt.Sprintf(`{"external":%q,"shared":"external"}`, testCase.externalState)
				if env.Driver == DriverPostgres {
					env.DB.Exec(t, `UPDATE river_job SET state = $1::river_job_state, finalized_at = $2,
						errors = ARRAY(SELECT jsonb_array_elements($3::jsonb)), metadata = metadata || $4::jsonb WHERE id = $5`,
						testCase.externalState, finalizedAt, errorsJSON, externalMetadata, inserted.ID)
				} else {
					env.DB.Exec(t, `UPDATE river_job SET state = ?, finalized_at = ?, errors = CASE WHEN ? = '[]' THEN NULL ELSE jsonb(?) END,
						metadata = jsonb_patch(metadata, ?) WHERE id = ?`,
						testCase.externalState, finalizedAt.Format(sqliteTimeLayout), errorsJSON, errorsJSON, externalMetadata, inserted.ID)
				}
				external := env.DB.MustJob(t, inserted.ID)

				worker.Release(t, barrier)
				stats := worker.WaitStats(t, fmt.Sprintf("the worker finishing case %d", i), func(stats *protocol.StatsResult) bool { return len(stats.Events) >= i+1 })
				require.Len(t, stats.Events, i+1, "events: %v", stats.Events)

				raced := env.DB.MustJob(t, inserted.ID)
				require.Equal(t, testCase.externalState, raced.State)
				require.Equal(t, external.FinalizedAt, raced.FinalizedAt)
				require.Equal(t, external.Errors, raced.Errors)
				require.Equal(t, testCase.externalState, raced.Metadata["external"])
				require.Equal(t, "external", raced.Metadata["shared"])
				if testCase.behavior == protocol.BehaviorBarrierOutput {
					require.Equal(t, map[string]any{"race": "worker"}, raced.Metadata["output"])
				} else {
					require.NotContains(t, raced.Metadata, "output")
				}
			}
			require.Equal(t, []string{"job_completed", "job_failed", "job_completed"}, worker.Stats(t).Events)
		})
	})

	// The rows each implementation stores when it inserts, cancels, and
	// retries the same jobs hold the same values in the same formats.
	t.Run("InsertRows", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			opts := protocol.InsertOpts{
				MaxAttempts: 7,
				Metadata: metadata(t, map[string]any{
					"nested": map[string]any{"alpha": []any{1, "<&>", nil}, "zeta": jobRowText},
					"note":   jobRowText,
					"number": 1.5,
				}),
				Priority:    2,
				ScheduledAt: new(time.Date(2031, 2, 3, 4, 5, 6, 789_000_000, time.UTC)),
				Tags:        []string{"job-rows", "tag_2"},
			}
			operations := []string{"defaults", "insert", "pending", "batch", "cancel", "retry"}
			write := func(env *Env, writer *Adapter) map[string]StoredRow {
				single := writer.InsertJob(t, echo(jobRowText, protocol.BehaviorComplete))
				inserted := writer.InsertJob(t, withOpts(echo(jobRowText, protocol.BehaviorComplete), opts))
				pending := writer.InsertJob(t, withOpts(echo(jobRowText, protocol.BehaviorComplete), protocol.InsertOpts{Pending: true}))
				batch := writer.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{
					withOpts(echo(jobRowText+" batch", protocol.BehaviorComplete), opts),
					withOpts(echo(jobRowText+" cancel", protocol.BehaviorComplete), opts),
					withOpts(echo(jobRowText+" retry", protocol.BehaviorComplete), opts),
				}})
				writer.Cancel(t, protocol.JobParams{ID: batch[1].Job.ID})
				writer.Cancel(t, protocol.JobParams{ID: batch[2].Job.ID})
				writer.Retry(t, protocol.JobParams{ID: batch[2].Job.ID})

				rows := map[string]StoredRow{}
				for i, id := range []int64{single.ID, inserted.ID, pending.ID, batch[0].Job.ID, batch[1].Job.ID, batch[2].Job.ID} {
					rows[operations[i]] = env.DB.StoredRow(t, writer.Label, id)
				}
				return rows
			}

			reference := write(env, env.Reference)
			other := env.Another(t)
			candidate := write(other, other.Candidate)
			for _, operation := range operations {
				RequireEquivalentRows(t, operation, reference[operation], candidate[operation])
			}
		})
	})

	// One implementation inserts a job and the other reads and works it, and
	// both read the result alike.
	t.Run("InsertThenWork", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			inserted := inserter.InsertJob(t, echo("insert then work", protocol.BehaviorComplete))
			require.Equal(t, "available", inserted.State)
			require.Equal(t, protocol.KindEcho, inserted.Kind)
			require.Zero(t, inserted.Attempt)
			require.Empty(t, inserted.AttemptedBy)
			require.Equal(t, 25, inserted.MaxAttempts)
			require.Equal(t, map[string]any{"behavior": "", "duration_ms": float64(0), "message": "insert then work"}, inserted.Args)
			require.Equal(t, env.DB.MustJob(t, inserted.ID), inserted)
			require.Equal(t, inserted, listOne(t, worker, inserted.ID))

			worked := workOne(t, env, worker, "insert-then-work", inserted.ID)
			requireWorkedOnceBy(t, worked, "insert-then-work")
			require.NotNil(t, worked.AttemptedAt)
			require.NotNil(t, worked.FinalizedAt)
			require.Equal(t, worked, listOne(t, inserter, inserted.ID))
			require.Equal(t, worked, listOne(t, worker, inserted.ID))
		})
	})

	// Jobs one implementation writes are cancelled and retried by the other,
	// and both read every step alike.
	t.Run("JobControl", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, writer, reader *Adapter) {
			inserted := writer.InsertJob(t, withOpts(echo("job control", protocol.BehaviorComplete), protocol.InsertOpts{
				Metadata: metadata(t, map[string]any{"writer": writer.Label}),
				Priority: 3,
				Tags:     []string{"all_jobs", "job_control"},
			}))
			listParams := protocol.ListParams{IDs: []int64{inserted.ID}, TagsAll: []string{"all_jobs", "job_control"}}
			require.Equal(t, []protocol.Job{*inserted}, reader.List(t, listParams).Jobs)
			require.Equal(t, writer.List(t, listParams), reader.List(t, listParams))

			cancelled := writer.Cancel(t, protocol.JobParams{ID: inserted.ID})
			require.Equal(t, "cancelled", cancelled.State)
			require.NotNil(t, cancelled.FinalizedAt)
			require.Equal(t, cancelled, listOne(t, reader, inserted.ID))

			retried := reader.Retry(t, protocol.JobParams{ID: inserted.ID})
			require.Equal(t, "available", retried.State)
			require.Nil(t, retried.FinalizedAt)
			require.Equal(t, retried, listOne(t, writer, inserted.ID))
			require.Equal(t, retried, env.DB.MustJob(t, inserted.ID))
		})
	})

	// Job IDs beyond JavaScript's safe integer range are read, listed, paged,
	// and cancelled exactly.
	t.Run("LargeIDs", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, first, second *Adapter) {
			const firstUnsafeID int64 = 9_007_199_254_740_993
			jobIDs := []int64{firstUnsafeID, firstUnsafeID + 1}
			for _, id := range jobIDs {
				require.Equal(t, id, env.DB.InsertRaw(t, RawJob{ID: id}))
				require.Equal(t, id, listOne(t, first, id).ID)
				require.Equal(t, id, listOne(t, second, id).ID)
			}

			page := func(adapter *Adapter, after string) *protocol.ListResult {
				return adapter.List(t, protocol.ListParams{After: after, IDs: jobIDs, Limit: 1})
			}
			firstPage, secondFirstPage := page(first, ""), page(second, "")
			require.Equal(t, firstPage, secondFirstPage)
			require.Equal(t, jobIDs[:1], listedIDs(firstPage.Jobs))
			require.NotNil(t, firstPage.Cursor)
			nextPage := page(second, *firstPage.Cursor)
			require.Equal(t, page(first, *secondFirstPage.Cursor), nextPage)
			require.Equal(t, jobIDs[1:], listedIDs(nextPage.Jobs))

			cancelled := second.Cancel(t, protocol.JobParams{ID: jobIDs[0]})
			require.Equal(t, jobIDs[0], cancelled.ID)
			require.Equal(t, "cancelled", cancelled.State)
			require.Equal(t, cancelled, listOne(t, first, jobIDs[0]))
		})
	})

	// Values beyond a float64's range or precision in metadata survive another
	// implementation's runtime writing the job's metadata.
	t.Run("LargeNumbers", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, nil, func(t *testing.T, env *Env, controller, worker *Adapter) {
			const numbers = `{"negative":-9223372036854775808,"big_integer":123456789012345678901234567890,` +
				`"beyond_float":1e400,"long_decimal":0.1000000000000000055511151231257827}`
			insert := func(behavior string) int64 {
				return env.DB.InsertRaw(t, RawJob{Args: &protocol.Args{Behavior: behavior, Message: "large numbers"}, Metadata: numbers})
			}
			numberValues := func(id int64) map[string]any {
				values, ok := env.DB.StoredRow(t, "the harness", id)["metadata"].(map[string]any)
				require.True(t, ok)
				for key := range values {
					if !strings.Contains(numbers, `"`+key+`"`) {
						delete(values, key)
					}
				}
				return values
			}
			output, snoozed, cancelled := insert(protocol.BehaviorOutput), insert(protocol.BehaviorSnoozeOnce), insert(protocol.BehaviorCooperativeCancel)
			before := numberValues(output)
			require.Equal(t, exactNumber("123456789012345678901234567890"), before["big_integer"])
			require.Equal(t, exactNumber("-9223372036854775808"), before["negative"])
			require.Equal(t, exactNumber("1000000000000000055511151231257827/10000000000000000000000000000000000"), before["long_decimal"])

			worker.Start(t, protocol.StartParams{ClientID: "large-numbers", MaxWorkers: 3})
			env.DB.WaitJob(t, cancelled, workWait, "running")
			// The job's metadata can't be decoded into a float64, so the
			// result is ignored.
			require.NoError(t, controller.Call(protocol.MethodCancel, &protocol.JobParams{ID: cancelled}, nil))
			for _, id := range []int64{output, snoozed, cancelled} {
				env.DB.WaitJob(t, id, workWait)
				require.Equal(t, before, numberValues(id), "job %d", id)
			}
		})
	})

	// Every column of a row written outside River reads the same in both
	// implementations.
	t.Run("RowRoundTrip", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			attemptedAt := time.Date(2026, 1, 2, 3, 4, 6, 123_456_000, time.UTC)
			createdAt := time.Date(2026, 1, 2, 3, 4, 5, 678_900_000, time.UTC)
			finalizedAt := time.Date(2026, 1, 2, 3, 4, 7, 1_000, time.UTC)
			scheduledAt := time.Date(2026, 1, 2, 3, 4, 5, 999_999_000, time.UTC)
			const (
				args      = `{"nested":{"enabled":true},"values":[1,"two",null]}`
				errorJSON = `{"at":"2026-01-02T03:04:06.123456Z","attempt":3,"error":"worker failed: escaped \"detail\"","trace":"frame one\nframe two"}`
				meta      = `{"output":{"ok":true},"river:rescue_count":2,"user":"metadata"}`
			)
			var id int64
			if env.Driver == DriverPostgres {
				env.DB.QueryRow(t, `INSERT INTO river_job (args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at,
					kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
					VALUES ($1, 3, $2, ARRAY['go-client','candidate-client'], $3, ARRAY[$4::jsonb], $5, 'conformance_full_row', 4,
					$6, 2, 'priority_jobs', $7, 'discarded', ARRAY['alpha_tag','beta_tag'], decode(repeat('ab', 32), 'hex'), B'11110101')
					RETURNING id`, []any{args, attemptedAt, createdAt, errorJSON, finalizedAt, meta, scheduledAt}, &id)
			} else {
				// SQLite stores milliseconds.
				attemptedAt, createdAt = attemptedAt.Truncate(time.Millisecond), createdAt.Truncate(time.Millisecond)
				finalizedAt, scheduledAt = finalizedAt.Truncate(time.Millisecond), scheduledAt.Truncate(time.Millisecond)
				env.DB.QueryRow(t, `INSERT INTO river_job (args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at,
					kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states)
					VALUES (jsonb(?), 3, ?, jsonb('["go-client","candidate-client"]'), ?, jsonb('[' || ? || ']'), ?, 'conformance_full_row', 4,
					jsonb(?), 2, 'priority_jobs', ?, 'discarded', jsonb('["alpha_tag","beta_tag"]'), unhex(?), 245)
					RETURNING id`, []any{
					args, attemptedAt.Format(sqliteTimeLayout), createdAt.Format(sqliteTimeLayout), errorJSON,
					finalizedAt.Format(sqliteTimeLayout), meta, scheduledAt.Format(sqliteTimeLayout), strings.Repeat("ab", 32),
				}, &id)
			}

			expected := &protocol.Job{
				Args:        map[string]any{"nested": map[string]any{"enabled": true}, "values": []any{float64(1), "two", nil}},
				Attempt:     3,
				AttemptedAt: &attemptedAt,
				AttemptedBy: []string{"go-client", "candidate-client"},
				CreatedAt:   createdAt,
				Errors: []protocol.AttemptError{{
					At:      time.Date(2026, 1, 2, 3, 4, 6, 123_456_000, time.UTC),
					Attempt: 3,
					Error:   `worker failed: escaped "detail"`,
					Trace:   "frame one\nframe two",
				}},
				FinalizedAt:  &finalizedAt,
				ID:           id,
				Kind:         "conformance_full_row",
				MaxAttempts:  4,
				Metadata:     map[string]any{"output": map[string]any{"ok": true}, "river:rescue_count": float64(2), "user": "metadata"},
				Priority:     2,
				Queue:        "priority_jobs",
				ScheduledAt:  scheduledAt,
				State:        "discarded",
				Tags:         []string{"alpha_tag", "beta_tag"},
				UniqueKey:    new(strings.Repeat("ab", 32)),
				UniqueStates: []string{"available", "completed", "pending", "retryable", "running", "scheduled"},
			}
			require.Equal(t, expected, env.DB.MustJob(t, id))
			require.Equal(t, expected, listOne(t, env.Reference, id))
			require.Equal(t, expected, listOne(t, env.Candidate, id))
		})
	})

	// River stores times in SQLite as millisecond text and compares them as
	// text, so every writer rounds as Go does: to the nearest millisecond,
	// halves up (toward the future even before 1970), carrying into the
	// second. Both implementations read every row alike and list them in
	// time order.
	t.Run("SQLiteTimestamps", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, &EnvOpts{Drivers: []string{DriverSQLite}}, func(t *testing.T, env *Env) {
			testCases := []struct {
				expected    string
				expectedRaw string
				input       string
			}{
				{expected: "2026-01-02T03:04:05.123Z", expectedRaw: "2026-01-02 03:04:05.123", input: "2026-01-02T03:04:05.1234Z"},
				{expected: "2026-01-02T03:04:05.124Z", expectedRaw: "2026-01-02 03:04:05.124", input: "2026-01-02T03:04:05.1238Z"},
				{expected: "2026-01-02T03:04:05Z", expectedRaw: "2026-01-02 03:04:05.000", input: "2026-01-02T03:04:05.0004999Z"},
				{expected: "2026-01-02T03:04:05.001Z", expectedRaw: "2026-01-02 03:04:05.001", input: "2026-01-02T03:04:05.0005Z"},
				{expected: "2026-01-02T03:04:06Z", expectedRaw: "2026-01-02 03:04:06.000", input: "2026-01-02T03:04:05.9995Z"},
				{expected: "1970-01-01T00:00:00Z", expectedRaw: "1970-01-01 00:00:00.000", input: "1969-12-31T23:59:59.9995Z"},
				{expected: "1969-12-31T23:59:59.998Z", expectedRaw: "1969-12-31 23:59:59.998", input: "1969-12-31T23:59:59.9975Z"},
			}
			type insertedJob struct {
				id          int64
				scheduledAt time.Time
			}
			inserted := make([]insertedJob, 0, 2*len(testCases))
			for _, writer := range []*Adapter{env.Reference, env.Candidate} {
				for _, testCase := range testCases {
					input, err := time.Parse(time.RFC3339Nano, testCase.input)
					require.NoError(t, err)
					expected, err := time.Parse(time.RFC3339Nano, testCase.expected)
					require.NoError(t, err)

					job := writer.InsertJob(t, withOpts(echo("timestamp "+testCase.input, protocol.BehaviorComplete),
						protocol.InsertOpts{ScheduledAt: &input, Tags: []string{"sqlite_timestamps"}}))
					require.Equal(t, expected, job.ScheduledAt, "%s writing %s", writer.Label, testCase.input)
					for _, reader := range []*Adapter{env.Reference, env.Candidate} {
						require.Equal(t, expected, listOne(t, reader, job.ID).ScheduledAt, "%s reading %s's %s", reader.Label, writer.Label, testCase.input)
					}
					var raw string
					env.DB.QueryRow(t, "SELECT CAST(scheduled_at AS TEXT) FROM river_job WHERE id = ?", []any{job.ID}, &raw)
					require.Equal(t, testCase.expectedRaw, raw, "%s's stored %s", writer.Label, testCase.input)
					inserted = append(inserted, insertedJob{id: job.ID, scheduledAt: expected})
				}
			}
			slices.SortStableFunc(inserted, func(a, b insertedJob) int {
				return cmp.Or(a.scheduledAt.Compare(b.scheduledAt), cmp.Compare(a.id, b.id))
			})
			expectedOrder := make([]int64, len(inserted))
			for i, job := range inserted {
				expectedOrder[i] = job.id
			}
			for _, reader := range []*Adapter{env.Reference, env.Candidate} {
				listed := reader.List(t, protocol.ListParams{
					Limit: len(expectedOrder), OrderBy: "scheduled_at", States: []string{"scheduled"}, TagsAll: []string{"sqlite_timestamps"},
				})
				require.Equal(t, expectedOrder, listedIDs(listed.Jobs), "%s listing by scheduled_at", reader.Label)
			}
		})
	})

	// Attempt counts wider than 16 bits, which River Go keeps as `int`s and
	// stores natively on SQLite, are inserted, listed, worked, and retried
	// without being rewritten. Postgres's columns are 16 bits, and River
	// Go's drivers clamp a wider max_attempts to 32,767 on insert instead of
	// failing it.
	t.Run("WideIntegers", func(t *testing.T) {
		t.Parallel()

		EachDirection(t, &EnvOpts{Drivers: []string{DriverPostgres}}, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			inserted := inserter.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{
				withOpts(echo("clamped max attempts", protocol.BehaviorComplete), protocol.InsertOpts{MaxAttempts: 40_000}),
				withOpts(echo("narrow max attempts", protocol.BehaviorComplete), protocol.InsertOpts{MaxAttempts: 7}),
			}})
			require.Len(t, inserted, 2)
			for i, expectedMaxAttempts := range []int{32_767, 7} {
				id := inserted[i].Job.ID
				require.Equal(t, expectedMaxAttempts, inserted[i].Job.MaxAttempts, "%s's insert result", inserter.Label)
				stored := env.DB.MustJob(t, id)
				require.Equal(t, expectedMaxAttempts, stored.MaxAttempts, "%s's stored row", inserter.Label)
				for _, reader := range []*Adapter{inserter, worker} {
					require.Equal(t, stored, listOne(t, reader, id), "%s listing job %d", reader.Label, id)
				}
			}
			worked := workOne(t, env, worker, "wide-integers", inserted[0].Job.ID)
			require.Equal(t, "completed", worked.State)
			require.Equal(t, 1, worked.Attempt)
			require.Equal(t, 32_767, worked.MaxAttempts)
		})

		EachDirection(t, &EnvOpts{Drivers: []string{DriverSQLite}}, func(t *testing.T, env *Env, inserter, worker *Adapter) {
			requireListed := func(id int64) *protocol.Job {
				t.Helper()

				stored := env.DB.MustJob(t, id)
				for _, reader := range []*Adapter{inserter, worker} {
					require.Equal(t, stored, listOne(t, reader, id), "%s listing job %d", reader.Label, id)
				}
				return stored
			}
			requireErrorAttempts := func(job *protocol.Job, expected ...int) {
				t.Helper()

				attempts := make([]int, len(job.Errors))
				for i, attemptError := range job.Errors {
					attempts[i] = attemptError.Attempt
				}
				require.Equal(t, expected, attempts, "job %d's attempt errors", job.ID)
			}

			// A wide max_attempts one implementation inserts is listed and
			// worked by the other.
			inserted := inserter.InsertJob(t, withOpts(echo("wide max attempts", protocol.BehaviorComplete), protocol.InsertOpts{MaxAttempts: 40_000}))
			require.Equal(t, 40_000, inserted.MaxAttempts)
			require.Equal(t, 40_000, requireListed(inserted.ID).MaxAttempts)
			worked := workOne(t, env, worker, "wide-integers", inserted.ID)
			require.Equal(t, "completed", worked.State)
			require.Equal(t, 1, worked.Attempt)
			require.Equal(t, 40_000, worked.MaxAttempts)
			require.Equal(t, worked, requireListed(inserted.ID))

			// Rows already beyond 32,767 attempts are worked to completion and
			// to an error, keeping every attempt count exact.
			completing := env.DB.InsertRaw(t, RawJob{
				Args: &protocol.Args{Message: "wide attempt complete"}, Attempt: 40_000, MaxAttempts: 40_001,
			})
			erroring := env.DB.InsertRaw(t, RawJob{
				Args: &protocol.Args{Behavior: protocol.BehaviorError, Message: "wide attempt error"}, Attempt: 40_000, MaxAttempts: 40_002,
			})
			require.Equal(t, 40_000, requireListed(erroring).Attempt)
			worker.Start(t, protocol.StartParams{ClientID: "wide-integers", MaxWorkers: 2, RetryDelayMS: time.Minute.Milliseconds()})
			completed := env.DB.WaitJob(t, completing, workWait)
			retryable := env.DB.WaitJob(t, erroring, workWait, "retryable")
			worker.Stop(t, protocol.StopParams{})
			require.Equal(t, "completed", completed.State)
			require.Equal(t, 40_001, completed.Attempt)
			require.Equal(t, 40_001, completed.MaxAttempts)
			require.Equal(t, completed, requireListed(completing))
			require.Equal(t, 40_001, retryable.Attempt)
			require.Equal(t, 40_002, retryable.MaxAttempts)
			requireErrorAttempts(retryable, 40_001)
			require.Equal(t, retryable, requireListed(erroring))

			// The inserter retries the job, the worker fails its last
			// attempt, and the inserter's retry of the discarded job raises
			// max_attempts past it.
			retried := inserter.Retry(t, protocol.JobParams{ID: erroring})
			require.Equal(t, "available", retried.State)
			require.Equal(t, 40_001, retried.Attempt)
			require.Equal(t, 40_002, retried.MaxAttempts)
			discarded := workOne(t, env, worker, "wide-integers", erroring)
			require.Equal(t, "discarded", discarded.State)
			require.Equal(t, 40_002, discarded.Attempt)
			require.Equal(t, 40_002, discarded.MaxAttempts)
			requireErrorAttempts(discarded, 40_001, 40_002)
			require.Equal(t, discarded, requireListed(erroring))
			retried = inserter.Retry(t, protocol.JobParams{ID: erroring})
			require.Equal(t, "available", retried.State)
			require.Equal(t, 40_002, retried.Attempt)
			require.Equal(t, 40_003, retried.MaxAttempts)
			requireErrorAttempts(retried, 40_001, 40_002)
			require.Equal(t, retried, requireListed(erroring))
		})
	})

	// The rows each implementation's runtime writes when it works the same
	// jobs to completion, failure, and recorded output hold the same values.
	t.Run("WorkedRows", func(t *testing.T) {
		t.Parallel()

		EachDriver(t, nil, func(t *testing.T, env *Env) {
			behaviors := []string{protocol.BehaviorComplete, protocol.BehaviorError, protocol.BehaviorOutput}
			write := func(env *Env, worker *Adapter) []StoredRow {
				jobs := make([]protocol.InsertJob, len(behaviors))
				for i, behavior := range behaviors {
					jobs[i] = withOpts(echo(jobRowText, behavior), protocol.InsertOpts{
						MaxAttempts: 1, Metadata: metadata(t, map[string]any{"note": jobRowText}), Tags: []string{"job-rows", "tag_2"},
					})
				}
				inserted := env.Reference.Insert(t, protocol.InsertParams{Jobs: jobs})
				worker.Start(t, protocol.StartParams{ClientID: "worked-rows", MaxWorkers: 1})
				rows := make([]StoredRow, len(inserted))
				for i, result := range inserted {
					env.DB.WaitJob(t, result.Job.ID, workWait)
					rows[i] = env.DB.StoredRow(t, worker.Label, result.Job.ID)
				}
				worker.Stop(t, protocol.StopParams{})
				return rows
			}

			reference := write(env, env.Reference)
			other := env.Another(t)
			candidate := write(other, other.Candidate)
			for i, behavior := range behaviors {
				RequireEquivalentRows(t, "work "+behavior, reference[i], candidate[i])
			}
		})
	})
}

// TestRuntimeRows compares the rows each implementation's client writes when
// it claims a job, snoozes one, discards a retry that conflicts with a unique
// job, and rescues an abandoned job. The reference sets up the same jobs for
// both.
//
//nolint:thelper // Scenario bodies take t but aren't helpers.
func TestRuntimeRows(t *testing.T) {
	t.Parallel()

	// A scheduler finalizes a discarded job at its look-ahead time, the
	// current time plus its interval, which only implementations that accept
	// a scheduler interval shorten.
	unpinned := map[string][]string{"scheduler discard": {".finalized_at"}}

	EachDriver(t, nil, func(t *testing.T, env *Env) {
		reference := writeRuntimeRows(t, env, env.Reference)
		other := env.Another(t)
		candidate := writeRuntimeRows(t, other, other.Candidate)
		for _, operation := range []string{"claim", "snooze", "scheduler discard", "rescue"} {
			RequireEquivalentRows(t, operation, reference[operation], candidate[operation], unpinned[operation]...)
		}
	})
}

func writeRuntimeRows(t *testing.T, env *Env, actor *Adapter) map[string]StoredRow {
	t.Helper()

	rows := map[string]StoredRow{}
	opts := protocol.InsertOpts{Metadata: metadata(t, map[string]any{"note": jobRowText}), Tags: []string{"job-rows"}}

	// Claim and snooze: the actor works jobs the reference inserts, one held
	// on a barrier while running and one snoozed past the scheduler's
	// look-ahead so it stays scheduled.
	actor.Start(t, protocol.StartParams{ClientID: "job-rows-runtime", MaxWorkers: 2})
	claimed := env.Reference.InsertJob(t, withOpts(echo("job-rows-claim", protocol.BehaviorBarrierWait), opts))
	env.DB.WaitJob(t, claimed.ID, workWait, "running")
	rows["claim"] = env.DB.StoredRow(t, actor.Label, claimed.ID)
	actor.Release(t, "job-rows-claim")
	snoozed := env.Reference.InsertJob(t, withDuration(withOpts(echo(jobRowText, protocol.BehaviorSnoozeOnce), opts), time.Minute))
	env.DB.WaitJob(t, snoozed.ID, workWait, "scheduled")
	rows["snooze"] = env.DB.StoredRow(t, actor.Label, snoozed.ID)
	env.DB.WaitJob(t, claimed.ID, workWait)
	actor.Stop(t, protocol.StopParams{})

	// Scheduler discard: a retryable unique job whose unique states exclude
	// retryable comes due while another job holds its key, so the leader's
	// scheduler discards it. The retry delay exceeds River Go's scheduler
	// interval, so the retry stays retryable until then.
	uniqueOpts := protocol.InsertOpts{
		MaxAttempts: 3, Queue: "job_rows_discard",
		Unique: &protocol.UniqueOpts{ByArgs: true, ByState: []string{"available", "pending", "running", "scheduled"}},
	}
	env.Reference.Start(t, protocol.StartParams{
		ClientID: "job-rows-setup", LeaderElectionDisabled: true, MaxWorkers: 1, Queues: []string{"job_rows_discard"}, RetryDelayMS: 5_500,
	})
	discarded := env.Reference.InsertJob(t, withOpts(echo(jobRowText, protocol.BehaviorError), uniqueOpts))
	discarded = env.DB.WaitJob(t, discarded.ID, workWait, "retryable")
	env.Reference.Stop(t, protocol.StopParams{})
	holder := env.Reference.InsertJob(t, withOpts(echo(jobRowText, protocol.BehaviorError), uniqueOpts))
	require.NotEqual(t, discarded.ID, holder.ID, "a retryable job outside its unique states blocked insertion")
	time.Sleep(time.Until(discarded.ScheduledAt.Add(100 * time.Millisecond)))
	actor.Start(t, protocol.StartParams{ClientID: "job-rows-scheduler", MaxWorkers: 1, Tuning: fastTuning})
	env.DB.WaitJob(t, discarded.ID, maintenanceWait, "discarded")
	rows["scheduler discard"] = env.DB.StoredRow(t, actor.Label, discarded.ID)
	actor.Stop(t, protocol.StopParams{})

	// Rescue: a process holding a running attempt dies, and the actor's
	// leader rescues the abandoned attempt. Its retry delay keeps the rescued
	// job retryable.
	const rescueAfter = time.Second
	crasher := env.StartAdapter(t, env.Reference.Implementation)
	crasher.Start(t, protocol.StartParams{ClientID: "job-rows-crasher", LeaderElectionDisabled: true, MaxWorkers: 1, Queues: []string{"job_rows_rescue"}})
	rescued := env.Reference.InsertJob(t, withDuration(withOpts(echo(jobRowText, protocol.BehaviorSleep), protocol.InsertOpts{
		MaxAttempts: 3, Queue: "job_rows_rescue", Tags: []string{"job-rows"},
	}), time.Minute))
	rescued = env.DB.WaitJob(t, rescued.ID, workWait, "running")
	crasher.Kill(t)
	waitUntilRescuable(t, rescued, rescueAfter)
	actor.Start(t, protocol.StartParams{
		ClientID: "job-rows-rescuer", JobTimeoutMS: rescueAfter.Milliseconds(), MaxWorkers: 1,
		RescueAfterMS: rescueAfter.Milliseconds(), RetryDelayMS: time.Minute.Milliseconds(), Tuning: fastTuning,
	})
	env.DB.WaitJob(t, rescued.ID, maintenanceWait, "retryable")
	rows["rescue"] = env.DB.StoredRow(t, actor.Label, rescued.ID)
	actor.Stop(t, protocol.StopParams{})
	return rows
}
