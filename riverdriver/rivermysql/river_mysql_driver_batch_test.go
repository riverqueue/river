package rivermysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/rivermysql/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func TestJobInsertBatch(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	type testBundle struct {
		exec   riverdriver.Executor
		params *riverdriver.JobInsertFastManyParams
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		ctx := t.Context()
		driver := New(riversharedtest.DBPoolMySQL(ctx, t))
		params := &riverdriver.JobInsertFastManyParams{Schema: riverdbtest.TestSchema(ctx, t, driver, nil)}
		for i := range 2 {
			params.Jobs = append(params.Jobs, &riverdriver.JobInsertFastParams{
				ID: new(int64(9007199254740993 + i)), EncodedArgs: []byte(`null`),
				Kind: "batch", MaxAttempts: 25, Metadata: []byte(`null`), Priority: 1,
				Queue: "default", State: rivertype.JobStateAvailable,
			})
		}
		return &testBundle{exec: driver.GetExecutor(), params: params}
	}

	t.Run("JSONNullsAndIDs", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		bundle.params.Jobs[1].EncodedArgs = []byte(`{"id":9007199254740993}`)
		bundle.params.Jobs[1].Metadata = []byte(`{"id":9007199254740993}`)
		count, err := bundle.exec.JobInsertFastManyNoReturning(ctx, bundle.params)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		for i, params := range bundle.params.Jobs {
			job, err := bundle.exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: *params.ID, Schema: bundle.params.Schema})
			require.NoError(t, err)
			require.Equal(t, *params.ID, job.ID)
			require.JSONEq(t, string(params.EncodedArgs), string(job.EncodedArgs))
			require.JSONEq(t, string(params.Metadata), string(job.Metadata))
			if i == 1 {
				require.Contains(t, string(job.EncodedArgs), "9007199254740993")
				require.Contains(t, string(job.Metadata), "9007199254740993")
			}
			require.Nil(t, job.Tags)
		}
	})

	t.Run("RejectInvalidBatch", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.exec.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		bundle.params.Jobs[1].EncodedArgs = []byte(`{`)
		_, err = tx.JobInsertFastManyNoReturning(ctx, bundle.params)
		require.Error(t, err)
		count, err := tx.JobCountByState(ctx, &riverdriver.JobCountByStateParams{Schema: bundle.params.Schema, State: rivertype.JobStateAvailable})
		require.NoError(t, err)
		require.Zero(t, count)
	})
}

func TestJobInsertBatchReturning(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	type testBundle struct {
		driver *Driver
		params *riverdriver.JobInsertFastManyParams
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		ctx := t.Context()
		driver := New(riversharedtest.DBPoolMySQL(ctx, t))
		params := &riverdriver.JobInsertFastManyParams{Schema: riverdbtest.TestSchema(ctx, t, driver, nil)}
		for i := range byte(2) {
			params.Jobs = append(params.Jobs, &riverdriver.JobInsertFastParams{
				EncodedArgs: []byte(fmt.Sprintf(`{"index":%d}`, i)),
				Kind:        "batch", MaxAttempts: 25, Metadata: []byte(`{"id":9007199254740993}`),
				Priority: 1, Queue: "default", State: rivertype.JobStateAvailable,
				UniqueKey: []byte{0xff, 0, i}, UniqueStates: 0xff,
			})
		}
		return &testBundle{driver: driver, params: params}
	}

	t.Run("AutoIncrementGaps", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.driver.dbPool.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = tx.ExecContext(context.Background(), "SET SESSION auto_increment_increment = DEFAULT")
			_ = tx.Rollback()
		})
		_, err = tx.ExecContext(ctx, "SET SESSION auto_increment_increment = 7")
		require.NoError(t, err)
		results, err := bundle.driver.UnwrapExecutor(tx).JobInsertFastMany(ctx, bundle.params)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Equal(t, int64(7), results[1].Job.ID-results[0].Job.ID)
		for i, result := range results {
			require.False(t, result.UniqueSkippedAsDuplicate)
			require.JSONEq(t, string(bundle.params.Jobs[i].EncodedArgs), string(result.Job.EncodedArgs))
			require.JSONEq(t, string(bundle.params.Jobs[i].Metadata), string(result.Job.Metadata))
			stored, err := bundle.driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{
				ID: result.Job.ID, Schema: bundle.params.Schema,
			})
			require.NoError(t, err)
			require.JSONEq(t, string(bundle.params.Jobs[i].Metadata), string(stored.Metadata))
		}
	})

	t.Run("ConcurrentBatches", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		for worker := range 4 {
			t.Run(fmt.Sprintf("Worker%d", worker), func(t *testing.T) {
				t.Parallel()

				for iteration := range 10 {
					params := &riverdriver.JobInsertFastManyParams{Schema: bundle.params.Schema}
					for i := range 20 {
						job := *bundle.params.Jobs[0]
						job.EncodedArgs = fmt.Appendf(nil, `{"worker":%d,"iteration":%d,"index":%d}`, worker, iteration, i)
						job.UniqueKey = nil
						if i%2 == 0 {
							job.UniqueKey = fmt.Appendf(nil, "%d/%d/%d", worker, iteration, i)
						}
						params.Jobs = append(params.Jobs, &job)
					}
					results, err := bundle.driver.GetExecutor().JobInsertFastMany(t.Context(), params)
					require.NoError(t, err)
					require.Len(t, results, len(params.Jobs))
					for i, result := range results {
						require.False(t, result.UniqueSkippedAsDuplicate)
						require.JSONEq(t, string(params.Jobs[i].EncodedArgs), string(result.Job.EncodedArgs))
					}
				}
			})
		}
	})

	t.Run("CurrentRead", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(ctx) })
		_, err = tx.JobCountByState(ctx, &riverdriver.JobCountByStateParams{Schema: bundle.params.Schema, State: rivertype.JobStateAvailable})
		require.NoError(t, err) // Establish a snapshot before the duplicate exists.
		seed, err := bundle.driver.GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs: bundle.params.Jobs[:1], Schema: bundle.params.Schema,
		})
		require.NoError(t, err)

		results, err := tx.JobInsertFastMany(ctx, bundle.params)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.True(t, results[0].UniqueSkippedAsDuplicate)
		require.Equal(t, seed[0].Job.ID, results[0].Job.ID)
		require.False(t, results[1].UniqueSkippedAsDuplicate)
	})

	t.Run("NonObjectAndReservedMetadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		for _, metadata := range []string{`null`, `[1,2]`, `{"river:unique_nonce":"caller"}`} {
			for _, job := range bundle.params.Jobs {
				job.Metadata = []byte(metadata)
				job.UniqueKey = nil
			}
			results, err := bundle.driver.GetExecutor().JobInsertFastMany(t.Context(), bundle.params)
			require.NoError(t, err)
			require.Len(t, results, 2)
			for _, result := range results {
				require.False(t, result.UniqueSkippedAsDuplicate)
				require.JSONEq(t, metadata, string(result.Job.Metadata))
			}
		}
	})

	t.Run("ReadDoesNotLockOtherInserts", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		for _, job := range bundle.params.Jobs {
			job.UniqueKey = nil
		}
		ctx := t.Context()
		tx, err := bundle.driver.dbPool.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		dbtx := templateReplaceWrapper{dbtx: tx, replacer: &bundle.driver.replacer}
		result, err := jobInsertBatch(schemaTemplateParam(ctx, bundle.params.Schema), dbtx, bundle.params, []string{"test:0", "test:1"})
		require.NoError(t, err)
		firstID, err := result.LastInsertId()
		require.NoError(t, err)

		otherTx, err := bundle.driver.GetExecutor().Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = otherTx.Rollback(ctx) })
		_, err = otherTx.JobInsertFastMany(ctx, bundle.params)
		require.NoError(t, err)

		// The primary-key range includes another session's uncommitted jobs.
		// Locking the whole range here could deadlock concurrent insertions.
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, err := dbsqlc.New().JobGetByInsertNonce(schemaTemplateParam(readCtx, bundle.params.Schema), dbtx,
			&dbsqlc.JobGetByInsertNonceParams{FirstID: firstID, NoncePrefix: []byte("test:%")})
		require.NoError(t, err)
		require.Len(t, rows, 2)
	})

	t.Run("UniqueLookupUsesIndex", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		_, err := bundle.driver.GetExecutor().JobInsertFastMany(ctx, bundle.params)
		require.NoError(t, err)
		var plan string
		err = bundle.driver.dbPool.QueryRowContext(ctx,
			"EXPLAIN FORMAT=JSON SELECT id FROM "+mysqlIdentifier(bundle.params.Schema)+".river_job WHERE ("+jobUniqueKeySQL+") IN (?, ?)",
			bundle.params.Jobs[0].UniqueKey, bundle.params.Jobs[1].UniqueKey).Scan(&plan)
		require.NoError(t, err)
		require.Contains(t, plan, `"key": "river_job_unique_idx"`)
	})

	t.Run("UnrelatedUniqueConflictIsAtomic", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		exec := bundle.driver.GetExecutor()
		require.NoError(t, exec.Exec(ctx, "CREATE UNIQUE INDEX kind_unique ON "+mysqlIdentifier(bundle.params.Schema)+".river_job (kind)"))
		t.Cleanup(func() {
			require.NoError(t, exec.Exec(context.Background(), "DROP INDEX kind_unique ON "+mysqlIdentifier(bundle.params.Schema)+".river_job"))
		})
		_, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{Jobs: bundle.params.Jobs[:1], Schema: bundle.params.Schema})
		require.NoError(t, err)
		bundle.params.Jobs[0].Kind = "new_kind"
		bundle.params.Jobs[0].UniqueKey = []byte("new_key")

		tx, err := bundle.driver.dbPool.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = tx.ExecContext(context.Background(), "SET SESSION sql_mode = DEFAULT")
			_ = tx.Rollback()
		})
		// Conflict rejection must not depend on strict SQL mode.
		_, err = tx.ExecContext(ctx, "SET SESSION sql_mode = ''")
		require.NoError(t, err)
		txExec := bundle.driver.UnwrapExecutor(tx)
		_, err = txExec.JobInsertFastMany(ctx, bundle.params)
		require.ErrorContains(t, err, "Invalid JSON text")
		count, err := txExec.JobCountByState(ctx, &riverdriver.JobCountByStateParams{Schema: bundle.params.Schema, State: rivertype.JobStateAvailable})
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
}

func TestJobSetStateBatch(t *testing.T) {
	t.Parallel()

	riversharedtest.SkipIfMySQLNotEnabled(t)

	type testBundle struct {
		driver *Driver
		jobs   []*rivertype.JobRow
		params *riverdriver.JobSetStateIfRunningManyParams
		schema string
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		ctx := t.Context()
		driver := New(riversharedtest.DBPoolMySQL(ctx, t))
		schema := riverdbtest.TestSchema(ctx, t, driver, nil)
		params := &riverdriver.JobSetStateIfRunningManyParams{Schema: schema}
		jobs := make([]*rivertype.JobRow, 0, 2)
		for range 2 {
			job, err := driver.GetExecutor().JobInsertFull(ctx, &riverdriver.JobInsertFullParams{
				Attempt: 3, EncodedArgs: []byte(`{}`), Kind: "batch", MaxAttempts: 25,
				Priority: 1, Queue: "default", Schema: schema, State: rivertype.JobStateRunning, Tags: []string{},
			})
			require.NoError(t, err)
			jobs = append(jobs, job)
			params.ID = append(params.ID, job.ID)
			params.Attempt = append(params.Attempt, nil)
			params.ErrData = append(params.ErrData, nil)
			params.FinalizedAt = append(params.FinalizedAt, nil)
			params.MetadataDoMerge = append(params.MetadataDoMerge, true)
			params.MetadataUpdates = append(params.MetadataUpdates, []byte(`{"result":"merged"}`))
			params.ScheduledAt = append(params.ScheduledAt, new(time.Now().Add(time.Hour)))
			params.State = append(params.State, rivertype.JobStateRetryable)
		}
		return &testBundle{driver: driver, jobs: jobs, params: params, schema: schema}
	}

	t.Run("CurrentRead", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		ctx := t.Context()
		tx, err := bundle.driver.dbPool.BeginTx(ctx, nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		// Ensure the joined batch query is used even if TEST_MYSQL_URL has no database.
		_, err = tx.ExecContext(ctx, "USE "+mysqlIdentifier(bundle.schema))
		require.NoError(t, err)
		exec := bundle.driver.UnwrapExecutor(tx)
		_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: bundle.jobs[0].ID, Schema: bundle.schema})
		require.NoError(t, err) // Establish a REPEATABLE READ snapshot before the changes below.

		now := time.Now().UTC().Truncate(time.Microsecond)
		_, err = bundle.driver.GetExecutor().JobCancel(ctx, &riverdriver.JobCancelParams{
			ID: bundle.jobs[0].ID, CancelAttemptedAt: now, Now: &now, Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.NoError(t, bundle.driver.GetExecutor().Exec(ctx,
			"UPDATE "+mysqlIdentifier(bundle.schema)+".river_job SET state = 'completed', finalized_at = ? WHERE id = ?", now, bundle.jobs[1].ID))

		rows, err := exec.JobSetStateIfRunningMany(ctx, bundle.params)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		require.Equal(t, rivertype.JobStateCancelled, rows[0].State)
		require.Equal(t, bundle.jobs[0].ScheduledAt, rows[0].ScheduledAt)
		require.Equal(t, rivertype.JobStateCompleted, rows[1].State)
		require.Equal(t, now, *rows[1].FinalizedAt)
		require.JSONEq(t, `{"result":"merged"}`, string(rows[1].Metadata))
	})

	t.Run("RepeatedIDs", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.params.ID[1] = bundle.params.ID[0]
		bundle.params.State[1] = rivertype.JobStateScheduled
		bundle.params.MetadataUpdates[0] = []byte(`{"first":true}`)
		bundle.params.MetadataUpdates[1] = []byte(`{"second":true}`)
		rows, err := bundle.driver.GetExecutor().JobSetStateIfRunningMany(t.Context(), bundle.params)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		for _, row := range rows {
			require.Equal(t, rivertype.JobStateRetryable, row.State)
			require.JSONEq(t, `{"first":true,"second":true}`, string(row.Metadata))
		}
	})
}
