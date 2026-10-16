package rivermysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
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
