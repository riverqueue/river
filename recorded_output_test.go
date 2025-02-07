package river

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func Test_RecordedOutput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type JobArgs struct {
		JobArgsReflectKind[JobArgs]
	}

	type myOutput struct {
		Message string `json:"message"`
	}

	type testBundle struct {
		dbPool *pgxpool.Pool
	}

	setup := func(t *testing.T) (*Client[pgx.Tx], *testBundle) {
		t.Helper()

		dbPool := riverinternaltest.TestDB(ctx, t)
		config := newTestConfig(t, nil)
		client := newTestClient(t, dbPool, config)
		t.Cleanup(func() { require.NoError(t, client.Stop(ctx)) })
		return client, &testBundle{dbPool: dbPool}
	}

	t.Run("ValidOutput", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		subChan := subscribe(t, client)
		startClient(ctx, t, client)

		validOutput := myOutput{Message: "it worked"}
		expectedOutput := `{"output":{"message":"it worked"}}`
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return RecordOutput(ctx, validOutput)
		}))

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		require.JSONEq(t, expectedOutput, string(event.Job.Metadata))

		jobFromDB, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.JSONEq(t, expectedOutput, string(jobFromDB.Metadata))
	})

	t.Run("InvalidOutput", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		// Subscribe to job failure events
		subChan := subscribe(t, client)
		startClient(ctx, t, client)

		// Use an invalid output value (a channel, which cannot be marshaled to JSON)
		var invalidOutput chan int
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return RecordOutput(ctx, invalidOutput)
		}))

		insertRes, err := client.Insert(ctx, JobArgs{}, nil)
		require.NoError(t, err)

		// Wait for the job failure event
		event := riversharedtest.WaitOrTimeout(t, subChan)
		require.Equal(t, EventKindJobFailed, event.Kind)
		require.NotEmpty(t, event.Job.Errors)
		require.Contains(t, event.Job.Errors[0].Error, "json")

		jobFromDB, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		var meta map[string]any
		require.NoError(t, json.Unmarshal(jobFromDB.Metadata, &meta))
		_, ok := meta["output"]
		require.False(t, ok, "output key should not be set in metadata")
	})

	t.Run("PreExistingMetadata", func(t *testing.T) {
		t.Parallel()

		client, _ := setup(t)

		subChan := subscribe(t, client)
		startClient(ctx, t, client)

		newOutput := myOutput{Message: "new output"}
		AddWorker(client.config.Workers, WorkFunc(func(ctx context.Context, job *Job[JobArgs]) error {
			return RecordOutput(ctx, newOutput)
		}))

		// Insert a job with pre-existing metadata (including an output key)
		initialMeta := `{"existing":"value","output":"old"}`
		insertRes, err := client.Insert(ctx, JobArgs{}, &InsertOpts{Metadata: []byte(initialMeta)})
		require.NoError(t, err)

		event := riversharedtest.WaitOrTimeout(t, subChan)
		require.Equal(t, EventKindJobCompleted, event.Kind)
		expectedMeta := `{"existing":"value","output":{"message":"new output"}}`
		require.JSONEq(t, expectedMeta, string(event.Job.Metadata))

		// Fetch the job from the database and verify
		jobFromDB, err := client.JobGet(ctx, insertRes.Job.ID)
		require.NoError(t, err)
		require.JSONEq(t, expectedMeta, string(jobFromDB.Metadata))
	})
}
