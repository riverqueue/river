package rivertest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/riverqueue/river/rivertype"
)

type testArgs struct {
	Value string `json:"value"`
}

func (testArgs) Kind() string { return "rivertest_work_test" }

func TestWorker_Work(t *testing.T) {
	t.Parallel()

	config := &river.Config{}
	driver := riverpgxv5.New(nil)

	t.Run("WorkASimpleJob", func(t *testing.T) {
		t.Parallel()

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, testArgs{Value: "test"}, job.Args)

			require.Positive(t, job.JobRow.ID)
			require.Equal(t, 1, job.JobRow.Attempt)
			require.NotNil(t, job.JobRow.AttemptedAt)
			require.WithinDuration(t, time.Now(), *job.JobRow.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"worker1"}, job.JobRow.AttemptedBy)
			require.WithinDuration(t, time.Now(), job.JobRow.CreatedAt, 5*time.Second)
			require.JSONEq(t, `{"value": "test"}`, string(job.JobRow.EncodedArgs))
			require.Equal(t, []rivertype.AttemptError{}, job.JobRow.Errors)
			require.Nil(t, job.JobRow.FinalizedAt)
			require.Equal(t, "rivertest_work_test", job.JobRow.Kind)
			require.Equal(t, river.MaxAttemptsDefault, job.JobRow.MaxAttempts)
			// &rivertype.JobRow{
			// 	ID:          1,
			// 	Attempt:     1,
			// 	AttemptedAt: ptrutil.Ptr(time.Now()),
			// 	AttemptedBy: []string{"worker1"},
			// 	CreatedAt:   created,
			// 	EncodedArgs: encodedArgs,
			// 	Errors:      []rivertype.AttemptError{},
			// 	FinalizedAt: nil,
			// 	Kind:        args.Kind(),
			// 	MaxAttempts: 10,
			// 	Metadata:    []byte(`{}`),
			// 	Priority:    1,
			// 	Queue:       "default",
			// 	ScheduledAt: created,
			// 	State:       rivertype.JobStateRunning,
			// 	Tags:        []string{},
			// 	UniqueKey:   []byte{},
			// },

			return nil
		})
		tw := rivertest.NewWorker(t, driver, config, worker)
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test"}))
	})

	t.Run("Reusable", func(t *testing.T) {
		t.Parallel()

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		})
		tw := rivertest.NewWorker(t, driver, config, worker)
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test"}))
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test2"}))
	})

	t.Run("WorkOpts", func(t *testing.T) {
		t.Parallel()

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		})
		tw := rivertest.NewWorker(t, driver, config, worker)
		// You can also use the WorkOpts method to pass in custom insert options:
		require.NoError(t, tw.WorkOpts(context.Background(), t, testArgs{Value: "test3"}, &river.InsertOpts{
			Metadata: []byte(`{"key": "value"}`),
		}))

		require.NoError(t, rivertest.NewWorker(t, driver, config, worker).Work(context.Background(), t, testArgs{Value: "test"}))
	})
}
