package rivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testfactory"
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
			require.Equal(t, 1, job.JobRow.Attempt)
			require.NotNil(t, job.JobRow.AttemptedAt)
			require.WithinDuration(t, time.Now(), *job.JobRow.AttemptedAt, 5*time.Second)
			require.Equal(t, []string{"worker1"}, job.JobRow.AttemptedBy)
			require.WithinDuration(t, time.Now(), job.JobRow.CreatedAt, 5*time.Second)
			require.JSONEq(t, `{"value": "test"}`, string(job.JobRow.EncodedArgs))
			require.Empty(t, job.JobRow.Errors)
			require.Nil(t, job.JobRow.FinalizedAt)
			require.Positive(t, job.JobRow.ID)
			require.Equal(t, "rivertest_work_test", job.JobRow.Kind)
			require.Equal(t, river.MaxAttemptsDefault, job.JobRow.MaxAttempts)
			require.Equal(t, []byte(`{}`), job.JobRow.Metadata)
			require.Equal(t, river.PriorityDefault, job.JobRow.Priority)
			require.Equal(t, river.QueueDefault, job.JobRow.Queue)
			require.WithinDuration(t, time.Now(), job.JobRow.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.JobRow.State)
			require.Equal(t, []string{}, job.JobRow.Tags)
			require.Nil(t, job.JobRow.UniqueKey)

			return nil
		})
		tw := NewWorker(t, driver, config, worker)
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test"}, nil))
	})

	t.Run("Reusable", func(t *testing.T) {
		t.Parallel()

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			return nil
		})
		tw := NewWorker(t, driver, config, worker)
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test"}, nil))
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test2"}, nil))
	})

	t.Run("SetsCustomInsertOpts", func(t *testing.T) {
		t.Parallel()

		uniqueOpts := river.UniqueOpts{ByQueue: true}
		hourFromNow := time.Now().Add(1 * time.Hour)
		internalUniqueOpts := (*dbunique.UniqueOpts)(&uniqueOpts)
		uniqueKey, err := dbunique.UniqueKey(&baseservice.UnStubbableTimeGenerator{}, internalUniqueOpts, &rivertype.JobInsertParams{
			Args:        testArgs{Value: "test3"},
			CreatedAt:   &hourFromNow,
			EncodedArgs: []byte(`{"value": "test3"}`),
			Kind:        "rivertest_work_test",
			MaxAttempts: 420,
			Metadata:    []byte(`{"key": "value"}`),
			Priority:    3,
			Queue:       "custom_queue",
			State:       rivertype.JobStateAvailable,
			Tags:        []string{"tag1", "tag2"},
		})
		require.NoError(t, err)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, testArgs{Value: "test3"}, job.Args)
			require.Equal(t, 1, job.JobRow.Attempt)
			require.NotNil(t, job.JobRow.AttemptedAt)
			require.WithinDuration(t, hourFromNow, *job.JobRow.AttemptedAt, 2*time.Second)
			require.Equal(t, []string{"worker1"}, job.JobRow.AttemptedBy)
			require.WithinDuration(t, hourFromNow, job.JobRow.CreatedAt, 2*time.Second)
			require.JSONEq(t, `{"value": "test3"}`, string(job.JobRow.EncodedArgs))
			require.Empty(t, job.JobRow.Errors)
			require.Nil(t, job.JobRow.FinalizedAt)
			require.Positive(t, job.JobRow.ID)
			require.Equal(t, "rivertest_work_test", job.JobRow.Kind)
			require.Equal(t, 420, job.JobRow.MaxAttempts)
			require.JSONEq(t, `{"key": "value"}`, string(job.JobRow.Metadata))
			require.Equal(t, 3, job.JobRow.Priority)
			require.Equal(t, "custom_queue", job.JobRow.Queue)
			require.WithinDuration(t, hourFromNow, job.JobRow.ScheduledAt, 2*time.Second)
			require.Equal(t, rivertype.JobStateRunning, job.JobRow.State)
			require.Equal(t, []string{"tag1", "tag2"}, job.JobRow.Tags)
			require.Equal(t, uniqueKey, job.JobRow.UniqueKey)

			return nil
		})
		tw := NewWorker(t, driver, config, worker)

		// You can also use the WorkOpts method to pass in custom insert options:
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test3"}, &river.InsertOpts{
			MaxAttempts: 420,
			Metadata:    []byte(`{"key": "value"}`),
			Pending:     true, // ignored but added to ensure non-default behavior
			Priority:    3,
			Queue:       "custom_queue",
			ScheduledAt: hourFromNow,
			Tags:        []string{"tag1", "tag2"},
			UniqueOpts:  uniqueOpts,
		}))
	})

	t.Run("UniqueOptsByPeriodRespectsCustomStubbedTime", func(t *testing.T) {
		t.Parallel()

		stubTime := &riversharedtest.TimeStub{}
		now := time.Now().UTC()
		stubTime.StubNowUTC(now)
		config := &river.Config{
			Test: river.TestConfig{Time: stubTime},
		}

		uniqueOpts := river.UniqueOpts{ByPeriod: 1 * time.Hour}
		internalUniqueOpts := (*dbunique.UniqueOpts)(&uniqueOpts)
		uniqueKey, err := dbunique.UniqueKey(stubTime, internalUniqueOpts, &rivertype.JobInsertParams{
			Args:        testArgs{Value: "test3"},
			CreatedAt:   &now,
			EncodedArgs: []byte(`{"value": "test3"}`),
			Kind:        "rivertest_work_test",
		})
		require.NoError(t, err)

		worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
			require.Equal(t, uniqueKey, job.JobRow.UniqueKey)

			return nil
		})
		tw := NewWorker(t, driver, config, worker)
		require.NoError(t, tw.Work(context.Background(), t, testArgs{Value: "test"}, &river.InsertOpts{
			UniqueOpts: uniqueOpts,
		}))
	})
}

func TestWorker_WorkJob(t *testing.T) {
	t.Parallel()

	config := &river.Config{}
	driver := riverpgxv5.New(nil)

	worker := river.WorkFunc(func(ctx context.Context, job *river.Job[testArgs]) error {
		require.Equal(t, []string{"worker123"}, job.JobRow.AttemptedBy)
		return nil
	})
	tw := NewWorker(t, driver, config, worker)

	now := time.Now()
	require.NoError(t, tw.WorkJob(context.Background(), t, makeJobFromFactoryBuild(t, testArgs{Value: "test"}, &testfactory.JobOpts{
		AttemptedAt: &now,
		AttemptedBy: []string{"worker123"},
		CreatedAt:   &now,
		EncodedArgs: []byte(`{"value": "test"}`),
		Errors:      nil,
	})))
}
