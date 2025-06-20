package river

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

func TestNeverSchedule(t *testing.T) {
	t.Parallel()

	t.Run("NextReturnsMaximumTime", func(t *testing.T) {
		t.Parallel()

		schedule := NeverSchedule()
		now := time.Now()
		next := schedule.Next(now)
		require.Equal(t, time.Unix(1<<63-62135596801, 999999999), next)
		require.False(t, next.Before(now))
		// use an arbitrary duration to check that
		// the next schedule is far in the future
		require.Greater(t, next.Year()-now.Year(), 1000)
	})
}

func TestPeriodicJobBundle(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*PeriodicJobBundle, *testBundle) { //nolint:unparam
		t.Helper()

		periodicJobEnqueuer, err := maintenance.NewPeriodicJobEnqueuer(
			riversharedtest.BaseServiceArchetype(t),
			&maintenance.PeriodicJobEnqueuerConfig{},
			nil,
		)
		require.NoError(t, err)

		return newPeriodicJobBundle(newTestConfig(t, ""), periodicJobEnqueuer), &testBundle{}
	}

	t.Run("ConstructorFuncGeneratesNewArgsOnEachCall", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		type TestJobArgs struct {
			testutil.JobArgsReflectKind[TestJobArgs]
			JobNum int `json:"job_num"`
		}

		var jobNum int

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) {
				jobNum++
				return TestJobArgs{JobNum: jobNum}, nil
			},
			nil,
		)

		internalPeriodicJob := periodicJobBundle.toInternal(periodicJob)

		insertParams1, err := internalPeriodicJob.ConstructorFunc()
		require.NoError(t, err)
		require.Equal(t, 1, mustUnmarshalJSON[TestJobArgs](t, insertParams1.EncodedArgs).JobNum)

		insertParams2, err := internalPeriodicJob.ConstructorFunc()
		require.NoError(t, err)
		require.Equal(t, 2, mustUnmarshalJSON[TestJobArgs](t, insertParams2.EncodedArgs).JobNum)
	})

	t.Run("ReturningNilDoesntInsertNewJob", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) {
				// Returning nil from the constructor function should not insert a new job.
				return nil, nil
			},
			nil,
		)

		internalPeriodicJob := periodicJobBundle.toInternal(periodicJob)

		_, err := internalPeriodicJob.ConstructorFunc()
		require.ErrorIs(t, err, maintenance.ErrNoJobToInsert)
	})

	t.Run("AddError", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) { return nil, nil },
			&PeriodicJobOpts{ID: "periodic_job_id"},
		)

		periodicJobBundle.Add(periodicJob)

		require.PanicsWithError(t, "periodic job with ID already registered: periodic_job_id", func() {
			periodicJobBundle.Add(periodicJob)
		})

		_, err := periodicJobBundle.AddSafely(periodicJob)
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_id")
	})

	t.Run("AddManyError", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		periodicJob := NewPeriodicJob(
			PeriodicInterval(15*time.Minute),
			func() (JobArgs, *InsertOpts) { return nil, nil },
			&PeriodicJobOpts{ID: "periodic_job_id"},
		)

		periodicJobBundle.Add(periodicJob)

		require.PanicsWithError(t, "periodic job with ID already registered: periodic_job_id", func() {
			periodicJobBundle.AddMany([]*PeriodicJob{periodicJob})
		})

		_, err := periodicJobBundle.AddManySafely([]*PeriodicJob{periodicJob})
		require.EqualError(t, err, "periodic job with ID already registered: periodic_job_id")
	})
}

func mustUnmarshalJSON[T any](t *testing.T, data []byte) *T {
	t.Helper()

	var val T
	err := json.Unmarshal(data, &val)
	require.NoError(t, err)
	return &val
}
