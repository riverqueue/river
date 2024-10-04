package river

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/rivershared/riversharedtest"
)

func TestPeriodicJobBundle(t *testing.T) {
	t.Parallel()

	type testBundle struct{}

	setup := func(t *testing.T) (*PeriodicJobBundle, *testBundle) { //nolint:unparam
		t.Helper()

		periodicJobEnqueuer := maintenance.NewPeriodicJobEnqueuer(
			riversharedtest.BaseServiceArchetype(t),
			&maintenance.PeriodicJobEnqueuerConfig{},
			nil,
		)

		return newPeriodicJobBundle(newTestConfig(t, nil), periodicJobEnqueuer), &testBundle{}
	}

	t.Run("ConstructorFuncGeneratesNewArgsOnEachCall", func(t *testing.T) {
		t.Parallel()

		periodicJobBundle, _ := setup(t)

		type TestJobArgs struct {
			JobArgsReflectKind[TestJobArgs]
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
}

func mustUnmarshalJSON[T any](t *testing.T, data []byte) *T {
	t.Helper()

	var val T
	err := json.Unmarshal(data, &val)
	require.NoError(t, err)
	return &val
}
