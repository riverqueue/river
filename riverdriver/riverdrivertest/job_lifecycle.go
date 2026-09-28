package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivertype"
)

func exerciseJobLifecycle[TTx any](ctx context.Context, t *testing.T, executorWithTx func(context.Context, *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	t.Run("JobLifecycle", func(t *testing.T) {
		t.Parallel()

		t.Run("ClaimFiltersBeforeLimit", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			now := time.Now().UTC()
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("other"), Priority: new(1)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("wanted"), Priority: new(1), ScheduledAt: new(now.Add(time.Hour)), State: new(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("wanted"), Queue: new("other")})
			wanted := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Kind: new("wanted"), Priority: new(2), ScheduledAt: new(now.Add(-time.Second))})

			jobs, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID: testClientID, Kind: []string{"wanted"}, MaxAttemptedBy: 5, MaxToLock: 1, Now: &now, Queue: "default",
			})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, wanted.ID, jobs[0].ID)
			require.Equal(t, 1, jobs[0].Attempt)
			require.Equal(t, []string{testClientID}, jobs[0].AttemptedBy)
			require.Equal(t, rivertype.JobStateRunning, jobs[0].State)

			jobs, err = exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{Kind: []string{}, MaxToLock: 100, Queue: "default"})
			require.NoError(t, err)
			require.Empty(t, jobs)
		})

		t.Run("CleanupReleasesUniqueKey", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: new(time.Now().Add(-time.Hour)), State: new(rivertype.JobStateCompleted),
				UniqueKey: []byte("cleanup"), UniqueStates: uniquestates.UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}),
			})
			count, err := exec.JobDeleteBefore(ctx, &riverdriver.JobDeleteBeforeParams{
				CompletedDoDelete: true, CompletedFinalizedAtHorizon: time.Now(), Max: 10,
			})
			require.NoError(t, err)
			require.Equal(t, 1, count)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			results, err := exec.JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{}`), Kind: job.Kind, MaxAttempts: 25, Priority: 1, Queue: "default", State: rivertype.JobStateAvailable, Tags: []string{},
				UniqueKey: job.UniqueKey, UniqueStates: uniquestates.UniqueStatesToBitmask(job.UniqueStates),
			}}})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.False(t, results[0].UniqueSkippedAsDuplicate)
		})

		t.Run("UniqueInsertAndRelease", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			params := &riverdriver.JobInsertFastManyParams{Jobs: []*riverdriver.JobInsertFastParams{{
				EncodedArgs: []byte(`{"hello":"world"}`), Kind: "unique", MaxAttempts: 25, Priority: 1, Queue: "default", State: rivertype.JobStateAvailable, Tags: []string{},
				UniqueKey: []byte("same"), UniqueStates: uniquestates.UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable}),
			}}}
			first, err := exec.JobInsertFastMany(ctx, params)
			require.NoError(t, err)
			require.Len(t, first, 1)
			require.False(t, first[0].UniqueSkippedAsDuplicate)
			duplicate, err := exec.JobInsertFastMany(ctx, params)
			require.NoError(t, err)
			require.True(t, duplicate[0].UniqueSkippedAsDuplicate)
			require.Equal(t, first[0].Job.ID, duplicate[0].Job.ID)
			_, err = exec.JobCancel(ctx, &riverdriver.JobCancelParams{ID: first[0].Job.ID, CancelAttemptedAt: time.Now()})
			require.NoError(t, err)
			next, err := exec.JobInsertFastMany(ctx, params)
			require.NoError(t, err)
			require.False(t, next[0].UniqueSkippedAsDuplicate)
			require.NotEqual(t, first[0].Job.ID, next[0].Job.ID)
		})
	})
}
