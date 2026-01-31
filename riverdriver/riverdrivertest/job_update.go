package riverdrivertest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseJobUpdate[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	type testBundle struct {
		driver riverdriver.Driver[TTx]
	}

	setup := func(ctx context.Context, t *testing.T) (riverdriver.Executor, *testBundle) {
		t.Helper()

		exec, driver := executorWithTx(ctx, t)

		return exec, &testBundle{
			driver: driver,
		}
	}

	t.Run("JobCancel", func(t *testing.T) {
		t.Parallel()

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			t.Run(fmt.Sprintf("CancelsJobIn%sState", strings.ToUpper(string(startingState[0]))+string(startingState)[1:]), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				now := time.Now().UTC()
				nowStr := now.Format(time.RFC3339Nano)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					State:     &startingState,
					UniqueKey: []byte("unique-key"),
				})
				require.Equal(t, startingState, job.State)

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: now,
					ControlTopic:      string(notifier.NotificationTopicControl),
				})
				require.NoError(t, err)
				require.NotNil(t, jobAfter)

				require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
				require.WithinDuration(t, time.Now(), *jobAfter.FinalizedAt, 2*time.Second)
				require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			})
		}

		t.Run("RunningJobIsNotImmediatelyCancelled", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			nowStr := now.Format(time.RFC3339Nano)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})
			require.Equal(t, rivertype.JobStateRunning, job.State)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                job.ID,
				CancelAttemptedAt: now,
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.NoError(t, err)
			require.NotNil(t, jobAfter)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)
			require.JSONEq(t, fmt.Sprintf(`{"cancel_attempted_at":%q}`, nowStr), string(jobAfter.Metadata))
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
		})

		for _, startingState := range []rivertype.JobState{
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
		} {
			t.Run(fmt.Sprintf("DoesNotAlterFinalizedJobIn%sState", startingState), func(t *testing.T) {
				t.Parallel()

				exec, _ := setup(ctx, t)

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: ptrutil.Ptr(time.Now()),
					State:       &startingState,
				})

				jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
					ID:                job.ID,
					CancelAttemptedAt: time.Now(),
					ControlTopic:      string(notifier.NotificationTopicControl),
				})
				require.NoError(t, err)
				require.Equal(t, startingState, jobAfter.State)
				require.WithinDuration(t, *job.FinalizedAt, *jobAfter.FinalizedAt, time.Microsecond)
				require.JSONEq(t, `{}`, string(jobAfter.Metadata))
			})
		}

		t.Run("ReturnsErrNotFoundIfJobDoesNotExist", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			jobAfter, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{
				ID:                1234567890,
				CancelAttemptedAt: time.Now(),
				ControlTopic:      string(notifier.NotificationTopicControl),
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
			require.Nil(t, jobAfter)
		})
	})

	t.Run("JobRescueMany", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		now := time.Now().UTC()

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Metadata: []byte(`{"river:rescue_count": 5, "something": "else"}`),
			State:    ptrutil.Ptr(rivertype.JobStateRunning),
		})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
			Metadata: []byte(`{}`),
			State:    ptrutil.Ptr(rivertype.JobStateRunning),
		})

		_, err := exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			ID: []int64{
				job1.ID,
				job2.ID,
			},
			Error: [][]byte{
				[]byte(`{"error": "message1"}`),
				[]byte(`{"error": "message2"}`),
			},
			FinalizedAt: []*time.Time{
				nil,
				&now,
			},
			ScheduledAt: []time.Time{
				now,
				now,
			},

			State: []string{
				string(rivertype.JobStateAvailable),
				string(rivertype.JobStateDiscarded),
			},
		})
		require.NoError(t, err)

		updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
		require.NoError(t, err)
		require.Equal(t, "message1", updatedJob1.Errors[0].Error)
		require.Nil(t, updatedJob1.FinalizedAt)
		require.WithinDuration(t, now, updatedJob1.ScheduledAt, bundle.driver.TimePrecision())
		require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)
		require.JSONEq(t, `{"river:rescue_count": 6, "something": "else"}`, string(updatedJob1.Metadata))

		updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
		require.NoError(t, err)
		require.Equal(t, "message2", updatedJob2.Errors[0].Error)
		require.WithinDuration(t, now, *updatedJob2.FinalizedAt, bundle.driver.TimePrecision())
		require.WithinDuration(t, now, updatedJob2.ScheduledAt, bundle.driver.TimePrecision())
		require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
		require.JSONEq(t, `{"river:rescue_count": 1}`, string(updatedJob2.Metadata))
	})

	t.Run("JobRetry", func(t *testing.T) {
		t.Parallel()

		t.Run("DoesNotUpdateARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State: ptrutil.Ptr(rivertype.JobStateRunning),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID: job.ID,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, jobUpdated.State)
		})

		for _, state := range []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStateCancelled,
			rivertype.JobStateCompleted,
			rivertype.JobStateDiscarded,
			rivertype.JobStatePending,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
		} {
			t.Run(fmt.Sprintf("UpdatesA_%s_JobToBeScheduledImmediately", state), func(t *testing.T) {
				t.Parallel()

				exec, bundle := setup(ctx, t)

				now := time.Now().UTC()

				setFinalized := slices.Contains([]rivertype.JobState{
					rivertype.JobStateCancelled,
					rivertype.JobStateCompleted,
					rivertype.JobStateDiscarded,
				}, state)

				var finalizedAt *time.Time
				if setFinalized {
					finalizedAt = &now
				}

				job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
					FinalizedAt: finalizedAt,
					ScheduledAt: ptrutil.Ptr(now.Add(1 * time.Hour)),
					State:       &state,
				})

				jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
					ID:  job.ID,
					Now: &now,
				})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
				require.WithinDuration(t, now, jobAfter.ScheduledAt, bundle.driver.TimePrecision())

				jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
				require.Nil(t, jobUpdated.FinalizedAt)
			})
		}

		t.Run("AltersScheduledAtForAlreadyCompletedJob", func(t *testing.T) {
			// A job which has already completed will have a ScheduledAt that could be
			// long in the past. Now that we're re-scheduling it, we should update that
			// to the current time to slot it in alongside other recently-scheduled jobs
			// and not skip the line; also, its wait duration can't be calculated
			// accurately if we don't reset the scheduled_at.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				FinalizedAt: &now,
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
				State:       ptrutil.Ptr(rivertype.JobStateCompleted),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID:  job.ID,
				Now: &now,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, 5*time.Second)
		})

		t.Run("DoesNotAlterScheduledAtIfInThePastAndJobAlreadyAvailable", func(t *testing.T) {
			// We don't want to update ScheduledAt if the job was already available
			// because doing so can make it lose its place in line.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt: ptrutil.Ptr(now.Add(-1 * time.Hour)),
			})

			jobAfter, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID:  job.ID,
				Now: &now,
			})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, jobUpdated.State)
		})

		t.Run("ReturnsErrNotFoundIfJobNotFound", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			_, err := exec.JobRetry(ctx, &riverdriver.JobRetryParams{
				ID: 0,
			})
			require.Error(t, err)
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})

	t.Run("JobSchedule", func(t *testing.T) {
		t.Parallel()

		t.Run("BasicScheduling", func(t *testing.T) {
			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
				afterHorizon  = horizon.Add(1 * time.Minute)
			)

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			// States that aren't scheduled.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateAvailable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateCompleted)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{FinalizedAt: &beforeHorizon, ScheduledAt: &beforeHorizon, State: ptrutil.Ptr(rivertype.JobStateDiscarded)})

			// Right state, but after horizon.
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateRetryable)})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &afterHorizon, State: ptrutil.Ptr(rivertype.JobStateScheduled)})

			// First two scheduled because of limit.
			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)
			require.Equal(t, job1.ID, result[0].Job.ID)
			require.False(t, result[0].ConflictDiscarded)
			require.Equal(t, job2.ID, result[1].Job.ID)
			require.False(t, result[1].ConflictDiscarded)

			// And then job3 scheduled.
			result, err = exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 2,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.Equal(t, job3.ID, result[0].Job.ID)
			require.False(t, result[0].ConflictDiscarded)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)

			updatedJob3, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
		})

		t.Run("HandlesUniqueConflicts", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			defaultUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRetryable,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}
			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// job3 has no conflict (it's the only one with this key), so it should be
			// scheduled.
			job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-3"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(defaultUniqueStates),
			})

			// This one is a conflict with job1 because it's already running and has
			// the same unique properties:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			// This one is *not* a conflict with job2 because it's completed, which
			// isn't in the unique states:
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateCompleted),
				UniqueKey:    []byte("unique-key-2"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 3)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob1.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").String())

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob2.State)
			require.False(t, gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").Exists())

			updatedJob3, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job3.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob3.State)
			require.False(t, gjson.GetBytes(updatedJob3.Metadata, "unique_key_conflict").Exists())
		})

		t.Run("SchedulingTwoRetryableJobsThatWillConflictWithEachOther", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			var (
				horizon       = time.Now()
				beforeHorizon = horizon.Add(-1 * time.Minute)
			)

			// The default unique state list, minus retryable to allow for these conflicts:
			nonRetryableUniqueStates := []rivertype.JobState{
				rivertype.JobStateAvailable,
				rivertype.JobStatePending,
				rivertype.JobStateRunning,
				rivertype.JobStateScheduled,
			}

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})
			job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				ScheduledAt:  &beforeHorizon,
				State:        ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey:    []byte("unique-key-1"),
				UniqueStates: uniquestates.UniqueStatesToBitmask(nonRetryableUniqueStates),
			})

			result, err := exec.JobSchedule(ctx, &riverdriver.JobScheduleParams{
				Max: 100,
				Now: &horizon,
			})
			require.NoError(t, err)
			require.Len(t, result, 2)

			updatedJob1, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, updatedJob1.State)
			require.False(t, gjson.GetBytes(updatedJob1.Metadata, "unique_key_conflict").Exists())

			updatedJob2, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job2.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob2.State)
			require.Equal(t, "scheduler_discarded", gjson.GetBytes(updatedJob2.Metadata, "unique_key_conflict").String())
		})
	})

	makeErrPayload := func(t *testing.T, now time.Time) []byte {
		t.Helper()

		errPayload, err := json.Marshal(rivertype.AttemptError{
			Attempt: 1, At: now, Error: "fake error", Trace: "foo.go:123\nbar.go:456",
		})
		require.NoError(t, err)
		return errPayload
	}

	setStateManyParams := func(params ...*riverdriver.JobSetStateIfRunningParams) *riverdriver.JobSetStateIfRunningManyParams {
		batchParams := &riverdriver.JobSetStateIfRunningManyParams{}
		for _, param := range params {
			var (
				attempt     *int
				errData     []byte
				finalizedAt *time.Time
				scheduledAt *time.Time
			)
			if param.Attempt != nil {
				attempt = param.Attempt
			}
			if param.ErrData != nil {
				errData = param.ErrData
			}
			if param.FinalizedAt != nil {
				finalizedAt = param.FinalizedAt
			}
			if param.ScheduledAt != nil {
				scheduledAt = param.ScheduledAt
			}

			batchParams.ID = append(batchParams.ID, param.ID)
			batchParams.Attempt = append(batchParams.Attempt, attempt)
			batchParams.ErrData = append(batchParams.ErrData, errData)
			batchParams.FinalizedAt = append(batchParams.FinalizedAt, finalizedAt)
			batchParams.MetadataDoMerge = append(batchParams.MetadataDoMerge, param.MetadataDoMerge)
			batchParams.MetadataUpdates = append(batchParams.MetadataUpdates, param.MetadataUpdates)
			batchParams.ScheduledAt = append(batchParams.ScheduledAt, scheduledAt)
			batchParams.State = append(batchParams.State, param.State)
		}

		return batchParams
	}

	t.Run("JobSetStateIfRunningMany_JobSetStateCompleted", func(t *testing.T) {
		t.Parallel()

		t.Run("CompletesARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("DoesNotCompleteARetryableJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRetryable),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.Nil(t, jobAfter.FinalizedAt)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("StoresMetadataUpdates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:  []byte(`{"foo":"baz", "something":"else"}`),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(job.ID, now, []byte(`{"a":"b", "foo":"bar"}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCompleted, jobAfter.State)
			require.JSONEq(t, `{"a":"b", "foo":"bar", "something":"else"}`, string(jobAfter.Metadata))
		})

		t.Run("UnknownJobIgnored", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			// The operation doesn't return anything like a "not found" in case
			// of an unknown job so that it doesn't fail in case a job is
			// deleted in the interim as a completer is trying to finalize it.
			_, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCompleted(0, time.Now().UTC(), nil)))
			require.NoError(t, err)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateErrored", func(t *testing.T) {
		t.Parallel()

		t.Run("SetsARunningJobToRetryable", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, now, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))

			// validate error payload:
			require.Len(t, jobAfter.Errors, 1)
			require.Equal(t, now, jobAfter.Errors[0].At)
			require.Equal(t, 1, jobAfter.Errors[0].Attempt)
			require.Equal(t, "fake error", jobAfter.Errors[0].Error)
			require.Equal(t, "foo.go:123\nbar.go:456", jobAfter.Errors[0].Trace)
		})

		t.Run("DoesNotTouchAlreadyRetryableJobWithNoMetadataUpdates", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})

		t.Run("UpdatesOnlyMetadataForAlreadyRetryableJobs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    []byte(`{"baz":"qux", "foo":"bar"}`),
				State:       ptrutil.Ptr(rivertype.JobStateRetryable),
				ScheduledAt: ptrutil.Ptr(now.Add(10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(
				riverdriver.JobSetStateErrorRetryable(job1.ID, now, makeErrPayload(t, now), []byte(`{"foo":"1", "output":{"a":"b"}}`)),
			))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateRetryable, jobAfter.State)
			require.JSONEq(t, `{"baz":"qux", "foo":"1", "output":{"a":"b"}}`, string(jobAfter.Metadata))
			require.Empty(t, jobAfter.Errors)
			require.Equal(t, job1.ScheduledAt, jobAfter.ScheduledAt)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job1.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRetryable, jobUpdated.State)
			require.JSONEq(t, `{"baz":"qux", "foo":"1", "output":{"a":"b"}}`, string(jobUpdated.Metadata))
			require.Empty(t, jobUpdated.Errors)
			require.Equal(t, job1.ScheduledAt, jobUpdated.ScheduledAt)
		})

		t.Run("SetsAJobWithCancelAttemptedAtToCancelled", func(t *testing.T) {
			// If a job has cancel_attempted_at in its metadata, it means that the user
			// tried to cancel the job with the Cancel API but that the job
			// finished/errored before the producer received the cancel notification.
			//
			// In this case, we want to move the job to cancelled instead of retryable
			// so that the job is not retried.
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata:    fmt.Appendf(nil, `{"cancel_attempted_at":"%s"}`, time.Now().UTC().Format(time.RFC3339)),
				State:       ptrutil.Ptr(rivertype.JobStateRunning),
				ScheduledAt: ptrutil.Ptr(now.Add(-10 * time.Second)),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateErrorRetryable(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.NotNil(t, jobAfter.FinalizedAt)
			// Loose assertion against FinalizedAt just to make sure it was set (it uses
			// the database's now() instead of a passed-in time):
			require.WithinDuration(t, time.Now().UTC(), *jobAfter.FinalizedAt, 2*time.Second)
			// ScheduledAt should not be touched:
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)

			// Errors should still be appended to:
			require.Len(t, jobAfter.Errors, 1)
			require.Contains(t, jobAfter.Errors[0].Error, "fake error")

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.WithinDuration(t, job.ScheduledAt, jobAfter.ScheduledAt, time.Microsecond)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateCancelled", func(t *testing.T) {
		t.Parallel()

		t.Run("CancelsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateCancelled(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateCancelled, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCancelled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateDiscarded", func(t *testing.T) {
		t.Parallel()

		t.Run("DiscardsARunningJob", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				State:        ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey:    []byte("unique-key"),
				UniqueStates: 0xFF,
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateDiscarded(job.ID, now, makeErrPayload(t, now), nil)))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, rivertype.JobStateDiscarded, jobAfter.State)
			require.WithinDuration(t, now, *jobAfter.FinalizedAt, time.Microsecond)
			require.Equal(t, "unique-key", string(jobAfter.UniqueKey))
			require.Equal(t, rivertype.JobStates(), jobAfter.UniqueStates)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateDiscarded, jobUpdated.State)
		})
	})

	t.Run("JobSetStateIfRunningMany_JobSetStateSnoozed", func(t *testing.T) {
		t.Parallel()

		t.Run("SnoozesARunningJob_WithNoPreexistingMetadata", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			snoozeUntil := now.Add(1 * time.Minute)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:   ptrutil.Ptr(5),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateSnoozed(job.ID, snoozeUntil, 4, []byte(`{"snoozes": 1}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, 4, jobAfter.Attempt)
			require.Equal(t, job.MaxAttempts, jobAfter.MaxAttempts)
			require.JSONEq(t, `{"snoozes": 1}`, string(jobAfter.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)
			require.WithinDuration(t, snoozeUntil, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, 4, jobUpdated.Attempt)
			require.Equal(t, job.MaxAttempts, jobUpdated.MaxAttempts)
			require.JSONEq(t, `{"snoozes": 1}`, string(jobUpdated.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})

		t.Run("SnoozesARunningJob_WithPreexistingMetadata", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			now := time.Now().UTC()
			snoozeUntil := now.Add(1 * time.Minute)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:   ptrutil.Ptr(5),
				State:     ptrutil.Ptr(rivertype.JobStateRunning),
				UniqueKey: []byte("unique-key"),
				Metadata:  []byte(`{"foo": "bar", "snoozes": 5}`),
			})

			jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(riverdriver.JobSetStateSnoozed(job.ID, snoozeUntil, 4, []byte(`{"snoozes": 6}`))))
			require.NoError(t, err)
			jobAfter := jobsAfter[0]
			require.Equal(t, 4, jobAfter.Attempt)
			require.Equal(t, job.MaxAttempts, jobAfter.MaxAttempts)
			require.JSONEq(t, `{"foo": "bar", "snoozes": 6}`, string(jobAfter.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobAfter.State)
			require.WithinDuration(t, snoozeUntil, jobAfter.ScheduledAt, time.Microsecond)

			jobUpdated, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: ""})
			require.NoError(t, err)
			require.Equal(t, 4, jobUpdated.Attempt)
			require.Equal(t, job.MaxAttempts, jobUpdated.MaxAttempts)
			require.JSONEq(t, `{"foo": "bar", "snoozes": 6}`, string(jobUpdated.Metadata))
			require.Equal(t, rivertype.JobStateScheduled, jobUpdated.State)
			require.Equal(t, "unique-key", string(jobUpdated.UniqueKey))
		})
	})

	t.Run("JobSetStateIfRunningMany_MultipleJobsAtOnce", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now().UTC()
		future := now.Add(10 * time.Second)

		job1 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job2 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})
		job3 := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: ptrutil.Ptr(rivertype.JobStateRunning)})

		jobsAfter, err := exec.JobSetStateIfRunningMany(ctx, setStateManyParams(
			riverdriver.JobSetStateCompleted(job1.ID, now, []byte(`{"a":"b"}`)),
			riverdriver.JobSetStateErrorRetryable(job2.ID, future, makeErrPayload(t, now), nil),
			riverdriver.JobSetStateCancelled(job3.ID, now, makeErrPayload(t, now), nil),
		))
		require.NoError(t, err)
		completedJob := jobsAfter[0]
		require.Equal(t, rivertype.JobStateCompleted, completedJob.State)
		require.WithinDuration(t, now, *completedJob.FinalizedAt, time.Microsecond)
		require.JSONEq(t, `{"a":"b"}`, string(completedJob.Metadata))

		retryableJob := jobsAfter[1]
		require.Equal(t, rivertype.JobStateRetryable, retryableJob.State)
		require.WithinDuration(t, future, retryableJob.ScheduledAt, time.Microsecond)
		// validate error payload:
		require.Len(t, retryableJob.Errors, 1)
		require.Equal(t, now, retryableJob.Errors[0].At)
		require.Equal(t, 1, retryableJob.Errors[0].Attempt)
		require.Equal(t, "fake error", retryableJob.Errors[0].Error)
		require.Equal(t, "foo.go:123\nbar.go:456", retryableJob.Errors[0].Trace)

		cancelledJob := jobsAfter[2]
		require.Equal(t, rivertype.JobStateCancelled, cancelledJob.State)
		require.WithinDuration(t, now, *cancelledJob.FinalizedAt, time.Microsecond)
	})

	t.Run("JobUpdate", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata: []byte(`{"key1":"val1"}`),
			})

			updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID:              job.ID,
				MetadataDoMerge: true,
				Metadata:        []byte(`{"key2":"val2"}`),
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"key1":"val1","key2":"val2"}`, string(updatedJob.Metadata))
		})

		t.Run("NoArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Metadata: []byte(`{"key1":"val1"}`),
			})

			updatedJob, err := exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
				ID: job.ID,
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"key1":"val1"}`, string(updatedJob.Metadata))
		})
	})

	t.Run("JobUpdateFull", func(t *testing.T) {
		t.Parallel()

		t.Run("AllArgs", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			now := time.Now().UTC()

			updatedJob, err := exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				ID:                  job.ID,
				AttemptDoUpdate:     true,
				Attempt:             7,
				AttemptedAtDoUpdate: true,
				AttemptedAt:         &now,
				AttemptedByDoUpdate: true,
				AttemptedBy:         []string{"worker1"},
				ErrorsDoUpdate:      true,
				Errors:              [][]byte{[]byte(`{"error":"message"}`)},
				FinalizedAtDoUpdate: true,
				FinalizedAt:         &now,
				MaxAttemptsDoUpdate: true,
				MaxAttempts:         99,
				MetadataDoUpdate:    true,
				Metadata:            []byte(`{"foo":"bar"}`),
				StateDoUpdate:       true,
				State:               rivertype.JobStateDiscarded,
			})
			require.NoError(t, err)
			require.Equal(t, 7, updatedJob.Attempt)
			require.WithinDuration(t, now, *updatedJob.AttemptedAt, bundle.driver.TimePrecision())
			require.Equal(t, []string{"worker1"}, updatedJob.AttemptedBy)
			require.Equal(t, "message", updatedJob.Errors[0].Error)
			require.WithinDuration(t, now, *updatedJob.FinalizedAt, bundle.driver.TimePrecision())
			require.Equal(t, 99, updatedJob.MaxAttempts)
			require.JSONEq(t, `{"foo":"bar"}`, string(updatedJob.Metadata))
			require.Equal(t, rivertype.JobStateDiscarded, updatedJob.State)
		})

		t.Run("NoArgs", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})

			updatedJob, err := exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{
				ID: job.ID,
			})
			require.NoError(t, err)
			require.Equal(t, job.Attempt, updatedJob.Attempt)
			require.Nil(t, updatedJob.AttemptedAt)
			require.Empty(t, updatedJob.AttemptedBy)
			require.Empty(t, updatedJob.Errors)
			require.Nil(t, updatedJob.FinalizedAt)
			require.Equal(t, job.MaxAttempts, updatedJob.MaxAttempts)
			require.Equal(t, job.Metadata, updatedJob.Metadata)
			require.Equal(t, job.State, updatedJob.State)
		})
	})
}
