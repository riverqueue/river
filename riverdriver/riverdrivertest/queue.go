package riverdrivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivertype"
)

func exerciseQueue[TTx any](ctx context.Context, t *testing.T, executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
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

	t.Run("QueueCreateOrSetUpdatedAt", func(t *testing.T) {
		t.Parallel()

		mustUnmarshalJSON := func(t *testing.T, data []byte) map[string]any {
			t.Helper()

			var dataMap map[string]any
			require.NoError(t, json.Unmarshal(data, &dataMap))
			return dataMap
		}

		t.Run("InsertsANewQueueWithDefaultUpdatedAt", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			now := time.Now().UTC()
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata: metadata,
				Name:     "new-queue",
				Now:      &now,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, queue.CreatedAt, bundle.driver.TimePrecision())
			require.JSONEq(t, string(metadata), string(queue.Metadata))
			require.Equal(t, "new-queue", queue.Name)
			require.Nil(t, queue.PausedAt)
			require.WithinDuration(t, now, queue.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("InsertsANewQueueWithCustomPausedAt", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-5 * time.Minute)
			queue, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Name:     "new-queue",
				PausedAt: ptrutil.Ptr(now),
			})
			require.NoError(t, err)
			require.Equal(t, "new-queue", queue.Name)
			require.WithinDuration(t, now, *queue.PausedAt, bundle.driver.TimePrecision())
		})

		t.Run("UpdatesTheUpdatedAtOfExistingQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			metadata := []byte(`{"foo": "bar"}`)
			tBefore := time.Now().UTC()
			queueBefore, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  metadata,
				Name:      "updatable-queue",
				UpdatedAt: &tBefore,
			})
			require.NoError(t, err)
			require.WithinDuration(t, tBefore, queueBefore.UpdatedAt, time.Millisecond)

			tAfter := tBefore.Add(2 * time.Second)
			queueAfter, err := exec.QueueCreateOrSetUpdatedAt(ctx, &riverdriver.QueueCreateOrSetUpdatedAtParams{
				Metadata:  []byte(`{"other": "metadata"}`),
				Name:      "updatable-queue",
				UpdatedAt: &tAfter,
			})
			require.NoError(t, err)

			// unchanged:
			require.Equal(t, queueBefore.CreatedAt, queueAfter.CreatedAt)
			require.Equal(t, mustUnmarshalJSON(t, metadata), mustUnmarshalJSON(t, queueAfter.Metadata))
			require.Equal(t, "updatable-queue", queueAfter.Name)
			require.Nil(t, queueAfter.PausedAt)

			// Timestamp is bumped:
			require.WithinDuration(t, tAfter, queueAfter.UpdatedAt, time.Millisecond)
		})
	})

	t.Run("QueueDeleteExpired", func(t *testing.T) {
		t.Parallel()

		exec, _ := setup(ctx, t)

		now := time.Now()
		_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now)})
		queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-25 * time.Hour))})
		queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-26 * time.Hour))})
		queue4 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-48 * time.Hour))})
		_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{UpdatedAt: ptrutil.Ptr(now.Add(-23 * time.Hour))})

		horizon := now.Add(-24 * time.Hour)
		deletedQueueNames, err := exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
		require.NoError(t, err)

		// queue2 and queue3 should be deleted, with queue4 being skipped due to max of 2:
		require.Equal(t, []string{queue2.Name, queue3.Name}, deletedQueueNames)

		// Try again, make sure queue4 gets deleted this time:
		deletedQueueNames, err = exec.QueueDeleteExpired(ctx, &riverdriver.QueueDeleteExpiredParams{Max: 2, UpdatedAtHorizon: horizon})
		require.NoError(t, err)

		require.Equal(t, []string{queue4.Name}, deletedQueueNames)
	})

	t.Run("QueueGet", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

		queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name: queue.Name,
		})
		require.NoError(t, err)

		require.WithinDuration(t, queue.CreatedAt, queueFetched.CreatedAt, bundle.driver.TimePrecision())
		require.Equal(t, queue.Metadata, queueFetched.Metadata)
		require.Equal(t, queue.Name, queueFetched.Name)
		require.Nil(t, queueFetched.PausedAt)
		require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision())

		queueFetched, err = exec.QueueGet(ctx, &riverdriver.QueueGetParams{
			Name: "nonexistent-queue",
		})
		require.ErrorIs(t, err, rivertype.ErrNotFound)
		require.Nil(t, queueFetched)
	})

	t.Run("QueueList", func(t *testing.T) {
		t.Parallel()

		exec, bundle := setup(ctx, t)

		requireQueuesEqual := func(t *testing.T, target, actual *rivertype.Queue) {
			t.Helper()
			require.WithinDuration(t, target.CreatedAt, actual.CreatedAt, bundle.driver.TimePrecision())
			require.Equal(t, target.Metadata, actual.Metadata)
			require.Equal(t, target.Name, actual.Name)
			if target.PausedAt == nil {
				require.Nil(t, actual.PausedAt)
			} else {
				require.NotNil(t, actual.PausedAt)
				require.WithinDuration(t, *target.PausedAt, *actual.PausedAt, bundle.driver.TimePrecision())
			}
		}

		queues, err := exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 10,
		})
		require.NoError(t, err)
		require.Empty(t, queues)

		// Make queue1, already paused:
		queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`), PausedAt: ptrutil.Ptr(time.Now())})
		require.NoError(t, err)

		queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
		queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})

		queues, err = exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 2,
		})
		require.NoError(t, err)

		require.Len(t, queues, 2)
		requireQueuesEqual(t, queue1, queues[0])
		requireQueuesEqual(t, queue2, queues[1])

		queues, err = exec.QueueList(ctx, &riverdriver.QueueListParams{
			Max: 3,
		})
		require.NoError(t, err)

		require.Len(t, queues, 3)
		requireQueuesEqual(t, queue3, queues[2])
	})

	t.Run("QueueNameList", func(t *testing.T) {
		t.Parallel()

		t.Run("ListsQueuesInOrderWithMaxLimit", func(t *testing.T) { //nolint:dupl
			t.Parallel()

			exec, _ := setup(ctx, t)

			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_zzz")})
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_aaa")})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_bbb")})
			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("different_prefix_queue")})

			queueNames, err := exec.QueueNameList(ctx, &riverdriver.QueueNameListParams{
				After:   "queue2",
				Exclude: nil,
				Match:   "queue",
				Max:     2,
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{queue2.Name, queue3.Name}, queueNames) // sorted by name
		})

		t.Run("ExcludesQueueNamesInExcludeList", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_zzz")})
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_aaa")})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("queue_bbb")})

			queueNames, err := exec.QueueNameList(ctx, &riverdriver.QueueNameListParams{
				After:   "queue2",
				Exclude: []string{queue2.Name},
				Match:   "queue",
				Max:     2,
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{queue3.Name, queue1.Name}, queueNames)
		})

		t.Run("ListsQueuesWithSubstringMatch", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("prefix_queue")})
			_ = testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("another_queue")})
			queue3 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Name: ptrutil.Ptr("suffix_queue")})

			queueNames, err := exec.QueueNameList(ctx, &riverdriver.QueueNameListParams{
				After:   "",
				Exclude: nil,
				Match:   "fix",
				Max:     3,
				Schema:  "",
			})
			require.NoError(t, err)
			require.Equal(t, []string{queue1.Name, queue3.Name}, queueNames)
		})
	})

	t.Run("QueuePause", func(t *testing.T) {
		t.Parallel()

		t.Run("ExistingPausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC().Add(-5 * time.Minute)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				PausedAt:  &now,
				UpdatedAt: &now,
			})

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: queue.Name,
			}))
			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queueFetched.PausedAt)
			require.WithinDuration(t, *queue.PausedAt, *queueFetched.PausedAt, bundle.driver.TimePrecision()) // paused_at stays unchanged
			require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision()) // updated_at stays unchanged
		})

		t.Run("ExistingUnpausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})
			require.Nil(t, queue.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: queue.Name,
				Now:  &now,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.WithinDuration(t, now, *queueFetched.PausedAt, bundle.driver.TimePrecision())
			require.WithinDuration(t, now, queueFetched.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("NonExistentQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			err := exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: "queue1",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("AllQueuesExistingQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, nil)
			require.Nil(t, queue1.PausedAt)
			queue2 := testfactory.Queue(ctx, t, exec, nil)
			require.Nil(t, queue2.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))

			now := time.Now()

			queue1Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue1.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queue1Fetched.PausedAt)
			require.WithinDuration(t, now, *(queue1Fetched.PausedAt), 500*time.Millisecond)

			queue2Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue2.Name,
			})
			require.NoError(t, err)
			require.NotNil(t, queue2Fetched.PausedAt)
			require.WithinDuration(t, now, *(queue2Fetched.PausedAt), 500*time.Millisecond)
		})

		t.Run("AllQueuesNoQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))
		})
	})

	t.Run("QueueResume", func(t *testing.T) {
		t.Parallel()

		t.Run("ExistingPausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				PausedAt:  ptrutil.Ptr(now.Add(-5 * time.Minute)),
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: queue.Name,
				Now:  &now,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queueFetched.PausedAt)
			require.WithinDuration(t, now, queueFetched.UpdatedAt, bundle.driver.TimePrecision())
		})

		t.Run("ExistingUnpausedQueue", func(t *testing.T) {
			t.Parallel()

			exec, bundle := setup(ctx, t)

			now := time.Now().UTC()

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{
				UpdatedAt: ptrutil.Ptr(now.Add(-5 * time.Minute)),
			})

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: queue.Name,
			}))

			queueFetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queueFetched.PausedAt)
			require.WithinDuration(t, queue.UpdatedAt, queueFetched.UpdatedAt, bundle.driver.TimePrecision()) // updated_at stays unchanged
		})

		t.Run("NonExistentQueue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			err := exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: "queue1",
			})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})

		t.Run("AllQueuesExistingQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue1 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
			require.Nil(t, queue1.PausedAt)
			queue2 := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{})
			require.Nil(t, queue2.PausedAt)

			require.NoError(t, exec.QueuePause(ctx, &riverdriver.QueuePauseParams{
				Name: rivercommon.AllQueuesString,
			}))
			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: rivercommon.AllQueuesString,
			}))

			queue1Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue1.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queue1Fetched.PausedAt)

			queue2Fetched, err := exec.QueueGet(ctx, &riverdriver.QueueGetParams{
				Name: queue2.Name,
			})
			require.NoError(t, err)
			require.Nil(t, queue2Fetched.PausedAt)
		})

		t.Run("AllQueuesNoQueues", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			require.NoError(t, exec.QueueResume(ctx, &riverdriver.QueueResumeParams{
				Name: rivercommon.AllQueuesString,
			}))
		})
	})

	t.Run("QueueUpdate", func(t *testing.T) {
		t.Parallel()

		t.Run("UpdatesFieldsIfDoUpdateIsTrue", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

			updatedQueue, err := exec.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
				Metadata:         []byte(`{"baz": "qux"}`),
				MetadataDoUpdate: true,
				Name:             queue.Name,
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"baz": "qux"}`, string(updatedQueue.Metadata))
		})

		t.Run("DoesNotUpdateFieldsIfDoUpdateIsFalse", func(t *testing.T) {
			t.Parallel()

			exec, _ := setup(ctx, t)

			queue := testfactory.Queue(ctx, t, exec, &testfactory.QueueOpts{Metadata: []byte(`{"foo": "bar"}`)})

			var myInt int
			err := exec.QueryRow(ctx, "SELECT 1").Scan(&myInt)
			require.NoError(t, err)
			require.Equal(t, 1, myInt)

			updatedQueue, err := exec.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
				Metadata:         []byte(`{"baz": "qux"}`),
				MetadataDoUpdate: false,
				Name:             queue.Name,
			})
			require.NoError(t, err)
			require.JSONEq(t, `{"foo": "bar"}`, string(updatedQueue.Metadata))
		})
	})
}
