package riverdriver

import (
	"testing"
	"time"

	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
)

func TestJobSetStateCancelled(t *testing.T) {
	t.Parallel()

	id := int64(1)
	finalizedAt := time.Now().Truncate(time.Second)
	errData := []byte("error occurred")
	result := JobSetStateCancelled(id, finalizedAt, errData)
	require.Equal(t, id, result.ID, "expected ID to match")
	require.NotNil(t, result.FinalizedAt, "FinalizedAt should not be nil")
	require.True(t, result.FinalizedAt.Equal(finalizedAt), "expected FinalizedAt to equal %v, got %v", finalizedAt, result.FinalizedAt)
	require.Equal(t, errData, result.ErrData, "expected ErrData to match")
	require.Equal(t, rivertype.JobStateCancelled, result.State, "expected State to match")
}
func TestJobSetStateCompleted(t *testing.T) {
	t.Parallel()

	t.Run("non-empty metadata", func(t *testing.T) {
		t.Parallel()

		id := int64(2)
		finalizedAt := time.Now().Truncate(time.Second)
		metadata := []byte("metadata change")
		result := JobSetStateCompleted(id, finalizedAt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.True(t, result.MetadataDoMerge, "expected MetadataDoMerge to be true when metadata is non-empty")
		require.Equal(t, metadata, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateCompleted, result.State)
	})

	t.Run("empty metadata", func(t *testing.T) {
		t.Parallel()

		id := int64(2)
		finalizedAt := time.Now().Truncate(time.Second)
		emptyMeta := []byte("")
		result := JobSetStateCompleted(id, finalizedAt, emptyMeta)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.False(t, result.MetadataDoMerge, "expected MetadataDoMerge to be false when metadata is empty")
		require.Equal(t, emptyMeta, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateCompleted, result.State)
	})
}
func TestJobSetStateDiscarded(t *testing.T) {
	t.Parallel()

	id := int64(3)
	finalizedAt := time.Now().Truncate(time.Second)
	errData := []byte("discard error")
	result := JobSetStateDiscarded(id, finalizedAt, errData)
	require.Equal(t, id, result.ID)
	require.NotNil(t, result.FinalizedAt)
	require.True(t, result.FinalizedAt.Equal(finalizedAt))
	require.Equal(t, errData, result.ErrData)
	require.Equal(t, rivertype.JobStateDiscarded, result.State)
}
func TestJobSetStateErrorAvailable(t *testing.T) {
	t.Parallel()

	id := int64(4)
	scheduledAt := time.Now().Truncate(time.Second)
	errData := []byte("error available")
	result := JobSetStateErrorAvailable(id, scheduledAt, errData)
	require.Equal(t, id, result.ID)
	require.NotNil(t, result.ScheduledAt)
	require.True(t, result.ScheduledAt.Equal(scheduledAt))
	require.Equal(t, errData, result.ErrData)
	require.Equal(t, rivertype.JobStateAvailable, result.State)
}
func TestJobSetStateErrorRetryable(t *testing.T) {
	t.Parallel()

	id := int64(5)
	scheduledAt := time.Now().Truncate(time.Second)
	errData := []byte("retryable error")
	result := JobSetStateErrorRetryable(id, scheduledAt, errData)
	require.Equal(t, id, result.ID)
	require.NotNil(t, result.ScheduledAt)
	require.True(t, result.ScheduledAt.Equal(scheduledAt))
	require.Equal(t, errData, result.ErrData)
	require.Equal(t, rivertype.JobStateRetryable, result.State)
}
func TestJobSetStateSnoozed(t *testing.T) {
	t.Parallel()

	t.Run("non-empty metadata", func(t *testing.T) {
		t.Parallel()
		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		metadata := []byte("snoozed metadata")
		result := JobSetStateSnoozed(id, scheduledAt, attempt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, result.Attempt)
		require.True(t, result.MetadataDoMerge, "expected MetadataDoMerge to be true when metadata is non-empty")
		require.Equal(t, metadata, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateScheduled, result.State)
	})
	t.Run("empty metadata", func(t *testing.T) {
		t.Parallel()

		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		emptyMeta := []byte("")
		result := JobSetStateSnoozed(id, scheduledAt, attempt, emptyMeta)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, result.Attempt)
		require.False(t, result.MetadataDoMerge, "expected MetadataDoMerge to be false when metadata is empty")
		require.Equal(t, emptyMeta, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateScheduled, result.State)
	})
}
func TestJobSetStateSnoozedAvailable(t *testing.T) {
	t.Parallel()

	t.Run("non-empty metadata", func(t *testing.T) {
		t.Parallel()

		id := int64(7)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 3
		metadata := []byte("snoozed available metadata")
		result := JobSetStateSnoozedAvailable(id, scheduledAt, attempt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, result.Attempt)
		if len(metadata) > 0 {
			require.True(t, result.MetadataDoMerge, "expected MetadataDoMerge to be true when metadata is non-empty")
		}
		require.Equal(t, metadata, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})
	t.Run("empty metadata", func(t *testing.T) {
		t.Parallel()

		id := int64(7)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 3
		emptyMeta := []byte("")
		result := JobSetStateSnoozedAvailable(id, scheduledAt, attempt, emptyMeta)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, result.Attempt)
		require.False(t, result.MetadataDoMerge, "expected MetadataDoMerge to be false when metadata is empty")
		require.Equal(t, emptyMeta, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})
}
