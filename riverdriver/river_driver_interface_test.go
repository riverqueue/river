package riverdriver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestJobSetStateCancelled(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()
		id := int64(1)
		finalizedAt := time.Now().Truncate(time.Second)
		errData := []byte("error occurred")
		result := JobSetStateCancelled(id, finalizedAt, errData, nil)
		require.Equal(t, id, result.ID)
		require.Equal(t, errData, result.ErrData)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt), "expected FinalizedAt to equal %v, got %v", finalizedAt, result.FinalizedAt)
		require.Nil(t, result.MetadataUpdates)
		require.False(t, result.MetadataDoMerge)
		require.Equal(t, rivertype.JobStateCancelled, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(1)
		finalizedAt := time.Now().Truncate(time.Second)
		errData := []byte("error occurred")
		metadata := []byte(`{"key": "value"}`)
		result := JobSetStateCancelled(id, finalizedAt, errData, metadata)
		require.Equal(t, id, result.ID)
		require.Equal(t, errData, result.ErrData)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt), "expected FinalizedAt to equal %v, got %v", finalizedAt, result.FinalizedAt)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, rivertype.JobStateCancelled, result.State)
	})
}

func TestJobSetStateCompleted(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(2)
		finalizedAt := time.Now().Truncate(time.Second)
		result := JobSetStateCompleted(id, finalizedAt, nil)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.True(t, result.FinalizedAt.Equal(finalizedAt), "expected FinalizedAt to equal %v, got %v", finalizedAt, result.FinalizedAt)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateCompleted, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(2)
		finalizedAt := time.Now().Truncate(time.Second)
		metadata := []byte(`{"key": "value"}`)
		result := JobSetStateCompleted(id, finalizedAt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateCompleted, result.State)
	})
}

func TestJobSetStateDiscarded(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(3)
		finalizedAt := time.Now().Truncate(time.Second)
		errData := []byte("discard error")
		result := JobSetStateDiscarded(id, finalizedAt, errData, nil)
		require.Equal(t, id, result.ID)
		require.Equal(t, errData, result.ErrData)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.Equal(t, rivertype.JobStateDiscarded, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(3)
		finalizedAt := time.Now().Truncate(time.Second)
		errData := []byte("discard error")
		metadata := []byte(`{"key": "value"}`)
		result := JobSetStateDiscarded(id, finalizedAt, errData, metadata)
		require.Equal(t, id, result.ID)
		require.Equal(t, errData, result.ErrData)
		require.NotNil(t, result.FinalizedAt)
		require.True(t, result.FinalizedAt.Equal(finalizedAt))
		require.Equal(t, metadata, result.MetadataUpdates)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, rivertype.JobStateDiscarded, result.State)
	})
}

func TestJobSetStateErrorAvailable(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(4)
		scheduledAt := time.Now().Truncate(time.Second)
		errData := []byte("error available")
		result := JobSetStateErrorAvailable(id, scheduledAt, errData, nil)
		require.Equal(t, id, result.ID)
		require.Equal(t, errData, result.ErrData)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(4)
		scheduledAt := time.Now().Truncate(time.Second)
		errData := []byte("error available")
		metadata := []byte(`{"key": "value"}`)
		result := JobSetStateErrorAvailable(id, scheduledAt, errData, metadata)
		require.Equal(t, id, result.ID)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, errData, result.ErrData)
	})
}

func TestJobSetStateErrorRetryable(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(5)
		scheduledAt := time.Now().Truncate(time.Second)
		errData := []byte("retryable error")
		result := JobSetStateErrorRetryable(id, scheduledAt, errData, nil)
		require.Equal(t, id, result.ID)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, errData, result.ErrData)
		require.Equal(t, rivertype.JobStateRetryable, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(5)
		scheduledAt := time.Now().Truncate(time.Second)
		errData := []byte("retryable error")
		metadata := []byte(`{"key": "value"}`)
		result := JobSetStateErrorRetryable(id, scheduledAt, errData, metadata)
		require.Equal(t, id, result.ID)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, errData, result.ErrData)
	})
}

func TestJobSetStateSnoozed(t *testing.T) { //nolint:dupl
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		result := JobSetStateSnoozed(id, scheduledAt, attempt, nil)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, rivertype.JobStateScheduled, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()
		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		metadata := []byte("snoozed metadata")
		result := JobSetStateSnoozed(id, scheduledAt, attempt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, rivertype.JobStateScheduled, result.State)
	})
}

func TestJobSetStateSnoozedAvailable(t *testing.T) { //nolint:dupl
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(7)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 3
		result := JobSetStateSnoozedAvailable(id, scheduledAt, attempt, nil)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(7)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 3
		metadata := []byte("snoozed available metadata")
		result := JobSetStateSnoozedAvailable(id, scheduledAt, attempt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})
}
