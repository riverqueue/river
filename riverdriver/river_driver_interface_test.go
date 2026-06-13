package riverdriver

import (
	"strconv"
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonCancelled, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonCancelled, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonCompleted, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonCompleted, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
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
		require.Nil(t, result.Attempt)
		require.Equal(t, errData, result.ErrData)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
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
		require.Nil(t, result.Attempt)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonFailed, result.Reason)
		require.Equal(t, errData, result.ErrData)
	})
}

func TestJobSetStateIfRunningManyParams(t *testing.T) {
	t.Parallel()

	for _, capacity := range []int{0, 4} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			params := NewJobSetStateIfRunningManyParams("custom_schema", capacity)
			params.Append(JobSetStateCompleted(1, now, nil))
			require.Nil(t, params.ExpectedAttempt)
			require.Nil(t, params.ExpectedAttemptDoCheck)
			require.Nil(t, params.ExpectedAttemptedAt)

			guarded := JobSetStateErrorAvailable(2, now, []byte(`{}`), nil)
			guarded.ExpectedAttempt = new(3)
			guarded.ExpectedAttemptedAt = &now
			params.Append(guarded)
			params.Append(JobSetStateCompleted(3, now, nil))
			params.Append(guarded)

			require.Equal(t, []int64{1, 2, 3, 2}, params.ID)
			require.Equal(t, []int{0, 3, 0, 3}, params.ExpectedAttempt)
			require.Equal(t, []bool{false, true, false, true}, params.ExpectedAttemptDoCheck)
			require.Equal(t, []time.Time{{}, now, {}, now}, params.ExpectedAttemptedAt)
			require.Equal(t, "custom_schema", params.Schema)
		})
	}
}

func TestJobSetStateInterrupted(t *testing.T) {
	t.Parallel()

	t.Run("EmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		result := JobSetStateInterrupted(id, scheduledAt, attempt, nil)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.Nil(t, result.ErrData)
		require.False(t, result.MetadataDoMerge)
		require.Nil(t, result.MetadataUpdates)
		require.Equal(t, JobSetStateReasonInterrupted, result.Reason)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Empty(t, result.Schema)
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})

	t.Run("NonEmptyMetadata", func(t *testing.T) {
		t.Parallel()

		id := int64(6)
		scheduledAt := time.Now().Truncate(time.Second)
		attempt := 2
		metadata := []byte("interrupted metadata")
		result := JobSetStateInterrupted(id, scheduledAt, attempt, metadata)
		require.Equal(t, id, result.ID)
		require.NotNil(t, result.Attempt)
		require.Equal(t, attempt, *result.Attempt)
		require.Nil(t, result.ErrData)
		require.True(t, result.MetadataDoMerge)
		require.Equal(t, metadata, result.MetadataUpdates)
		require.Equal(t, JobSetStateReasonInterrupted, result.Reason)
		require.NotNil(t, result.ScheduledAt)
		require.True(t, result.ScheduledAt.Equal(scheduledAt))
		require.Empty(t, result.Schema)
		require.Equal(t, rivertype.JobStateAvailable, result.State)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonSnoozed, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonSnoozed, result.Reason)
		require.Equal(t, rivertype.JobStateScheduled, result.State)
	})
}

func TestMigrationLineMainTruncateTables(t *testing.T) {
	t.Parallel()

	t.Run("ZeroValueDoesNotPanic", func(t *testing.T) {
		t.Parallel()

		require.NotPanics(t, func() {
			tables := MigrationLineMainTruncateTables(0)
			require.NotEmpty(t, tables)
		})
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonSnoozed, result.Reason)
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
		require.Empty(t, result.Schema)
		require.Equal(t, JobSetStateReasonSnoozed, result.Reason)
		require.Equal(t, rivertype.JobStateAvailable, result.State)
	})
}
