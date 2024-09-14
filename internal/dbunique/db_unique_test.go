package dbunique

import (
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func TestDefaultUniqueStatesSorted(t *testing.T) {
	t.Parallel()

	states := slices.Clone(defaultUniqueStates)
	slices.Sort(states)
	require.Equal(t, states, defaultUniqueStates, "Default unique states should be sorted")
}

func TestUniqueKey(t *testing.T) {
	t.Parallel()

	// Fixed time for deterministic tests
	fixedTime := time.Date(2024, 9, 14, 12, 0, 0, 0, time.UTC)
	// timeGen := &MockTimeGenerator{FixedTime: fixedTime}
	stubSvc := &riversharedtest.TimeStub{}
	stubSvc.StubNowUTC(fixedTime)

	// Define test parameters
	params := &riverdriver.JobInsertFastParams{
		Kind:        "email",
		EncodedArgs: []byte(`{"to":"user@example.com","subject":"Test Email"}`),
		Queue:       "default",
	}

	shasum := func(s string) []byte {
		value := sha256.Sum256([]byte(s))
		return value[:]
	}

	tests := []struct {
		name        string
		uniqueOpts  *UniqueOpts
		params      *riverdriver.JobInsertFastParams
		expectedKey []byte
	}{
		{
			name:       "DefaultOptions",
			uniqueOpts: &UniqueOpts{},
			params:     params,
			expectedKey: shasum(
				"&kind=email&state=Available,Completed,Pending,Retryable,Running,Scheduled",
			),
		},
		{
			name: "ExcludeKind",
			uniqueOpts: &UniqueOpts{
				ExcludeKind: true,
			},
			params: params,
			expectedKey: shasum(
				"&state=Available,Completed,Pending,Retryable,Running,Scheduled",
			),
		},
		{
			name: "ByArgs",
			uniqueOpts: &UniqueOpts{
				ByArgs: true,
			},
			params: params,
			expectedKey: shasum(
				"&kind=email&args={\"to\":\"user@example.com\",\"subject\":\"Test Email\"}&state=Available,Completed,Pending,Retryable,Running,Scheduled",
			),
		},
		{
			name: "ByPeriod",
			uniqueOpts: &UniqueOpts{
				ByPeriod: 2 * time.Hour,
			},
			params: params,
			expectedKey: shasum(
				"&kind=email&period=2024-09-14T12:00:00Z&state=Available,Completed,Pending,Retryable,Running,Scheduled",
			),
		},
		{
			name: "ByQueue",
			uniqueOpts: &UniqueOpts{
				ByQueue: true,
			},
			params:      params,
			expectedKey: shasum("&kind=email&queue=default&state=Available,Completed,Pending,Retryable,Running,Scheduled"),
		},
		{
			name: "ByState",
			uniqueOpts: &UniqueOpts{
				ByState: []rivertype.JobState{
					rivertype.JobStateCancelled,
					rivertype.JobStateDiscarded,
				},
			},
			params:      params,
			expectedKey: shasum("&kind=email&state=Cancelled,Discarded"),
		},
		{
			name: "CombinationOptions",
			uniqueOpts: &UniqueOpts{
				ByArgs:      true,
				ByPeriod:    1 * time.Hour,
				ByQueue:     true,
				ByState:     []rivertype.JobState{rivertype.JobStateRunning, rivertype.JobStatePending},
				ExcludeKind: false,
			},
			params: params,
			expectedKey: shasum(
				"&kind=email&args={\"to\":\"user@example.com\",\"subject\":\"Test Email\"}&period=2024-09-14T12:00:00Z&queue=default&state=Pending,Running",
			),
		},
		{
			name: "EmptyUniqueOpts",
			uniqueOpts: &UniqueOpts{
				ByArgs:   false,
				ByPeriod: 0,
				ByQueue:  false,
				ByState:  nil,
			},
			params:      params,
			expectedKey: shasum("&kind=email&state=Available,Completed,Pending,Retryable,Running,Scheduled"),
		},
		{
			name: "EmptyEncodedArgs",
			uniqueOpts: &UniqueOpts{
				ByArgs: true,
			},
			params: &riverdriver.JobInsertFastParams{
				Kind:        "email",
				EncodedArgs: []byte{},
				Queue:       "default",
			},
			expectedKey: shasum("&kind=email&args=&state=Available,Completed,Pending,Retryable,Running,Scheduled"),
		},
		{
			name: "SpecialCharactersInKindAndQueue",
			uniqueOpts: &UniqueOpts{
				ByQueue: true,
			},
			params: &riverdriver.JobInsertFastParams{
				Kind:        "email&notification",
				EncodedArgs: []byte(`{"to":"user@example.com","subject":"Test Email"}`),
				Queue:       "default/queue",
			},
			expectedKey: shasum(
				"&kind=email&notification&queue=default/queue&state=Available,Completed,Pending,Retryable,Running,Scheduled",
			),
		},
		{
			name: "UnknownJobState",
			uniqueOpts: &UniqueOpts{
				ByState: []rivertype.JobState{
					"UnknownState",
					rivertype.JobStateRunning,
				},
			},
			params:      params,
			expectedKey: shasum("&kind=email&state=Running"),
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uniqueKey := UniqueKey(stubSvc, tt.uniqueOpts, tt.params)

			// Compare the generated unique key with the expected hash
			require.Equal(t, tt.expectedKey, uniqueKey, "UniqueKey hash does not match expected value")
		})
	}

	// Additional tests to ensure uniqueness
	t.Run("DifferentUniqueOptsProduceDifferentKeys", func(t *testing.T) {
		t.Parallel()

		opts1 := &UniqueOpts{ByArgs: true}
		opts2 := &UniqueOpts{ByQueue: true}

		key1 := UniqueKey(stubSvc, opts1, params)
		key2 := UniqueKey(stubSvc, opts2, params)

		require.NotEqual(t, key1, key2, "UniqueKeys should differ for different UniqueOpts")
	})

	t.Run("SameInputsProduceSameKey", func(t *testing.T) {
		t.Parallel()

		opts := &UniqueOpts{
			ByArgs:      true,
			ByPeriod:    30 * time.Minute,
			ByQueue:     true,
			ByState:     []rivertype.JobState{rivertype.JobStateRunning},
			ExcludeKind: false,
		}
		key1 := UniqueKey(stubSvc, opts, params)
		key2 := UniqueKey(stubSvc, opts, params)

		require.Equal(t, key1, key2, "UniqueKeys should be identical for the same inputs")
	})
}

func TestUniqueOptsIsEmpty(t *testing.T) {
	t.Parallel()

	emptyOpts := &UniqueOpts{}
	require.True(t, emptyOpts.IsEmpty(), "Empty unique options should be empty")

	require.False(t, (&UniqueOpts{ByArgs: true}).IsEmpty(), "Unique options with ByArgs should not be empty")
	require.False(t, (&UniqueOpts{ByPeriod: time.Minute}).IsEmpty(), "Unique options with ByPeriod should not be empty")
	require.False(t, (&UniqueOpts{ByQueue: true}).IsEmpty(), "Unique options with ByQueue should not be empty")
	require.False(t, (&UniqueOpts{ByState: []rivertype.JobState{rivertype.JobStateAvailable}}).IsEmpty(), "Unique options with ByState should not be empty")
	require.False(t, (&UniqueOpts{ExcludeKind: true}).IsEmpty(), "Unique options with ExcludeKind should not be empty")

	nonEmptyOpts := &UniqueOpts{
		ByArgs:      true,
		ByPeriod:    time.Minute,
		ByQueue:     true,
		ByState:     []rivertype.JobState{rivertype.JobStateAvailable},
		ExcludeKind: true,
	}
	require.False(t, nonEmptyOpts.IsEmpty(), "Non-empty unique options should not be empty")
}

func TestUniqueOptsStateBitmask(t *testing.T) {
	t.Parallel()

	emptyOpts := &UniqueOpts{}
	require.Equal(t, UniqueStatesToBitmask(defaultUniqueStates), emptyOpts.StateBitmask(), "Empty unique options should have default bitmask")

	otherStates := []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}
	nonEmptyOpts := &UniqueOpts{
		ByState: otherStates,
	}
	require.Equal(t, UniqueStatesToBitmask([]rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCompleted}), nonEmptyOpts.StateBitmask(), "Non-empty unique options should have correct bitmask")
}

func TestUniqueStatesToBitmask(t *testing.T) {
	t.Parallel()

	bitmask := UniqueStatesToBitmask(defaultUniqueStates)
	require.Equal(t, byte(0b11110101), bitmask, "Default unique states should be all set except cancelled and discarded")

	for state, position := range jobStateBitPositions {
		bitmask = UniqueStatesToBitmask([]rivertype.JobState{state})
		// Bit shifting uses postgres bit numbering with MSB on the right, so we
		// need to flip the position when shifting manually:
		require.Equal(t, byte(1<<(7-position)), bitmask, "Bitmask should be set for single state %s", state)
	}
}

// TODO(bgentry): tests for new functions/methods in dbunique
