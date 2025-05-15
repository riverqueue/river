package uniquestates

import (
	"slices"

	"github.com/riverqueue/river/rivertype"
)

func UniqueBitmaskToStates(mask byte) []rivertype.JobState {
	var states []rivertype.JobState

	for state, bitIndex := range jobStateBitPositions {
		bitPosition := 7 - (bitIndex % 8)
		if mask&(1<<bitPosition) != 0 {
			states = append(states, state)
		}
	}

	slices.Sort(states)
	return states
}

var jobStateBitPositions = map[rivertype.JobState]uint{ //nolint:gochecknoglobals
	rivertype.JobStateAvailable: 7,
	rivertype.JobStateCancelled: 6,
	rivertype.JobStateCompleted: 5,
	rivertype.JobStateDiscarded: 4,
	rivertype.JobStatePending:   3,
	rivertype.JobStateRetryable: 2,
	rivertype.JobStateRunning:   1,
	rivertype.JobStateScheduled: 0,
}

func UniqueStatesToBitmask(states []rivertype.JobState) byte {
	var val byte

	for _, state := range states {
		bitIndex, exists := jobStateBitPositions[state]
		if !exists {
			continue // Ignore unknown states
		}
		bitPosition := 7 - (bitIndex % 8)
		val |= 1 << bitPosition
	}

	return val
}
