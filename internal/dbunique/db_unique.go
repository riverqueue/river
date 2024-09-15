package dbunique

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/util/hashutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

// When a job has specified unique options, but has not set the ByState
// parameter explicitly, this is the set of default states that are used to
// determine uniqueness. So for example, a new unique job may be inserted even
// if another job already exists, as long as that other job is set `cancelled`
// or `discarded`.
var defaultUniqueStates = []rivertype.JobState{ //nolint:gochecknoglobals
	rivertype.JobStateAvailable,
	rivertype.JobStateCompleted,
	rivertype.JobStatePending,
	rivertype.JobStateRetryable,
	rivertype.JobStateRunning,
	rivertype.JobStateScheduled,
}

var defaultUniqueStatesStrings = sliceutil.Map(defaultUniqueStates, func(s rivertype.JobState) string { return string(s) }) //nolint:gochecknoglobals

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

type UniqueOpts struct {
	ByArgs      bool
	ByPeriod    time.Duration
	ByQueue     bool
	ByState     []rivertype.JobState
	ExcludeKind bool
}

func (o *UniqueOpts) IsEmpty() bool {
	return !o.ByArgs &&
		o.ByPeriod == time.Duration(0) &&
		!o.ByQueue &&
		o.ByState == nil &&
		!o.ExcludeKind
}

func (o *UniqueOpts) StateBitmask() byte {
	states := defaultUniqueStates
	if len(o.ByState) > 0 {
		states = o.ByState
	}
	return UniqueStatesToBitmask(states)
}

func UniqueKey(timeGen baseservice.TimeGenerator, uniqueOpts *UniqueOpts, params *riverdriver.JobInsertFastParams) ([]byte, error) {
	uniqueKeyString, err := buildUniqueKeyString(timeGen, uniqueOpts, params)
	if err != nil {
		return nil, err
	}
	uniqueKeyHash := sha256.Sum256([]byte(uniqueKeyString))
	return uniqueKeyHash[:], nil
}

// Builds a unique key made up of the unique options in place. The key is hashed
// to become a value for `unique_key` in the fast insertion path, or hashed and
// used for an advisory lock on the slow insertion path.
func buildUniqueKeyString(timeGen baseservice.TimeGenerator, uniqueOpts *UniqueOpts, params *riverdriver.JobInsertFastParams) (string, error) {
	var sb strings.Builder

	if !uniqueOpts.ExcludeKind {
		sb.WriteString("&kind=" + params.Kind)
	}

	if uniqueOpts.ByArgs {
		var encodedArgsForUnique []byte
		// Get unique JSON keys from the JobArgs struct:
		uniqueFields, err := getSortedUniqueFieldsCached(params.Args)
		if err != nil {
			return "", err
		}

		if len(uniqueFields) > 0 {
			// Extract unique values from the EncodedArgs JSON
			uniqueValues := extractUniqueValues(params.EncodedArgs, uniqueFields)

			// Assemble the JSON object using bytes.Buffer
			// Better to overallocate a bit than to allocate multiple times, so just
			// assume we'll cap out at the length of the full encoded args.
			sortedJSONWithOnlyUniqueValues := make([]byte, 0, len(params.EncodedArgs))

			sjsonOpts := &sjson.Options{ReplaceInPlace: true}
			for i, key := range uniqueFields {
				if uniqueValues[i] == "undefined" {
					continue
				}
				sortedJSONWithOnlyUniqueValues, err = sjson.SetRawBytesOptions(sortedJSONWithOnlyUniqueValues, key, []byte(uniqueValues[i]), sjsonOpts)
				if err != nil {
					// Should not happen unless key was invalid
					return "", err
				}
			}
			encodedArgsForUnique = sortedJSONWithOnlyUniqueValues
		} else {
			// Use all keys from EncodedArgs sorted alphabetically
			keys := sliceutil.Map(gjson.GetBytes(params.EncodedArgs, "@keys").Array(), func(v gjson.Result) string { return v.String() })
			slices.Sort(keys)

			sortedJSON := make([]byte, 0, len(params.EncodedArgs))
			sortedJSON = append(sortedJSON, "{}"...)
			sjsonOpts := &sjson.Options{ReplaceInPlace: true}
			for _, key := range keys {
				sortedJSON, err = sjson.SetRawBytesOptions(sortedJSON, key, []byte(gjson.GetBytes(params.EncodedArgs, key).Raw), sjsonOpts)
				if err != nil {
					// Should not happen unless key was invalid
					return "", err
				}
			}
			encodedArgsForUnique = sortedJSON
		}

		sb.WriteString("&args=")
		sb.Write(encodedArgsForUnique)
	}

	if uniqueOpts.ByPeriod != time.Duration(0) {
		lowerPeriodBound := timeGen.NowUTC().Truncate(uniqueOpts.ByPeriod)
		sb.WriteString("&period=" + lowerPeriodBound.Format(time.RFC3339))
	}

	if uniqueOpts.ByQueue {
		sb.WriteString("&queue=" + params.Queue)
	}

	return sb.String(), nil
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

type UniqueInserter struct {
	baseservice.BaseService
	AdvisoryLockPrefix int32
}

func (i *UniqueInserter) JobInsert(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastParams, uniqueOpts *UniqueOpts) (*riverdriver.JobInsertFastResult, error) {
	// This should never get called with no unique options going forward.
	if uniqueOpts == nil || uniqueOpts.IsEmpty() {
		panic("UniqueInserter.JobInsert called with no unique options")
	}

	// Build a unique key for use in an advisory lock prefix:
	uniqueKey, doUniqueInsert := i.buildUniqueKey(params, uniqueOpts)
	if !doUniqueInsert {
		return insertNonUnique(ctx, exec, params)
	}

	// Sort so we can more easily compare against default state list.
	if uniqueOpts.ByState != nil {
		slices.Sort(uniqueOpts.ByState)
	}

	// Slow path: open a subtransaction, take an advisory lock, check to see if
	// a job with the given criteria exists, then either return an existing row
	// or insert a new one.

	advisoryLockHash := hashutil.NewAdvisoryLockHash(i.AdvisoryLockPrefix)
	advisoryLockHash.Write([]byte("unique_key"))
	advisoryLockHash.Write([]byte("kind=" + params.Kind))
	advisoryLockHash.Write([]byte(uniqueKey))

	getParams := i.buildGetParams(params, uniqueOpts)

	// Begin a subtransaction
	subExec, err := exec.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer subExec.Rollback(ctx)

	// The wrapping transaction should maintain snapshot consistency even if we
	// were to only have a SELECT + INSERT, but given that a conflict is
	// possible, obtain an advisory lock based on the parameters of the unique
	// job first, and have contending inserts wait for it. This is a synchronous
	// lock so we rely on context timeout in case something goes wrong and it's
	// blocking for too long.
	if _, err := subExec.PGAdvisoryXactLock(ctx, advisoryLockHash.Key()); err != nil {
		return nil, fmt.Errorf("error acquiring unique lock: %w", err)
	}

	existing, err := subExec.JobGetByKindAndUniqueProperties(ctx, getParams)
	if err != nil {
		if !errors.Is(err, rivertype.ErrNotFound) {
			return nil, fmt.Errorf("error getting unique job: %w", err)
		}
	}

	if existing != nil {
		// Insert skipped; returns an existing row.
		return &riverdriver.JobInsertFastResult{Job: existing, UniqueSkippedAsDuplicate: true}, nil
	}

	result, err := subExec.JobInsertFast(ctx, params)
	if err != nil {
		return nil, err
	}

	if err := subExec.Commit(ctx); err != nil {
		return nil, err
	}

	return result, nil
}

// Builds a unique key made up of the unique options in place. The key is hashed
// to become a value for `unique_key` in the fast insertion path, or hashed and
// used for an advisory lock on the slow insertion path.
func (i *UniqueInserter) buildUniqueKey(params *riverdriver.JobInsertFastParams, uniqueOpts *UniqueOpts) (string, bool) {
	var sb strings.Builder

	if uniqueOpts.ByArgs {
		sb.WriteString("&args=")
		sb.Write(params.EncodedArgs)
	}

	if uniqueOpts.ByPeriod != time.Duration(0) {
		lowerPeriodBound := i.Time.NowUTC().Truncate(uniqueOpts.ByPeriod)
		sb.WriteString("&period=" + lowerPeriodBound.Format(time.RFC3339))
	}

	if uniqueOpts.ByQueue {
		sb.WriteString("&queue=" + params.Queue)
	}

	{
		stateSet := defaultUniqueStatesStrings
		if len(uniqueOpts.ByState) > 0 {
			stateSet = sliceutil.Map(uniqueOpts.ByState, func(s rivertype.JobState) string { return string(s) })
			slices.Sort(stateSet)
		}

		sb.WriteString("&state=" + strings.Join(stateSet, ","))

		if !slices.Contains(stateSet, string(params.State)) {
			return "", false
		}
	}

	return sb.String(), true
}

// Builds get parameters suitable for looking up a unique job on the slow unique
// insertion path.
func (i *UniqueInserter) buildGetParams(params *riverdriver.JobInsertFastParams, uniqueOpts *UniqueOpts) *riverdriver.JobGetByKindAndUniquePropertiesParams {
	getParams := riverdriver.JobGetByKindAndUniquePropertiesParams{
		Kind: params.Kind,
	}

	if uniqueOpts.ByArgs {
		getParams.Args = params.EncodedArgs
		getParams.ByArgs = true
	}

	if uniqueOpts.ByPeriod != time.Duration(0) {
		lowerPeriodBound := i.Time.NowUTC().Truncate(uniqueOpts.ByPeriod)

		getParams.ByCreatedAt = true
		getParams.CreatedAtBegin = lowerPeriodBound
		getParams.CreatedAtEnd = lowerPeriodBound.Add(uniqueOpts.ByPeriod)
	}

	if uniqueOpts.ByQueue {
		getParams.ByQueue = true
		getParams.Queue = params.Queue
	}

	{
		stateSet := defaultUniqueStatesStrings
		if len(uniqueOpts.ByState) > 0 {
			stateSet = sliceutil.Map(uniqueOpts.ByState, func(s rivertype.JobState) string { return string(s) })
		}

		getParams.ByState = true
		getParams.State = stateSet
	}

	return &getParams
}

func insertNonUnique(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastParams) (*riverdriver.JobInsertFastResult, error) {
	result, err := exec.JobInsertFast(ctx, params)
	if err != nil {
		return nil, err
	}
	return result, nil
}
