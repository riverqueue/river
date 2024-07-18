package dbunique

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

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

type UniqueOpts struct {
	ByArgs   bool
	ByPeriod time.Duration
	ByQueue  bool
	ByState  []rivertype.JobState
}

func (o *UniqueOpts) IsEmpty() bool {
	return !o.ByArgs &&
		o.ByPeriod == time.Duration(0) &&
		!o.ByQueue &&
		o.ByState == nil
}

type UniqueInserter struct {
	baseservice.BaseService
	AdvisoryLockPrefix int32
}

func (i *UniqueInserter) JobInsert(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastParams, uniqueOpts *UniqueOpts) (*rivertype.JobInsertResult, error) {
	// With no unique options set, do a normal non-unique insert.
	if uniqueOpts == nil || uniqueOpts.IsEmpty() {
		return insertNonUnique(ctx, exec, params)
	}

	// Build a unique key for use in either the `(kind, unique_key)` index or in
	// an advisory lock prefix if we end up taking the slow path.
	uniqueKey, doUniqueInsert := i.buildUniqueKey(params, uniqueOpts)
	if !doUniqueInsert {
		return insertNonUnique(ctx, exec, params)
	}

	// Sort so we can more easily compare against default state list.
	if uniqueOpts.ByState != nil {
		slices.Sort(uniqueOpts.ByState)
	}

	// Fast path: as long as uniqueness uses the default set of lifecycle states
	// we can take the fast path wherein uniqueness is determined based on an
	// upsert to a unique index on `(kind, unique_key)`. This works because when
	// cancelling or discarding jobs the executor/completer will zero the job's
	// `unique_key` field, taking it out of consideration for future inserts
	// given the same unique opts.
	if uniqueOpts.ByState == nil || slices.Compare(defaultUniqueStates, uniqueOpts.ByState) == 0 {
		uniqueKeyHash := sha256.Sum256([]byte(uniqueKey))

		insertRes, err := exec.JobInsertUnique(ctx, &riverdriver.JobInsertUniqueParams{
			JobInsertFastParams: params,
			UniqueKey:           uniqueKeyHash[:],
		})
		if err != nil {
			return nil, err
		}

		return (*rivertype.JobInsertResult)(insertRes), nil
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
		return &rivertype.JobInsertResult{Job: existing, UniqueSkippedAsDuplicate: true}, nil
	}

	jobRow, err := subExec.JobInsertFast(ctx, params)
	if err != nil {
		return nil, err
	}

	if err := subExec.Commit(ctx); err != nil {
		return nil, err
	}

	return &rivertype.JobInsertResult{Job: jobRow}, nil
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

// Shared shortcut for inserting a row without uniqueness.
func insertNonUnique(ctx context.Context, exec riverdriver.Executor, params *riverdriver.JobInsertFastParams) (*rivertype.JobInsertResult, error) {
	jobRow, err := exec.JobInsertFast(ctx, params)
	if err != nil {
		return nil, err
	}
	return &rivertype.JobInsertResult{Job: jobRow}, nil
}
