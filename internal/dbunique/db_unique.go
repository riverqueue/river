package dbunique

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/util/hashutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// When a job has specified unique options, but has not set the ByState
// parameter explicitly, this is the set of default states that are used to
// determine uniqueness. So for example, a new unique job may be inserted even
// if another job already exists, as long as that other job is set `cancelled`
// or `discarded`.
var defaultUniqueStates = []string{ //nolint:gochecknoglobals
	string(rivertype.JobStateAvailable),
	string(rivertype.JobStateCompleted),
	string(rivertype.JobStateRunning),
	string(rivertype.JobStateRetryable),
	string(rivertype.JobStateScheduled),
}

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
	var tx riverdriver.ExecutorTx

	if uniqueOpts != nil && !uniqueOpts.IsEmpty() {
		// For uniqueness checks returns an advisory lock hash to use for lock,
		// parameters to check for an existing unique job with the same
		// properties, and a boolean indicating whether a uniqueness check
		// should be performed at all (in some cases the check can be skipped if
		// we can determine ahead of time that this insert will not violate
		// uniqueness conditions).
		buildUniqueParams := func() (*hashutil.AdvisoryLockHash, *riverdriver.JobGetByKindAndUniquePropertiesParams, bool) {
			advisoryLockHash := hashutil.NewAdvisoryLockHash(i.AdvisoryLockPrefix)
			advisoryLockHash.Write([]byte("unique_key"))
			advisoryLockHash.Write([]byte("kind=" + params.Kind))

			getParams := riverdriver.JobGetByKindAndUniquePropertiesParams{
				Kind: params.Kind,
			}

			if uniqueOpts.ByArgs {
				advisoryLockHash.Write([]byte("&args="))
				advisoryLockHash.Write(params.EncodedArgs)

				getParams.Args = params.EncodedArgs
				getParams.ByArgs = true
			}

			if uniqueOpts.ByPeriod != time.Duration(0) {
				lowerPeriodBound := i.Time.NowUTC().Truncate(uniqueOpts.ByPeriod)

				advisoryLockHash.Write([]byte("&period=" + lowerPeriodBound.Format(time.RFC3339)))

				getParams.ByCreatedAt = true
				getParams.CreatedAtBegin = lowerPeriodBound
				getParams.CreatedAtEnd = lowerPeriodBound.Add(uniqueOpts.ByPeriod)
			}

			if uniqueOpts.ByQueue {
				advisoryLockHash.Write([]byte("&queue=" + params.Queue))

				getParams.ByQueue = true
				getParams.Queue = params.Queue
			}

			{
				stateSet := defaultUniqueStates
				if len(uniqueOpts.ByState) > 0 {
					stateSet = sliceutil.Map(uniqueOpts.ByState, func(s rivertype.JobState) string { return string(s) })
				}

				advisoryLockHash.Write([]byte("&state=" + strings.Join(stateSet, ",")))

				if !slices.Contains(stateSet, string(params.State)) {
					return nil, nil, false
				}

				getParams.ByState = true
				getParams.State = stateSet
			}

			return advisoryLockHash, &getParams, true
		}

		if advisoryLockHash, getParams, doUniquenessCheck := buildUniqueParams(); doUniquenessCheck {
			// Begin a subtransaction
			exec, err := exec.Begin(ctx)
			if err != nil {
				return nil, err
			}
			defer exec.Rollback(ctx)

			// Make the subtransaction available at function scope so it can be
			// committed in cases where we insert a job.
			tx = exec

			// The wrapping transaction should maintain snapshot consistency
			// even if we were to only have a SELECT + INSERT, but given that a
			// conflict is possible, obtain an advisory lock based on the
			// parameters of the unique job first, and have contending inserts
			// wait for it. This is a synchronous lock so we rely on context
			// timeout in case something goes wrong and it's blocking for too
			// long.
			if _, err := exec.PGAdvisoryXactLock(ctx, advisoryLockHash.Key()); err != nil {
				return nil, fmt.Errorf("error acquiring unique lock: %w", err)
			}

			existing, err := exec.JobGetByKindAndUniqueProperties(ctx, getParams)
			if err != nil {
				if !errors.Is(err, rivertype.ErrNotFound) {
					return nil, fmt.Errorf("error getting unique job: %w", err)
				}
			}

			if existing != nil {
				// Insert skipped; returns an existing row.
				return &rivertype.JobInsertResult{Job: existing, UniqueSkippedAsDuplicate: true}, nil
			}
		}
	}

	jobRow, err := exec.JobInsertFast(ctx, params)
	if err != nil {
		return nil, err
	}

	if tx != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}

	return &rivertype.JobInsertResult{Job: jobRow}, nil
}
