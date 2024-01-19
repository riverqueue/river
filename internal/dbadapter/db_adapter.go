package dbadapter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/hashutil"
	"github.com/riverqueue/river/internal/util/ptrutil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/internal/util/valutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// When a job has specified unique options, but has not set the ByState
// parameter explicitly, this is the set of default states that are used to
// determine uniqueness. So for example, a new unique job may be inserted even
// if another job already exists, as long as that other job is set `cancelled`
// or `discarded`.
var defaultUniqueStates = []string{ //nolint:gochecknoglobals
	string(dbsqlc.JobStateAvailable),
	string(dbsqlc.JobStateCompleted),
	string(dbsqlc.JobStateRunning),
	string(dbsqlc.JobStateRetryable),
	string(dbsqlc.JobStateScheduled),
}

type JobToComplete struct {
	ID          int64
	FinalizedAt time.Time
}

// JobInsertParams are parameters for Adapter's `JobInsert*“ functions. They
// roughly reflect the properties of an inserted job, but only ones that are
// allowed to be used on input.
type JobInsertParams struct {
	EncodedArgs    []byte
	Kind           string
	MaxAttempts    int
	Metadata       []byte
	Priority       int
	Queue          string
	ScheduledAt    time.Time
	State          dbsqlc.JobState
	Tags           []string
	Unique         bool
	UniqueByArgs   bool
	UniqueByPeriod time.Duration
	UniqueByQueue  bool
	UniqueByState  []dbsqlc.JobState
}

type JobInsertResult struct {
	// Job is information about an inserted job.
	//
	// For an insertion that was skipped due to a duplicate, contains the job
	// that already existed.
	Job *dbsqlc.RiverJob

	// UniqueSkippedAsDuplicate indicates that the insert didn't occur because
	// it was a unique job, and another unique job within the unique parameters
	// was already in the database.
	UniqueSkippedAsDuplicate bool
}

type SortOrder int

const (
	SortOrderUnspecified SortOrder = iota
	SortOrderAsc
	SortOrderDesc
)

type JobListOrderBy struct {
	Expr  string
	Order SortOrder
}

type JobListParams struct {
	Conditions string
	LimitCount int32
	NamedArgs  map[string]any
	OrderBy    []JobListOrderBy
	Priorities []int16
	Queues     []string
	State      rivertype.JobState
}

// Adapter is an interface to the various database-level operations which River
// needs to operate. It's quite non-generic for the moment, but the idea is that
// it'd give us a way to implement access to non-Postgres databases, and may be
// reimplemented for pro features or exposed to users for customization.
//
// TODO: If exposing publicly, we must first make sure to add an intermediary
// layer between Adapter types and dbsqlc types. We return `dbsqlc.RiverJob` for
// expedience, but this should be converted to a more stable API if Adapter
// would be exported.
type Adapter interface {
	JobCancel(ctx context.Context, id int64) (*dbsqlc.RiverJob, error)
	JobCancelTx(ctx context.Context, tx pgx.Tx, id int64) (*dbsqlc.RiverJob, error)

	JobInsert(ctx context.Context, params *JobInsertParams) (*JobInsertResult, error)
	JobInsertTx(ctx context.Context, tx pgx.Tx, params *JobInsertParams) (*JobInsertResult, error)

	// TODO: JobInsertMany functions don't support unique jobs.
	JobInsertMany(ctx context.Context, params []*JobInsertParams) (int64, error)
	JobInsertManyTx(ctx context.Context, tx pgx.Tx, params []*JobInsertParams) (int64, error)

	JobGetAvailable(ctx context.Context, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)
	JobGetAvailableTx(ctx context.Context, tx pgx.Tx, queueName string, limit int32) ([]*dbsqlc.RiverJob, error)

	JobList(ctx context.Context, params JobListParams) ([]*dbsqlc.RiverJob, error)
	JobListTx(ctx context.Context, tx pgx.Tx, params JobListParams) ([]*dbsqlc.RiverJob, error)

	// JobSetStateIfRunning sets the state of a currently running job. Jobs which are not
	// running (i.e. which have already have had their state set to something
	// new through an explicit snooze or cancellation), are ignored.
	JobSetStateIfRunning(ctx context.Context, params *JobSetStateIfRunningParams) (*dbsqlc.RiverJob, error)

	// LeadershipAttemptElect attempts to elect a leader for the given name. The
	// bool alreadyElected indicates whether this is a potential reelection of
	// an already-elected leader. If the election is successful because there is
	// no leader or the previous leader expired, the provided leaderID will be
	// set as the new leader with a TTL of ttl.
	//
	// Returns whether this leader was successfully elected or an error if one
	// occurred.
	LeadershipAttemptElect(ctx context.Context, alreadyElected bool, name, leaderID string, ttl time.Duration) (bool, error)

	// LeadershipResign resigns any currently held leaderships for the given name
	// and leader ID.
	LeadershipResign(ctx context.Context, name, leaderID string) error
}

type StandardAdapter struct {
	baseservice.BaseService

	Config          *StandardAdapterConfig // exported so top-level package can test against it; unexport if adapterdb is ever made public
	deadlineTimeout time.Duration
	executor        dbutil.Executor
	queries         *dbsqlc.Queries
	workerName      string
}

type StandardAdapterConfig struct {
	// AdvisoryLockPrefix is a configurable 32-bit prefix that River will use
	// when generating any key to acquire a Postgres advisory lock.
	AdvisoryLockPrefix int32

	// Executor is a database executor to perform database operations with. In
	// non-test environments it's a database pool.
	Executor dbutil.Executor

	// DeadlineTimeout is a timeout used to set a context deadline for every
	// adapter operation.
	DeadlineTimeout time.Duration

	// WorkerName is a name to assign this worker.
	WorkerName string
}

// TODO: If `StandardAdapter` is ever exposed publicly, we should find a way to
// internalize archetype. Some options might be for `NewStandardAdapter` to
// return the `Adapter` interface instead of a concrete struct so that its
// properties aren't visible, and we could move base service initialization out
// to the client that accepts it so the user is never aware of its existence.
func NewStandardAdapter(archetype *baseservice.Archetype, config *StandardAdapterConfig) *StandardAdapter {
	return baseservice.Init(archetype, &StandardAdapter{
		Config:          config,
		deadlineTimeout: valutil.ValOrDefault(config.DeadlineTimeout, 5*time.Second),
		executor:        config.Executor,
		queries:         dbsqlc.New(),
		workerName:      config.WorkerName,
	})
}

func (a *StandardAdapter) JobCancel(ctx context.Context, id int64) (*dbsqlc.RiverJob, error) {
	return dbutil.WithTxV(ctx, a.executor, func(ctx context.Context, tx pgx.Tx) (*dbsqlc.RiverJob, error) {
		return a.JobCancelTx(ctx, tx, id)
	})
}

func (a *StandardAdapter) JobCancelTx(ctx context.Context, tx pgx.Tx, id int64) (*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	cancelledAt, err := a.TimeNowUTC().MarshalJSON()
	if err != nil {
		return nil, err
	}

	job, err := a.queries.JobCancel(ctx, a.executor, dbsqlc.JobCancelParams{
		CancelAttemptedAt: cancelledAt,
		ID:                id,
		JobControlTopic:   string(notifier.NotificationTopicJobControl),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, riverdriver.ErrNoRows
	}
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (a *StandardAdapter) JobInsert(ctx context.Context, params *JobInsertParams) (*JobInsertResult, error) {
	return dbutil.WithTxV(ctx, a.executor, func(ctx context.Context, tx pgx.Tx) (*JobInsertResult, error) {
		return a.JobInsertTx(ctx, tx, params)
	})
}

func (a *StandardAdapter) JobInsertTx(ctx context.Context, tx pgx.Tx, params *JobInsertParams) (*JobInsertResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if params.Unique {
		// For uniqueness checks returns an advisory lock hash to use for lock,
		// parameters to check for an existing unique job with the same
		// properties, and a boolean indicating whether a uniqueness check
		// should be performed at all (in some cases the check can be skipped if
		// we can determine ahead of time that this insert will not violate
		// uniqueness conditions).
		buildUniqueParams := func() (*hashutil.AdvisoryLockHash, *dbsqlc.JobGetByKindAndUniquePropertiesParams, bool) {
			advisoryLockHash := hashutil.NewAdvisoryLockHash(a.Config.AdvisoryLockPrefix)
			advisoryLockHash.Write([]byte("unique_key"))
			advisoryLockHash.Write([]byte("kind=" + params.Kind))

			getParams := dbsqlc.JobGetByKindAndUniquePropertiesParams{
				Kind: params.Kind,
			}

			if params.UniqueByArgs {
				advisoryLockHash.Write([]byte("&args="))
				advisoryLockHash.Write(params.EncodedArgs)

				getParams.Args = params.EncodedArgs
				getParams.ByArgs = true
			}

			if params.UniqueByPeriod != time.Duration(0) {
				lowerPeriodBound := a.TimeNowUTC().Truncate(params.UniqueByPeriod)

				advisoryLockHash.Write([]byte("&period=" + lowerPeriodBound.Format(time.RFC3339)))

				getParams.ByCreatedAt = true
				getParams.CreatedAtStart = lowerPeriodBound
				getParams.CreatedAtEnd = lowerPeriodBound.Add(params.UniqueByPeriod)
			}

			if params.UniqueByQueue {
				advisoryLockHash.Write([]byte("&queue=" + params.Queue))

				getParams.ByQueue = true
				getParams.Queue = params.Queue
			}

			{
				stateSet := defaultUniqueStates
				if len(params.UniqueByState) > 0 {
					stateSet = sliceutil.Map(params.UniqueByState, func(s dbsqlc.JobState) string { return string(s) })
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
			// The wrapping transaction should maintain snapshot consistency even if
			// we were to only have a SELECT + INSERT, but given that a conflict is
			// possible, obtain an advisory lock based on the parameters of the
			// unique job first, and have contending inserts wait for it. This is a
			// synchronous lock so we rely on context timeout in case something goes
			// wrong and it's blocking for too long.
			if err := a.queries.PGAdvisoryXactLock(ctx, tx, advisoryLockHash.Key()); err != nil {
				return nil, fmt.Errorf("error acquiring unique lock: %w", err)
			}

			existing, err := a.queries.JobGetByKindAndUniqueProperties(ctx, tx, *getParams)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					return nil, fmt.Errorf("error getting unique job: %w", err)
				}
			}

			if !existing.CreatedAt.IsZero() {
				return &JobInsertResult{Job: existing, UniqueSkippedAsDuplicate: true}, nil
			}
		}
	}

	var scheduledAt *time.Time
	if !params.ScheduledAt.IsZero() {
		scheduledAt = ptrutil.Ptr(params.ScheduledAt.UTC())
	}

	// TODO: maybe want to handle defaults (queue name, priority, etc) at a higher level
	//       so that it's applied for all adapters consistently.
	inserted, err := a.queries.JobInsert(ctx, tx, dbsqlc.JobInsertParams{
		Args:        params.EncodedArgs,
		CreatedAt:   ptrutil.Ptr(a.TimeNowUTC()),
		Kind:        params.Kind,
		MaxAttempts: int16(min(params.MaxAttempts, math.MaxInt16)),
		Metadata:    params.Metadata,
		Priority:    int16(min(params.Priority, math.MaxInt16)),
		Queue:       params.Queue,
		ScheduledAt: scheduledAt,
		State:       params.State,
		Tags:        params.Tags,
	})
	if err != nil {
		return nil, err
	}

	return &JobInsertResult{Job: inserted}, nil
}

func (a *StandardAdapter) JobInsertMany(ctx context.Context, params []*JobInsertParams) (int64, error) {
	return dbutil.WithTxV(ctx, a.executor, func(ctx context.Context, tx pgx.Tx) (int64, error) {
		return a.JobInsertManyTx(ctx, tx, params)
	})
}

func (a *StandardAdapter) JobInsertManyTx(ctx context.Context, tx pgx.Tx, params []*JobInsertParams) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	insertJobsParams := make([]dbsqlc.JobInsertManyParams, len(params))

	now := a.TimeNowUTC()

	for i := 0; i < len(params); i++ {
		params := params[i]

		metadata := params.Metadata
		if metadata == nil {
			metadata = []byte("{}")
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}
		scheduledAt := now
		if !params.ScheduledAt.IsZero() {
			scheduledAt = params.ScheduledAt.UTC()
		}

		insertJobsParams[i] = dbsqlc.JobInsertManyParams{
			Args:        params.EncodedArgs,
			Kind:        params.Kind,
			MaxAttempts: int16(min(params.MaxAttempts, math.MaxInt16)),
			Metadata:    metadata,
			Priority:    int16(min(params.Priority, math.MaxInt16)),
			Queue:       params.Queue,
			State:       params.State,
			ScheduledAt: scheduledAt,
			Tags:        tags,
		}
	}

	numInserted, err := a.queries.JobInsertMany(ctx, tx, insertJobsParams)
	if err != nil {
		return 0, fmt.Errorf("error inserting many jobs: %w", err)
	}

	return numInserted, nil
}

func (a *StandardAdapter) JobGetAvailable(ctx context.Context, queueName string, limit int32) ([]*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	tx, err := a.executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	jobs, err := a.JobGetAvailableTx(ctx, tx, queueName, limit)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *StandardAdapter) JobGetAvailableTx(ctx context.Context, tx pgx.Tx, queueName string, limit int32) ([]*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	jobs, err := a.queries.JobGetAvailable(ctx, tx, dbsqlc.JobGetAvailableParams{
		LimitCount: limit,
		Queue:      queueName,
		Worker:     a.workerName,
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *StandardAdapter) JobList(ctx context.Context, params JobListParams) ([]*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	tx, err := a.executor.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	jobs, err := a.JobListTx(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *StandardAdapter) JobListTx(ctx context.Context, tx pgx.Tx, params JobListParams) ([]*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	var conditionsBuilder strings.Builder

	orderBy := make([]dblist.JobListOrderBy, len(params.OrderBy))
	for i, o := range params.OrderBy {
		orderBy[i] = dblist.JobListOrderBy{
			Expr:  o.Expr,
			Order: dblist.SortOrder(o.Order),
		}
	}

	namedArgs := params.NamedArgs
	if namedArgs == nil {
		namedArgs = make(map[string]any)
	}

	if len(params.Queues) > 0 {
		namedArgs["queues"] = params.Queues
		conditionsBuilder.WriteString("queue = any(@queues::text[])")
		if params.Conditions != "" {
			conditionsBuilder.WriteString("\n  AND ")
		}
	}

	if params.Conditions != "" {
		conditionsBuilder.WriteString(params.Conditions)
	}

	jobs, err := dblist.JobList(ctx, tx, dblist.JobListParams{
		Conditions: conditionsBuilder.String(),
		LimitCount: params.LimitCount,
		NamedArgs:  namedArgs,
		OrderBy:    orderBy,
		Priorities: params.Priorities,
		State:      dbsqlc.JobState(params.State),
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// JobSetStateIfRunningParams are parameters to update the state of a currently running
// job. Use one of the constructors below to ensure a correct combination of
// parameters.
type JobSetStateIfRunningParams struct {
	ID          int64
	errData     []byte
	finalizedAt *time.Time
	maxAttempts *int
	scheduledAt *time.Time
	state       dbsqlc.JobState
}

func JobSetStateCancelled(id int64, finalizedAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, errData: errData, finalizedAt: &finalizedAt, state: dbsqlc.JobStateCancelled}
}

func JobSetStateCompleted(id int64, finalizedAt time.Time) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, finalizedAt: &finalizedAt, state: dbsqlc.JobStateCompleted}
}

func JobSetStateDiscarded(id int64, finalizedAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, errData: errData, finalizedAt: &finalizedAt, state: dbsqlc.JobStateDiscarded}
}

func JobSetStateErrorAvailable(id int64, scheduledAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, errData: errData, scheduledAt: &scheduledAt, state: dbsqlc.JobStateAvailable}
}

func JobSetStateErrorRetryable(id int64, scheduledAt time.Time, errData []byte) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, errData: errData, scheduledAt: &scheduledAt, state: dbsqlc.JobStateRetryable}
}

func JobSetStateSnoozed(id int64, scheduledAt time.Time, maxAttempts int) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, maxAttempts: &maxAttempts, scheduledAt: &scheduledAt, state: dbsqlc.JobStateScheduled}
}

func JobSetStateSnoozedAvailable(id int64, scheduledAt time.Time, maxAttempts int) *JobSetStateIfRunningParams {
	return &JobSetStateIfRunningParams{ID: id, maxAttempts: &maxAttempts, scheduledAt: &scheduledAt, state: dbsqlc.JobStateAvailable}
}

func (a *StandardAdapter) JobSetStateIfRunning(ctx context.Context, params *JobSetStateIfRunningParams) (*dbsqlc.RiverJob, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	return a.queries.JobSetStateIfRunning(ctx, a.executor, dbsqlc.JobSetStateIfRunningParams{
		ID:                  params.ID,
		ErrorDoUpdate:       params.errData != nil,
		Error:               params.errData,
		FinalizedAtDoUpdate: params.finalizedAt != nil,
		FinalizedAt:         params.finalizedAt,
		MaxAttemptsUpdate:   params.maxAttempts != nil,
		MaxAttempts:         int16(ptrutil.ValOrDefault(params.maxAttempts, 0)), // default never used
		ScheduledAtDoUpdate: params.scheduledAt != nil,
		ScheduledAt:         ptrutil.ValOrDefault(params.scheduledAt, time.Time{}), // default never used
		State:               params.state,
	})
}

func (a *StandardAdapter) LeadershipAttemptElect(ctx context.Context, alreadyElected bool, name, leaderID string, ttl time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	return dbutil.WithTxV(ctx, a.executor, func(ctx context.Context, tx pgx.Tx) (bool, error) {
		if err := a.queries.LeadershipDeleteExpired(ctx, tx, name); err != nil {
			return false, err
		}

		var (
			electionsWon int64
			err          error
		)
		if alreadyElected {
			electionsWon, err = a.queries.LeadershipAttemptReelect(ctx, tx, dbsqlc.LeadershipAttemptReelectParams{
				LeaderID: leaderID,
				Name:     name,
				TTL:      ttl,
			})
		} else {
			electionsWon, err = a.queries.LeadershipAttemptElect(ctx, tx, dbsqlc.LeadershipAttemptElectParams{
				LeaderID: leaderID,
				Name:     name,
				TTL:      ttl,
			})
		}
		if err != nil {
			return false, err
		}

		return electionsWon > 0, nil
	})
}

func (a *StandardAdapter) LeadershipResign(ctx context.Context, name, leaderID string) error {
	ctx, cancel := context.WithTimeout(ctx, a.deadlineTimeout)
	defer cancel()

	return a.queries.LeadershipResign(ctx, a.executor, dbsqlc.LeadershipResignParams{
		LeaderID:        leaderID,
		LeadershipTopic: string(notifier.NotificationTopicLeadership),
		Name:            name,
	})
}
