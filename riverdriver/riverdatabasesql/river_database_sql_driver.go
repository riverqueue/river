// Package riverdatabasesql bundles a River driver for Go's built-in
// database/sql, making it interoperable with ORMs like Bun and GORM. It's
// generally still powered under the hood by Pgx because it's the only
// maintained, fully functional Postgres driver in the Go ecosystem, but it uses
// some lib/pq constructs internally by virtue of being implemented with Sqlc.
package riverdatabasesql

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/dbsqlc"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/pgtypealias"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// Driver is an implementation of riverdriver.Driver for database/sql.
type Driver struct {
	dbPool *sql.DB
}

// New returns a new database/sql River driver for use with River.
//
// It takes an sql.DB to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field. The pool
// must not be closed while associated River objects are running.
func New(dbPool *sql.DB) *Driver {
	return &Driver{dbPool: dbPool}
}

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, d.dbPool}
}

func (d *Driver) GetListener() riverdriver.Listener { panic(riverdriver.ErrNotImplemented) }
func (d *Driver) GetMigrationFS(line string) fs.FS {
	if line == riverdriver.MigrationLineMain {
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}
func (d *Driver) GetMigrationLines() []string { return []string{riverdriver.MigrationLineMain} }
func (d *Driver) HasPool() bool               { return d.dbPool != nil }
func (d *Driver) SupportsListener() bool      { return false }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	return &ExecutorTx{Executor: Executor{nil, tx}, tx: tx}
}

type Executor struct {
	dbPool *sql.DB
	dbtx   dbsqlc.DBTX
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{nil, tx}, tx: tx}, nil
}

func (e *Executor) ColumnExists(ctx context.Context, tableName, columnName string) (bool, error) {
	exists, err := dbsqlc.New().ColumnExists(ctx, e.dbtx, &dbsqlc.ColumnExistsParams{
		ColumnName: columnName,
		TableName:  tableName,
	})
	return exists, interpretError(err)
}

func (e *Executor) Exec(ctx context.Context, sql string) (struct{}, error) {
	_, err := e.dbtx.ExecContext(ctx, sql)
	return struct{}{}, interpretError(err)
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	cancelledAt, err := params.CancelAttemptedAt.MarshalJSON()
	if err != nil {
		return nil, err
	}

	job, err := dbsqlc.New().JobCancel(ctx, e.dbtx, &dbsqlc.JobCancelParams{
		ID:                params.ID,
		CancelAttemptedAt: string(cancelledAt),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobCountByState(ctx context.Context, state rivertype.JobState) (int, error) {
	numJobs, err := dbsqlc.New().JobCountByState(ctx, e.dbtx, dbsqlc.RiverJobState(state))
	if err != nil {
		return 0, err
	}
	return int(numJobs), nil
}

func (e *Executor) JobDelete(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobDelete(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	if job.State == dbsqlc.RiverJobStateRunning {
		return nil, rivertype.ErrJobRunning
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	numDeleted, err := dbsqlc.New().JobDeleteBefore(ctx, e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledFinalizedAtHorizon: params.CancelledFinalizedAtHorizon,
		CompletedFinalizedAtHorizon: params.CompletedFinalizedAtHorizon,
		DiscardedFinalizedAtHorizon: params.DiscardedFinalizedAtHorizon,
		Max:                         int64(params.Max),
	})
	return int(numDeleted), interpretError(err)
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetAvailable(ctx, e.dbtx, &dbsqlc.JobGetAvailableParams{
		AttemptedBy: params.AttemptedBy,
		Max:         int32(min(params.Max, math.MaxInt32)), //nolint:gosec
		Queue:       params.Queue,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByID(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobGetByID(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByIDMany(ctx context.Context, id []int64) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetByIDMany(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByKindAndUniqueProperties(ctx context.Context, params *riverdriver.JobGetByKindAndUniquePropertiesParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobGetByKindAndUniqueProperties(ctx, e.dbtx, &dbsqlc.JobGetByKindAndUniquePropertiesParams{
		Args:           valutil.ValOrDefault(string(params.Args), "{}"),
		ByArgs:         params.ByArgs,
		ByCreatedAt:    params.ByCreatedAt,
		ByQueue:        params.ByQueue,
		ByState:        params.ByState,
		CreatedAtBegin: params.CreatedAtBegin,
		CreatedAtEnd:   params.CreatedAtEnd,
		Kind:           params.Kind,
		Queue:          params.Queue,
		State:          params.State,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByKindMany(ctx context.Context, kind []string) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetByKindMany(ctx, e.dbtx, kind)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetStuck(ctx, e.dbtx, &dbsqlc.JobGetStuckParams{Max: int32(min(params.Max, math.MaxInt32)), StuckHorizon: params.StuckHorizon}) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFast(ctx context.Context, params *riverdriver.JobInsertFastParams) (*riverdriver.JobInsertFastResult, error) {
	result, err := dbsqlc.New().JobInsertFast(ctx, e.dbtx, &dbsqlc.JobInsertFastParams{
		Args:         string(params.EncodedArgs),
		CreatedAt:    params.CreatedAt,
		Kind:         params.Kind,
		MaxAttempts:  int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
		Metadata:     valutil.ValOrDefault(string(params.Metadata), "{}"),
		Priority:     int16(min(params.Priority, math.MaxInt16)), //nolint:gosec
		Queue:        params.Queue,
		ScheduledAt:  params.ScheduledAt,
		State:        dbsqlc.RiverJobState(params.State),
		Tags:         params.Tags,
		UniqueKey:    params.UniqueKey,
		UniqueStates: pgtypealias.Bits{Bits: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0}},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	externalJob, err := jobRowFromInternal(&result.RiverJob)
	if err != nil {
		return nil, err
	}
	return &riverdriver.JobInsertFastResult{Job: externalJob, UniqueSkippedAsDuplicate: result.UniqueSkippedAsDuplicate}, nil
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params []*riverdriver.JobInsertFastParams) ([]*riverdriver.JobInsertFastResult, error) {
	insertJobsParams := &dbsqlc.JobInsertFastManyParams{
		Args:         make([]string, len(params)),
		Kind:         make([]string, len(params)),
		MaxAttempts:  make([]int16, len(params)),
		Metadata:     make([]string, len(params)),
		Priority:     make([]int16, len(params)),
		Queue:        make([]string, len(params)),
		ScheduledAt:  make([]time.Time, len(params)),
		State:        make([]string, len(params)),
		Tags:         make([]string, len(params)),
		UniqueKey:    make([][]byte, len(params)),
		UniqueStates: make([]pgtypealias.Bits, len(params)),
	}
	now := time.Now()

	for i := 0; i < len(params); i++ {
		params := params[i]

		scheduledAt := now
		if params.ScheduledAt != nil {
			scheduledAt = *params.ScheduledAt
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}

		defaultObject := "{}"

		insertJobsParams.Args[i] = valutil.ValOrDefault(string(params.EncodedArgs), defaultObject)
		insertJobsParams.Kind[i] = params.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(params.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = valutil.ValOrDefault(string(params.Metadata), defaultObject)
		insertJobsParams.Priority[i] = int16(min(params.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = params.Queue
		insertJobsParams.ScheduledAt[i] = scheduledAt
		insertJobsParams.State[i] = string(params.State)
		insertJobsParams.Tags[i] = strings.Join(tags, ",")
		insertJobsParams.UniqueKey[i] = sliceutil.DefaultIfEmpty(params.UniqueKey, nil)
		insertJobsParams.UniqueStates[i] = pgtypealias.Bits{Bits: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0}}
	}

	items, err := dbsqlc.New().JobInsertFastMany(ctx, e.dbtx, insertJobsParams)
	if err != nil {
		return nil, interpretError(err)
	}

	return mapSliceError(items, func(row *dbsqlc.JobInsertFastManyRow) (*riverdriver.JobInsertFastResult, error) {
		job, err := jobRowFromInternal(&row.RiverJob)
		if err != nil {
			return nil, err
		}
		return &riverdriver.JobInsertFastResult{Job: job, UniqueSkippedAsDuplicate: row.UniqueSkippedAsDuplicate}, nil
	})
}

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params []*riverdriver.JobInsertFastParams) (int, error) {
	insertJobsParams := &dbsqlc.JobInsertFastManyNoReturningParams{
		Args:         make([]string, len(params)),
		Kind:         make([]string, len(params)),
		MaxAttempts:  make([]int16, len(params)),
		Metadata:     make([]string, len(params)),
		Priority:     make([]int16, len(params)),
		Queue:        make([]string, len(params)),
		ScheduledAt:  make([]time.Time, len(params)),
		State:        make([]dbsqlc.RiverJobState, len(params)),
		Tags:         make([]string, len(params)),
		UniqueKey:    make([][]byte, len(params)),
		UniqueStates: make([]pgtypealias.Bits, len(params)),
	}
	now := time.Now()

	for i := 0; i < len(params); i++ {
		params := params[i]

		scheduledAt := now
		if params.ScheduledAt != nil {
			scheduledAt = *params.ScheduledAt
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}

		defaultObject := "{}"

		insertJobsParams.Args[i] = valutil.ValOrDefault(string(params.EncodedArgs), defaultObject)
		insertJobsParams.Kind[i] = params.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(params.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = valutil.ValOrDefault(string(params.Metadata), defaultObject)
		insertJobsParams.Priority[i] = int16(min(params.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = params.Queue
		insertJobsParams.ScheduledAt[i] = scheduledAt
		insertJobsParams.State[i] = dbsqlc.RiverJobState(params.State)
		insertJobsParams.Tags[i] = strings.Join(tags, ",")
		insertJobsParams.UniqueKey[i] = params.UniqueKey
		insertJobsParams.UniqueStates[i] = pgtypealias.Bits{Bits: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0}}
	}

	numInserted, err := dbsqlc.New().JobInsertFastManyNoReturning(ctx, e.dbtx, insertJobsParams)
	if err != nil {
		return 0, interpretError(err)
	}

	return int(numInserted), nil
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobInsertFull(ctx, e.dbtx, &dbsqlc.JobInsertFullParams{
		Attempt:      int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		AttemptedAt:  params.AttemptedAt,
		Args:         string(params.EncodedArgs),
		CreatedAt:    params.CreatedAt,
		Errors:       sliceutil.Map(params.Errors, func(e []byte) string { return string(e) }),
		FinalizedAt:  params.FinalizedAt,
		Kind:         params.Kind,
		MaxAttempts:  int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
		Metadata:     valutil.ValOrDefault(string(params.Metadata), "{}"),
		Priority:     int16(min(params.Priority, math.MaxInt16)), //nolint:gosec
		Queue:        params.Queue,
		ScheduledAt:  params.ScheduledAt,
		State:        dbsqlc.RiverJobState(params.State),
		Tags:         params.Tags,
		UniqueKey:    params.UniqueKey,
		UniqueStates: pgtypealias.Bits{Bits: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0}},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobList(ctx context.Context, query string, namedArgs map[string]any) ([]*rivertype.JobRow, error) {
	query, err := replaceNamed(query, namedArgs)
	if err != nil {
		return nil, err
	}

	rows, err := e.dbtx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*dbsqlc.RiverJob
	for rows.Next() {
		var i dbsqlc.RiverJob
		if err := rows.Scan(
			&i.ID,
			&i.Args,
			&i.Attempt,
			&i.AttemptedAt,
			pq.Array(&i.AttemptedBy),
			&i.CreatedAt,
			pq.Array(&i.Errors),
			&i.FinalizedAt,
			&i.Kind,
			&i.MaxAttempts,
			&i.Metadata,
			&i.Priority,
			&i.Queue,
			&i.State,
			&i.ScheduledAt,
			pq.Array(&i.Tags),
			&i.UniqueKey,
			&i.UniqueStates,
		); err != nil {
			return nil, err
		}
		items = append(items, &i)
	}
	if err := rows.Err(); err != nil {
		return nil, interpretError(err)
	}

	return mapSliceError(items, jobRowFromInternal)
}

func escapeSinglePostgresValue(value any) string {
	switch typedValue := value.(type) {
	case bool:
		return strconv.FormatBool(typedValue)
	case float32:
		// The `-1` arg tells Go to represent the number with as few digits as
		// possible. i.e. No unnecessary trailing zeroes.
		return strconv.FormatFloat(float64(typedValue), 'f', -1, 32)
	case float64:
		// The `-1` arg tells Go to represent the number with as few digits as
		// possible. i.e. No unnecessary trailing zeroes.
		return strconv.FormatFloat(typedValue, 'f', -1, 64)
	case int, int16, int32, int64, uint, uint16, uint32, uint64:
		return fmt.Sprintf("%d", value)
	case string:
		return "'" + strings.ReplaceAll(typedValue, "'", "''") + "'"
	default:
		// unreachable as long as new types aren't added to the switch in `replacedNamed` below
		panic("type not supported")
	}
}

func makePostgresArray[T any](values []T) string {
	var sb strings.Builder
	sb.WriteString("ARRAY[")

	for i, value := range values {
		sb.WriteString(escapeSinglePostgresValue(value))

		if i < len(values)-1 {
			sb.WriteString(",")
		}
	}

	sb.WriteString("]")
	return sb.String()
}

// `database/sql` has an `sql.Named` system that should theoretically work for
// named parameters, but neither Pgx or lib/pq implement it, so just use dumb
// string replacement given we're only injecting a very basic value anyway.
func replaceNamed(query string, namedArgs map[string]any) (string, error) {
	for name, value := range namedArgs {
		var escapedValue string

		switch typedValue := value.(type) {
		case bool, float32, float64, int, int16, int32, int64, string, uint, uint16, uint32, uint64:
			escapedValue = escapeSinglePostgresValue(value)

			// This is pretty awkward, but typedValue reverts back to `any` if
			// any of these conditions are combined together, and that prevents
			// us from ranging over the slice. Technically only `[]string` is
			// needed right now, but I included other slice types just so there
			// isn't a surprise later on.
		case []bool:
			escapedValue = makePostgresArray(typedValue)
		case []float32:
			escapedValue = makePostgresArray(typedValue)
		case []float64:
			escapedValue = makePostgresArray(typedValue)
		case []int:
			escapedValue = makePostgresArray(typedValue)
		case []int16:
			escapedValue = makePostgresArray(typedValue)
		case []int32:
			escapedValue = makePostgresArray(typedValue)
		case []int64:
			escapedValue = makePostgresArray(typedValue)
		case []string:
			escapedValue = makePostgresArray(typedValue)
		case []uint:
			escapedValue = makePostgresArray(typedValue)
		case []uint16:
			escapedValue = makePostgresArray(typedValue)
		case []uint32:
			escapedValue = makePostgresArray(typedValue)
		case []uint64:
			escapedValue = makePostgresArray(typedValue)
		default:
			return "", fmt.Errorf("named query parameter @%s is not a supported type", name)
		}

		newQuery := strings.Replace(query, "@"+name, escapedValue, 1)
		if newQuery == query {
			return "", fmt.Errorf("named query parameter @%s not found in query", name)
		}
		query = newQuery
	}

	return query, nil
}

func (e *Executor) JobListFields() string {
	return "id, args, attempt, attempted_at, attempted_by, created_at, errors, finalized_at, kind, max_attempts, metadata, priority, queue, state, scheduled_at, tags, unique_key, unique_states"
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	err := dbsqlc.New().JobRescueMany(ctx, e.dbtx, &dbsqlc.JobRescueManyParams{
		ID:          params.ID,
		Error:       sliceutil.Map(params.Error, func(e []byte) string { return string(e) }),
		FinalizedAt: params.FinalizedAt,
		ScheduledAt: params.ScheduledAt,
		State:       params.State,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobRetry(ctx, e.dbtx, id)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	scheduleResults, err := dbsqlc.New().JobSchedule(ctx, e.dbtx, &dbsqlc.JobScheduleParams{
		Max: int64(params.Max),
		Now: params.Now,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(scheduleResults, func(result *dbsqlc.JobScheduleRow) (*riverdriver.JobScheduleResult, error) {
		job, err := jobRowFromInternal(&result.RiverJob)
		if err != nil {
			return nil, err
		}
		return &riverdriver.JobScheduleResult{ConflictDiscarded: result.ConflictDiscarded, Job: *job}, nil
	})
}

func (e *Executor) JobSetCompleteIfRunningMany(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobSetCompleteIfRunningMany(ctx, e.dbtx, &dbsqlc.JobSetCompleteIfRunningManyParams{
		ID:          params.ID,
		FinalizedAt: params.FinalizedAt,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobSetStateIfRunning(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
	var maxAttempts int16
	if params.MaxAttempts != nil {
		maxAttempts = int16(min(*params.MaxAttempts, math.MaxInt16)) //nolint:gosec
	}

	job, err := dbsqlc.New().JobSetStateIfRunning(ctx, e.dbtx, &dbsqlc.JobSetStateIfRunningParams{
		ID:                  params.ID,
		ErrorDoUpdate:       params.ErrData != nil,
		Error:               valutil.ValOrDefault(string(params.ErrData), "{}"),
		FinalizedAtDoUpdate: params.FinalizedAt != nil,
		FinalizedAt:         params.FinalizedAt,
		MaxAttemptsUpdate:   params.MaxAttempts != nil,
		MaxAttempts:         maxAttempts,
		ScheduledAtDoUpdate: params.ScheduledAt != nil,
		ScheduledAt:         params.ScheduledAt,
		State:               dbsqlc.RiverJobState(params.State),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobUpdate(ctx, e.dbtx, &dbsqlc.JobUpdateParams{
		ID:                  params.ID,
		AttemptedAtDoUpdate: params.AttemptedAtDoUpdate,
		AttemptedAt:         params.AttemptedAt,
		AttemptDoUpdate:     params.AttemptDoUpdate,
		Attempt:             int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		ErrorsDoUpdate:      params.ErrorsDoUpdate,
		Errors:              sliceutil.Map(params.Errors, func(e []byte) string { return string(e) }),
		FinalizedAtDoUpdate: params.FinalizedAtDoUpdate,
		FinalizedAt:         params.FinalizedAt,
		StateDoUpdate:       params.StateDoUpdate,
		State:               dbsqlc.RiverJobState(params.State),
	})
	if err != nil {
		return nil, interpretError(err)
	}

	return jobRowFromInternal(job)
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptElect(ctx, e.dbtx, &dbsqlc.LeaderAttemptElectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptReelect(ctx, e.dbtx, &dbsqlc.LeaderAttemptReelectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context) (int, error) {
	numDeleted, err := dbsqlc.New().LeaderDeleteExpired(ctx, e.dbtx)
	if err != nil {
		return 0, interpretError(err)
	}
	return int(numDeleted), nil
}

func (e *Executor) LeaderGetElectedLeader(ctx context.Context) (*riverdriver.Leader, error) {
	leader, err := dbsqlc.New().LeaderGetElectedLeader(ctx, e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	leader, err := dbsqlc.New().LeaderInsert(ctx, e.dbtx, &dbsqlc.LeaderInsertParams{
		ElectedAt: params.ElectedAt,
		ExpiresAt: params.ExpiresAt,
		LeaderID:  params.LeaderID,
		TTL:       params.TTL,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	numResigned, err := dbsqlc.New().LeaderResign(ctx, e.dbtx, &dbsqlc.LeaderResignParams{
		LeaderID:        params.LeaderID,
		LeadershipTopic: params.LeadershipTopic,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numResigned > 0, nil
}

func (e *Executor) MigrationDeleteAssumingMainMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationDeleteAssumingMainMany(ctx, e.dbtx,
		sliceutil.Map(versions, func(v int) int64 { return int64(v) }))
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, func(internal *dbsqlc.RiverMigrationDeleteAssumingMainManyRow) *riverdriver.Migration {
		return &riverdriver.Migration{
			CreatedAt: internal.CreatedAt.UTC(),
			Line:      riverdriver.MigrationLineMain,
			Version:   int(internal.Version),
		}
	}), nil
}

func (e *Executor) MigrationDeleteByLineAndVersionMany(ctx context.Context, line string, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationDeleteByLineAndVersionMany(ctx, e.dbtx, &dbsqlc.RiverMigrationDeleteByLineAndVersionManyParams{
		Line:    line,
		Version: sliceutil.Map(versions, func(v int) int64 { return int64(v) }),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationGetAllAssumingMain(ctx context.Context) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationGetAllAssumingMain(ctx, e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, func(internal *dbsqlc.RiverMigrationGetAllAssumingMainRow) *riverdriver.Migration {
		return &riverdriver.Migration{
			CreatedAt: internal.CreatedAt.UTC(),
			Line:      riverdriver.MigrationLineMain,
			Version:   int(internal.Version),
		}
	}), nil
}

func (e *Executor) MigrationGetByLine(ctx context.Context, line string) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationGetByLine(ctx, e.dbtx, line)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertMany(ctx context.Context, line string, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationInsertMany(ctx, e.dbtx, &dbsqlc.RiverMigrationInsertManyParams{
		Line:    line,
		Version: sliceutil.Map(versions, func(v int) int64 { return int64(v) }),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertManyAssumingMain(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationInsertManyAssumingMain(ctx, e.dbtx,
		sliceutil.Map(versions, func(v int) int64 { return int64(v) }),
	)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, func(internal *dbsqlc.RiverMigrationInsertManyAssumingMainRow) *riverdriver.Migration {
		return &riverdriver.Migration{
			CreatedAt: internal.CreatedAt.UTC(),
			Line:      riverdriver.MigrationLineMain,
			Version:   int(internal.Version),
		}
	}), nil
}

func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	return dbsqlc.New().PGNotifyMany(ctx, e.dbtx, &dbsqlc.PGNotifyManyParams{
		Payload: params.Payload,
		Topic:   params.Topic,
	})
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	err := dbsqlc.New().PGAdvisoryXactLock(ctx, e.dbtx, key)
	return &struct{}{}, interpretError(err)
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueCreateOrSetUpdatedAt(ctx, e.dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtParams{
		Metadata:  valutil.ValOrDefault(string(params.Metadata), "{}"),
		Name:      params.Name,
		PausedAt:  params.PausedAt,
		UpdatedAt: params.UpdatedAt,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	queues, err := dbsqlc.New().QueueDeleteExpired(ctx, e.dbtx, &dbsqlc.QueueDeleteExpiredParams{
		Max:              int64(params.Max),
		UpdatedAtHorizon: params.UpdatedAtHorizon,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	queueNames := make([]string, len(queues))
	for i, q := range queues {
		queueNames[i] = q.Name
	}
	return queueNames, nil
}

func (e *Executor) QueueGet(ctx context.Context, name string) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueGet(ctx, e.dbtx, name)
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueList(ctx context.Context, limit int) ([]*rivertype.Queue, error) {
	internalQueues, err := dbsqlc.New().QueueList(ctx, e.dbtx, int32(min(limit, math.MaxInt32))) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	queues := make([]*rivertype.Queue, len(internalQueues))
	for i, q := range internalQueues {
		queues[i] = queueFromInternal(q)
	}
	return queues, nil
}

func (e *Executor) QueuePause(ctx context.Context, name string) error {
	res, err := dbsqlc.New().QueuePause(ctx, e.dbtx, name)
	if err != nil {
		return interpretError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return interpretError(err)
	}
	if rowsAffected == 0 && name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueResume(ctx context.Context, name string) error {
	res, err := dbsqlc.New().QueueResume(ctx, e.dbtx, name)
	if err != nil {
		return interpretError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return interpretError(err)
	}
	if rowsAffected == 0 && name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) TableExists(ctx context.Context, tableName string) (bool, error) {
	exists, err := dbsqlc.New().TableExists(ctx, e.dbtx, tableName)
	return exists, interpretError(err)
}

type ExecutorTx struct {
	Executor
	tx *sql.Tx
}

func (t *ExecutorTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	return (&ExecutorSubTx{Executor: Executor{nil, t.tx}, savepointNum: 0, single: &singleTransaction{}, tx: t.tx}).Begin(ctx)
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Commit()
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Rollback()
}

type ExecutorSubTx struct {
	Executor
	savepointNum int
	single       *singleTransaction
	tx           *sql.Tx
}

const savepointPrefix = "river_savepoint_"

func (t *ExecutorSubTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if err := t.single.begin(); err != nil {
		return nil, err
	}

	nextSavepointNum := t.savepointNum + 1
	_, err := t.Exec(ctx, fmt.Sprintf("SAVEPOINT %s%02d", savepointPrefix, nextSavepointNum))
	if err != nil {
		return nil, err
	}
	return &ExecutorSubTx{Executor: Executor{nil, t.tx}, savepointNum: nextSavepointNum, single: &singleTransaction{parent: t.single}, tx: t.tx}, nil
}

func (t *ExecutorSubTx) Commit(ctx context.Context) error {
	defer t.single.setDone()

	if t.single.done {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	// Release destroys a savepoint, keeping all the effects of commands that
	// were run within it (so it's effectively COMMIT for savepoints).
	_, err := t.Exec(ctx, fmt.Sprintf("RELEASE %s%02d", savepointPrefix, t.savepointNum))
	if err != nil {
		return err
	}

	return nil
}

func (t *ExecutorSubTx) Rollback(ctx context.Context) error {
	defer t.single.setDone()

	if t.single.done {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	_, err := t.Exec(ctx, fmt.Sprintf("ROLLBACK TO %s%02d", savepointPrefix, t.savepointNum))
	if err != nil {
		return err
	}

	return nil
}

func interpretError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

// Not strictly necessary, but a small struct designed to help us route out
// problems where `Begin` might be called multiple times on the same
// subtransaction, which would silently produce the wrong result.
type singleTransaction struct {
	done            bool
	parent          *singleTransaction
	subTxInProgress bool
}

func (t *singleTransaction) begin() error {
	if t.subTxInProgress {
		return errors.New("subtransaction already in progress")
	}
	t.subTxInProgress = true
	return nil
}

func (t *singleTransaction) setDone() {
	t.done = true
	if t.parent != nil {
		t.parent.subTxInProgress = false
	}
}

func jobRowFromInternal(internal *dbsqlc.RiverJob) (*rivertype.JobRow, error) {
	var attemptedAt *time.Time
	if internal.AttemptedAt != nil {
		t := internal.AttemptedAt.UTC()
		attemptedAt = &t
	}

	errors := make([]rivertype.AttemptError, len(internal.Errors))
	for i, rawError := range internal.Errors {
		if err := json.Unmarshal([]byte(rawError), &errors[i]); err != nil {
			return nil, err
		}
	}

	var finalizedAt *time.Time
	if internal.FinalizedAt != nil {
		t := internal.FinalizedAt.UTC()
		finalizedAt = &t
	}

	var uniqueStatesByte byte
	if internal.UniqueStates.Valid && len(internal.UniqueStates.Bytes) > 0 {
		uniqueStatesByte = internal.UniqueStates.Bytes[0]
	}

	return &rivertype.JobRow{
		ID:           internal.ID,
		Attempt:      max(int(internal.Attempt), 0),
		AttemptedAt:  attemptedAt,
		AttemptedBy:  internal.AttemptedBy,
		CreatedAt:    internal.CreatedAt.UTC(),
		EncodedArgs:  []byte(internal.Args),
		Errors:       errors,
		FinalizedAt:  finalizedAt,
		Kind:         internal.Kind,
		MaxAttempts:  max(int(internal.MaxAttempts), 0),
		Metadata:     []byte(internal.Metadata),
		Priority:     max(int(internal.Priority), 0),
		Queue:        internal.Queue,
		ScheduledAt:  internal.ScheduledAt.UTC(),
		State:        rivertype.JobState(internal.State),
		Tags:         internal.Tags,
		UniqueKey:    internal.UniqueKey,
		UniqueStates: dbunique.UniqueBitmaskToStates(uniqueStatesByte),
	}, nil
}

func leaderFromInternal(internal *dbsqlc.RiverLeader) *riverdriver.Leader {
	return &riverdriver.Leader{
		ElectedAt: internal.ElectedAt.UTC(),
		ExpiresAt: internal.ExpiresAt.UTC(),
		LeaderID:  internal.LeaderID,
	}
}

// mapSliceError manipulates a slice and transforms it to a slice of another
// type, returning the first error that occurred invoking the map function, if
// there was one.
func mapSliceError[T any, R any](collection []T, mapFunc func(T) (R, error)) ([]R, error) {
	if collection == nil {
		return nil, nil
	}

	result := make([]R, len(collection))

	for i, item := range collection {
		var err error
		result[i], err = mapFunc(item)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func migrationFromInternal(internal *dbsqlc.RiverMigration) *riverdriver.Migration {
	return &riverdriver.Migration{
		CreatedAt: internal.CreatedAt.UTC(),
		Line:      internal.Line,
		Version:   int(internal.Version),
	}
}

func queueFromInternal(internal *dbsqlc.RiverQueue) *rivertype.Queue {
	var pausedAt *time.Time
	if internal.PausedAt != nil {
		t := internal.PausedAt.UTC()
		pausedAt = &t
	}
	return &rivertype.Queue{
		CreatedAt: internal.CreatedAt.UTC(),
		Metadata:  []byte(internal.Metadata),
		Name:      internal.Name,
		PausedAt:  pausedAt,
		UpdatedAt: internal.UpdatedAt.UTC(),
	}
}
