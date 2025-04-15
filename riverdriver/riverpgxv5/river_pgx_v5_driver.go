// Package riverpgxv5 provides a River driver implementation for Pgx v5.
//
// This is currently the only supported driver for River and will therefore be
// used by all projects using River, but the code is organized this way so that
// other database packages can be supported in future River versions.
package riverpgxv5

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"

	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// Driver is an implementation of riverdriver.Driver for Pgx v5.
type Driver struct {
	dbPool   *pgxpool.Pool
	replacer sqlctemplate.Replacer
}

// New returns a new Pgx v5 River driver for use with River.
//
// It takes a pgxpool.Pool to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field. The pool
// must not be closed while associated River objects are running.
//
// The database pool may be nil. If it is, a client that it's sent into will not
// be able to start up (calls to Start will error) and the Insert and InsertMany
// functions will be disabled, but the transactional-variants InsertTx and
// InsertManyTx continue to function. This behavior may be particularly useful
// in testing so that inserts can be performed and verified on a test
// transaction that will be rolled back.
func New(dbPool *pgxpool.Pool) *Driver {
	return &Driver{
		dbPool: dbPool,
	}
}

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{templateReplaceWrapper{d.dbPool, &d.replacer}, d}
}

func (d *Driver) GetListener(schema string) riverdriver.Listener {
	return &Listener{dbPool: d.dbPool, schema: schema}
}

func (d *Driver) GetMigrationFS(line string) fs.FS {
	if line == riverdriver.MigrationLineMain {
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}
func (d *Driver) GetMigrationLines() []string { return []string{riverdriver.MigrationLineMain} }
func (d *Driver) HasPool() bool               { return d.dbPool != nil }
func (d *Driver) SupportsListener() bool      { return true }

func (d *Driver) UnwrapExecutor(tx pgx.Tx) riverdriver.ExecutorTx {
	// Allows UnwrapExecutor to be invoked even if driver is nil.
	var replacer *sqlctemplate.Replacer
	if d == nil {
		replacer = &sqlctemplate.Replacer{}
	} else {
		replacer = &d.replacer
	}

	return &ExecutorTx{Executor: Executor{templateReplaceWrapper{tx, replacer}, d}, tx: tx}
}

type Executor struct {
	dbtx   templateReplaceWrapper
	driver *Driver
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.dbtx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{templateReplaceWrapper{tx, &e.driver.replacer}, e.driver}, tx: tx}, nil
}

func (e *Executor) ColumnExists(ctx context.Context, params *riverdriver.ColumnExistsParams) (bool, error) {
	// Schema injection is a bit different on this one because we're querying a table with a schema name.
	schema := "CURRENT_SCHEMA"
	if params.Schema != "" {
		schema = "'" + params.Schema + "'"
	}
	ctx = sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}, nil)

	exists, err := dbsqlc.New().ColumnExists(ctx, e.dbtx, &dbsqlc.ColumnExistsParams{
		ColumnName: params.Column,
		TableName:  params.Table,
	})
	return exists, interpretError(err)
}

func (e *Executor) Exec(ctx context.Context, sql string) (struct{}, error) {
	_, err := e.dbtx.Exec(ctx, sql)
	return struct{}{}, interpretError(err)
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	cancelledAt, err := params.CancelAttemptedAt.MarshalJSON()
	if err != nil {
		return nil, err
	}

	job, err := dbsqlc.New().JobCancel(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobCancelParams{
		ID:                params.ID,
		CancelAttemptedAt: cancelledAt,
		ControlTopic:      params.ControlTopic,
		Schema:            pgtype.Text{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobCountByState(ctx context.Context, params *riverdriver.JobCountByStateParams) (int, error) {
	numJobs, err := dbsqlc.New().JobCountByState(schemaTemplateParam(ctx, params.Schema), e.dbtx, dbsqlc.RiverJobState(params.State))
	if err != nil {
		return 0, err
	}
	return int(numJobs), nil
}

func (e *Executor) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobDelete(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	if job.State == dbsqlc.RiverJobStateRunning {
		return nil, rivertype.ErrJobRunning
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	numDeleted, err := dbsqlc.New().JobDeleteBefore(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledFinalizedAtHorizon: params.CancelledFinalizedAtHorizon,
		CompletedFinalizedAtHorizon: params.CompletedFinalizedAtHorizon,
		DiscardedFinalizedAtHorizon: params.DiscardedFinalizedAtHorizon,
		Max:                         int64(params.Max),
	})
	return int(numDeleted), interpretError(err)
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetAvailable(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailableParams{
		AttemptedBy: params.ClientID,
		Max:         int32(min(params.Max, math.MaxInt32)), //nolint:gosec
		Now:         params.Now,
		Queue:       params.Queue,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByID(ctx context.Context, params *riverdriver.JobGetByIDParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobGetByID(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobGetByIDMany(ctx context.Context, params *riverdriver.JobGetByIDManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetByIDMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByKindMany(ctx context.Context, params *riverdriver.JobGetByKindManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetByKindMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Kind)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetStuck(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetStuckParams{Max: int32(min(params.Max, math.MaxInt32)), StuckHorizon: params.StuckHorizon}) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	insertJobsParams := &dbsqlc.JobInsertFastManyParams{
		Args:         make([][]byte, len(params.Jobs)),
		CreatedAt:    make([]time.Time, len(params.Jobs)),
		Kind:         make([]string, len(params.Jobs)),
		MaxAttempts:  make([]int16, len(params.Jobs)),
		Metadata:     make([][]byte, len(params.Jobs)),
		Priority:     make([]int16, len(params.Jobs)),
		Queue:        make([]string, len(params.Jobs)),
		ScheduledAt:  make([]time.Time, len(params.Jobs)),
		State:        make([]string, len(params.Jobs)),
		Tags:         make([]string, len(params.Jobs)),
		UniqueKey:    make([][]byte, len(params.Jobs)),
		UniqueStates: make([]pgtype.Bits, len(params.Jobs)),
	}
	now := time.Now().UTC()

	for i := range len(params.Jobs) {
		params := params.Jobs[i]

		createdAt := now
		if params.CreatedAt != nil {
			createdAt = *params.CreatedAt
		}

		scheduledAt := now
		if params.ScheduledAt != nil {
			scheduledAt = *params.ScheduledAt
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}

		defaultObject := []byte("{}")

		insertJobsParams.Args[i] = sliceutil.FirstNonEmpty(params.EncodedArgs, defaultObject)
		insertJobsParams.CreatedAt[i] = createdAt
		insertJobsParams.Kind[i] = params.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(params.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = sliceutil.FirstNonEmpty(params.Metadata, defaultObject)
		insertJobsParams.Priority[i] = int16(min(params.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = params.Queue
		insertJobsParams.ScheduledAt[i] = scheduledAt
		insertJobsParams.State[i] = string(params.State)
		insertJobsParams.Tags[i] = strings.Join(tags, ",")
		insertJobsParams.UniqueKey[i] = sliceutil.FirstNonEmpty(params.UniqueKey)
		insertJobsParams.UniqueStates[i] = pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0}
	}

	items, err := dbsqlc.New().JobInsertFastMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, insertJobsParams)
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

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	insertJobsParams := make([]*dbsqlc.JobInsertFastManyCopyFromParams, len(params.Jobs))
	now := time.Now().UTC()

	for i := range len(params.Jobs) {
		params := params.Jobs[i]

		createdAt := now
		if params.CreatedAt != nil {
			createdAt = *params.CreatedAt
		}

		metadata := params.Metadata
		if metadata == nil {
			metadata = []byte("{}")
		}

		scheduledAt := now
		if params.ScheduledAt != nil {
			scheduledAt = *params.ScheduledAt
		}

		tags := params.Tags
		if tags == nil {
			tags = []string{}
		}

		insertJobsParams[i] = &dbsqlc.JobInsertFastManyCopyFromParams{
			Args:         params.EncodedArgs,
			CreatedAt:    createdAt,
			Kind:         params.Kind,
			MaxAttempts:  int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
			Metadata:     metadata,
			Priority:     int16(min(params.Priority, math.MaxInt16)), //nolint:gosec
			Queue:        params.Queue,
			ScheduledAt:  scheduledAt,
			State:        dbsqlc.RiverJobState(params.State),
			Tags:         tags,
			UniqueKey:    params.UniqueKey,
			UniqueStates: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0},
		}
	}

	numInserted, err := dbsqlc.New().JobInsertFastManyCopyFrom(schemaTemplateParam(ctx, params.Schema), e.dbtx, insertJobsParams)
	if err != nil {
		return 0, interpretError(err)
	}

	return int(numInserted), nil
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobInsertFull(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobInsertFullParams{
		Attempt:      int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		AttemptedAt:  params.AttemptedAt,
		AttemptedBy:  params.AttemptedBy,
		Args:         params.EncodedArgs,
		CreatedAt:    params.CreatedAt,
		Errors:       params.Errors,
		FinalizedAt:  params.FinalizedAt,
		Kind:         params.Kind,
		MaxAttempts:  int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
		Metadata:     params.Metadata,
		Priority:     int16(min(params.Priority, math.MaxInt16)), //nolint:gosec
		Queue:        params.Queue,
		ScheduledAt:  params.ScheduledAt,
		State:        dbsqlc.RiverJobState(params.State),
		Tags:         params.Tags,
		UniqueKey:    params.UniqueKey,
		UniqueStates: pgtype.Bits{Bytes: []byte{params.UniqueStates}, Len: 8, Valid: params.UniqueStates != 0},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobList(ctx context.Context, params *riverdriver.JobListParams) ([]*rivertype.JobRow, error) {
	ctx = sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"order_by_clause": {Value: params.OrderByClause},
		"where_clause":    {Value: params.WhereClause},
	}, params.NamedArgs)

	jobs, err := dbsqlc.New().JobList(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Max)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	err := dbsqlc.New().JobRescueMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobRescueManyParams{
		ID:          params.ID,
		Error:       params.Error,
		FinalizedAt: params.FinalizedAt,
		ScheduledAt: params.ScheduledAt,
		State:       params.State,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobRetry(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.ID)
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	scheduleResults, err := dbsqlc.New().JobSchedule(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobScheduleParams{
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

func (e *Executor) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	setStateParams := &dbsqlc.JobSetStateIfRunningManyParams{
		IDs:                 params.ID,
		Attempt:             make([]int32, len(params.ID)),
		AttemptDoUpdate:     make([]bool, len(params.ID)),
		Errors:              params.ErrData,
		ErrorsDoUpdate:      make([]bool, len(params.ID)),
		FinalizedAt:         make([]time.Time, len(params.ID)),
		FinalizedAtDoUpdate: make([]bool, len(params.ID)),
		MetadataDoMerge:     make([]bool, len(params.ID)),
		MetadataUpdates:     make([][]byte, len(params.ID)),
		ScheduledAt:         make([]time.Time, len(params.ID)),
		ScheduledAtDoUpdate: make([]bool, len(params.ID)),
		State:               make([]string, len(params.ID)),
	}

	for i := range len(params.ID) {
		if params.Attempt[i] != nil {
			setStateParams.AttemptDoUpdate[i] = true
			setStateParams.Attempt[i] = int32(*params.Attempt[i]) //nolint:gosec
		}
		if params.ErrData[i] != nil {
			setStateParams.ErrorsDoUpdate[i] = true
		}
		if params.FinalizedAt[i] != nil {
			setStateParams.FinalizedAtDoUpdate[i] = true
			setStateParams.FinalizedAt[i] = *params.FinalizedAt[i]
		}
		if params.MetadataDoMerge[i] {
			setStateParams.MetadataDoMerge[i] = true
			setStateParams.MetadataUpdates[i] = params.MetadataUpdates[i]
		}
		if params.ScheduledAt[i] != nil {
			setStateParams.ScheduledAtDoUpdate[i] = true
			setStateParams.ScheduledAt[i] = *params.ScheduledAt[i]
		}
		setStateParams.State[i] = string(params.State[i])
	}

	jobs, err := dbsqlc.New().JobSetStateIfRunningMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, setStateParams)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSliceError(jobs, jobRowFromInternal)
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobUpdate(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobUpdateParams{
		ID:                  params.ID,
		AttemptedAtDoUpdate: params.AttemptedAtDoUpdate,
		Attempt:             int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		AttemptDoUpdate:     params.AttemptDoUpdate,
		AttemptedAt:         params.AttemptedAt,
		AttemptedBy:         params.AttemptedBy,
		AttemptedByDoUpdate: params.AttemptedByDoUpdate,
		ErrorsDoUpdate:      params.ErrorsDoUpdate,
		Errors:              params.Errors,
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
	numElectionsWon, err := dbsqlc.New().LeaderAttemptElect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptElectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptReelect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptReelectParams{
		LeaderID: params.LeaderID,
		TTL:      params.TTL,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	numDeleted, err := dbsqlc.New().LeaderDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx)
	if err != nil {
		return 0, interpretError(err)
	}
	return int(numDeleted), nil
}

func (e *Executor) LeaderGetElectedLeader(ctx context.Context, params *riverdriver.LeaderGetElectedLeaderParams) (*riverdriver.Leader, error) {
	leader, err := dbsqlc.New().LeaderGetElectedLeader(schemaTemplateParam(ctx, params.Schema), e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	leader, err := dbsqlc.New().LeaderInsert(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderInsertParams{
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
	numResigned, err := dbsqlc.New().LeaderResign(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderResignParams{
		LeaderID:        params.LeaderID,
		LeadershipTopic: params.LeadershipTopic,
		Schema:          pgtype.Text{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numResigned > 0, nil
}

func (e *Executor) MigrationDeleteAssumingMainMany(ctx context.Context, params *riverdriver.MigrationDeleteAssumingMainManyParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationDeleteAssumingMainMany(schemaTemplateParam(ctx, params.Schema), e.dbtx,
		sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) }))
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

func (e *Executor) MigrationDeleteByLineAndVersionMany(ctx context.Context, params *riverdriver.MigrationDeleteByLineAndVersionManyParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationDeleteByLineAndVersionMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.RiverMigrationDeleteByLineAndVersionManyParams{
		Line:    params.Line,
		Version: sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) }),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationGetAllAssumingMain(ctx context.Context, params *riverdriver.MigrationGetAllAssumingMainParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationGetAllAssumingMain(schemaTemplateParam(ctx, params.Schema), e.dbtx)
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

func (e *Executor) MigrationGetByLine(ctx context.Context, params *riverdriver.MigrationGetByLineParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationGetByLine(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Line)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertMany(ctx context.Context, params *riverdriver.MigrationInsertManyParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationInsertMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.RiverMigrationInsertManyParams{
		Line:    params.Line,
		Version: sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) }),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertManyAssumingMain(ctx context.Context, params *riverdriver.MigrationInsertManyAssumingMainParams) ([]*riverdriver.Migration, error) {
	migrations, err := dbsqlc.New().RiverMigrationInsertManyAssumingMain(schemaTemplateParam(ctx, params.Schema), e.dbtx,
		sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) }),
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
		Schema:  pgtype.Text{String: params.Schema, Valid: params.Schema != ""},
		Topic:   params.Topic,
	})
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	err := dbsqlc.New().PGAdvisoryXactLock(ctx, e.dbtx, key)
	return &struct{}{}, interpretError(err)
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueCreateOrSetUpdatedAt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtParams{
		Metadata:  params.Metadata,
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
	queues, err := dbsqlc.New().QueueDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueDeleteExpiredParams{
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

func (e *Executor) QueueGet(ctx context.Context, params *riverdriver.QueueGetParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueGet(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Name)
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueList(ctx context.Context, params *riverdriver.QueueListParams) ([]*rivertype.Queue, error) {
	internalQueues, err := dbsqlc.New().QueueList(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(min(params.Limit, math.MaxInt32))) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	queues := make([]*rivertype.Queue, len(internalQueues))
	for i, q := range internalQueues {
		queues[i] = queueFromInternal(q)
	}
	return queues, nil
}

func (e *Executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	res, err := dbsqlc.New().QueuePause(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Name)
	if err != nil {
		return interpretError(err)
	}
	if res.RowsAffected() == 0 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueResume(ctx context.Context, params *riverdriver.QueueResumeParams) error {
	res, err := dbsqlc.New().QueueResume(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Name)
	if err != nil {
		return interpretError(err)
	}
	if res.RowsAffected() == 0 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueUpdate(ctx context.Context, params *riverdriver.QueueUpdateParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueUpdate(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueUpdateParams{
		Metadata:         params.Metadata,
		MetadataDoUpdate: params.MetadataDoUpdate,
		Name:             params.Name,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) TableExists(ctx context.Context, params *riverdriver.TableExistsParams) (bool, error) {
	// Different from other operations because the schemaAndTable name is a parameter.
	schemaAndTable := params.Table
	if params.Schema != "" {
		schemaAndTable = params.Schema + "." + schemaAndTable
	}

	exists, err := dbsqlc.New().TableExists(ctx, e.dbtx, schemaAndTable)
	return exists, interpretError(err)
}

type ExecutorTx struct {
	Executor
	tx pgx.Tx
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

type Listener struct {
	conn   *pgx.Conn
	dbPool *pgxpool.Pool
	prefix string // schema with a dot on the end (very minor optimization)
	mu     sync.Mutex
	schema string
}

func (l *Listener) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn == nil {
		return nil
	}

	// Release below would take care of cleanup and potentially put the
	// connection back into rotation, but in case a Listen was invoked without a
	// subsequent Unlisten on the same topic, close the connection explicitly to
	// guarantee no other caller will receive a partially tainted connection.
	err := l.conn.Close(ctx)

	// Even in the event of an error, make sure conn is set back to nil so that
	// the listener can be reused.
	l.conn = nil

	return err
}

func (l *Listener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		return errors.New("connection already established")
	}

	poolConn, err := l.dbPool.Acquire(ctx)
	if err != nil {
		return err
	}

	// Use a configured schema if non-empty, otherwise try to select the current
	// schema based on `search_path`.
	schema := l.schema
	if schema == "" {
		if err := poolConn.QueryRow(ctx, "SELECT current_schema();").Scan(&schema); err != nil {
			poolConn.Release()
			return err
		}
		l.schema = schema
	}

	l.prefix = schema + "."
	// Assume full ownership of the conn so that it doesn't get released back to
	// the pool or auto-closed by the pool.
	l.conn = poolConn.Hijack()

	return nil
}

func (l *Listener) Listen(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "LISTEN \""+l.prefix+topic+"\"")
	return err
}

func (l *Listener) Ping(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.conn.Ping(ctx)
}

func (l *Listener) Schema() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.schema
}

func (l *Listener) Unlisten(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "UNLISTEN \""+l.prefix+topic+"\"")
	return err
}

func (l *Listener) WaitForNotification(ctx context.Context) (*riverdriver.Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	notification, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return nil, err
	}

	return &riverdriver.Notification{
		Topic:   strings.TrimPrefix(notification.Channel, l.prefix),
		Payload: notification.Payload,
	}, nil
}

type templateReplaceWrapper struct {
	dbtx interface {
		dbsqlc.DBTX
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	replacer *sqlctemplate.Replacer
}

func (w templateReplaceWrapper) Begin(ctx context.Context) (pgx.Tx, error) {
	return w.dbtx.Begin(ctx)
}

func (w templateReplaceWrapper) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	sql, args = w.replacer.Run(ctx, sql, args)
	return w.dbtx.Exec(ctx, sql, args...)
}

func (w templateReplaceWrapper) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	sql, args = w.replacer.Run(ctx, sql, args)
	return w.dbtx.Query(ctx, sql, args...)
}

func (w templateReplaceWrapper) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	sql, args = w.replacer.Run(ctx, sql, args)
	return w.dbtx.QueryRow(ctx, sql, args...)
}

func (w templateReplaceWrapper) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return w.dbtx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func interpretError(err error) error {
	if errors.Is(err, puddle.ErrClosedPool) {
		return riverdriver.ErrClosedPool
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

func jobRowFromInternal(internal *dbsqlc.RiverJob) (*rivertype.JobRow, error) {
	var attemptedAt *time.Time
	if internal.AttemptedAt != nil {
		t := internal.AttemptedAt.UTC()
		attemptedAt = &t
	}

	errors := make([]rivertype.AttemptError, len(internal.Errors))
	for i, rawError := range internal.Errors {
		if err := json.Unmarshal(rawError, &errors[i]); err != nil {
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
		EncodedArgs:  internal.Args,
		Errors:       errors,
		FinalizedAt:  finalizedAt,
		Kind:         internal.Kind,
		MaxAttempts:  max(int(internal.MaxAttempts), 0),
		Metadata:     internal.Metadata,
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
		Metadata:  internal.Metadata,
		Name:      internal.Name,
		PausedAt:  pausedAt,
		UpdatedAt: internal.UpdatedAt.UTC(),
	}
}

func schemaTemplateParam(ctx context.Context, schema string) context.Context {
	if schema != "" {
		schema += "."
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}, nil)
}
