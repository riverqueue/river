// Package riverdatabasesql bundles a River driver for Go's built-in
// database/sql, making it interoperable with ORMs like Bun and GORM. It's
// generally still powered under the hood by Pgx because it's the only
// maintained, fully functional Postgres driver in the Go ecosystem, but it uses
// some lib/pq constructs internally by virtue of being implemented with Sqlc.
package riverdatabasesql

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/savepointutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// Driver is an implementation of riverdriver.Driver for database/sql.
type Driver struct {
	dbPool   *sql.DB
	replacer sqlctemplate.Replacer
}

// New returns a new database/sql River driver for use with River.
//
// It takes an sql.DB to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field. The pool
// must not be closed while associated River objects are running.
func New(dbPool *sql.DB) *Driver {
	return &Driver{
		dbPool: dbPool,
	}
}

const argPlaceholder = "$"

func (d *Driver) ArgPlaceholder() string { return argPlaceholder }
func (d *Driver) DatabaseName() string   { return "postgres" }

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, templateReplaceWrapper{d.dbPool, &d.replacer}, d}
}

func (d *Driver) GetListener(params *riverdriver.GetListenenerParams) riverdriver.Listener {
	panic(riverdriver.ErrNotImplemented)
}

func (d *Driver) GetMigrationDefaultLines() []string { return []string{riverdriver.MigrationLineMain} }
func (d *Driver) GetMigrationFS(line string) fs.FS {
	if line == riverdriver.MigrationLineMain {
		return migrationFS
	}
	panic("migration line does not exist: " + line)
}
func (d *Driver) GetMigrationLines() []string { return []string{riverdriver.MigrationLineMain} }
func (d *Driver) GetMigrationTruncateTables(line string, version int) []string {
	if line == riverdriver.MigrationLineMain {
		return riverdriver.MigrationLineMainTruncateTables(version)
	}
	panic("migration line does not exist: " + line)
}

func (d *Driver) PoolIsSet() bool          { return d.dbPool != nil }
func (d *Driver) PoolSet(dbPool any) error { return riverdriver.ErrNotImplemented }

func (d *Driver) SQLFragmentColumnIn(column string, values any) (string, any, error) {
	// Identical to the Pgx implementation except for use of `pg.Array`.
	return fmt.Sprintf("%s = any(@%s)", column, column), pq.Array(values), nil
}

func (d *Driver) SupportsListener() bool       { return false }
func (d *Driver) SupportsListenNotify() bool   { return true }
func (d *Driver) TimePrecision() time.Duration { return time.Microsecond }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	// Allows UnwrapExecutor to be invoked even if driver is nil.
	var replacer *sqlctemplate.Replacer
	if d == nil {
		replacer = &sqlctemplate.Replacer{}
	} else {
		replacer = &d.replacer
	}

	return &ExecutorTx{Executor: Executor{nil, templateReplaceWrapper{tx, replacer}, d}, tx: tx}
}

func (d *Driver) UnwrapTx(execTx riverdriver.ExecutorTx) *sql.Tx { return execTx.(*ExecutorTx).tx } //nolint:forcetypeassert

type Executor struct {
	dbPool *sql.DB
	dbtx   templateReplaceWrapper
	driver *Driver
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{nil, templateReplaceWrapper{tx, &e.driver.replacer}, e.driver}, tx: tx}, nil
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

func (e *Executor) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := e.dbtx.ExecContext(ctx, sql, args...)
	return interpretError(err)
}

func (e *Executor) IndexDropIfExists(ctx context.Context, params *riverdriver.IndexDropIfExistsParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = params.Schema + "."
	}

	_, err := e.dbtx.ExecContext(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+maybeSchema+params.Index)
	return interpretError(err)
}

func (e *Executor) IndexExists(ctx context.Context, params *riverdriver.IndexExistsParams) (bool, error) {
	exists, err := dbsqlc.New().IndexExists(ctx, e.dbtx, &dbsqlc.IndexExistsParams{
		Index:  params.Index,
		Schema: sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return false, interpretError(err)
	}
	return exists, nil
}

func (e *Executor) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = params.Schema + "."
	}

	_, err := e.dbtx.ExecContext(ctx, "REINDEX INDEX CONCURRENTLY "+maybeSchema+params.Index)
	return interpretError(err)
}

func (e *Executor) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	rows, err := dbsqlc.New().IndexesExist(ctx, e.dbtx, &dbsqlc.IndexesExistParams{
		IndexNames: params.IndexNames,
		Schema:     sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return nil, interpretError(err)
	}

	exists := make(map[string]bool)
	for _, row := range rows {
		exists[row.IndexName] = row.Exists
	}
	return exists, nil
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	cancelledAt, err := params.CancelAttemptedAt.MarshalJSON()
	if err != nil {
		return nil, err
	}

	job, err := dbsqlc.New().JobCancel(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobCancelParams{
		ID:                params.ID,
		CancelAttemptedAt: string(cancelledAt),
		ControlTopic:      params.ControlTopic,
		Now:               params.Now,
		Schema:            sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobCountByAllStates(ctx context.Context, params *riverdriver.JobCountByAllStatesParams) (map[rivertype.JobState]int, error) {
	counts, err := dbsqlc.New().JobCountByAllStates(schemaTemplateParam(ctx, params.Schema), e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	countsMap := make(map[rivertype.JobState]int)
	for _, state := range rivertype.JobStates() {
		countsMap[state] = 0
	}
	for _, count := range counts {
		countsMap[rivertype.JobState(count.State)] = int(count.Count)
	}
	return countsMap, nil
}

func (e *Executor) JobCountByQueueAndState(ctx context.Context, params *riverdriver.JobCountByQueueAndStateParams) ([]*riverdriver.JobCountByQueueAndStateResult, error) {
	rows, err := dbsqlc.New().JobCountByQueueAndState(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.QueueNames)
	if err != nil {
		return nil, interpretError(err)
	}
	results := make([]*riverdriver.JobCountByQueueAndStateResult, len(rows))
	for i, row := range rows {
		results[i] = &riverdriver.JobCountByQueueAndStateResult{
			CountAvailable: row.CountAvailable,
			CountRunning:   row.CountRunning,
			Queue:          row.Queue,
		}
	}
	return results, nil
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
	res, err := dbsqlc.New().JobDeleteBefore(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledDoDelete:           params.CancelledDoDelete,
		CancelledFinalizedAtHorizon: params.CancelledFinalizedAtHorizon,
		CompletedDoDelete:           params.CompletedDoDelete,
		CompletedFinalizedAtHorizon: params.CompletedFinalizedAtHorizon,
		DiscardedDoDelete:           params.DiscardedDoDelete,
		DiscardedFinalizedAtHorizon: params.DiscardedFinalizedAtHorizon,
		Max:                         int64(params.Max),
		QueuesExcluded:              params.QueuesExcluded,
		QueuesIncluded:              params.QueuesIncluded,
	})
	if err != nil {
		return 0, interpretError(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, interpretError(err)
	}
	return int(rowsAffected), nil
}

func (e *Executor) JobDeleteMany(ctx context.Context, params *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error) {
	ctx = sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"order_by_clause": {Value: params.OrderByClause},
		"where_clause":    {Value: params.WhereClause},
	}, params.NamedArgs)

	jobs, err := dbsqlc.New().JobDeleteMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Max)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetAvailable(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailableParams{
		AttemptedBy:    params.ClientID,
		MaxAttemptedBy: int32(min(params.MaxAttemptedBy, math.MaxInt32)), //nolint:gosec
		MaxToLock:      int32(min(params.MaxToLock, math.MaxInt32)),      //nolint:gosec
		Now:            params.Now,
		Queue:          params.Queue,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
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
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetByKindMany(ctx context.Context, params *riverdriver.JobGetByKindManyParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetByKindMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Kind)
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetStuck(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetStuckParams{
		Max:          int32(min(params.Max, math.MaxInt32)), //nolint:gosec
		StuckHorizon: params.StuckHorizon,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	insertJobsParams := &dbsqlc.JobInsertFastManyParams{
		ID:           make([]int64, len(params.Jobs)),
		Args:         make([]string, len(params.Jobs)),
		CreatedAt:    make([]time.Time, len(params.Jobs)),
		Kind:         make([]string, len(params.Jobs)),
		MaxAttempts:  make([]int16, len(params.Jobs)),
		Metadata:     make([]string, len(params.Jobs)),
		Priority:     make([]int16, len(params.Jobs)),
		Queue:        make([]string, len(params.Jobs)),
		ScheduledAt:  make([]time.Time, len(params.Jobs)),
		State:        make([]string, len(params.Jobs)),
		Tags:         make([]string, len(params.Jobs)),
		UniqueKey:    make([][]byte, len(params.Jobs)),
		UniqueStates: make([]int32, len(params.Jobs)),
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

		insertJobsParams.ID[i] = ptrutil.ValOrDefault(params.ID, 0)
		insertJobsParams.Args[i] = cmp.Or(string(params.EncodedArgs), "{}")
		insertJobsParams.CreatedAt[i] = createdAt
		insertJobsParams.Kind[i] = params.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(params.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = cmp.Or(string(params.Metadata), "{}")
		insertJobsParams.Priority[i] = int16(min(params.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = params.Queue
		insertJobsParams.ScheduledAt[i] = scheduledAt
		insertJobsParams.State[i] = string(params.State)
		insertJobsParams.Tags[i] = strings.Join(tags, ",")
		insertJobsParams.UniqueKey[i] = params.UniqueKey
		insertJobsParams.UniqueStates[i] = int32(params.UniqueStates)
	}

	items, err := dbsqlc.New().JobInsertFastMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, insertJobsParams)
	if err != nil {
		return nil, interpretError(err)
	}

	return sliceutil.MapError(items, func(row *dbsqlc.JobInsertFastManyRow) (*riverdriver.JobInsertFastResult, error) {
		job, err := jobRowFromInternal(&row.RiverJob)
		if err != nil {
			return nil, err
		}
		return &riverdriver.JobInsertFastResult{Job: job, UniqueSkippedAsDuplicate: row.UniqueSkippedAsDuplicate}, nil
	})
}

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	insertJobsParams := &dbsqlc.JobInsertFastManyNoReturningParams{
		Args:         make([]string, len(params.Jobs)),
		CreatedAt:    make([]time.Time, len(params.Jobs)),
		Kind:         make([]string, len(params.Jobs)),
		MaxAttempts:  make([]int16, len(params.Jobs)),
		Metadata:     make([]string, len(params.Jobs)),
		Priority:     make([]int16, len(params.Jobs)),
		Queue:        make([]string, len(params.Jobs)),
		ScheduledAt:  make([]time.Time, len(params.Jobs)),
		State:        make([]dbsqlc.RiverJobState, len(params.Jobs)),
		Tags:         make([]string, len(params.Jobs)),
		UniqueKey:    make([][]byte, len(params.Jobs)),
		UniqueStates: make([]int32, len(params.Jobs)),
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

		insertJobsParams.Args[i] = cmp.Or(string(params.EncodedArgs), "{}")
		insertJobsParams.CreatedAt[i] = createdAt
		insertJobsParams.Kind[i] = params.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(params.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = cmp.Or(string(params.Metadata), "{}")
		insertJobsParams.Priority[i] = int16(min(params.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = params.Queue
		insertJobsParams.ScheduledAt[i] = scheduledAt
		insertJobsParams.State[i] = dbsqlc.RiverJobState(params.State)
		insertJobsParams.Tags[i] = strings.Join(tags, ",")
		insertJobsParams.UniqueKey[i] = params.UniqueKey
		insertJobsParams.UniqueStates[i] = int32(params.UniqueStates)
	}

	numInserted, err := dbsqlc.New().JobInsertFastManyNoReturning(schemaTemplateParam(ctx, params.Schema), e.dbtx, insertJobsParams)
	if err != nil {
		return 0, interpretError(err)
	}

	return int(numInserted), nil
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	var errors []string
	if len(params.Errors) > 0 {
		errors = sliceutil.Map(params.Errors, func(e []byte) string { return string(e) })
	}

	job, err := dbsqlc.New().JobInsertFull(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobInsertFullParams{
		Attempt:      int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		AttemptedAt:  params.AttemptedAt,
		AttemptedBy:  params.AttemptedBy,
		Args:         string(params.EncodedArgs),
		CreatedAt:    params.CreatedAt,
		Errors:       errors,
		FinalizedAt:  params.FinalizedAt,
		Kind:         params.Kind,
		MaxAttempts:  int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
		Metadata:     cmp.Or(string(params.Metadata), "{}"),
		Priority:     int16(min(params.Priority, math.MaxInt16)), //nolint:gosec
		Queue:        params.Queue,
		ScheduledAt:  params.ScheduledAt,
		State:        dbsqlc.RiverJobState(params.State),
		Tags:         params.Tags,
		UniqueKey:    string(params.UniqueKey),
		UniqueStates: int32(params.UniqueStates),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobInsertFullMany(ctx context.Context, params *riverdriver.JobInsertFullManyParams) ([]*rivertype.JobRow, error) {
	insertJobsParams := &dbsqlc.JobInsertFullManyParams{
		Args:         make([]string, len(params.Jobs)),
		Attempt:      make([]int16, len(params.Jobs)),
		AttemptedAt:  make([]time.Time, len(params.Jobs)),
		CreatedAt:    make([]time.Time, len(params.Jobs)),
		FinalizedAt:  make([]time.Time, len(params.Jobs)),
		Kind:         make([]string, len(params.Jobs)),
		MaxAttempts:  make([]int16, len(params.Jobs)),
		Metadata:     make([]string, len(params.Jobs)),
		Priority:     make([]int16, len(params.Jobs)),
		Queue:        make([]string, len(params.Jobs)),
		ScheduledAt:  make([]time.Time, len(params.Jobs)),
		State:        make([]string, len(params.Jobs)),
		Tags:         make([]string, len(params.Jobs)),
		UniqueKey:    make([]string, len(params.Jobs)),
		UniqueStates: make([]int32, len(params.Jobs)),
	}
	now := time.Now().UTC()

	for i := range len(params.Jobs) {
		jobParams := params.Jobs[i]

		insertJobsParams.Args[i] = cmp.Or(string(jobParams.EncodedArgs), "{}")
		insertJobsParams.Attempt[i] = int16(min(jobParams.Attempt, math.MaxInt16)) //nolint:gosec
		insertJobsParams.AttemptedAt[i] = ptrutil.ValOrDefault(jobParams.AttemptedAt, time.Time{})
		insertJobsParams.CreatedAt[i] = ptrutil.ValOrDefault(jobParams.CreatedAt, now)
		insertJobsParams.FinalizedAt[i] = ptrutil.ValOrDefault(jobParams.FinalizedAt, time.Time{})
		insertJobsParams.Kind[i] = jobParams.Kind
		insertJobsParams.MaxAttempts[i] = int16(min(jobParams.MaxAttempts, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Metadata[i] = string(jobParams.Metadata)
		insertJobsParams.Priority[i] = int16(min(jobParams.Priority, math.MaxInt16)) //nolint:gosec
		insertJobsParams.Queue[i] = jobParams.Queue
		insertJobsParams.ScheduledAt[i] = ptrutil.ValOrDefault(jobParams.ScheduledAt, now)
		insertJobsParams.State[i] = string(jobParams.State)
		insertJobsParams.Tags[i] = strings.Join(sliceutil.FirstNonEmpty(jobParams.Tags, []string{}), ",")
		insertJobsParams.UniqueKey[i] = string(jobParams.UniqueKey)
		insertJobsParams.UniqueStates[i] = int32(jobParams.UniqueStates)
	}

	items, err := dbsqlc.New().JobInsertFullMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, insertJobsParams)
	if err != nil {
		return nil, interpretError(err)
	}

	return sliceutil.MapError(items, jobRowFromInternal)
}

func (e *Executor) JobKindList(ctx context.Context, params *riverdriver.JobKindListParams) ([]string, error) {
	kinds, err := dbsqlc.New().JobKindList(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobKindListParams{
		After:   params.After,
		Exclude: params.Exclude,
		Match:   params.Match,
		Max:     int32(params.Max), //nolint:gosec
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return kinds, nil
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
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	if err := dbsqlc.New().JobRescueMany(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobRescueManyParams{
		ID:          params.ID,
		Error:       sliceutil.Map(params.Error, func(e []byte) string { return string(e) }),
		FinalizedAt: sliceutil.Map(params.FinalizedAt, func(t *time.Time) time.Time { return ptrutil.ValOrDefault(t, time.Time{}) }),
		ScheduledAt: params.ScheduledAt,
		State:       params.State,
	}); err != nil {
		return nil, interpretError(err)
	}
	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	job, err := dbsqlc.New().JobRetry(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobRetryParams{
		ID:  params.ID,
		Now: params.Now,
	})
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
	return sliceutil.MapError(scheduleResults, func(result *dbsqlc.JobScheduleRow) (*riverdriver.JobScheduleResult, error) {
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
		Errors:              make([]string, len(params.ID)),
		ErrorsDoUpdate:      make([]bool, len(params.ID)),
		FinalizedAt:         make([]time.Time, len(params.ID)),
		FinalizedAtDoUpdate: make([]bool, len(params.ID)),
		MetadataDoMerge:     make([]bool, len(params.ID)),
		MetadataUpdates:     make([]string, len(params.ID)),
		ScheduledAt:         make([]time.Time, len(params.ID)),
		ScheduledAtDoUpdate: make([]bool, len(params.ID)),
		State:               make([]string, len(params.ID)),
	}

	const defaultObject = "{}"

	for i := range len(params.ID) {
		setStateParams.Errors[i] = cmp.Or(string(params.ErrData[i]), defaultObject)
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
			setStateParams.MetadataUpdates[i] = string(params.MetadataUpdates[i])
		} else {
			// Work around JSON encoding issues with database/sql which blow up on nil
			// JSON values:
			setStateParams.MetadataUpdates[i] = "{}"
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
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	metadata := params.Metadata
	if metadata == nil {
		metadata = []byte("{}")
	}

	job, err := dbsqlc.New().JobUpdate(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobUpdateParams{
		ID:                  params.ID,
		Attempt:             int16(min(params.Attempt, math.MaxInt16)), //nolint:gosec
		AttemptDoUpdate:     params.AttemptDoUpdate,
		AttemptedAt:         params.AttemptedAt,
		AttemptedAtDoUpdate: params.AttemptedAtDoUpdate,
		AttemptedBy:         params.AttemptedBy,
		AttemptedByDoUpdate: params.AttemptedByDoUpdate,
		ErrorsDoUpdate:      params.ErrorsDoUpdate,
		Errors:              sliceutil.Map(params.Errors, func(e []byte) string { return string(e) }),
		FinalizedAtDoUpdate: params.FinalizedAtDoUpdate,
		FinalizedAt:         params.FinalizedAt,
		MaxAttemptsDoUpdate: params.MaxAttemptsDoUpdate,
		MaxAttempts:         int16(min(params.MaxAttempts, math.MaxInt16)), //nolint:gosec
		MetadataDoUpdate:    params.MetadataDoUpdate,
		Metadata:            string(metadata),
		StateDoUpdate:       params.StateDoUpdate,
		State:               dbsqlc.RiverJobState(cmp.Or(params.State, rivertype.JobStateAvailable)), // can't send empty job state, so provider default value that may not be set
	})
	if err != nil {
		return nil, interpretError(err)
	}

	return jobRowFromInternal(job)
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptElect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptElectParams{
		LeaderID: params.LeaderID,
		Now:      params.Now,
		TTL:      params.TTL.Seconds(),
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptReelect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptReelectParams{
		LeaderID: params.LeaderID,
		Now:      params.Now,
		TTL:      params.TTL.Seconds(),
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	numDeleted, err := dbsqlc.New().LeaderDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Now)
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
		Now:       params.Now,
		TTL:       params.TTL.Seconds(),
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
		Schema:          sql.NullString{String: params.Schema, Valid: params.Schema != ""},
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
		Schema:  sql.NullString{String: params.Schema, Valid: params.Schema != ""},
		Topic:   params.Topic,
	})
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	err := dbsqlc.New().PGAdvisoryXactLock(ctx, e.dbtx, key)
	return &struct{}{}, interpretError(err)
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueCreateOrSetUpdatedAt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtParams{
		Metadata:  cmp.Or(string(params.Metadata), "{}"),
		Name:      params.Name,
		Now:       params.Now,
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
	queues, err := dbsqlc.New().QueueList(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(min(params.Max, math.MaxInt32))) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(queues, queueFromInternal), nil
}

func (e *Executor) QueueNameList(ctx context.Context, params *riverdriver.QueueNameListParams) ([]string, error) {
	queueNames, err := dbsqlc.New().QueueNameList(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueNameListParams{
		After:   params.After,
		Exclude: params.Exclude,
		Match:   params.Match,
		Max:     int32(min(params.Max, math.MaxInt32)), //nolint:gosec
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueNames, nil
}

func (e *Executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	rowsAffected, err := dbsqlc.New().QueuePause(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueuePauseParams{
		Name: params.Name,
		Now:  params.Now,
	})
	if err != nil {
		return interpretError(err)
	}
	if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueResume(ctx context.Context, params *riverdriver.QueueResumeParams) error {
	rowsAffected, err := dbsqlc.New().QueueResume(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueResumeParams{
		Name: params.Name,
		Now:  params.Now,
	})
	if err != nil {
		return interpretError(err)
	}
	if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
		return rivertype.ErrNotFound
	}
	return nil
}

func (e *Executor) QueueUpdate(ctx context.Context, params *riverdriver.QueueUpdateParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueUpdate(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueUpdateParams{
		Metadata:         string(params.Metadata),
		MetadataDoUpdate: params.MetadataDoUpdate,
		Name:             params.Name,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	return e.dbtx.QueryRowContext(ctx, sql, args...)
}

func (e *Executor) SchemaCreate(ctx context.Context, params *riverdriver.SchemaCreateParams) error {
	_, err := e.dbtx.ExecContext(ctx, "CREATE SCHEMA "+params.Schema)
	return interpretError(err)
}

func (e *Executor) SchemaDrop(ctx context.Context, params *riverdriver.SchemaDropParams) error {
	_, err := e.dbtx.ExecContext(ctx, "DROP SCHEMA "+params.Schema+" CASCADE")
	return interpretError(err)
}

func (e *Executor) SchemaGetExpired(ctx context.Context, params *riverdriver.SchemaGetExpiredParams) ([]string, error) {
	schemas, err := dbsqlc.New().SchemaGetExpired(ctx, e.dbtx, &dbsqlc.SchemaGetExpiredParams{
		BeforeName: params.BeforeName,
		Prefix:     params.Prefix + "%",
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return schemas, nil
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

func (e *Executor) TableTruncate(ctx context.Context, params *riverdriver.TableTruncateParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = params.Schema + "."
	}

	// Uses raw SQL so we can truncate multiple tables at once.
	_, err := e.dbtx.ExecContext(ctx, "TRUNCATE TABLE "+
		strings.Join(
			sliceutil.Map(
				params.Table,
				func(table string) string { return maybeSchema + table },
			),
			", ",
		),
	)
	return interpretError(err)
}

type ExecutorTx struct {
	Executor

	tx *sql.Tx
}

func (t *ExecutorTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	return (&ExecutorSubTx{
		Executor:     Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver},
		beginOnce:    &savepointutil.BeginOnlyOnce{},
		savepointNum: 0,
		tx:           t.tx,
	}).Begin(ctx)
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

	beginOnce    *savepointutil.BeginOnlyOnce
	savepointNum int
	tx           *sql.Tx
}

const savepointPrefix = "river_savepoint_"

func (t *ExecutorSubTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if err := t.beginOnce.Begin(); err != nil {
		return nil, err
	}

	nextSavepointNum := t.savepointNum + 1
	if err := t.Exec(ctx, fmt.Sprintf("SAVEPOINT %s%02d", savepointPrefix, nextSavepointNum)); err != nil {
		return nil, err
	}

	return &ExecutorSubTx{Executor: Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver}, savepointNum: nextSavepointNum, beginOnce: savepointutil.NewBeginOnlyOnce(t.beginOnce), tx: t.tx}, nil
}

func (t *ExecutorSubTx) Commit(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	// Release destroys a savepoint, keeping all the effects of commands that
	// were run within it (so it's effectively COMMIT for savepoints).
	if err := t.Exec(ctx, fmt.Sprintf("RELEASE %s%02d", savepointPrefix, t.savepointNum)); err != nil {
		return err
	}

	return nil
}

func (t *ExecutorSubTx) Rollback(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed") // mirrors pgx's behavior for this condition
	}

	if err := t.Exec(ctx, fmt.Sprintf("ROLLBACK TO %s%02d", savepointPrefix, t.savepointNum)); err != nil {
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

type templateReplaceWrapper struct {
	dbtx     dbsqlc.DBTX
	replacer *sqlctemplate.Replacer
}

func (w templateReplaceWrapper) ExecContext(ctx context.Context, sql string, args ...interface{}) (sql.Result, error) {
	sql, args = w.replacer.Run(ctx, argPlaceholder, sql, args)
	return w.dbtx.ExecContext(ctx, sql, args...)
}

func (w templateReplaceWrapper) PrepareContext(ctx context.Context, sql string) (*sql.Stmt, error) {
	sql, _ = w.replacer.Run(ctx, argPlaceholder, sql, nil)
	return w.dbtx.PrepareContext(ctx, sql)
}

func (w templateReplaceWrapper) QueryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	sql, args = w.replacer.Run(ctx, argPlaceholder, sql, args)
	return w.dbtx.QueryContext(ctx, sql, args...)
}

func (w templateReplaceWrapper) QueryRowContext(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	sql, args = w.replacer.Run(ctx, argPlaceholder, sql, args)
	return w.dbtx.QueryRowContext(ctx, sql, args...)
}

// lib/pq reads in `bits` (like `bits(8)`) as a bit string that's a normal
// decimal integer like 1111_1111 (_not_ 0b1111_1111). This function converts it
// back to binary representation. See the test suite for examples.
func bitIntegerToBits(bitInteger, numBits int) int {
	var bits int
	for i := range numBits {
		bits += bitInteger % 10 * int(math.Pow(2, float64(i)))
		bitInteger /= 10
	}
	return bits
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
		UniqueStates: uniquestates.UniqueBitmaskToStates(byte(bitIntegerToBits(ptrutil.ValOrDefault(internal.UniqueStates, 0), 8))),
	}, nil
}

func leaderFromInternal(internal *dbsqlc.RiverLeader) *riverdriver.Leader {
	return &riverdriver.Leader{
		ElectedAt: internal.ElectedAt.UTC(),
		ExpiresAt: internal.ExpiresAt.UTC(),
		LeaderID:  internal.LeaderID,
	}
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

func schemaTemplateParam(ctx context.Context, schema string) context.Context {
	if schema != "" {
		schema += "."
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}, nil)
}
