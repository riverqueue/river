// Package riversqlite provides a River driver implementation for SQLite.
//
// This driver is currently in early testing. It's exercised reasonably
// thoroughly in the test suite, but has minimal real world use as of yet.
//
// River makes extensive use of internal operations that might run in parallel,
// which doesn't naturally play well with SQLite, which only allows one
// operation at a time, returning errors like "database is locked (5)
// (SQLITE_BUSY)" in case another tries to access it. A good workaround to avoid
// errors is to set the maximum pool size to one connection like
// `dbPool.SetMaxOpenConns(1)`.
//
// A known deficiency in this driver compared to Postgres is that due to
// limitations in sqlc, it performs bulk operations like non-standard
// completions and `InsertMany` one row at a time instead of in batches. This
// means that it processes batches more slowly than the Postgres driver.
package riversqlite

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riversqlite/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
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

const argPlaceholder = "?"

func (d *Driver) ArgPlaceholder() string { return argPlaceholder }
func (d *Driver) DatabaseName() string   { return "sqlite" }

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, templateReplaceWrapper{d.dbPool, &d.replacer}, d, nil}
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

func (d *Driver) PoolIsSet() bool { return d.dbPool != nil }
func (d *Driver) PoolSet(dbPool any) error {
	if d.dbPool != nil {
		return errors.New("cannot PoolSet when internal pool is already non-nil")
	}
	d.dbPool = dbPool.(*sql.DB) //nolint:forcetypeassert
	return nil
}

func (d *Driver) SQLFragmentColumnIn(column string, values any) (string, any, error) {
	arg, err := json.Marshal(values)
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("%s IN (SELECT value FROM json_each(cast(@%s AS blob)))", column, column), arg, nil
}

func (d *Driver) SupportsListener() bool       { return false }
func (d *Driver) SupportsListenNotify() bool   { return false }
func (d *Driver) TimePrecision() time.Duration { return time.Millisecond }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	// Allows UnwrapExecutor to be invoked even if driver is nil.
	var replacer *sqlctemplate.Replacer
	if d == nil {
		replacer = &sqlctemplate.Replacer{}
	} else {
		replacer = &d.replacer
	}

	executorTx := &ExecutorTx{tx: tx}
	executorTx.Executor = Executor{nil, templateReplaceWrapper{tx, replacer}, d, executorTx}

	return executorTx
}

func (d *Driver) UnwrapTx(execTx riverdriver.ExecutorTx) *sql.Tx {
	switch execTx := execTx.(type) {
	case *ExecutorSubTx:
		return execTx.tx
	case *ExecutorTx:
		return execTx.tx
	}
	panic("unhandled executor type")
}

type Executor struct {
	dbPool *sql.DB
	dbtx   templateReplaceWrapper
	driver *Driver
	execTx riverdriver.ExecutorTx
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if e.execTx != nil {
		return e.execTx.Begin(ctx)
	}

	tx, err := e.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	executorTx := &ExecutorTx{tx: tx}
	executorTx.Executor = Executor{nil, templateReplaceWrapper{tx, &e.driver.replacer}, e.driver, executorTx}

	return executorTx, nil
}

func (e *Executor) ColumnExists(ctx context.Context, params *riverdriver.ColumnExistsParams) (bool, error) {
	// Unfortunately this doesn't work in sqlc because of the "table value"
	// pragma isn't supported. This seems like it should be fixable, but for now
	// run the raw SQL to accomplish it.
	const sql = `
	SELECT EXISTS (
		SELECT 1
		FROM pragma_table_info
		WHERE schema = ? AND arg = ? AND name = ?
	)`
	var exists int64
	if err := e.dbtx.QueryRowContext(ctx, sql, cmp.Or(params.Schema, "main"), params.Table, params.Column).Scan(&exists); err != nil {
		return false, interpretError(err)
	}

	return exists > 0, nil
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

	_, err := e.dbtx.ExecContext(ctx, "DROP INDEX IF EXISTS "+maybeSchema+params.Index)
	return interpretError(err)
}

func (e *Executor) IndexExists(ctx context.Context, params *riverdriver.IndexExistsParams) (bool, error) {
	exists, err := dbsqlc.New().IndexExists(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Index)
	return exists > 0, interpretError(err)
}

func (e *Executor) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = params.Schema + "."
	}

	_, err := e.dbtx.ExecContext(ctx, "REINDEX "+maybeSchema+params.Index)
	return interpretError(err)
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	// Unlike Postgres, this must be carried out in two operations because
	// SQLite doesn't support CTEs containing `UPDATE`. As long as the job
	// exists and is not running, only one database operation is needed, but if
	// the initial update comes back empty, it does one more fetch to return the
	// most appropriate error.
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		cancelledAt, err := params.CancelAttemptedAt.UTC().MarshalJSON()
		if err != nil {
			return nil, err
		}
		cancelledAt = cancelledAt[1 : len(cancelledAt)-1] // remove quotes around the time so we don't end up with doubled up quotes

		job, err := dbsqlc.New().JobCancel(schemaTemplateParam(ctx, params.Schema), dbtx, &dbsqlc.JobCancelParams{
			ID:                params.ID,
			CancelAttemptedAt: string(cancelledAt),
			Now:               timeStringNullable(params.Now),
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				job, err := execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
					ID:     params.ID,
					Schema: params.Schema,
				})
				if err != nil {
					return nil, interpretError(err)
				}
				return job, nil
			}

			return nil, interpretError(err)
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobCountByState(ctx context.Context, params *riverdriver.JobCountByStateParams) (int, error) {
	numJobs, err := dbsqlc.New().JobCountByState(schemaTemplateParam(ctx, params.Schema), e.dbtx, string(params.State))
	if err != nil {
		return 0, err
	}
	return int(numJobs), nil
}

func (e *Executor) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	// Unlike Postgres, this must be carried out in two operations because
	// SQLite doesn't support CTEs containing `DELETE`. As long as the job
	// exists and is not running, only one database operation is needed, but if
	// the initial delete comes back empty, it does one more fetch to return the
	// most appropriate error.
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		job, err := dbsqlc.New().JobDelete(schemaTemplateParam(ctx, params.Schema), dbtx, params.ID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				job, err := execTx.JobGetByID(ctx, (*riverdriver.JobGetByIDParams)(params))
				if err != nil {
					return nil, err
				}
				if job.State == rivertype.JobStateRunning {
					return nil, rivertype.ErrJobRunning
				}
				return nil, fmt.Errorf("bug; expected only to fetch a job with state %q, but was: %q", rivertype.JobStateRunning, job.State)
			}

			return nil, interpretError(err)
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	res, err := dbsqlc.New().JobDeleteBefore(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledFinalizedAtHorizon: timeString(params.CancelledFinalizedAtHorizon),
		CompletedFinalizedAtHorizon: timeString(params.CompletedFinalizedAtHorizon),
		DiscardedFinalizedAtHorizon: timeString(params.DiscardedFinalizedAtHorizon),
		Max:                         int64(params.Max),
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

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	jobs, err := dbsqlc.New().JobGetAvailable(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobGetAvailableParams{
		AttemptedBy: params.ClientID,
		Max:         int64(params.Max),
		Now:         timeStringNullable(params.Now),
		Queue:       params.Queue,
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
		Max:          int64(params.Max),
		StuckHorizon: timeString(params.StuckHorizon),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	var (
		insertRes = make([]*riverdriver.JobInsertFastResult, len(params.Jobs))

		// We use a special `(xmax != 0)` trick in Postgres to determine whether
		// an upserted row was inserted or skipped, but as far as I can find,
		// there's no such trick possible in SQLite. Instead, we roll a random
		// nonce and insert it to metadata. If the same nonce coes back, we know
		// we really inserted the row. If not, we're getting an existing row back.
		uniqueNonce = randutil.Hex(8)
	)

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i, params := range params.Jobs {
			metadata, err := sjson.SetBytes(sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")), rivercommon.MetadataKeyUniqueNonce, uniqueNonce)
			if err != nil {
				return err
			}

			tags, err := json.Marshal(params.Tags)
			if err != nil {
				return fmt.Errorf("error marshaling tags: %w", err)
			}

			var uniqueStates *int64
			if params.UniqueStates != 0 {
				uniqueStates = ptrutil.Ptr(int64(params.UniqueStates))
			}

			internal, err := dbsqlc.New().JobInsertFast(ctx, dbtx, &dbsqlc.JobInsertFastParams{
				Args:         params.EncodedArgs,
				CreatedAt:    timeStringNullable(params.CreatedAt),
				Kind:         params.Kind,
				MaxAttempts:  int64(params.MaxAttempts),
				Metadata:     metadata,
				Priority:     int64(params.Priority),
				Queue:        params.Queue,
				ScheduledAt:  timeStringNullable(params.ScheduledAt),
				State:        string(params.State),
				Tags:         tags,
				UniqueKey:    params.UniqueKey,
				UniqueStates: uniqueStates,
			})
			if err != nil {
				return interpretError(err)
			}

			job, err := jobRowFromInternal(internal)
			if err != nil {
				return err
			}

			insertRes[i] = &riverdriver.JobInsertFastResult{
				Job:                      job,
				UniqueSkippedAsDuplicate: gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyUniqueNonce).Str != uniqueNonce,
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return insertRes, nil
}

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	var totalRowsAffected int

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for _, params := range params.Jobs {
			tags, err := json.Marshal(params.Tags)
			if err != nil {
				// return nil, err
				return fmt.Errorf("error marshaling tags: %w", err)
			}

			var uniqueStates *int64
			if params.UniqueStates != 0 {
				uniqueStates = ptrutil.Ptr(int64(params.UniqueStates))
			}

			rowsAffected, err := dbsqlc.New().JobInsertFastNoReturning(ctx, dbtx, &dbsqlc.JobInsertFastNoReturningParams{
				Args:         params.EncodedArgs,
				CreatedAt:    timeStringNullable(params.CreatedAt),
				Kind:         params.Kind,
				MaxAttempts:  int64(params.MaxAttempts),
				Metadata:     sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
				Priority:     int64(params.Priority),
				Queue:        params.Queue,
				ScheduledAt:  timeStringNullable(params.ScheduledAt),
				State:        string(params.State),
				Tags:         tags,
				UniqueKey:    params.UniqueKey,
				UniqueStates: uniqueStates,
			})
			if err != nil {
				return interpretError(err)
			}

			totalRowsAffected += int(rowsAffected)
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return totalRowsAffected, nil
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	var attemptedBy []byte
	if params.AttemptedBy != nil {
		var err error
		attemptedBy, err = json.Marshal(params.AttemptedBy)
		if err != nil {
			return nil, err
		}
	}

	var errors []byte
	if len(params.Errors) > 0 {
		var err error
		errors, err = json.Marshal(sliceutil.Map(params.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
		if err != nil {
			return nil, err
		}
	}

	tags, err := json.Marshal(params.Tags)
	if err != nil {
		return nil, err
	}

	var uniqueStates *int64
	if params.UniqueStates != 0 {
		uniqueStates = ptrutil.Ptr(int64(params.UniqueStates))
	}

	job, err := dbsqlc.New().JobInsertFull(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobInsertFullParams{
		Attempt:      int64(params.Attempt),
		AttemptedAt:  timeStringNullable(params.AttemptedAt),
		AttemptedBy:  attemptedBy,
		Args:         params.EncodedArgs,
		CreatedAt:    timeStringNullable(params.CreatedAt),
		Errors:       errors,
		FinalizedAt:  timeStringNullable(params.FinalizedAt),
		Kind:         params.Kind,
		MaxAttempts:  int64(params.MaxAttempts),
		Metadata:     sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
		Priority:     int64(params.Priority),
		Queue:        params.Queue,
		ScheduledAt:  timeStringNullable(params.ScheduledAt),
		State:        string(params.State),
		Tags:         tags,
		UniqueKey:    params.UniqueKey,
		UniqueStates: uniqueStates,
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return jobRowFromInternal(job)
}

func (e *Executor) JobInsertFullMany(ctx context.Context, params *riverdriver.JobInsertFullManyParams) ([]*rivertype.JobRow, error) {
	insertRes := make([]*rivertype.JobRow, len(params.Jobs))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i, jobParams := range params.Jobs {
			var attemptedBy []byte
			if jobParams.AttemptedBy != nil {
				var err error
				attemptedBy, err = json.Marshal(jobParams.AttemptedBy)
				if err != nil {
					return err
				}
			}

			var errors []byte
			if len(jobParams.Errors) > 0 {
				var err error
				errors, err = json.Marshal(sliceutil.Map(jobParams.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
				if err != nil {
					return err
				}
			}

			tags, err := json.Marshal(jobParams.Tags)
			if err != nil {
				return err
			}

			var uniqueStates *int64
			if jobParams.UniqueStates != 0 {
				uniqueStates = ptrutil.Ptr(int64(jobParams.UniqueStates))
			}

			job, err := dbsqlc.New().JobInsertFull(ctx, dbtx, &dbsqlc.JobInsertFullParams{
				Attempt:      int64(jobParams.Attempt),
				AttemptedAt:  timeStringNullable(jobParams.AttemptedAt),
				AttemptedBy:  attemptedBy,
				Args:         jobParams.EncodedArgs,
				CreatedAt:    timeStringNullable(jobParams.CreatedAt),
				Errors:       errors,
				FinalizedAt:  timeStringNullable(jobParams.FinalizedAt),
				Kind:         jobParams.Kind,
				MaxAttempts:  int64(jobParams.MaxAttempts),
				Metadata:     sliceutil.FirstNonEmpty(jobParams.Metadata, []byte("{}")),
				Priority:     int64(jobParams.Priority),
				Queue:        jobParams.Queue,
				ScheduledAt:  timeStringNullable(jobParams.ScheduledAt),
				State:        string(jobParams.State),
				Tags:         tags,
				UniqueKey:    jobParams.UniqueKey,
				UniqueStates: uniqueStates,
			})
			if err != nil {
				return interpretError(err)
			}

			insertRes[i], err = jobRowFromInternal(job)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return insertRes, nil
}

func (e *Executor) JobList(ctx context.Context, params *riverdriver.JobListParams) ([]*rivertype.JobRow, error) {
	ctx = sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"order_by_clause": {Value: params.OrderByClause},
		"where_clause":    {Value: params.WhereClause},
	}, params.NamedArgs)

	jobs, err := dbsqlc.New().JobList(schemaTemplateParam(ctx, params.Schema), e.dbtx, int64(params.Max))
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch rescue, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i := range params.ID {
			if err := dbsqlc.New().JobRescue(ctx, dbtx, &dbsqlc.JobRescueParams{
				ID:          params.ID[i],
				Error:       params.Error[i],
				FinalizedAt: timeStringNullable(params.FinalizedAt[i]),
				ScheduledAt: params.ScheduledAt[i].UTC(),
				State:       params.State[i],
			}); err != nil {
				return interpretError(err)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (e *Executor) JobRetry(ctx context.Context, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	// Unlike Postgres, this must be carried out in two operations because
	// SQLite doesn't support CTEs containing `UPDATE`. As long as the job
	// exists and is not running, only one database operation is needed, but if
	// the initial update comes back empty, it does one more fetch to return the
	// most appropriate error.
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		job, err := dbsqlc.New().JobRetry(schemaTemplateParam(ctx, params.Schema), dbtx, &dbsqlc.JobRetryParams{
			ID:  params.ID,
			Now: timeStringNullable(params.Now),
		})
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				job, err := execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{
					ID:     params.ID,
					Schema: params.Schema,
				})
				if err != nil {
					return nil, interpretError(err)
				}
				return job, nil
			}

			return nil, interpretError(err)
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	// This operation diverges the most from the Postgres version out of all the
	// others by far. The Postgres version is one gigantic query that can't
	// reproduce here because (1) SQLite doesn't support `UPDATE` in CTEs, and
	// (2) sqlc's handling around any kind of bulk upsert mechanisms (i.e. via
	// arrays, which don't exist, or jsonb) is completely non-functional.
	//
	// Instead, we break the operation up into four much smaller queries. Each
	// query is mostly trivial, so this does have the effect of simplifying
	// things conceptually by quite a lot. It is more Go code though, and one
	// could argue that it's not as concurrently safe as the single query
	// version (i.e. an advisory lock should maybe be taken around when checking
	// rows to avoid non-repeatable read anomalies), but since only one writer
	// at a time is possible for SQLite, this should be okay.
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*riverdriver.JobScheduleResult, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		eligibleJobs, err := dbsqlc.New().JobScheduleGetEligible(schemaTemplateParam(ctx, params.Schema), dbtx, &dbsqlc.JobScheduleGetEligibleParams{
			Max: int64(params.Max),
			Now: timeStringNullable(params.Now),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		var (
			discardIDs      []int64
			nonUniqueIDs    []int64
			scheduledResMap = make(map[int64]*riverdriver.JobScheduleResult)
		)

		for _, eligibleJob := range eligibleJobs {
			if eligibleJob.UniqueKey == nil {
				nonUniqueIDs = append(nonUniqueIDs, eligibleJob.ID)
				continue
			}

			internal, err := dbsqlc.New().JobScheduleGetCollision(ctx, dbtx, &dbsqlc.JobScheduleGetCollisionParams{
				ID:        eligibleJob.ID,
				UniqueKey: eligibleJob.UniqueKey,
			})
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, interpretError(err)
			}

			if internal.ID != 0 {
				discardIDs = append(discardIDs, eligibleJob.ID)
				continue
			}

			// We must set available immediately rather than in the batch below
			// because it's possible for eligible jobs to be unique conflicting
			// with each other. So when we start this loop two jobs could be
			// scheduled with no unique collision, but after the first one is
			// scheduled, then the second one no longer can and is discarded.
			updatedJobs, err := dbsqlc.New().JobScheduleSetAvailable(ctx, dbtx, []int64{eligibleJob.ID})
			if err != nil {
				return nil, interpretError(err)
			}
			updatedJob, err := jobRowFromInternal(updatedJobs[0])
			if err != nil {
				return nil, err
			}
			scheduledResMap[updatedJob.ID] = &riverdriver.JobScheduleResult{Job: *updatedJob}
		}

		if len(discardIDs) > 0 {
			updatedJobs, err := dbsqlc.New().JobScheduleSetDiscarded(ctx, dbtx, &dbsqlc.JobScheduleSetDiscardedParams{
				ID:  discardIDs,
				Now: timeStringNullable(params.Now),
			})
			if err != nil {
				return nil, interpretError(err)
			}

			for _, internal := range updatedJobs {
				updatedJob, err := jobRowFromInternal(internal)
				if err != nil {
					return nil, err
				}
				scheduledResMap[updatedJob.ID] = &riverdriver.JobScheduleResult{ConflictDiscarded: true, Job: *updatedJob}
			}
		}

		if len(nonUniqueIDs) > 0 {
			updatedJobs, err := dbsqlc.New().JobScheduleSetAvailable(ctx, dbtx, nonUniqueIDs)
			if err != nil {
				return nil, interpretError(err)
			}

			for _, internal := range updatedJobs {
				updatedJob, err := jobRowFromInternal(internal)
				if err != nil {
					return nil, err
				}
				scheduledResMap[updatedJob.ID] = &riverdriver.JobScheduleResult{Job: *updatedJob}
			}
		}

		// Return jobs in the same order we fetched them.
		return sliceutil.Map(eligibleJobs, func(eligibleJob *dbsqlc.RiverJob) *riverdriver.JobScheduleResult {
			return scheduledResMap[eligibleJob.ID]
		}), nil
	})
}

func (e *Executor) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	setRes := make([]*rivertype.JobRow, len(params.ID))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Because it's by far the most common path, put in an optimization for
		// jobs that we're setting to `completed` that don't have any metadata
		// updates needed. Group those jobs out and complete them all in one
		// query, then continue on and do all the other updates.
		var (
			completedIDs     = make([]int64, 0, len(params.ID))
			completedIndexes = make(map[int64]int, len(params.ID)) // job ID -> params index (for setting result)
		)
		for i, id := range params.ID {
			if params.State[i] == rivertype.JobStateCompleted && !params.MetadataDoMerge[i] {
				completedIDs = append(completedIDs, id)
				completedIndexes[id] = i
			}
		}

		if len(completedIDs) > 0 {
			jobs, err := dbsqlc.New().JobSetCompletedIfRunning(ctx, dbtx, &dbsqlc.JobSetCompletedIfRunningParams{
				ID:          completedIDs,
				FinalizedAt: timeStringNullable(params.Now),
			})
			if err != nil {
				return fmt.Errorf("error setting completed state on jobs: %w", err)
			}

			for _, job := range jobs {
				setRes[completedIndexes[job.ID]], err = jobRowFromInternal(job)
				if err != nil {
					return err
				}
				delete(completedIndexes, job.ID)
			}

			// Fetch any jobs that weren't set by the query above because they
			// weren't `running`. In practice this should be quite rare, but we
			// check for it in the test suite.
			if len(completedIndexes) > 0 {
				jobs, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, maputil.Keys(completedIndexes))
				if err != nil {
					return fmt.Errorf("error getting non-running jobs: %w", err)
				}

				for _, job := range jobs {
					setRes[completedIndexes[job.ID]], err = jobRowFromInternal(job)
					if err != nil {
						return err
					}
				}
			}
		}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i, id := range params.ID {
			// Skip job if we handled it in the happy path optimization above.
			if _, ok := completedIndexes[id]; ok {
				continue
			}

			setStateParams := &dbsqlc.JobSetStateIfRunningParams{
				ID:              params.ID[i],
				Error:           []byte("{}"), // even if not used, must be valid JSON because it's bed into the `json` function
				MetadataUpdates: []byte("{}"), // even if not used, must be valid JSON because it's bed into the `json` function
				Now:             timeStringNullable(params.Now),
				State:           string(params.State[i]),
			}

			if params.Attempt[i] != nil {
				setStateParams.AttemptDoUpdate = true
				setStateParams.Attempt = int64(*params.Attempt[i])
			}
			if params.ErrData[i] != nil {
				setStateParams.ErrorsDoUpdate = true
				setStateParams.Error = params.ErrData[i]
			}
			if params.FinalizedAt[i] != nil {
				setStateParams.FinalizedAtDoUpdate = true
				setStateParams.FinalizedAt = params.FinalizedAt[i]
			}
			if params.MetadataDoMerge[i] {
				setStateParams.MetadataDoMerge = true
				setStateParams.MetadataUpdates = params.MetadataUpdates[i]
			}
			if params.ScheduledAt[i] != nil {
				setStateParams.ScheduledAtDoUpdate = true
				setStateParams.ScheduledAt = *params.ScheduledAt[i]
			}

			job, err := dbsqlc.New().JobSetStateIfRunning(ctx, dbtx, setStateParams)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					var err error
					job, err = dbsqlc.New().JobSetMetadataIfNotRunning(ctx, dbtx, &dbsqlc.JobSetMetadataIfNotRunningParams{
						ID:              params.ID[i],
						MetadataUpdates: sliceutil.FirstNonEmpty(params.MetadataUpdates[i], []byte("{}")),
					})
					if err != nil {
						// Allow a job to have been deleted in the interim.
						if errors.Is(err, sql.ErrNoRows) {
							return nil
						}

						return fmt.Errorf("error setting job metadata: %w", err)
					}
				} else {
					return fmt.Errorf("error setting job state: %w", err)
				}
			}
			setRes[i], err = jobRowFromInternal(job)
			if err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return setRes, nil
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	attemptedAt := params.AttemptedAt
	if attemptedAt != nil {
		attemptedAt = ptrutil.Ptr(attemptedAt.UTC())
	}

	attemptedBy, err := json.Marshal(params.AttemptedBy)
	if err != nil {
		return nil, err
	}

	errors, err := json.Marshal(sliceutil.Map(params.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
	if err != nil {
		return nil, err
	}

	finalizedAt := params.FinalizedAt
	if finalizedAt != nil {
		finalizedAt = ptrutil.Ptr(finalizedAt.UTC())
	}

	job, err := dbsqlc.New().JobUpdate(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobUpdateParams{
		ID:                  params.ID,
		Attempt:             int64(params.Attempt),
		AttemptDoUpdate:     params.AttemptDoUpdate,
		AttemptedAt:         attemptedAt,
		AttemptedAtDoUpdate: params.AttemptedAtDoUpdate,
		AttemptedBy:         attemptedBy,
		AttemptedByDoUpdate: params.AttemptedByDoUpdate,
		ErrorsDoUpdate:      params.ErrorsDoUpdate,
		Errors:              errors,
		FinalizedAtDoUpdate: params.FinalizedAtDoUpdate,
		FinalizedAt:         finalizedAt,
		StateDoUpdate:       params.StateDoUpdate,
		State:               string(params.State),
	})
	if err != nil {
		return nil, interpretError(err)
	}

	return jobRowFromInternal(job)
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptElect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptElectParams{
		LeaderID: params.LeaderID,
		Now:      timeStringNullable(params.Now),
		TTL:      durationAsString(params.TTL),
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	numElectionsWon, err := dbsqlc.New().LeaderAttemptReelect(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderAttemptReelectParams{
		LeaderID: params.LeaderID,
		Now:      timeStringNullable(params.Now),
		TTL:      durationAsString(params.TTL),
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numElectionsWon > 0, nil
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	numDeleted, err := dbsqlc.New().LeaderDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx, timeStringNullable(params.Now))
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
		ElectedAt: timeStringNullable(params.ElectedAt),
		ExpiresAt: timeStringNullable(params.ExpiresAt),
		Now:       timeStringNullable(params.Now),
		LeaderID:  params.LeaderID,
		TTL:       durationAsString(params.TTL),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return leaderFromInternal(leader), nil
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	numResigned, err := dbsqlc.New().LeaderResign(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.LeaderID)
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
	migrations := make([]*riverdriver.Migration, len(params.Versions))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i, version := range params.Versions {
			migration, err := dbsqlc.New().RiverMigrationInsert(schemaTemplateParam(ctx, params.Schema), dbtx, &dbsqlc.RiverMigrationInsertParams{
				Line:    params.Line,
				Version: int64(version),
			})
			if err != nil {
				return interpretError(err)
			}

			migrations[i] = migrationFromInternal(migration)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (e *Executor) MigrationInsertManyAssumingMain(ctx context.Context, params *riverdriver.MigrationInsertManyAssumingMainParams) ([]*riverdriver.Migration, error) {
	migrations := make([]*riverdriver.Migration, len(params.Versions))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Should be a batch insert, but that's currently impossible with SQLite/sqlc. https://github.com/sqlc-dev/sqlc/issues/3802
		for i, version := range params.Versions {
			internal, err := dbsqlc.New().RiverMigrationInsertAssumingMain(ctx, dbtx, int64(version))
			if err != nil {
				return interpretError(err)
			}

			migrations[i] = &riverdriver.Migration{
				CreatedAt: internal.CreatedAt.UTC(),
				Line:      riverdriver.MigrationLineMain,
				Version:   int(internal.Version),
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	return riverdriver.ErrNotImplemented
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueCreateOrSetUpdatedAt(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtParams{
		Metadata:  sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
		Name:      params.Name,
		Now:       timeStringNullable(params.Now),
		PausedAt:  timeStringNullable(params.PausedAt),
		UpdatedAt: timeStringNullable(params.UpdatedAt),
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	queues, err := dbsqlc.New().QueueDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueDeleteExpiredParams{
		Max:              int64(params.Max),
		UpdatedAtHorizon: params.UpdatedAtHorizon.UTC(),
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
	queues, err := dbsqlc.New().QueueList(schemaTemplateParam(ctx, params.Schema), e.dbtx, int64(params.Limit))
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(queues, queueFromInternal), nil
}

func (e *Executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	// `execresult` doesn't seem to return the right number of rows affected in
	// SQLite under every circumstance, so use a `many` return instead.
	rowsAffected, err := dbsqlc.New().QueuePause(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueuePauseParams{
		Name: params.Name,
		Now:  timeStringNullable(params.Now),
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
	// `execresult` doesn't seem to return the right number of rows affected in
	// SQLite under every circumstance, so use a `many` return instead.
	rowsAffected, err := dbsqlc.New().QueueResume(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueResumeParams{
		Name: params.Name,
		Now:  timeStringNullable(params.Now),
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
		Metadata:         sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
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

const sqliteTestDir = "./sqlite"

func (e *Executor) SchemaCreate(ctx context.Context, params *riverdriver.SchemaCreateParams) error {
	return nil
}

// Most of these probably don't exist, but may have been leftover.
var possibleExtensions = []string{ //nolint:gochecknoglobals
	"sqlite3",
	"sqlite3-journal",
	"sqlite3-shm",
	"sqlite3-wal",
}

func (e *Executor) SchemaDrop(ctx context.Context, params *riverdriver.SchemaDropParams) error {
	for _, possibleExtension := range possibleExtensions {
		if err := os.Remove(fmt.Sprintf("%s/%s.%s", sqliteTestDir, params.Schema, possibleExtension)); !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func (e *Executor) SchemaGetExpired(ctx context.Context, params *riverdriver.SchemaGetExpiredParams) ([]string, error) {
	if err := os.MkdirAll(sqliteTestDir, 0o700); err != nil {
		return nil, err
	}

	schemaEntries, err := os.ReadDir(sqliteTestDir)
	if err != nil {
		return nil, err
	}

	expiredSchemas := make([]string, 0, len(schemaEntries))
	for _, schemaEntry := range schemaEntries {
		if !strings.HasPrefix(schemaEntry.Name(), params.Prefix) {
			continue
		}

		if strings.Compare(schemaEntry.Name(), params.BeforeName) >= 0 {
			continue
		}

		// Strip the extension because it'll be added back by SchemaDrop.
		expiredSchemas = append(expiredSchemas,
			strings.TrimSuffix(schemaEntry.Name(), filepath.Ext(schemaEntry.Name())),
		)
	}

	return expiredSchemas, nil
}

func (e *Executor) TableExists(ctx context.Context, params *riverdriver.TableExistsParams) (bool, error) {
	exists, err := dbsqlc.New().TableExists(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Table)
	return exists > 0, interpretError(err)
}

func (e *Executor) TableTruncate(ctx context.Context, params *riverdriver.TableTruncateParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = params.Schema + "."
	}

	// SQLite doesn't have a `TRUNCATE` command, but `DELETE FROM` is optimized
	// as a truncate as long as it doesn't have a `RETURNING` clause or
	// triggers. Unfortunately unlike `TRUNCATE`, we do have to  issue
	// operations in a loop, but given SQLite is probably largely local, that
	// should be okay.
	for _, table := range params.Table {
		_, err := e.dbtx.ExecContext(ctx, "DELETE FROM "+maybeSchema+table)
		if err != nil {
			return interpretError(err)
		}
	}

	return nil
}

type ExecutorTx struct {
	Executor
	tx *sql.Tx
}

func (t *ExecutorTx) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	executorSubTx := &ExecutorSubTx{savepointNum: 0, single: &singleTransaction{}, tx: t.tx}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver, executorSubTx}
	return executorSubTx.Begin(ctx)
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
	if err := t.Exec(ctx, fmt.Sprintf("SAVEPOINT %s%02d", savepointPrefix, nextSavepointNum)); err != nil {
		return nil, err
	}

	executorSubTx := &ExecutorSubTx{savepointNum: nextSavepointNum, single: &singleTransaction{parent: t.single}, tx: t.tx}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver, executorSubTx}

	return executorSubTx, nil
}

func (t *ExecutorSubTx) Commit(ctx context.Context) error {
	defer t.single.setDone()

	if t.single.done {
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
	defer t.single.setDone()

	if t.single.done {
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

func durationAsString(duration time.Duration) string {
	return strconv.FormatFloat(duration.Seconds(), 'f', 3, 64) + " seconds"
}

func jobRowFromInternal(internal *dbsqlc.RiverJob) (*rivertype.JobRow, error) {
	var attemptedAt *time.Time
	if internal.AttemptedAt != nil {
		t := internal.AttemptedAt.UTC()
		attemptedAt = &t
	}

	var attemptedBy []string
	if internal.AttemptedBy != nil {
		if err := json.Unmarshal(internal.AttemptedBy, &attemptedBy); err != nil {
			return nil, fmt.Errorf("error unmarshaling `attempted_by`: %w", err)
		}
	}

	var errors []rivertype.AttemptError
	if internal.Errors != nil {
		if err := json.Unmarshal(internal.Errors, &errors); err != nil {
			return nil, fmt.Errorf("error unmarshaling `errors`: %w", err)
		}
	}

	var finalizedAt *time.Time
	if internal.FinalizedAt != nil {
		t := internal.FinalizedAt.UTC()
		finalizedAt = &t
	}

	var tags []string
	if err := json.Unmarshal(internal.Tags, &tags); err != nil {
		return nil, fmt.Errorf("error unmarshaling `tags`: %w", err)
	}

	var uniqueStatesByte byte
	if internal.UniqueStates != nil {
		uniqueStatesByte = byte(*internal.UniqueStates)
	}

	return &rivertype.JobRow{
		ID:           internal.ID,
		Attempt:      max(int(internal.Attempt), 0),
		AttemptedAt:  attemptedAt,
		AttemptedBy:  attemptedBy,
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
		Tags:         tags,
		UniqueKey:    internal.UniqueKey,
		UniqueStates: uniquestates.UniqueBitmaskToStates(uniqueStatesByte),
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

func schemaTemplateParam(ctx context.Context, schema string) context.Context {
	if schema != "" {
		schema += "."
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}, nil)
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

func timeString(t time.Time) string {
	// SQLite times are really strings, and may god help your immortal soul if
	// you don't use this exact format when storing them (including similar
	// looking more common formats like RFC3339). They'll store fine, produce no
	// warnings, and then just won't compare properly against built-ins, causing
	// everything to fail in non-obvious ways.
	const sqliteFormat = "2006-01-02 15:04:05.999"

	return t.UTC().Round(time.Millisecond).Format(sqliteFormat)
}

// This is kind of unfortunate, but I've found it the easiest way to encode an
// optional date/time. Unfortunately sqlc (or the driver? not sure) will write a
// `*time.Time` as an integer (presumably a Unix timestamp), and then be unable
// to scan it back into a `time.Time`. This workaround has us cast input time
// parameters as `text`, then do the encoding to a string ourselves (SQLite has
// no date/time type, only `integer` or `text`) to make sure it's in our desired
// target format.
func timeStringNullable(t *time.Time) *string {
	if t == nil {
		return nil
	}

	str := timeString(*t)
	return &str
}
