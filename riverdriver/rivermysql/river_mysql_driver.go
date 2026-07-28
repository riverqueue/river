// Package rivermysql provides a River driver implementation for MySQL.
//
// This driver targets MySQL 8.0+ and requires `go-sql-driver/mysql` or a
// compatible driver registered with `database/sql`. DSNs used with River
// should include `parseTime=true`, `loc=UTC`, and `multiStatements=true`.
// `parseTime` and `loc` ensure `DATETIME` columns are scanned as UTC
// [time.Time] values, while `multiStatements` is needed to run River's
// migrations.
//
// MySQL does not support LISTEN/NOTIFY, so this driver uses a
// `river_notification` outbox table for notifications. It also does not support
// `RETURNING` clauses, so most write operations are carried out as two-step
// operations (write + read).
//
// This driver is currently in early development. It's exercised in the test
// suite, but has minimal real world use as of yet.
package rivermysql

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/rivermysql/internal/dbsqlc"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/uniquestates"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/ptrutil"
	"github.com/riverqueue/river/rivershared/util/randutil"
	"github.com/riverqueue/river/rivershared/util/savepointutil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivertype"
)

//go:embed migration/*/*.sql
var migrationFS embed.FS

// Driver is an implementation of riverdriver.Driver for MySQL.
type Driver struct {
	dbPool   *sql.DB
	replacer sqlctemplate.Replacer
}

// New returns a new MySQL driver for use with River.
//
// It takes an sql.DB to use with River. The DSN should include
// `parseTime=true&loc=UTC&multiStatements=true`; see the package documentation
// for details. The pool must not be closed while associated River objects are
// running.
func New(dbPool *sql.DB) *Driver {
	return &Driver{
		dbPool:   dbPool,
		replacer: sqlctemplate.Replacer{UnnumberedPlaceholders: true},
	}
}

const argPlaceholder = "?"

func (d *Driver) ArgPlaceholder() string             { return argPlaceholder }
func (d *Driver) DatabaseName() string               { return riverdriver.DatabaseNameMySQL }
func (d *Driver) SafeIdentifier(ident string) string { return mysqlIdentifier(ident) }

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, templateReplaceWrapper{d.dbPool, &d.replacer}, d, nil}
}

func (d *Driver) GetListener(params *riverdriver.GetListenenerParams) riverdriver.Listener {
	return &Listener{
		dbPool:       d.dbPool,
		pollInterval: notificationPollIntervalDefault,
		replacer:     &d.replacer,
		schema:       params.Schema,
		topics:       make(map[string]struct{}),
	}
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

	// Use JSON_TABLE to expand the JSON array into rows for an IN clause.
	// The arg is passed through the template system's NamedArgs as @column,
	// which the template replacer turns into a positional placeholder. The
	// templateReplaceWrapper strips the number suffix to produce plain `?`
	// that MySQL expects. Use VARCHAR(255) as the column type since it
	// handles both integer and string comparisons via implicit conversion. Use
	// River's binary collation explicitly so string comparisons remain
	// case-sensitive and don't depend on the database's default collation.
	return fmt.Sprintf("%s IN (SELECT jt.val FROM JSON_TABLE(CAST(@%s AS JSON), '$[*]' COLUMNS(val VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) AS jt)", column, column), arg, nil
}

func (d *Driver) SupportsListener() bool       { return true }
func (d *Driver) SupportsListenNotify() bool   { return true }
func (d *Driver) TimePrecision() time.Duration { return time.Microsecond }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	// Allows UnwrapExecutor to be invoked even if driver is nil.
	var replacer *sqlctemplate.Replacer
	if d == nil {
		replacer = &sqlctemplate.Replacer{UnnumberedPlaceholders: true}
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
	exists, err := dbsqlc.New().ColumnExists(informationSchemaParam(ctx), e.dbtx, &dbsqlc.ColumnExistsParams{
		ColumnName: params.Column,
		Schema:     sql.NullString{String: params.Schema, Valid: params.Schema != ""},
		TableName:  params.Table,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return exists, nil
}

func (e *Executor) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := e.dbtx.ExecContext(ctx, sql, args...)
	return interpretError(err)
}

func (e *Executor) IndexDropIfExists(ctx context.Context, params *riverdriver.IndexDropIfExistsParams) error {
	indexName := strings.TrimSpace(params.Index)

	exists, err := e.IndexExists(ctx, &riverdriver.IndexExistsParams{
		Index:  indexName,
		Schema: params.Schema,
	})
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	// MySQL's DROP INDEX requires the table name. Look it up from the index.
	tableName, err := dbsqlc.New().IndexGetTableName(informationSchemaParam(ctx), e.dbtx, &dbsqlc.IndexGetTableNameParams{
		IndexName: indexName,
		Schema:    sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return interpretError(err)
	}

	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = mysqlIdentifier(params.Schema) + "."
	}

	_, err = e.dbtx.ExecContext(ctx, "DROP INDEX "+mysqlIdentifier(indexName)+" ON "+maybeSchema+mysqlIdentifier(tableName))
	return interpretError(err)
}

func (e *Executor) IndexExists(ctx context.Context, params *riverdriver.IndexExistsParams) (bool, error) {
	ctx = informationSchemaParam(ctx)

	exists, err := dbsqlc.New().IndexExists(ctx, e.dbtx, &dbsqlc.IndexExistsParams{
		IndexName: params.Index,
		Schema:    sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return false, interpretError(err)
	}
	return exists, nil
}

func (e *Executor) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	// MySQL has no per-index REINDEX operation. ANALYZE TABLE is the safe
	// online approximation: it refreshes the selected index's table statistics
	// without rebuilding and locking the entire table as OPTIMIZE TABLE would.
	tableName, err := dbsqlc.New().IndexGetTableName(informationSchemaParam(ctx), e.dbtx, &dbsqlc.IndexGetTableNameParams{
		IndexName: params.Index,
		Schema:    sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return interpretError(err)
	}

	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = mysqlIdentifier(params.Schema) + "."
	}

	_, err = e.dbtx.ExecContext(ctx, "ANALYZE TABLE "+maybeSchema+mysqlIdentifier(tableName))
	return interpretError(err)
}

func (e *Executor) IndexReindexArtifacts(ctx context.Context, params *riverdriver.IndexReindexArtifactsParams) ([]string, error) {
	artifacts, err := dbsqlc.New().IndexReindexArtifacts(informationSchemaParam(ctx), e.dbtx, &dbsqlc.IndexReindexArtifactsParams{
		ArtifactPattern: "^" + regexp.QuoteMeta(params.Index) + `_(ccnew|ccold)[0-9]*$`,
		Schema:          sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	return artifacts, interpretError(err)
}

func (e *Executor) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	foundNames, err := dbsqlc.New().IndexesExist(informationSchemaParam(ctx), e.dbtx, &dbsqlc.IndexesExistParams{
		IndexNames: params.IndexNames,
		Schema:     sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return nil, interpretError(err)
	}

	exists := make(map[string]bool, len(params.IndexNames))
	for _, name := range foundNames {
		exists[name] = true
	}
	return exists, nil
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		res, err := dbsqlc.New().JobCancelExec(ctx, dbtx, &dbsqlc.JobCancelExecParams{
			ID:                params.ID,
			CancelAttemptedAt: params.CancelAttemptedAt.UTC().Format(time.RFC3339Nano),
			Now:               nullTimeFromPtr(params.Now),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return nil, interpretError(err)
		}

		job, err := dbsqlc.New().JobGetByID(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}

		if rowsAffected < 1 {
			// No rows were updated, return the job as-is (already finalized)
			return jobRowFromInternal(job)
		}

		return jobRowFromInternal(job)
	})
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
	rows, err := dbsqlc.New().JobCountByQueueAndState(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobCountByQueueAndStateParams{
		QueueNames: params.QueueNames,
	})
	if err != nil {
		return nil, interpretError(err)
	}

	// MySQL's GROUP BY only returns queues that have jobs, so fill in zero
	// counts for any requested queues not in the result.
	countsByQueue := make(map[string]*riverdriver.JobCountByQueueAndStateResult, len(rows))
	for _, row := range rows {
		countsByQueue[row.Queue] = &riverdriver.JobCountByQueueAndStateResult{
			CountAvailable: row.CountAvailable,
			CountRunning:   row.CountRunning,
			Queue:          row.Queue,
		}
	}

	var (
		queueNames = slices.Compact(slices.Sorted(slices.Values(params.QueueNames)))
		results    = make([]*riverdriver.JobCountByQueueAndStateResult, len(queueNames))
	)
	for i, name := range queueNames {
		if result, ok := countsByQueue[name]; ok {
			results[i] = result
		} else {
			results[i] = &riverdriver.JobCountByQueueAndStateResult{Queue: name}
		}
	}

	return results, nil
}

func (e *Executor) JobCountByState(ctx context.Context, params *riverdriver.JobCountByStateParams) (int, error) {
	numJobs, err := dbsqlc.New().JobCountByState(schemaTemplateParam(ctx, params.Schema), e.dbtx, string(params.State))
	if err != nil {
		return 0, err
	}
	return int(numJobs), nil
}

func (e *Executor) JobDelete(ctx context.Context, params *riverdriver.JobDeleteParams) (*rivertype.JobRow, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Lock the job while reading it so a concurrent producer can't change it
		// to running between the read and delete statements.
		job, err := dbsqlc.New().JobDeleteSelect(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}

		res, err := dbsqlc.New().JobDeleteExec(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return nil, interpretError(err)
		}

		if rowsAffected < 1 {
			if rivertype.JobState(job.State) == rivertype.JobStateRunning {
				return nil, rivertype.ErrJobRunning
			}
			return nil, fmt.Errorf("bug; expected only to fetch a job with state %q, but was: %q", rivertype.JobStateRunning, job.State)
		}

		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	queuesExcluded := params.QueuesExcluded
	if len(queuesExcluded) < 1 {
		queuesExcluded = []string{""}
	}

	queuesIncluded := params.QueuesIncluded
	if len(queuesIncluded) < 1 {
		queuesIncluded = []string{""}
	}

	res, err := dbsqlc.New().JobDeleteBefore(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobDeleteBeforeParams{
		CancelledDoDelete:           boolToInt64(params.CancelledDoDelete),
		CancelledFinalizedAtHorizon: sql.NullTime{Time: params.CancelledFinalizedAtHorizon.UTC(), Valid: true},
		CompletedDoDelete:           boolToInt64(params.CompletedDoDelete),
		CompletedFinalizedAtHorizon: sql.NullTime{Time: params.CompletedFinalizedAtHorizon.UTC(), Valid: true},
		DiscardedDoDelete:           boolToInt64(params.DiscardedDoDelete),
		DiscardedFinalizedAtHorizon: sql.NullTime{Time: params.DiscardedFinalizedAtHorizon.UTC(), Valid: true},
		Limit:                       int32(params.Max), //nolint:gosec
		QueuesExcluded:              queuesExcluded,
		QueuesExcludedEmpty:         boolToInt64(len(params.QueuesExcluded) < 1),
		QueuesIncluded:              queuesIncluded,
		QueuesIncludedEmpty:         boolToInt64(len(params.QueuesIncluded) < 1),
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
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobRow, error) {
		baseCtx := ctx
		selectCtx := sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
			"order_by_clause": {Value: params.OrderByClause},
			"where_clause":    {Value: params.WhereClause},
		}, params.NamedArgs)
		selectCtx = schemaTemplateParam(selectCtx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Select and lock the rows before deleting them so that a producer can't
		// start one of the selected jobs between the two statements.
		jobs, err := dbsqlc.New().JobDeleteManySelect(selectCtx, dbtx, params.Max)
		if err != nil {
			return nil, interpretError(err)
		}

		if len(jobs) > 0 {
			// Use a fresh context with only the schema replacement. The select
			// context has templates that aren't present in the DELETE query.
			deleteCtx := schemaTemplateParam(baseCtx, params.Schema)
			ids := sliceutil.Map(jobs, func(j *dbsqlc.RiverJob) int64 { return j.ID })
			if err := dbsqlc.New().JobDeleteManyExec(deleteCtx, dbtx, ids); err != nil {
				return nil, interpretError(err)
			}
		}

		return sliceutil.MapError(jobs, jobRowFromInternal)
	})
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		ids, err := dbsqlc.New().JobGetAvailableIDs(ctx, dbtx, &dbsqlc.JobGetAvailableIDsParams{
			Queue: params.Queue,
			Now:   nullTimeFromPtr(params.Now),
			Limit: int32(params.MaxToLock), //nolint:gosec
		})
		if err != nil {
			return nil, interpretError(err)
		}

		if len(ids) == 0 {
			return nil, nil
		}

		if err := dbsqlc.New().JobGetAvailableUpdate(ctx, dbtx, &dbsqlc.JobGetAvailableUpdateParams{
			Now:            nullTimeFromPtr(params.Now),
			MaxAttemptedBy: int64(params.MaxAttemptedBy),
			AttemptedBy:    params.ClientID,
			ID:             ids,
		}); err != nil {
			return nil, interpretError(err)
		}

		jobs, err := dbsqlc.New().JobGetByIDManyOrdered(ctx, dbtx, ids)
		if err != nil {
			return nil, interpretError(err)
		}

		return sliceutil.MapError(jobs, jobRowFromInternal)
	})
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
		AfterID:      params.AfterID,
		Limit:        int32(params.Max), //nolint:gosec
		StuckHorizon: sql.NullTime{Time: params.StuckHorizon.UTC(), Valid: true},
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.MapError(jobs, jobRowFromInternal)
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	var (
		insertRes          = make([]*riverdriver.JobInsertFastResult, len(params.Jobs))
		uniqueNonceByIndex = make([]string, len(params.Jobs))
	)

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		baseCtx := ctx
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		plainInsertCtx := schemaTemplateParam(baseCtx, params.Schema)
		plainInsertCtx = sqlctemplate.WithReplacements(plainInsertCtx, map[string]sqlctemplate.Replacement{
			"on_duplicate_key": {Value: ""},
		}, nil)
		uniqueInsertCtx := schemaTemplateParam(baseCtx, params.Schema)
		uniqueInsertCtx = sqlctemplate.WithReplacements(uniqueInsertCtx, map[string]sqlctemplate.Replacement{
			"on_duplicate_key": {Value: "ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)"},
		}, nil)

		var idsWithNonce []int64
		ids := make([]int64, len(params.Jobs))
		for i, jobParams := range params.Jobs {
			// Only use MySQL's broad ON DUPLICATE KEY behavior when a River unique
			// key is present. Otherwise, a primary key conflict or a user-added
			// unique constraint must be returned as an error.
			insertCtx := plainInsertCtx
			if len(jobParams.UniqueKey) > 0 {
				insertCtx = uniqueInsertCtx

				// Postgres can inspect xmax to determine whether an upsert inserted a
				// row. MySQL has no equivalent, so add a per-row nonce to metadata and
				// compare it with the selected result. Each input needs its own nonce
				// so duplicates within this batch are classified correctly.
				uniqueNonceByIndex[i] = randutil.Hex(8)
			}

			insertParams, err := jobInsertFastParams(jobParams, uniqueNonceByIndex[i])
			if err != nil {
				return err
			}

			res, err := dbsqlc.New().JobInsertFast(insertCtx, dbtx, insertParams)
			if err != nil {
				return interpretError(err)
			}
			ids[i], err = res.LastInsertId()
			if err != nil {
				return err
			}
			if uniqueNonceByIndex[i] != "" {
				idsWithNonce = append(idsWithNonce, ids[i])
			}
		}

		jobs, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, ids)
		if err != nil {
			return interpretError(err)
		}

		jobsByID := make(map[int64]*dbsqlc.RiverJob, len(jobs))
		for _, j := range jobs {
			jobsByID[j.ID] = j
		}

		for i, id := range ids {
			job, err := jobRowFromInternal(jobsByID[id])
			if err != nil {
				return err
			}

			insertRes[i] = &riverdriver.JobInsertFastResult{Job: job}
			if uniqueNonceByIndex[i] != "" {
				uniqueSkippedAsDuplicate := gjson.GetBytes(job.Metadata, rivercommon.MetadataKeyUniqueNonce).Str != uniqueNonceByIndex[i]
				riverUniqueConstraintConflict := bytes.Equal(job.UniqueKey, params.Jobs[i].UniqueKey) &&
					slices.Contains(job.UniqueStates, job.State) &&
					slices.Contains(uniquestates.UniqueBitmaskToStates(params.Jobs[i].UniqueStates), params.Jobs[i].State)
				if uniqueSkippedAsDuplicate && !riverUniqueConstraintConflict {
					// Unlike PostgreSQL and SQLite, MySQL's ON DUPLICATE KEY
					// cannot target River's unique-job index. Reject a conflict
					// on any other unique constraint instead of silently treating
					// the unrelated row as the requested unique job.
					return fmt.Errorf("job insert at index %d conflicted on a non-River unique constraint", i)
				}
				insertRes[i].UniqueSkippedAsDuplicate = uniqueSkippedAsDuplicate
				job.Metadata, err = sjson.DeleteBytes(job.Metadata, rivercommon.MetadataKeyUniqueNonce)
				if err != nil {
					return fmt.Errorf("error removing unique nonce from returned job metadata: %w", err)
				}
			}
		}

		if len(idsWithNonce) > 0 {
			if err := dbsqlc.New().JobClearUniqueNonce(ctx, dbtx, idsWithNonce); err != nil {
				return interpretError(err)
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return insertRes, nil
}

func (e *Executor) JobInsertFastManyNoReturning(ctx context.Context, params *riverdriver.JobInsertFastManyParams) (int, error) {
	// RowsAffected can't distinguish a no-op duplicate from an insert when the
	// DSN enables clientFoundRows. Fall back to nonce-based duplicate detection
	// for any batch where a conflict is possible.
	if slices.ContainsFunc(params.Jobs, func(job *riverdriver.JobInsertFastParams) bool {
		return len(job.UniqueKey) > 0
	}) {
		results, err := e.JobInsertFastMany(ctx, params)
		if err != nil {
			return 0, err
		}

		numInserted := 0
		for _, result := range results {
			if !result.UniqueSkippedAsDuplicate {
				numInserted++
			}
		}
		return numInserted, nil
	}

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		baseCtx := ctx
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		insertCtx := schemaTemplateParam(baseCtx, params.Schema)
		insertCtx = sqlctemplate.WithReplacements(insertCtx, map[string]sqlctemplate.Replacement{
			"on_duplicate_key": {Value: ""},
		}, nil)

		for _, jobParams := range params.Jobs {
			insertParams, err := jobInsertFastParams(jobParams, "")
			if err != nil {
				return err
			}

			if _, err := dbsqlc.New().JobInsertFast(insertCtx, dbtx, insertParams); err != nil {
				return interpretError(err)
			}
		}

		return nil
	}); err != nil {
		return 0, err
	}

	return len(params.Jobs), nil
}

// jobInsertFastParams builds the common insert parameters for a single job.
// If uniqueNonce is non-empty, it's set in the job's metadata for duplicate
// detection.
func jobInsertFastParams(params *riverdriver.JobInsertFastParams, uniqueNonce string) (*dbsqlc.JobInsertFastParams, error) {
	metadata := sliceutil.FirstNonEmpty(params.Metadata, []byte("{}"))
	if uniqueNonce != "" {
		var err error
		metadata, err = sjson.SetBytes(metadata, rivercommon.MetadataKeyUniqueNonce, uniqueNonce)
		if err != nil {
			return nil, fmt.Errorf("error adding unique nonce to job metadata: %w", err)
		}
	}

	tags, err := json.Marshal(params.Tags)
	if err != nil {
		return nil, err
	}

	var uniqueStates sql.NullInt16
	if params.UniqueStates != 0 {
		uniqueStates = sql.NullInt16{Int16: int16(params.UniqueStates), Valid: true}
	}

	var id sql.NullInt64
	if params.ID != nil {
		id = sql.NullInt64{Int64: *params.ID, Valid: true}
	}

	return &dbsqlc.JobInsertFastParams{
		ID:           id,
		Args:         params.EncodedArgs,
		CreatedAt:    utcTimePtr(params.CreatedAt),
		Kind:         params.Kind,
		MaxAttempts:  int64(params.MaxAttempts),
		Metadata:     metadata,
		Priority:     int16(params.Priority), //nolint:gosec
		Queue:        params.Queue,
		ScheduledAt:  utcTimePtr(params.ScheduledAt),
		State:        string(params.State),
		Tags:         tags,
		UniqueKey:    nullStringFromBytes(params.UniqueKey),
		UniqueStates: uniqueStates,
	}, nil
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

	var errorsData []byte
	if len(params.Errors) > 0 {
		var err error
		errorsData, err = json.Marshal(sliceutil.Map(params.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
		if err != nil {
			return nil, err
		}
	}

	tags, err := json.Marshal(params.Tags)
	if err != nil {
		return nil, err
	}

	var uniqueStates sql.NullInt16
	if params.UniqueStates != 0 {
		uniqueStates = sql.NullInt16{Int16: int16(params.UniqueStates), Valid: true}
	}

	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		lastInsertID, err := dbsqlc.New().JobInsertFullExec(ctx, dbtx, &dbsqlc.JobInsertFullExecParams{
			Attempt:      int64(params.Attempt),
			AttemptedAt:  nullTimeFromPtr(params.AttemptedAt),
			AttemptedBy:  attemptedBy,
			Args:         params.EncodedArgs,
			CreatedAt:    utcTimePtr(params.CreatedAt),
			Errors:       errorsData,
			FinalizedAt:  nullTimeFromPtr(params.FinalizedAt),
			Kind:         params.Kind,
			MaxAttempts:  int64(params.MaxAttempts),
			Metadata:     sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
			Priority:     int16(params.Priority), //nolint:gosec
			Queue:        params.Queue,
			ScheduledAt:  utcTimePtr(params.ScheduledAt),
			State:        string(params.State),
			Tags:         tags,
			UniqueKey:    nullStringFromBytes(params.UniqueKey),
			UniqueStates: uniqueStates,
		})
		if err != nil {
			return nil, interpretError(err)
		}

		job, err := dbsqlc.New().JobGetByID(ctx, dbtx, lastInsertID)
		if err != nil {
			return nil, interpretError(err)
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobInsertFullMany(ctx context.Context, params *riverdriver.JobInsertFullManyParams) ([]*rivertype.JobRow, error) {
	insertRes := make([]*rivertype.JobRow, len(params.Jobs))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		ids := make([]int64, len(params.Jobs))

		for i, jobParams := range params.Jobs {
			var attemptedBy []byte
			if jobParams.AttemptedBy != nil {
				var err error
				attemptedBy, err = json.Marshal(jobParams.AttemptedBy)
				if err != nil {
					return err
				}
			}

			var errorsData []byte
			if len(jobParams.Errors) > 0 {
				var err error
				errorsData, err = json.Marshal(sliceutil.Map(jobParams.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
				if err != nil {
					return err
				}
			}

			tags, err := json.Marshal(jobParams.Tags)
			if err != nil {
				return err
			}

			var uniqueStates sql.NullInt16
			if jobParams.UniqueStates != 0 {
				uniqueStates = sql.NullInt16{Int16: int16(jobParams.UniqueStates), Valid: true}
			}

			ids[i], err = dbsqlc.New().JobInsertFullExec(ctx, dbtx, &dbsqlc.JobInsertFullExecParams{
				Attempt:      int64(jobParams.Attempt),
				AttemptedAt:  nullTimeFromPtr(jobParams.AttemptedAt),
				AttemptedBy:  attemptedBy,
				Args:         jobParams.EncodedArgs,
				CreatedAt:    utcTimePtr(jobParams.CreatedAt),
				Errors:       errorsData,
				FinalizedAt:  nullTimeFromPtr(jobParams.FinalizedAt),
				Kind:         jobParams.Kind,
				MaxAttempts:  int64(jobParams.MaxAttempts),
				Metadata:     sliceutil.FirstNonEmpty(jobParams.Metadata, []byte("{}")),
				Priority:     int16(jobParams.Priority), //nolint:gosec
				Queue:        jobParams.Queue,
				ScheduledAt:  utcTimePtr(jobParams.ScheduledAt),
				State:        string(jobParams.State),
				Tags:         tags,
				UniqueKey:    nullStringFromBytes(jobParams.UniqueKey),
				UniqueStates: uniqueStates,
			})
			if err != nil {
				return interpretError(err)
			}
		}

		jobs, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, ids)
		if err != nil {
			return interpretError(err)
		}

		jobsByID := make(map[int64]*dbsqlc.RiverJob, len(jobs))
		for _, j := range jobs {
			jobsByID[j.ID] = j
		}

		for i, id := range ids {
			insertRes[i], err = jobRowFromInternal(jobsByID[id])
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

func (e *Executor) JobKindList(ctx context.Context, params *riverdriver.JobKindListParams) ([]string, error) {
	exclude := params.Exclude
	if len(exclude) == 0 {
		exclude = []string{""}
	}

	kinds, err := dbsqlc.New().JobKindList(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.JobKindListParams{
		After:   params.After,
		Exclude: exclude,
		Match:   params.Match,
		Limit:   int32(min(params.Max, math.MaxInt32)), //nolint:gosec
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
	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		for i := range params.ID {
			if err := dbsqlc.New().JobRescue(ctx, dbtx, &dbsqlc.JobRescueParams{
				ID:          params.ID[i],
				Error:       params.Error[i],
				FinalizedAt: nullTimeFromPtr(params.FinalizedAt[i]),
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
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		_, err := dbsqlc.New().JobRetryExec(ctx, dbtx, &dbsqlc.JobRetryExecParams{
			ID:  params.ID,
			Now: nullTimeFromPtr(params.Now),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		job, err := dbsqlc.New().JobGetByID(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}
		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*riverdriver.JobScheduleResult, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*riverdriver.JobScheduleResult, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		scheduleResults, err := dbsqlc.New().JobSchedule(ctx, dbtx, &dbsqlc.JobScheduleParams{
			Limit: int32(params.Max), //nolint:gosec
			Now:   nullTimeFromPtr(params.Now),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		var (
			allIDs     []int64
			availIDs   []int64
			discardIDs []int64
			discardSet = make(map[int64]bool)
		)

		for _, result := range scheduleResults {
			allIDs = append(allIDs, result.ID)
			if result.ConflictDiscarded != 0 {
				discardIDs = append(discardIDs, result.ID)
				discardSet[result.ID] = true
			} else {
				availIDs = append(availIDs, result.ID)
			}
		}

		if len(availIDs) > 0 {
			if err := dbsqlc.New().JobScheduleSetAvailableExec(ctx, dbtx, availIDs); err != nil {
				return nil, interpretError(err)
			}
		}

		if len(discardIDs) > 0 {
			if err := dbsqlc.New().JobScheduleSetDiscardedExec(ctx, dbtx, &dbsqlc.JobScheduleSetDiscardedExecParams{
				ID:  discardIDs,
				Now: nullTimeFromPtr(params.Now),
			}); err != nil {
				return nil, interpretError(err)
			}
		}

		if len(allIDs) == 0 {
			return nil, nil
		}

		updatedJobs, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, allIDs)
		if err != nil {
			return nil, interpretError(err)
		}

		jobsByID := make(map[int64]*dbsqlc.RiverJob, len(updatedJobs))
		for _, j := range updatedJobs {
			jobsByID[j.ID] = j
		}

		// Return results in the same order as scheduleResults.
		results := make([]*riverdriver.JobScheduleResult, len(scheduleResults))
		for i, sr := range scheduleResults {
			job, err := jobRowFromInternal(jobsByID[sr.ID])
			if err != nil {
				return nil, err
			}
			results[i] = &riverdriver.JobScheduleResult{ConflictDiscarded: discardSet[sr.ID], Job: *job}
		}

		return results, nil
	})
}

func (e *Executor) JobSetStateIfRunningMany(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
	setRes := make([]*rivertype.JobRow, 0, len(params.ID))

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		// Step 1: Execute all state changes.
		for i := range params.ID {
			setStateParams := &dbsqlc.JobSetStateIfRunningExecParams{
				ID:              params.ID[i],
				Error:           []byte("{}"),
				MetadataUpdates: []byte("{}"),
				Now:             nullTimeFromPtr(params.Now),
				State:           string(params.State[i]),
			}

			if params.Attempt[i] != nil {
				setStateParams.AttemptDoUpdate = 1
				setStateParams.Attempt = int64(*params.Attempt[i])
			}
			if params.ErrData[i] != nil {
				setStateParams.ErrorsDoUpdate = 1
				setStateParams.Error = params.ErrData[i]
			}
			if params.FinalizedAt[i] != nil {
				setStateParams.FinalizedAtDoUpdate = 1
				setStateParams.FinalizedAt = nullTimeFromPtr(params.FinalizedAt[i])
			}
			if params.MetadataDoMerge[i] {
				setStateParams.MetadataDoMerge = 1
				setStateParams.MetadataUpdates = params.MetadataUpdates[i]
			}
			if params.ScheduledAt[i] != nil {
				setStateParams.ScheduledAtDoUpdate = 1
				setStateParams.ScheduledAt = params.ScheduledAt[i].UTC()
			}

			if err := dbsqlc.New().JobSetStateIfRunningExec(ctx, dbtx, setStateParams); err != nil {
				return fmt.Errorf("error setting job state: %w", err)
			}
		}

		// Step 2: Batch fetch all jobs.
		jobs, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, params.ID)
		if err != nil {
			return interpretError(err)
		}

		jobsByID := make(map[int64]*dbsqlc.RiverJob, len(jobs))
		for _, j := range jobs {
			jobsByID[j.ID] = j
		}

		// Step 3: For jobs that weren't running, merge metadata if requested.
		var metadataMergedIDs []int64
		for i := range params.ID {
			job := jobsByID[params.ID[i]]
			if job == nil {
				continue
			}

			if rivertype.JobState(job.State) != rivertype.JobStateRunning && params.MetadataDoMerge[i] {
				res, err := dbsqlc.New().JobSetMetadataIfNotRunningExec(ctx, dbtx, &dbsqlc.JobSetMetadataIfNotRunningExecParams{
					ID:              params.ID[i],
					MetadataUpdates: sliceutil.FirstNonEmpty(params.MetadataUpdates[i], []byte("{}")),
				})
				if err != nil {
					return fmt.Errorf("error setting job metadata: %w", err)
				}

				rowsAffected, err := res.RowsAffected()
				if err != nil {
					return err
				}

				if rowsAffected > 0 {
					metadataMergedIDs = append(metadataMergedIDs, params.ID[i])
				}
			}
		}

		// Step 4: Re-fetch jobs that had metadata merged.
		if len(metadataMergedIDs) > 0 {
			refreshed, err := dbsqlc.New().JobGetByIDMany(ctx, dbtx, metadataMergedIDs)
			if err != nil {
				return interpretError(err)
			}
			for _, j := range refreshed {
				jobsByID[j.ID] = j
			}
		}

		// Step 5: Build results in original order.
		for _, id := range params.ID {
			job := jobsByID[id]
			if job == nil {
				continue
			}

			jobRow, err := jobRowFromInternal(job)
			if err != nil {
				return err
			}
			setRes = append(setRes, jobRow)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return setRes, nil
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	metadata := params.Metadata
	if metadata == nil {
		metadata = []byte("{}")
	}

	var metadataDoMerge int64
	if params.MetadataDoMerge {
		metadataDoMerge = 1
	}

	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		if err := dbsqlc.New().JobUpdateExec(ctx, dbtx, &dbsqlc.JobUpdateExecParams{
			ID:              params.ID,
			MetadataDoMerge: metadataDoMerge,
			Metadata:        metadata,
		}); err != nil {
			return nil, interpretError(err)
		}

		job, err := dbsqlc.New().JobGetByID(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}

		return jobRowFromInternal(job)
	})
}

func (e *Executor) JobUpdateFull(ctx context.Context, params *riverdriver.JobUpdateFullParams) (*rivertype.JobRow, error) {
	attemptedAt := params.AttemptedAt
	if attemptedAt != nil {
		attemptedAt = ptrutil.Ptr(attemptedAt.UTC())
	}

	attemptedBy, err := json.Marshal(params.AttemptedBy)
	if err != nil {
		return nil, err
	}

	errorsData, err := json.Marshal(sliceutil.Map(params.Errors, func(e []byte) json.RawMessage { return json.RawMessage(e) }))
	if err != nil {
		return nil, err
	}

	finalizedAt := params.FinalizedAt
	if finalizedAt != nil {
		finalizedAt = ptrutil.Ptr(finalizedAt.UTC())
	}

	metadata := params.Metadata
	if metadata == nil {
		metadata = []byte("{}")
	}

	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.JobRow, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		if err := dbsqlc.New().JobUpdateFullExec(ctx, dbtx, &dbsqlc.JobUpdateFullExecParams{
			ID:                  params.ID,
			Attempt:             int64(params.Attempt),
			AttemptDoUpdate:     boolToInt64(params.AttemptDoUpdate),
			AttemptedAt:         nullTimeFromPtr(attemptedAt),
			AttemptedAtDoUpdate: boolToInt64(params.AttemptedAtDoUpdate),
			AttemptedBy:         attemptedBy,
			AttemptedByDoUpdate: boolToInt64(params.AttemptedByDoUpdate),
			ErrorsDoUpdate:      boolToInt64(params.ErrorsDoUpdate),
			Errors:              errorsData,
			FinalizedAtDoUpdate: boolToInt64(params.FinalizedAtDoUpdate),
			FinalizedAt:         nullTimeFromPtr(finalizedAt),
			MaxAttemptsDoUpdate: boolToInt64(params.MaxAttemptsDoUpdate),
			MaxAttempts:         int64(min(params.MaxAttempts, math.MaxInt64)),
			MetadataDoUpdate:    boolToInt64(params.MetadataDoUpdate),
			Metadata:            metadata,
			StateDoUpdate:       boolToInt64(params.StateDoUpdate),
			State:               string(params.State),
		}); err != nil {
			return nil, interpretError(err)
		}

		job, err := dbsqlc.New().JobGetByID(ctx, dbtx, params.ID)
		if err != nil {
			return nil, interpretError(err)
		}

		return jobRowFromInternal(job)
	})
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (*riverdriver.Leader, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*riverdriver.Leader, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		err := dbsqlc.New().LeaderAttemptElectExec(ctx, dbtx, &dbsqlc.LeaderAttemptElectExecParams{
			LeaderID: params.LeaderID,
			Now:      nullTimeFromPtr(params.Now),
			TTL:      params.TTL.Microseconds(),
		})
		if err != nil {
			if isDuplicateEntry(err) {
				return nil, rivertype.ErrNotFound
			}
			return nil, interpretError(err)
		}

		leader, err := dbsqlc.New().LeaderGetElectedLeader(ctx, dbtx)
		if err != nil {
			return nil, interpretError(err)
		}
		return leaderFromInternal(leader), nil
	})
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderReelectParams) (*riverdriver.Leader, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*riverdriver.Leader, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		res, err := dbsqlc.New().LeaderAttemptReelectExec(ctx, dbtx, &dbsqlc.LeaderAttemptReelectExecParams{
			ElectedAt: params.ElectedAt.UTC(),
			LeaderID:  params.LeaderID,
			Now:       nullTimeFromPtr(params.Now),
			TTL:       params.TTL.Microseconds(),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			return nil, rivertype.ErrNotFound
		}

		leader, err := dbsqlc.New().LeaderGetElectedLeader(ctx, dbtx)
		if err != nil {
			return nil, interpretError(err)
		}
		return leaderFromInternal(leader), nil
	})
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, params *riverdriver.LeaderDeleteExpiredParams) (int, error) {
	numDeleted, err := dbsqlc.New().LeaderDeleteExpired(schemaTemplateParam(ctx, params.Schema), e.dbtx, nullTimeFromPtr(params.Now))
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
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*riverdriver.Leader, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		if err := dbsqlc.New().LeaderInsertExec(ctx, dbtx, &dbsqlc.LeaderInsertExecParams{
			ElectedAt: utcTimePtr(params.ElectedAt),
			ExpiresAt: utcTimePtr(params.ExpiresAt),
			Now:       nullTimeFromPtr(params.Now),
			LeaderID:  params.LeaderID,
			TTL:       params.TTL.Microseconds(),
		}); err != nil {
			return nil, interpretError(err)
		}

		leader, err := dbsqlc.New().LeaderGetElectedLeader(ctx, dbtx)
		if err != nil {
			return nil, interpretError(err)
		}
		return leaderFromInternal(leader), nil
	})
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	numResigned, err := dbsqlc.New().LeaderResign(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.LeaderResignParams{
		ElectedAt: params.ElectedAt.UTC(),
		LeaderID:  params.LeaderID,
	})
	if err != nil {
		return false, interpretError(err)
	}
	return numResigned > 0, nil
}

func (e *Executor) MigrationDeleteAssumingMainMany(ctx context.Context, params *riverdriver.MigrationDeleteAssumingMainManyParams) ([]*riverdriver.Migration, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*riverdriver.Migration, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		versions := sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) })

		migrations, err := dbsqlc.New().RiverMigrationDeleteAssumingMainMany(ctx, dbtx, versions)
		if err != nil {
			return nil, interpretError(err)
		}

		if len(versions) > 0 {
			if err := dbsqlc.New().RiverMigrationDeleteAssumingMainManyExec(ctx, dbtx, versions); err != nil {
				return nil, interpretError(err)
			}
		}

		return sliceutil.Map(migrations, func(internal *dbsqlc.RiverMigrationDeleteAssumingMainManyRow) *riverdriver.Migration {
			return &riverdriver.Migration{
				CreatedAt: internal.CreatedAt.UTC(),
				Line:      riverdriver.MigrationLineMain,
				Version:   int(internal.Version),
			}
		}), nil
	})
}

func (e *Executor) MigrationDeleteByLineAndVersionMany(ctx context.Context, params *riverdriver.MigrationDeleteByLineAndVersionManyParams) ([]*riverdriver.Migration, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*riverdriver.Migration, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		versions := sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) })

		migrations, err := dbsqlc.New().RiverMigrationDeleteByLineAndVersionMany(ctx, dbtx, &dbsqlc.RiverMigrationDeleteByLineAndVersionManyParams{
			Line:    params.Line,
			Version: versions,
		})
		if err != nil {
			return nil, interpretError(err)
		}

		if len(versions) > 0 {
			if err := dbsqlc.New().RiverMigrationDeleteByLineAndVersionManyExec(ctx, dbtx, &dbsqlc.RiverMigrationDeleteByLineAndVersionManyExecParams{
				Line:    params.Line,
				Version: versions,
			}); err != nil {
				return nil, interpretError(err)
			}
		}

		return sliceutil.Map(migrations, migrationFromInternal), nil
	})
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
	var migrations []*riverdriver.Migration

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		for _, version := range params.Versions {
			if err := dbsqlc.New().RiverMigrationInsertExec(ctx, dbtx, &dbsqlc.RiverMigrationInsertExecParams{
				Line:    params.Line,
				Version: int64(version),
			}); err != nil {
				return interpretError(err)
			}
		}

		versions := sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) })

		internals, err := dbsqlc.New().RiverMigrationGetByLineAndVersionMany(ctx, dbtx, &dbsqlc.RiverMigrationGetByLineAndVersionManyParams{
			Line:    params.Line,
			Version: versions,
		})
		if err != nil {
			return interpretError(err)
		}

		migrations = sliceutil.Map(internals, migrationFromInternal)
		return nil
	}); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (e *Executor) MigrationInsertManyAssumingMain(ctx context.Context, params *riverdriver.MigrationInsertManyAssumingMainParams) ([]*riverdriver.Migration, error) {
	var migrations []*riverdriver.Migration

	if err := dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		for _, version := range params.Versions {
			if err := dbsqlc.New().RiverMigrationInsertAssumingMainExec(ctx, dbtx, int64(version)); err != nil {
				return interpretError(err)
			}
		}

		versions := sliceutil.Map(params.Versions, func(v int) int64 { return int64(v) })

		internals, err := dbsqlc.New().RiverMigrationGetByVersionMany(ctx, dbtx, versions)
		if err != nil {
			return interpretError(err)
		}

		migrations = sliceutil.Map(internals, func(internal *dbsqlc.RiverMigrationGetByVersionManyRow) *riverdriver.Migration {
			return &riverdriver.Migration{
				CreatedAt: internal.CreatedAt.UTC(),
				Line:      riverdriver.MigrationLineMain,
				Version:   int(internal.Version),
			}
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return migrations, nil
}

func (e *Executor) NotificationDeleteBefore(ctx context.Context, params *riverdriver.NotificationDeleteBeforeParams) (int, error) {
	numDeleted, err := dbsqlc.New().NotificationDeleteBefore(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.CreatedAtHorizon.UTC())
	return int(numDeleted), interpretError(err)
}

func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	if len(params.Payload) < 1 {
		return nil
	}

	return dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		for _, payload := range params.Payload {
			if err := dbsqlc.New().NotificationInsert(ctx, dbtx, &dbsqlc.NotificationInsertParams{
				Payload: payload,
				Topic:   params.Topic,
			}); err != nil {
				return interpretError(err)
			}
		}

		return nil
	})
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	// MySQL has GET_LOCK which is similar to PostgreSQL advisory locks, but
	// it's session-scoped rather than transaction-scoped. For now, return
	// not implemented.
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) QueueCreateOrSetUpdatedAt(ctx context.Context, params *riverdriver.QueueCreateOrSetUpdatedAtParams) (*rivertype.Queue, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.Queue, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		if err := dbsqlc.New().QueueCreateOrSetUpdatedAtExec(ctx, dbtx, &dbsqlc.QueueCreateOrSetUpdatedAtExecParams{
			Metadata:  sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
			Name:      params.Name,
			Now:       nullTimeFromPtr(params.Now),
			PausedAt:  nullTimeFromPtr(params.PausedAt),
			UpdatedAt: nullTimeFromPtr(params.UpdatedAt),
		}); err != nil {
			return nil, interpretError(err)
		}

		queue, err := dbsqlc.New().QueueGet(ctx, dbtx, params.Name)
		if err != nil {
			return nil, interpretError(err)
		}
		return queueFromInternal(queue), nil
	})
}

func (e *Executor) QueueDeleteExpired(ctx context.Context, params *riverdriver.QueueDeleteExpiredParams) ([]string, error) {
	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]string, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		queueNames, err := dbsqlc.New().QueueDeleteExpiredSelect(ctx, dbtx, &dbsqlc.QueueDeleteExpiredSelectParams{
			Limit:            int32(params.Max), //nolint:gosec
			UpdatedAtHorizon: params.UpdatedAtHorizon.UTC(),
		})
		if err != nil {
			return nil, interpretError(err)
		}

		if len(queueNames) > 0 {
			if err := dbsqlc.New().QueueDeleteExpiredExec(ctx, dbtx, queueNames); err != nil {
				return nil, interpretError(err)
			}
		}

		return queueNames, nil
	})
}

func (e *Executor) QueueGet(ctx context.Context, params *riverdriver.QueueGetParams) (*rivertype.Queue, error) {
	queue, err := dbsqlc.New().QueueGet(schemaTemplateParam(ctx, params.Schema), e.dbtx, params.Name)
	if err != nil {
		return nil, interpretError(err)
	}
	return queueFromInternal(queue), nil
}

func (e *Executor) QueueList(ctx context.Context, params *riverdriver.QueueListParams) ([]*rivertype.Queue, error) {
	queues, err := dbsqlc.New().QueueList(schemaTemplateParam(ctx, params.Schema), e.dbtx, int32(params.Max)) //nolint:gosec
	if err != nil {
		return nil, interpretError(err)
	}
	return sliceutil.Map(queues, queueFromInternal), nil
}

func (e *Executor) QueueNameList(ctx context.Context, params *riverdriver.QueueNameListParams) ([]string, error) {
	exclude := params.Exclude
	if len(exclude) == 0 {
		exclude = []string{""}
	}
	queueNames, err := dbsqlc.New().QueueNameList(schemaTemplateParam(ctx, params.Schema), e.dbtx, &dbsqlc.QueueNameListParams{
		After:   params.After,
		Exclude: exclude,
		Match:   params.Match,
		Limit:   int32(min(params.Max, math.MaxInt32)), //nolint:gosec
	})
	if err != nil {
		return nil, interpretError(err)
	}
	return queueNames, nil
}

func (e *Executor) QueuePause(ctx context.Context, params *riverdriver.QueuePauseParams) error {
	return dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		now := nullTimeFromPtr(params.Now)

		var (
			res sql.Result
			err error
		)

		if params.Name == riverdriver.AllQueuesString {
			res, err = dbsqlc.New().QueuePauseAll(ctx, dbtx, &dbsqlc.QueuePauseAllParams{Now: now})
		} else {
			res, err = dbsqlc.New().QueuePauseByName(ctx, dbtx, &dbsqlc.QueuePauseByNameParams{
				Name: params.Name,
				Now:  now,
			})
		}
		if err != nil {
			return interpretError(err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return interpretError(err)
		}

		// MySQL only reports rows that actually changed values, not rows
		// matched. If no rows were affected for a named queue, check if the
		// queue exists before returning ErrNotFound (it may already be paused).
		if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
			if _, err := dbsqlc.New().QueueGet(ctx, dbtx, params.Name); err != nil {
				return interpretError(err)
			}
		}
		return nil
	})
}

func (e *Executor) QueueResume(ctx context.Context, params *riverdriver.QueueResumeParams) error {
	return dbutil.WithTx(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}
		now := nullTimeFromPtr(params.Now)

		var (
			res sql.Result
			err error
		)

		if params.Name == riverdriver.AllQueuesString {
			res, err = dbsqlc.New().QueueResumeAll(ctx, dbtx, now)
		} else {
			res, err = dbsqlc.New().QueueResumeByName(ctx, dbtx, &dbsqlc.QueueResumeByNameParams{
				Name: params.Name,
				Now:  now,
			})
		}
		if err != nil {
			return interpretError(err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return interpretError(err)
		}

		// MySQL only reports rows that actually changed values, not rows
		// matched. If no rows were affected for a named queue, check if the
		// queue exists before returning ErrNotFound (it may already be resumed).
		if rowsAffected < 1 && params.Name != riverdriver.AllQueuesString {
			if _, err := dbsqlc.New().QueueGet(ctx, dbtx, params.Name); err != nil {
				return interpretError(err)
			}
		}
		return nil
	})
}

func (e *Executor) QueueUpdate(ctx context.Context, params *riverdriver.QueueUpdateParams) (*rivertype.Queue, error) {
	var metadataDoUpdate int64
	if params.MetadataDoUpdate {
		metadataDoUpdate = 1
	}

	return dbutil.WithTxV(ctx, e, func(ctx context.Context, execTx riverdriver.ExecutorTx) (*rivertype.Queue, error) {
		ctx = schemaTemplateParam(ctx, params.Schema)
		dbtx := templateReplaceWrapper{dbtx: e.driver.UnwrapTx(execTx), replacer: &e.driver.replacer}

		if err := dbsqlc.New().QueueUpdateExec(ctx, dbtx, &dbsqlc.QueueUpdateExecParams{
			Metadata:         sliceutil.FirstNonEmpty(params.Metadata, []byte("{}")),
			MetadataDoUpdate: metadataDoUpdate,
			Name:             params.Name,
		}); err != nil {
			return nil, interpretError(err)
		}

		queue, err := dbsqlc.New().QueueGet(ctx, dbtx, params.Name)
		if err != nil {
			return nil, interpretError(err)
		}
		return queueFromInternal(queue), nil
	})
}

func (e *Executor) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	return e.dbtx.QueryRowContext(ctx, sql, args...)
}

func (e *Executor) SchemaCreate(ctx context.Context, params *riverdriver.SchemaCreateParams) error {
	// In MySQL, schemas are databases. Create one if it doesn't exist.
	if params.Schema != "" {
		_, err := e.dbtx.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+mysqlIdentifier(params.Schema))
		return interpretError(err)
	}
	return nil
}

func (e *Executor) SchemaDrop(ctx context.Context, params *riverdriver.SchemaDropParams) error {
	if params.Schema != "" {
		_, err := e.dbtx.ExecContext(ctx, "DROP DATABASE IF EXISTS "+mysqlIdentifier(params.Schema))
		return interpretError(err)
	}
	return nil
}

func (e *Executor) SchemaGetExpired(ctx context.Context, params *riverdriver.SchemaGetExpiredParams) ([]string, error) {
	schemas, err := dbsqlc.New().SchemaGetExpired(informationSchemaParam(ctx), e.dbtx, &dbsqlc.SchemaGetExpiredParams{
		BeforeName: params.BeforeName,
		Prefix:     params.Prefix,
	})
	return schemas, interpretError(err)
}

func (e *Executor) TableExists(ctx context.Context, params *riverdriver.TableExistsParams) (bool, error) {
	ctx = informationSchemaParam(ctx)

	exists, err := dbsqlc.New().TableExists(ctx, e.dbtx, &dbsqlc.TableExistsParams{
		TableName: params.Table,
		Schema:    sql.NullString{String: params.Schema, Valid: params.Schema != ""},
	})
	if err != nil {
		return false, interpretError(err)
	}
	return exists, nil
}

func (e *Executor) TableTruncate(ctx context.Context, params *riverdriver.TableTruncateParams) error {
	var maybeSchema string
	if params.Schema != "" {
		maybeSchema = mysqlIdentifier(params.Schema) + "."
	}

	for _, table := range params.Table {
		// MySQL's TRUNCATE TABLE is DDL and can't be used in transactions.
		// Use DELETE FROM instead for transactional safety.
		_, err := e.dbtx.ExecContext(ctx, "DELETE FROM "+maybeSchema+mysqlIdentifier(table))
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
	executorSubTx := &ExecutorSubTx{
		beginOnce:    &savepointutil.BeginOnlyOnce{},
		savepointNum: 0,
		tx:           t.tx,
	}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver, executorSubTx}
	return executorSubTx.Begin(ctx)
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	return t.tx.Commit()
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
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

	executorSubTx := &ExecutorSubTx{
		beginOnce:    savepointutil.NewBeginOnlyOnce(t.beginOnce),
		savepointNum: nextSavepointNum,
		tx:           t.tx,
	}
	executorSubTx.Executor = Executor{nil, templateReplaceWrapper{t.tx, &t.driver.replacer}, t.driver, executorSubTx}

	return executorSubTx, nil
}

func (t *ExecutorSubTx) Commit(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed")
	}

	if err := t.Exec(ctx, fmt.Sprintf("RELEASE SAVEPOINT %s%02d", savepointPrefix, t.savepointNum)); err != nil {
		// MySQL DDL statements (CREATE TABLE, ALTER TABLE, etc.) cause an
		// implicit COMMIT which destroys savepoints. If the savepoint no
		// longer exists, the work was already committed by the DDL.
		if isSavepointDoesNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func (t *ExecutorSubTx) Rollback(ctx context.Context) error {
	defer t.beginOnce.Done()

	if t.beginOnce.IsDone() {
		return errors.New("tx is closed")
	}

	if err := t.Exec(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s%02d", savepointPrefix, t.savepointNum)); err != nil {
		// MySQL DDL statements cause implicit COMMIT which destroys
		// savepoints. If the savepoint no longer exists, there's nothing
		// to roll back to.
		if isSavepointDoesNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

// isSavepointDoesNotExist checks if a MySQL error indicates that a savepoint
// does not exist (error 1305). This happens when DDL statements cause an
// implicit COMMIT that destroys active savepoints.
func isSavepointDoesNotExist(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1305
}

func isDuplicateEntry(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func interpretError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

// mysqlIdentifier quotes an identifier with backticks for MySQL.
// MySQL uses backticks instead of double quotes for identifier quoting.
func mysqlIdentifier(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

type templateReplaceWrapper struct {
	dbtx     dbsqlc.DBTX
	replacer *sqlctemplate.Replacer
}

func (w templateReplaceWrapper) ExecContext(ctx context.Context, rawSQL string, rawArgs ...any) (sql.Result, error) {
	sqlStr, args := w.replacer.Run(ctx, argPlaceholder, rawSQL, rawArgs)
	return w.dbtx.ExecContext(ctx, sqlStr, args...)
}

func (w templateReplaceWrapper) PrepareContext(ctx context.Context, rawSQL string) (*sql.Stmt, error) {
	sqlStr, _ := w.replacer.Run(ctx, argPlaceholder, rawSQL, nil)
	return w.dbtx.PrepareContext(ctx, sqlStr)
}

func (w templateReplaceWrapper) QueryContext(ctx context.Context, rawSQL string, rawArgs ...any) (*sql.Rows, error) {
	sqlStr, args := w.replacer.Run(ctx, argPlaceholder, rawSQL, rawArgs)
	return w.dbtx.QueryContext(ctx, sqlStr, args...)
}

func (w templateReplaceWrapper) QueryRowContext(ctx context.Context, rawSQL string, rawArgs ...any) *sql.Row {
	sqlStr, args := w.replacer.Run(ctx, argPlaceholder, rawSQL, rawArgs)
	return w.dbtx.QueryRowContext(ctx, sqlStr, args...)
}

func jobRowFromInternal(internal *dbsqlc.RiverJob) (*rivertype.JobRow, error) {
	attemptedAt := ptrFromNullTime(internal.AttemptedAt)
	if attemptedAt != nil {
		t := attemptedAt.UTC()
		attemptedAt = &t
	}

	var attemptedBy []string
	if internal.AttemptedBy != nil {
		if err := json.Unmarshal(internal.AttemptedBy, &attemptedBy); err != nil {
			return nil, fmt.Errorf("error unmarshaling `attempted_by`: %w", err)
		}
	}

	var errs []rivertype.AttemptError
	if internal.Errors != nil {
		if err := json.Unmarshal(internal.Errors, &errs); err != nil {
			return nil, fmt.Errorf("error unmarshaling `errors`: %w", err)
		}
	}

	finalizedAt := ptrFromNullTime(internal.FinalizedAt)
	if finalizedAt != nil {
		t := finalizedAt.UTC()
		finalizedAt = &t
	}

	var tags []string
	if err := json.Unmarshal(internal.Tags, &tags); err != nil {
		return nil, fmt.Errorf("error unmarshaling `tags`: %w", err)
	}

	var uniqueStatesByte byte
	if internal.UniqueStates.Valid {
		if internal.UniqueStates.Int16 < 0 || internal.UniqueStates.Int16 > 255 {
			return nil, fmt.Errorf("value out of range for byte: %d", internal.UniqueStates.Int16)
		}
		uniqueStatesByte = byte(internal.UniqueStates.Int16)
	}

	return &rivertype.JobRow{
		ID:           internal.ID,
		Attempt:      max(int(internal.Attempt), 0),
		AttemptedAt:  attemptedAt,
		AttemptedBy:  attemptedBy,
		CreatedAt:    internal.CreatedAt.UTC(),
		EncodedArgs:  internal.Args,
		Errors:       errs,
		FinalizedAt:  finalizedAt,
		Kind:         internal.Kind,
		MaxAttempts:  max(int(internal.MaxAttempts), 0),
		Metadata:     internal.Metadata,
		Priority:     max(int(internal.Priority), 0),
		Queue:        internal.Queue,
		ScheduledAt:  internal.ScheduledAt.UTC(),
		State:        rivertype.JobState(internal.State),
		Tags:         tags,
		UniqueKey:    bytesFromNullString(internal.UniqueKey),
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
		schema = mysqlIdentifier(schema) + "."
	}

	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}, nil)
}

func informationSchemaParam(ctx context.Context) context.Context {
	return sqlctemplate.WithReplacements(ctx, map[string]sqlctemplate.Replacement{
		"information_schema": {Stable: true, Value: "INFORMATION_SCHEMA."},
	}, nil)
}

func queueFromInternal(internal *dbsqlc.RiverQueue) *rivertype.Queue {
	pausedAt := ptrFromNullTime(internal.PausedAt)
	if pausedAt != nil {
		t := pausedAt.UTC()
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

func nullTimeFromPtr(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: t.UTC(), Valid: true}
}

func ptrFromNullTime(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

func nullStringFromBytes(b []byte) sql.NullString {
	if b == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}

func bytesFromNullString(s sql.NullString) []byte {
	if !s.Valid {
		return nil
	}
	return []byte(s.String)
}

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	return ptrutil.Ptr(t.UTC())
}
