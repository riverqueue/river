// Package riverdatabasesql bundles a River driver for Go's built in database/sql.
//
// This is _not_ a fully functional driver, and only supports use through
// rivermigrate for purposes of interacting with migration frameworks like
// Goose. Using it with a River client will panic.
package riverdatabasesql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/dbsqlc"
	"github.com/riverqueue/river/rivertype"
)

// Driver is an implementation of riverdriver.Driver for database/sql.
type Driver struct {
	dbPool  *sql.DB
	queries *dbsqlc.Queries
}

// New returns a new database/sql River driver for use with River.
//
// It takes an sql.DB to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field. The pool
// must not be closed while associated River objects are running.
//
// This is _not_ a fully functional driver, and only supports use through
// rivermigrate for purposes of interacting with migration frameworks like
// Goose. Using it with a River client will panic.
func New(dbPool *sql.DB) *Driver {
	return &Driver{dbPool: dbPool, queries: dbsqlc.New()}
}

func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, d.dbPool, dbsqlc.New()}
}

func (d *Driver) GetListener() riverdriver.Listener { panic(riverdriver.ErrNotImplemented) }

func (d *Driver) HasPool() bool { return d.dbPool != nil }

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.ExecutorTx {
	return &ExecutorTx{Executor: Executor{nil, tx, dbsqlc.New()}, tx: tx}
}

type Executor struct {
	dbPool  *sql.DB
	dbtx    dbsqlc.DBTX
	queries *dbsqlc.Queries
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	if e.dbPool == nil {
		return nil, riverdriver.ErrSubTxNotSupported
	}

	tx, err := e.dbPool.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{nil, tx, e.queries}, tx: tx}, nil
}

func (e *Executor) Exec(ctx context.Context, sql string) (struct{}, error) {
	_, err := e.dbtx.ExecContext(ctx, sql)
	return struct{}{}, interpretError(err)
}

func (e *Executor) JobCancel(ctx context.Context, params *riverdriver.JobCancelParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobCountByState(ctx context.Context, state rivertype.JobState) (int, error) {
	return 0, riverdriver.ErrNotImplemented
}

func (e *Executor) JobDeleteBefore(ctx context.Context, params *riverdriver.JobDeleteBeforeParams) (int, error) {
	return 0, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetAvailable(ctx context.Context, params *riverdriver.JobGetAvailableParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetByID(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetByIDMany(ctx context.Context, id []int64) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetByKindAndUniqueProperties(ctx context.Context, params *riverdriver.JobGetByKindAndUniquePropertiesParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetByKindMany(ctx context.Context, kind []string) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobGetStuck(ctx context.Context, params *riverdriver.JobGetStuckParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobInsertFast(ctx context.Context, params *riverdriver.JobInsertFastParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobInsertFastMany(ctx context.Context, params []*riverdriver.JobInsertFastParams) (int, error) {
	return 0, riverdriver.ErrNotImplemented
}

func (e *Executor) JobInsertFull(ctx context.Context, params *riverdriver.JobInsertFullParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobList(ctx context.Context, sql string, namedArgs map[string]any) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobListFields() string {
	panic(riverdriver.ErrNotImplemented)
}

func (e *Executor) JobRescueMany(ctx context.Context, params *riverdriver.JobRescueManyParams) (*struct{}, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobSchedule(ctx context.Context, params *riverdriver.JobScheduleParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobSetCompleteIfRunningMany(ctx context.Context, params *riverdriver.JobSetCompleteIfRunningManyParams) ([]*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobSetStateIfRunning(ctx context.Context, params *riverdriver.JobSetStateIfRunningParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) JobUpdate(ctx context.Context, params *riverdriver.JobUpdateParams) (*rivertype.JobRow, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderAttemptElect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	return false, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderAttemptReelect(ctx context.Context, params *riverdriver.LeaderElectParams) (bool, error) {
	return false, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderDeleteExpired(ctx context.Context, name string) (int, error) {
	return 0, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderGetElectedLeader(ctx context.Context, name string) (*riverdriver.Leader, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderInsert(ctx context.Context, params *riverdriver.LeaderInsertParams) (*riverdriver.Leader, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) LeaderResign(ctx context.Context, params *riverdriver.LeaderResignParams) (bool, error) {
	return false, riverdriver.ErrNotImplemented
}

func (e *Executor) MigrationDeleteByVersionMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationDeleteByVersionMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationGetAll(ctx context.Context) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationGetAll(ctx, e.dbtx)
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) MigrationInsertMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationInsertMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	if err != nil {
		return nil, interpretError(err)
	}
	return mapSlice(migrations, migrationFromInternal), nil
}

func (e *Executor) NotifyMany(ctx context.Context, params *riverdriver.NotifyManyParams) error {
	return riverdriver.ErrNotImplemented
}

func (e *Executor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	return nil, riverdriver.ErrNotImplemented
}

func (e *Executor) TableExists(ctx context.Context, tableName string) (bool, error) {
	exists, err := e.queries.TableExists(ctx, e.dbtx, tableName)
	return exists, interpretError(err)
}

type ExecutorTx struct {
	Executor
	tx *sql.Tx
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Commit()
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	// unfortunately, `database/sql` does not take a context ...
	return t.tx.Rollback()
}

func interpretError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return rivertype.ErrNotFound
	}
	return err
}

// mapSlice manipulates a slice and transforms it to a slice of another type.
func mapSlice[T any, R any](collection []T, mapFunc func(T) R) []R {
	if collection == nil {
		return nil
	}

	result := make([]R, len(collection))

	for i, item := range collection {
		result[i] = mapFunc(item)
	}

	return result
}

func migrationFromInternal(internal *dbsqlc.RiverMigration) *riverdriver.Migration {
	return &riverdriver.Migration{
		ID:        int(internal.ID),
		CreatedAt: internal.CreatedAt.UTC(),
		Version:   int(internal.Version),
	}
}
