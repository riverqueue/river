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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql/internal/dbsqlc"
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

func (d *Driver) GetDBPool() *pgxpool.Pool { panic(riverdriver.ErrNotImplemented) }
func (d *Driver) GetExecutor() riverdriver.Executor {
	return &Executor{d.dbPool, d.dbPool, dbsqlc.New()}
}

func (d *Driver) UnwrapExecutor(tx *sql.Tx) riverdriver.Executor {
	return &Executor{nil, tx, dbsqlc.New()}
}
func (d *Driver) UnwrapTx(tx *sql.Tx) pgx.Tx { panic(riverdriver.ErrNotImplemented) }

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

func (e *Executor) MigrationDeleteByVersionMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationDeleteByVersionMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	return mapMigrations(migrations), interpretError(err)
}

func (e *Executor) MigrationGetAll(ctx context.Context) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationGetAll(ctx, e.dbtx)
	return mapMigrations(migrations), interpretError(err)
}

func (e *Executor) MigrationInsertMany(ctx context.Context, versions []int) ([]*riverdriver.Migration, error) {
	migrations, err := e.queries.RiverMigrationInsertMany(ctx, e.dbtx,
		mapSlice(versions, func(v int) int64 { return int64(v) }))
	return mapMigrations(migrations), interpretError(err)
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
		return riverdriver.ErrNoRows
	}
	return err
}

func mapMigrations(migrations []*dbsqlc.RiverMigration) []*riverdriver.Migration {
	if migrations == nil {
		return nil
	}

	return mapSlice(migrations, func(m *dbsqlc.RiverMigration) *riverdriver.Migration {
		return &riverdriver.Migration{
			ID:        int(m.ID),
			CreatedAt: m.CreatedAt,
			Version:   int(m.Version),
		}
	})
}

// mapSlice manipulates a slice and transforms it to a slice of another type.
func mapSlice[T any, R any](collection []T, mapFunc func(T) R) []R {
	result := make([]R, len(collection))

	for i, item := range collection {
		result[i] = mapFunc(item)
	}

	return result
}
