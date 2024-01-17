// Package riverpgxv5 provides a River driver implementation for Pgx v5.
//
// This is currently the only supported driver for River and will therefore be
// used by all projects using River, but the code is organized this way so that
// other database packages can be supported in future River versions.
package riverpgxv5

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"weavelab.xyz/river/riverdriver"
	"weavelab.xyz/river/riverdriver/riverpgxv5/internal/dbsqlc"
)

// Driver is an implementation of riverdriver.Driver for Pgx v5.
type Driver struct {
	dbPool  *pgxpool.Pool
	queries *dbsqlc.Queries
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
	return &Driver{dbPool: dbPool, queries: dbsqlc.New()}
}

func (d *Driver) GetDBPool() *pgxpool.Pool                      { return d.dbPool }
func (d *Driver) GetExecutor() riverdriver.Executor             { return &Executor{d.dbPool, dbsqlc.New()} }
func (d *Driver) UnwrapExecutor(tx pgx.Tx) riverdriver.Executor { return &Executor{tx, dbsqlc.New()} }
func (d *Driver) UnwrapTx(tx pgx.Tx) pgx.Tx                     { return tx }

type Executor struct {
	dbtx interface {
		dbsqlc.DBTX
		Begin(ctx context.Context) (pgx.Tx, error)
	}
	queries *dbsqlc.Queries
}

func (e *Executor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	tx, err := e.dbtx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &ExecutorTx{Executor: Executor{tx, e.queries}, tx: tx}, nil
}

func (e *Executor) Exec(ctx context.Context, sql string) (struct{}, error) {
	_, err := e.dbtx.Exec(ctx, sql)
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
	tx pgx.Tx
}

func (t *ExecutorTx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *ExecutorTx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

func interpretError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
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
