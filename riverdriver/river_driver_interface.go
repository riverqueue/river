// Package riverdriver exposes generic constructs to be implemented by specific
// drivers that wrap third party database packages, with the aim being to keep
// the main River interface decoupled from a specific database package so that
// other packages or other major versions of packages can be supported in future
// River versions.
//
// River currently only supports Pgx v5, and the interface here wrap it with
// only the thinnest possible layer. Adding support for alternate packages will
// require the interface to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to interfaces in this package
// WILL NOT be considered breaking changes for purposes of River's semantic
// versioning.
package riverdriver

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotImplemented    = errors.New("driver does not implement this functionality")
	ErrNoRows            = errors.New("no rows found")
	ErrSubTxNotSupported = errors.New("subtransactions not supported for this driver")
)

// Driver provides a database driver for use with river.Client.
//
// Its purpose is to wrap the interface of a third party database package, with
// the aim being to keep the main River interface decoupled from a specific
// database package so that other packages or major versions of packages can be
// supported in future River versions.
//
// River currently only supports Pgx v5, and this interface wraps it with only
// the thinnest possible layer. Adding support for alternate packages will
// require it to change substantially, and therefore it should not be
// implemented or invoked by user code. Changes to this interface WILL NOT be
// considered breaking changes for purposes of River's semantic versioning.
type Driver[TTx any] interface {
	// GetDBPool returns a database pool.This doesn't make sense in a world
	// where multiple drivers are supported and is subject to change.
	//
	// API is not stable. DO NOT USE.
	GetDBPool() *pgxpool.Pool

	// GetExecutor gets an executor for the driver.
	//
	// API is not stable. DO NOT USE.
	GetExecutor() Executor

	// UnwrapExecutor gets unwraps executor from a driver transaction.
	//
	// API is not stable. DO NOT USE.
	UnwrapExecutor(tx TTx) Executor

	// UnwrapTx turns a generically typed transaction into a pgx.Tx for use with
	// internal infrastructure. This doesn't make sense in a world where
	// multiple drivers are supported and is subject to change.
	//
	// API is not stable. DO NOT USE.
	UnwrapTx(tx TTx) pgx.Tx
}

// Executor provides River operations against a database. It may be a database
// pool or transaction.
type Executor interface {
	// Begin begins a new subtransaction. ErrSubTxNotSupported may be returned
	// if the executor is a transaction and the driver doesn't support
	// subtransactions (like riverdriver/riverdatabasesql for database/sql).
	//
	// API is not stable. DO NOT USE.
	Begin(ctx context.Context) (ExecutorTx, error)

	// Exec executes raw SQL. Used for migrations.
	//
	// API is not stable. DO NOT USE.
	Exec(ctx context.Context, sql string) (struct{}, error)

	// MigrationDeleteByVersionMany deletes many migration versions.
	//
	// API is not stable. DO NOT USE.
	MigrationDeleteByVersionMany(ctx context.Context, versions []int) ([]*Migration, error)

	// MigrationGetAll gets all currently applied migrations.
	//
	// API is not stable. DO NOT USE.
	MigrationGetAll(ctx context.Context) ([]*Migration, error)

	// MigrationInsertMany inserts many migration versions.
	//
	// API is not stable. DO NOT USE.
	MigrationInsertMany(ctx context.Context, versions []int) ([]*Migration, error)

	// TableExists checks whether a table exists for the schema in the current
	// search schema.
	//
	// API is not stable. DO NOT USE.
	TableExists(ctx context.Context, tableName string) (bool, error)
}

// ExecutorTx is an executor which is a transaction. In addition to standard
// Executor operations, it may be committed or rolled back.
type ExecutorTx interface {
	Executor

	// Commit commits the transaction.
	//
	// API is not stable. DO NOT USE.
	Commit(ctx context.Context) error

	// Rollback rolls back the transaction.
	//
	// API is not stable. DO NOT USE.
	Rollback(ctx context.Context) error
}

// Migration represents a River migration.
type Migration struct {
	// ID is an automatically generated primary key for the migration.
	//
	// API is not stable. DO NOT USE.
	ID int

	// CreatedAt is when the migration was initially created.
	//
	// API is not stable. DO NOT USE.
	CreatedAt time.Time

	// Version is the version of the migration.
	//
	// API is not stable. DO NOT USE.
	Version int
}
