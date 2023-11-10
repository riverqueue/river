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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	GetDBPool() *pgxpool.Pool

	// UnwrapTx turns a generically typed transaction into a pgx.Tx for use with
	// internal infrastructure. This doesn't make sense in a world where
	// multiple drivers are supported and is subject to change.
	UnwrapTx(tx TTx) pgx.Tx
}
