// Package riverpgxv5 provides a River driver implementation for Pgx v5.
//
// This is currently the only supported driver for River and will therefore be
// used by all projects using River, but the code is organized this way so that
// other database packages can be supported in future River versions.
package riverpgxv5

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Driver is an implementation of riverdriver.Driver for Pgx v5.
type Driver struct {
	dbPool *pgxpool.Pool
}

// New returns a new Pgx v5 River driver for use with river.Client.
//
// It takes a pgxpool.Pool to use for use with River. The pool should already be
// configured to use the schema specified in the client's Schema field.
//
// The pool must not be closed while the associated client is running (not until
// graceful shutdown has completed).
func New(dbPool *pgxpool.Pool) *Driver {
	return &Driver{dbPool: dbPool}
}

func (d *Driver) GetDBPool() *pgxpool.Pool  { return d.dbPool }
func (d *Driver) UnwrapTx(tx pgx.Tx) pgx.Tx { return tx }
