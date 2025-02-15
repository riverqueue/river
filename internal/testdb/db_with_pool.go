package testdb

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

// DBWithPool is a wrapper for a puddle resource for a test database. The
// database is made available via a preconfigured pgxpool.Pool.
type DBWithPool struct {
	res     *puddle.Resource[*poolWithDBName]
	manager *Manager
	dbName  string
	logger  *slog.Logger

	closeOnce sync.Once
}

// Release releases the DBWithPool back to the Manager. This should be called
// when the test is finished with the database.
func (db *DBWithPool) Release() {
	db.closeOnce.Do(db.release)
}

func (db *DBWithPool) release() {
	db.logger.Debug("DBWithPool: release called", "dbName", db.dbName)
	// Close and recreate the connection pool for 2 reasons:
	// 1. ensure tests don't hold on to connections
	// 2. If a test happens to close the pool as a matter of course (i.e. as part of a defer)
	//    then we don't reuse a closed pool.
	db.res.Value().pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	newPgxPool, err := pgxpool.NewWithConfig(ctx, db.res.Value().config)
	if err != nil {
		db.res.Destroy()
		return
	}
	db.res.Value().pool = newPgxPool

	if db.manager.cleanup != nil {
		db.logger.Debug("DBWithPool: release calling cleanup", "dbName", db.dbName)
		if err := db.manager.cleanup(ctx, newPgxPool); err != nil {
			db.logger.Error("testdb.DBWithPool: Error during release cleanup", "err", err)
			db.res.Destroy()
			return
		}
		db.logger.Debug("DBWithPool: release done with cleanup", "dbName", db.dbName)
	}

	// Finally this resource is ready to be reused:
	db.res.Release()
}

// Pool returns the underlying pgxpool.Pool for the test database.
func (db *DBWithPool) Pool() *pgxpool.Pool {
	return db.res.Value().pool
}
