package testdb

import (
	"context"
	"errors"
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.res.Value().pool.Ping(ctx); err != nil {
		// If the pgx pool is already closed, Ping returns puddle.ErrClosedPool.
		// When this happens, we need to re-create the pool.
		if errors.Is(err, puddle.ErrClosedPool) {
			db.logger.Debug("DBWithPool: pool is closed, re-creating", "dbName", db.dbName)

			if err := db.recreatePool(ctx); err != nil {
				db.res.Destroy()
				return
			}
		} else {
			// Log any other ping error but proceed with cleanup.
			db.logger.Debug("DBWithPool: pool ping returned error", "dbName", db.dbName, "err", err)
		}
	}

	if db.manager.cleanup != nil {
		db.logger.Debug("DBWithPool: release calling cleanup", "dbName", db.dbName)
		if err := db.manager.cleanup(ctx, db.res.Value().pool); err != nil {
			db.logger.Error("testdb.DBWithPool: Error during release cleanup", "err", err)

			if err := db.recreatePool(ctx); err != nil {
				db.res.Destroy()
				return
			}
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

func (db *DBWithPool) recreatePool(ctx context.Context) error {
	db.logger.Debug("DBWithPool: recreatePool called", "dbName", db.dbName)
	db.Pool().Close()

	newPgxPool, err := pgxpool.NewWithConfig(ctx, db.res.Value().config)
	if err != nil {
		db.res.Destroy()
		db.logger.Error("DBWithPool: recreatePool failed", "dbName", db.dbName, "err", err)
		return err
	}
	db.logger.Debug("DBWithPool: recreatePool succeeded", "dbName", db.dbName)
	db.res.Value().pool = newPgxPool
	return nil
}
