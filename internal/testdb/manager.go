package testdb

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

type PrepareFunc func(ctx context.Context, pool *pgxpool.Pool) error

type CleanupFunc func(ctx context.Context, pool *pgxpool.Pool) error

type poolWithDBName struct {
	pool *pgxpool.Pool

	// We will need to recreate the actual pool each time this DB is reused, so we
	// need the config for creating it:
	config *pgxpool.Config

	// dbName is needed to be able to drop the database in the destructor.
	dbName string
}

// Manager manages a pool of test databases up to a max size. Each DB keeps a
// pgxpool.Pool which is available when one is acquired from the Manager.
// Databases can optionally be prepared with a PrepareFunc before being added
// into the pool, and cleaned up with a CleanupFunc before being returned to the
// pool for reuse.
//
// This setup makes it trivial to run fully isolated tests in parallel.
type Manager struct {
	pud        *puddle.Pool[*poolWithDBName]
	baseConfig *pgxpool.Config
	cleanup    CleanupFunc
	logger     *slog.Logger
	prepare    PrepareFunc

	mu        sync.Mutex // protects nextDbNum
	nextDBNum int
}

// NewManager creates a new Manager with the given databaseURL, maxPoolSize, and
// optional prepare/cleanup funcs.
func NewManager(config *pgxpool.Config, maxPoolSize int32, prepare PrepareFunc, cleanup CleanupFunc) (*Manager, error) {
	manager := &Manager{
		baseConfig: config,
		cleanup:    cleanup,
		logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
		prepare:    prepare,
	}

	pool, err := puddle.NewPool(&puddle.Config[*poolWithDBName]{
		Constructor: manager.allocatePool,
		Destructor:  manager.closePool,
		MaxSize:     maxPoolSize,
	})
	if err != nil {
		return nil, err
	}
	manager.pud = pool
	return manager, nil
}

// Acquire returns a DBWithPool which contains a pgxpool.Pool. The DBWithPool
// must be released after use.
func (m *Manager) Acquire(ctx context.Context) (*DBWithPool, error) {
	m.logger.Debug("DBManager: Acquire called")
	res, err := m.pud.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.Debug("DBManager: Acquire returned pool", "pool", res.Value().pool, "error", err, "dbName", res.Value().dbName)

	return &DBWithPool{res: res, logger: m.logger, manager: m, dbName: res.Value().dbName}, nil
}

// Close closes the Manager and all of its databases + pools. It blocks until
// all those underlying resources are unused and closed.
func (m *Manager) Close() {
	m.logger.Debug("DBManager: Close called")
	m.pud.Close()
	m.logger.Debug("DBManager: Close returned")
}

func (m *Manager) allocatePool(ctx context.Context) (*poolWithDBName, error) {
	nextDBNum := m.getNextDBNum()
	dbName := fmt.Sprintf("%s_%d", m.baseConfig.ConnConfig.Database, nextDBNum)

	m.logger.Debug("Using test database", "name", dbName)

	newPoolConfig := m.baseConfig.Copy()
	newPoolConfig.ConnConfig.Database = dbName

	pgxp, err := pgxpool.NewWithConfig(ctx, newPoolConfig)
	if err != nil {
		return nil, err
	}

	if m.cleanup != nil {
		m.logger.Debug("DBManager: allocatePool calling cleanup", "dbName", dbName)
		if err := m.cleanup(ctx, pgxp); err != nil {
			m.logger.Error("DBManager: error during allocatePool cleanup", "error", err)
			pgxp.Close()
			return nil, fmt.Errorf("error during cleanup: %w", err)
		}
		m.logger.Debug("DBManager: allocatePool cleanup returned", "dbName", dbName)
	}

	if m.prepare != nil {
		m.logger.Debug("DBManager: allocatePool calling prepare", "dbName", dbName)
		if err = m.prepare(ctx, pgxp); err != nil {
			pgxp.Close()
			return nil, fmt.Errorf("error during prepare: %w", err)
		}
		m.logger.Debug("DBManager: allocatePool prepare returned", "dbName", dbName)
	}

	return &poolWithDBName{
		config: newPoolConfig,
		dbName: dbName,
		pool:   pgxp,
	}, nil
}

func (m *Manager) closePool(pwn *poolWithDBName) {
	// Close the pool so that there are no active connections on the database:
	m.logger.Debug("DBManager: closePool called", "pool", pwn.pool, "dbName", pwn.dbName)
	pwn.pool.Close()
	m.logger.Debug("DBManager: closePool returned")
}

func (m *Manager) getNextDBNum() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	nextNum := m.nextDBNum
	m.nextDBNum++
	return nextNum
}
