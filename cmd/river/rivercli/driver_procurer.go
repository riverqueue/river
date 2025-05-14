package rivercli

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/cmd/river/riverbench"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
)

// DriverProcurer is an interface that provides a way of procuring River modules
// like a benchmarker or migrator in such a way that their generic type
// parameters are abstracted away so they don't leak out into parent container.
type DriverProcurer interface {
	GetBenchmarker(config *riverbench.Config) BenchmarkerInterface
	GetMigrator(config *rivermigrate.Config) (MigratorInterface, error)
	QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row
}

// BenchmarkerInterface is an interface to a Benchmarker. Its reason for
// existence is to wrap a benchmarker to strip it of its generic parameter,
// letting us pass it around without having to know the transaction type.
type BenchmarkerInterface interface {
	Run(ctx context.Context, duration time.Duration, numTotalJobs int) error
}

// MigratorInterface is an interface to a Migrator. Its reason for existence is
// to wrap a migrator to strip it of its generic parameter, letting us pass it
// around without having to know the transaction type.
type MigratorInterface interface {
	AllVersions() []rivermigrate.Migration
	ExistingVersions(ctx context.Context) ([]rivermigrate.Migration, error)
	GetVersion(version int) (rivermigrate.Migration, error)
	Migrate(ctx context.Context, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) (*rivermigrate.MigrateResult, error)
	Validate(ctx context.Context) (*rivermigrate.ValidateResult, error)
}

type pgxV5DriverProcurer struct {
	dbPool *pgxpool.Pool
}

func (p *pgxV5DriverProcurer) GetBenchmarker(config *riverbench.Config) BenchmarkerInterface {
	return riverbench.NewBenchmarker(riverpgxv5.New(p.dbPool), config)
}

func (p *pgxV5DriverProcurer) GetMigrator(config *rivermigrate.Config) (MigratorInterface, error) {
	return rivermigrate.New(riverpgxv5.New(p.dbPool), config)
}

func (p *pgxV5DriverProcurer) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	return riverpgxv5.New(p.dbPool).GetExecutor().QueryRow(ctx, sql, args...)
}

type sqliteDriverProcurer struct {
	dbPool *sql.DB
}

func (p *sqliteDriverProcurer) GetBenchmarker(config *riverbench.Config) BenchmarkerInterface {
	return riverbench.NewBenchmarker(riversqlite.New(p.dbPool), config)
}

func (p *sqliteDriverProcurer) GetMigrator(config *rivermigrate.Config) (MigratorInterface, error) {
	return rivermigrate.New(riversqlite.New(p.dbPool), config)
}

func (p *sqliteDriverProcurer) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	return riversqlite.New(p.dbPool).GetExecutor().QueryRow(ctx, sql, args...)
}
