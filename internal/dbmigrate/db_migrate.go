package dbmigrate

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/maputil"
	"github.com/riverqueue/river/internal/util/sliceutil"
)

var (
	//go:embed 001_create_river_migration.down.sql
	sql001CreateRiverMigrationDown string

	//go:embed 001_create_river_migration.up.sql
	sql001CreateRiverMigrationUp string

	//go:embed 002_initial_schema.down.sql
	sql002InitialSchemaDown string

	//go:embed 002_initial_schema.up.sql
	sql002InitialSchemaUp string

	//go:embed 003_river_job_tags_non_null.down.sql
	sql003RiverJobTagsNonNullDown string

	//go:embed 003_river_job_tags_non_null.up.sql
	sql003RiverJobTagsNonNullUp string
)

type migrationBundle struct {
	Version int64
	Up      string
	Down    string
}

//nolint:gochecknoglobals
var (
	riverMigrations = []*migrationBundle{
		{Version: 1, Up: sql001CreateRiverMigrationUp, Down: sql001CreateRiverMigrationDown},
		{Version: 2, Up: sql002InitialSchemaUp, Down: sql002InitialSchemaDown},
		{Version: 3, Up: sql003RiverJobTagsNonNullUp, Down: sql003RiverJobTagsNonNullDown},
	}

	riverMigrationsMap = validateAndInit(riverMigrations)
)

// Migrator is a database migration tool for River which can run up or down
// migrations in order to establish the schema that the queue needs to run.
type Migrator struct {
	baseservice.BaseService

	migrations map[int64]*migrationBundle
	queries    *dbsqlc.Queries
}

func NewMigrator(archetype *baseservice.Archetype) *Migrator {
	return baseservice.Init(archetype, &Migrator{
		migrations: riverMigrationsMap,
		queries:    dbsqlc.New(),
	})
}

// MigrateOptions are options for a migrate operation.
type MigrateOptions struct {
	// MaxSteps is the maximum number of migrations to apply either up or down.
	// Leave nil or set -1 for an unlimited number.
	MaxSteps *int
}

// MigrateResult is the result of a migrate operation.
type MigrateResult struct {
	// Versions are migration versions that were added (for up migrations) or
	// removed (for down migrations) for this run.
	Versions []int64
}

// Down runs down migrations.
func (m *Migrator) Down(ctx context.Context, txBeginner dbutil.TxBeginner, opts *MigrateOptions) (*MigrateResult, error) {
	return dbutil.WithTxV(ctx, txBeginner, func(ctx context.Context, tx pgx.Tx) (*MigrateResult, error) {
		existingMigrations, err := m.existingMigrations(ctx, tx)
		if err != nil {
			return nil, err
		}
		existingMigrationsMap := sliceutil.KeyBy(existingMigrations,
			func(m *dbsqlc.RiverMigration) (int64, struct{}) { return m.Version, struct{}{} })

		targetMigrations := maps.Clone(m.migrations)
		for version := range targetMigrations {
			if _, ok := existingMigrationsMap[version]; !ok {
				delete(targetMigrations, version)
			}
		}

		sortedTargetMigrations := maputil.Values(targetMigrations)
		slices.SortFunc(sortedTargetMigrations, func(a, b *migrationBundle) int { return int(b.Version - a.Version) }) // reverse order

		res, err := m.applyMigrations(ctx, tx, opts, sortedTargetMigrations, true)
		if err != nil {
			return nil, err
		}

		// If we did no work, leave early. This allows a zero-migrated database
		// that's being no-op downmigrated again to succeed because otherwise
		// the delete below would cause it to error.
		if len(res.Versions) < 1 {
			return res, nil
		}

		// Migration version 1 is special-cased because if it was downmigrated
		// it means the `river_migration` table is no longer present so there's
		// nothing to delete out of.
		if slices.Contains(res.Versions, 1) {
			return res, nil
		}

		if _, err := m.queries.RiverMigrationDeleteByVersionMany(ctx, tx, res.Versions); err != nil {
			return nil, fmt.Errorf("error deleting migration rows for versions %+v: %w", res.Versions, err)
		}

		return res, nil
	})
}

// Up runs up migrations.
func (m *Migrator) Up(ctx context.Context, txBeginner dbutil.TxBeginner, opts *MigrateOptions) (*MigrateResult, error) {
	return dbutil.WithTxV(ctx, txBeginner, func(ctx context.Context, tx pgx.Tx) (*MigrateResult, error) {
		existingMigrations, err := m.existingMigrations(ctx, tx)
		if err != nil {
			return nil, err
		}

		targetMigrations := maps.Clone(m.migrations)
		for _, migrateRow := range existingMigrations {
			delete(targetMigrations, migrateRow.Version)
		}

		sortedTargetMigrations := maputil.Values(targetMigrations)
		slices.SortFunc(sortedTargetMigrations, func(a, b *migrationBundle) int { return int(a.Version - b.Version) })

		res, err := m.applyMigrations(ctx, tx, opts, sortedTargetMigrations, false)
		if err != nil {
			return nil, err
		}

		if _, err := m.queries.RiverMigrationInsertMany(ctx, tx, res.Versions); err != nil {
			return nil, fmt.Errorf("error inserting migration rows for versions %+v: %w", res.Versions, err)
		}

		return res, nil
	})
}

// Common code shared between the up and down migration directions that walks
// through each target migration and applies it, logging appropriately.
func (m *Migrator) applyMigrations(ctx context.Context, tx pgx.Tx, opts *MigrateOptions, sortedTargetMigrations []*migrationBundle, down bool) (*MigrateResult, error) {
	if opts.MaxSteps != nil && *opts.MaxSteps >= 0 {
		sortedTargetMigrations = sortedTargetMigrations[0:min(*opts.MaxSteps, len(sortedTargetMigrations))]
	}

	res := &MigrateResult{Versions: make([]int64, 0, len(sortedTargetMigrations))}

	// Short circuit early if there's nothing to do.
	if len(sortedTargetMigrations) < 1 {
		m.Logger.InfoContext(ctx, m.Name+": No migrations to apply")
		return res, nil
	}

	direction := "up"
	if down {
		direction = "down"
	}

	for _, versionBundle := range sortedTargetMigrations {
		sql := versionBundle.Up
		if down {
			sql = versionBundle.Down
		}

		m.Logger.InfoContext(ctx, fmt.Sprintf(m.Name+": Applying migration %03d [%s]", versionBundle.Version, strings.ToUpper(direction)),
			slog.String("direction", direction),
			slog.Int64("version", versionBundle.Version),
		)

		_, err := tx.Exec(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("error applying version %03d [%s]: %w",
				versionBundle.Version, strings.ToUpper(direction), err)
		}

		res.Versions = append(res.Versions, versionBundle.Version)
	}

	// Only prints if more steps than available were requested.
	if opts.MaxSteps != nil && *opts.MaxSteps >= 0 && len(res.Versions) < *opts.MaxSteps {
		m.Logger.InfoContext(ctx, m.Name+": No more migrations to apply")
	}

	return res, nil
}

// Get existing migrations that've already been run in the database. This is
// encapsulated to run a check in a subtransaction and the handle the case of
// the `river_migration` table not existing yet. (The subtransaction is needed
// because otherwise the existing transaction would become aborted on an
// unsuccessful `river_migration` check.)
func (m *Migrator) existingMigrations(ctx context.Context, tx pgx.Tx) ([]*dbsqlc.RiverMigration, error) {
	// We start another inner transaction here because in case this is the first
	// ever migration run, the transaction may become aborted if `river_migration`
	// doesn't exist, a condition which we must handle gracefully.
	migrations, err := dbutil.WithTxV(ctx, tx, func(ctx context.Context, tx pgx.Tx) ([]*dbsqlc.RiverMigration, error) {
		migrations, err := m.queries.RiverMigrationGetAll(ctx, tx)
		if err != nil {
			return nil, fmt.Errorf("error getting current migrate rows: %w", err)
		}
		return migrations, nil
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			if pgErr.Code == pgerrcode.UndefinedTable && strings.Contains(pgErr.Message, "river_migration") {
				return nil, nil
			}
		}

		return nil, err
	}

	return migrations, nil
}

// Validates and fully initializes a set of migrations to reduce the probability
// of configuration problems as new migrations are introduced. e.g. Checks for
// missing fields or accidentally duplicated version numbers from copy/pasta
// problems.
func validateAndInit(versions []*migrationBundle) map[int64]*migrationBundle {
	lastVersion := int64(0)
	migrations := make(map[int64]*migrationBundle, len(versions))

	for _, versionBundle := range versions {
		if versionBundle.Down == "" {
			panic(fmt.Sprintf("version bundle should specify Down: %+v", versionBundle))
		}
		if versionBundle.Up == "" {
			panic(fmt.Sprintf("version bundle should specify Up: %+v", versionBundle))
		}
		if versionBundle.Version == 0 {
			panic(fmt.Sprintf("version bundle should specify Version: %+v", versionBundle))
		}

		if _, ok := migrations[versionBundle.Version]; ok {
			panic(fmt.Sprintf("duplicate version: %03d", versionBundle.Version))
		}
		if versionBundle.Version <= lastVersion {
			panic(fmt.Sprintf("versions should be ascending; current: %03d, last: %03d", versionBundle.Version, lastVersion))
		}
		if versionBundle.Version > lastVersion+1 {
			panic(fmt.Sprintf("versions shouldn't skip a sequence number; current: %03d, last: %03d", versionBundle.Version, lastVersion))
		}

		lastVersion = versionBundle.Version
		migrations[versionBundle.Version] = versionBundle
	}

	return migrations
}
