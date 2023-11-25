// Package rivermigrate provides a Go API for running migrations as alternative
// to migrating via the bundled CLI.
package rivermigrate

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbsqlc"
	"github.com/riverqueue/river/internal/util/dbutil"
	"github.com/riverqueue/river/internal/util/maputil"
	"github.com/riverqueue/river/internal/util/sliceutil"
	"github.com/riverqueue/river/riverdriver"
)

// A bundled migration containing a version (1, 2, 3), and SQL for up and down
// directions.
type migrationBundle struct {
	Version int
	Up      string
	Down    string
}

//nolint:gochecknoglobals
var (
	//go:embed migration/*.sql
	migrationFS embed.FS

	riverMigrations    = mustMigrationsFromFS(migrationFS)
	riverMigrationsMap = validateAndInit(riverMigrations)
)

// Config contains configuration for Migrator.
type Config struct {
	// Logger is the structured logger to use for logging purposes. If none is
	// specified, logs will be emitted to STDOUT with messages at warn level
	// or higher.
	Logger *slog.Logger
}

// Migrator is a database migration tool for River which can run up or down
// migrations in order to establish the schema that the queue needs to run.
type Migrator[TTx any] struct {
	baseservice.BaseService

	driver     riverdriver.Driver[TTx]
	migrations map[int]*migrationBundle // allows us to inject test migrations
	queries    *dbsqlc.Queries
}

// New returns a new migrator with the given database driver and configuration.
// The config parameter may be omitted as nil.
//
// Currently only one driver is supported, which is Pgx v5. See package
// riverpgxv5.
//
// The function takes a generic parameter TTx representing a transaction type,
// but it can be omitted because it'll generally always be inferred from the
// driver. For example:
//
//	import "github.com/riverqueue/river/riverdriver/riverpgxv5"
//	import "github.com/riverqueue/rivermigrate"
//
//	...
//
//	dbPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	if err != nil {
//		// handle error
//	}
//	defer dbPool.Close()
//
//	migrator, err := rivermigrate.New(riverpgxv5.New(dbPool), nil)
//	if err != nil {
//		// handle error
//	}
func New[TTx any](driver riverdriver.Driver[TTx], config *Config) *Migrator[TTx] {
	if config == nil {
		config = &Config{}
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	archetype := &baseservice.Archetype{
		Logger:     logger,
		TimeNowUTC: func() time.Time { return time.Now().UTC() },
	}

	return baseservice.Init(archetype, &Migrator[TTx]{
		driver:     driver,
		migrations: riverMigrationsMap,
		queries:    dbsqlc.New(),
	})
}

// MigrateOpts are options for a migrate operation.
type MigrateOpts struct {
	// MaxSteps is the maximum number of migrations to apply either up or down.
	// Leave zero for an unlimited number. Set to -1 to apply no migrations (for
	// testing/checking purposes).
	MaxSteps int

	// TargetVersion is a specific migration version to apply migrations to. The
	// version must exist and it must be in the possible list of migrations to
	// apply. e.g. If requesting an up migration with version 3, version 3 not
	// already be applied.
	//
	// When applying migrations up, migrations are applied including the target
	// version, so when starting at version 0 and requesting version 3, versions
	// 1, 2, and 3 would be applied. When applying migrations down, down
	// migrations are applied excluding the target version, so when starting at
	// version 5 an requesting version 3, down migrations for versions 5 and 4
	// would be applied, leaving the final schema at version 3.
	TargetVersion int
}

// MigrateResult is the result of a migrate operation.
type MigrateResult struct {
	// Versions are migration versions that were added (for up migrations) or
	// removed (for down migrations) for this run.
	Versions []MigrateVersion
}

// MigrateVersion is the result for a single applied migration.
type MigrateVersion struct {
	// Version is the version of the migration applied.
	Version int
}

func migrateVersionToInt(version MigrateVersion) int     { return version.Version }
func migrateVersionToInt64(version MigrateVersion) int64 { return int64(version.Version) }

type Direction string

const (
	DirectionDown Direction = "down"
	DirectionUp   Direction = "up"
)

// Migrate migrates the database in the given direction (up or down). The opts
// parameter may be omitted for convenience.
//
// By default, applies all outstanding migrations possible in either direction.
// When migrating up all outstanding migrations are applied, and when migrating
// down all existing migrations are unapplied.
//
// When migrating down, use with caution. MigrateOpts.MaxSteps should be set to
// 1 to only migrate down one step.
//
//	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
//	if err != nil {
//		// handle error
//	}
func (m *Migrator[TTx]) Migrate(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	return dbutil.WithTxV(ctx, m.driver.GetDBPool(), func(ctx context.Context, tx pgx.Tx) (*MigrateResult, error) {
		switch direction {
		case DirectionDown:
			return m.migrateDownTx(ctx, tx, direction, opts)
		case DirectionUp:
			return m.migrateUpTx(ctx, tx, direction, opts)
		}

		panic("invalid direction: " + direction)
	})
}

// Migrate migrates the database in the given direction (up or down). The opts
// parameter may be omitted for convenience.
//
// By default, applies all outstanding migrations possible in either direction.
// When migrating up all outstanding migrations are applied, and when migrating
// down all existing migrations are unapplied.
//
// When migrating down, use with caution. MigrateOpts.MaxSteps should be set to
// 1 to only migrate down one step.
//
//	res, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, nil)
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller run migrations within a transaction. Postgres DDL
// is transactional, so migration changes aren't visible until the transaction
// commits, and are rolled back if the transaction rolls back.
func (m *Migrator[TTx]) MigrateTx(ctx context.Context, tx TTx, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	switch direction {
	case DirectionDown:
		return m.migrateDownTx(ctx, m.driver.UnwrapTx(tx), direction, opts)
	case DirectionUp:
		return m.migrateUpTx(ctx, m.driver.UnwrapTx(tx), direction, opts)
	}

	panic("invalid direction: " + direction)
}

// migrateDownTx runs down migrations.
func (m *Migrator[TTx]) migrateDownTx(ctx context.Context, tx pgx.Tx, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx, tx)
	if err != nil {
		return nil, err
	}
	existingMigrationsMap := sliceutil.KeyBy(existingMigrations,
		func(m *dbsqlc.RiverMigration) (int, struct{}) { return int(m.Version), struct{}{} })

	targetMigrations := maps.Clone(m.migrations)
	for version := range targetMigrations {
		if _, ok := existingMigrationsMap[version]; !ok {
			delete(targetMigrations, version)
		}
	}

	sortedTargetMigrations := maputil.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b *migrationBundle) int { return b.Version - a.Version }) // reverse order

	res, err := m.applyMigrations(ctx, tx, direction, opts, sortedTargetMigrations)
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
	if slices.ContainsFunc(res.Versions, func(v MigrateVersion) bool { return v.Version == 1 }) {
		return res, nil
	}

	if _, err := m.queries.RiverMigrationDeleteByVersionMany(ctx, tx, sliceutil.Map(res.Versions, migrateVersionToInt64)); err != nil {
		return nil, fmt.Errorf("error deleting migration rows for versions %+v: %w", res.Versions, err)
	}

	return res, nil
}

// migrateUpTx runs up migrations.
func (m *Migrator[TTx]) migrateUpTx(ctx context.Context, tx pgx.Tx, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx, tx)
	if err != nil {
		return nil, err
	}

	targetMigrations := maps.Clone(m.migrations)
	for _, migrateRow := range existingMigrations {
		delete(targetMigrations, int(migrateRow.Version))
	}

	sortedTargetMigrations := maputil.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b *migrationBundle) int { return a.Version - b.Version })

	res, err := m.applyMigrations(ctx, tx, direction, opts, sortedTargetMigrations)
	if err != nil {
		return nil, err
	}

	if _, err := m.queries.RiverMigrationInsertMany(ctx, tx, sliceutil.Map(res.Versions, migrateVersionToInt64)); err != nil {
		return nil, fmt.Errorf("error inserting migration rows for versions %+v: %w", res.Versions, err)
	}

	return res, nil
}

// Common code shared between the up and down migration directions that walks
// through each target migration and applies it, logging appropriately.
func (m *Migrator[TTx]) applyMigrations(ctx context.Context, tx pgx.Tx, direction Direction, opts *MigrateOpts, sortedTargetMigrations []*migrationBundle) (*MigrateResult, error) {
	if opts == nil {
		opts = &MigrateOpts{}
	}

	switch {
	case opts.MaxSteps < 0:
		sortedTargetMigrations = []*migrationBundle{}
	case opts.MaxSteps > 0:
		sortedTargetMigrations = sortedTargetMigrations[0:min(opts.MaxSteps, len(sortedTargetMigrations))]
	}

	if opts.TargetVersion > 0 {
		if _, ok := m.migrations[opts.TargetVersion]; !ok {
			return nil, fmt.Errorf("version %d is not a valid River migration version", opts.TargetVersion)
		}

		targetIndex := slices.IndexFunc(sortedTargetMigrations, func(b *migrationBundle) bool { return b.Version == opts.TargetVersion })
		if targetIndex == -1 {
			return nil, fmt.Errorf("version %d is not in target list of valid migrations to apply", opts.TargetVersion)
		}

		// Replace target list with list up to target index. Migrations are
		// sorted according to the direction we're migrating in, so when down
		// migration, the list is already reversed, so this will truncate it so
		// it's the most current migration down to the target.
		sortedTargetMigrations = sortedTargetMigrations[0 : targetIndex+1]

		if direction == DirectionDown && len(sortedTargetMigrations) > 0 {
			sortedTargetMigrations = sortedTargetMigrations[0 : len(sortedTargetMigrations)-1]
		}
	}

	res := &MigrateResult{Versions: make([]MigrateVersion, 0, len(sortedTargetMigrations))}

	// Short circuit early if there's nothing to do.
	if len(sortedTargetMigrations) < 1 {
		m.Logger.InfoContext(ctx, m.Name+": No migrations to apply")
		return res, nil
	}

	for _, versionBundle := range sortedTargetMigrations {
		sql := versionBundle.Up
		if direction == DirectionDown {
			sql = versionBundle.Down
		}

		m.Logger.InfoContext(ctx, fmt.Sprintf(m.Name+": Applying migration %03d [%s]", versionBundle.Version, strings.ToUpper(string(direction))),
			slog.String("direction", string(direction)),
			slog.Int("version", versionBundle.Version),
		)

		_, err := tx.Exec(ctx, sql)
		if err != nil {
			return nil, fmt.Errorf("error applying version %03d [%s]: %w",
				versionBundle.Version, strings.ToUpper(string(direction)), err)
		}

		res.Versions = append(res.Versions, MigrateVersion{Version: versionBundle.Version})
	}

	// Only prints if more steps than available were requested.
	if opts.MaxSteps > 0 && len(res.Versions) < opts.MaxSteps {
		m.Logger.InfoContext(ctx, m.Name+": No more migrations to apply")
	}

	return res, nil
}

// Get existing migrations that've already been run in the database. This is
// encapsulated to run a check in a subtransaction and the handle the case of
// the `river_migration` table not existing yet. (The subtransaction is needed
// because otherwise the existing transaction would become aborted on an
// unsuccessful `river_migration` check.)
func (m *Migrator[TTx]) existingMigrations(ctx context.Context, tx pgx.Tx) ([]*dbsqlc.RiverMigration, error) {
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

// Reads a series of migration bundles from a file system, which practically
// speaking will always be the embedded FS read from the contents of the
// `migration/` subdirectory.
func migrationsFromFS(migrationFS fs.FS) ([]*migrationBundle, error) {
	const subdir = "migration"

	var (
		bundles    []*migrationBundle
		lastBundle *migrationBundle
	)

	err := fs.WalkDir(migrationFS, subdir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error walking FS: %w", err)
		}

		// Gets called one with the name of the subdirectory. Continue.
		if path == subdir {
			return nil
		}

		// Invoked with the full path name. Strip `migration/` from the front so
		// we have a name that we can parse with.
		if !strings.HasPrefix(path, subdir) {
			return fmt.Errorf("expected path %q to start with subdir %q", path, subdir)
		}
		name := path[len(subdir)+1:]

		versionStr, _, _ := strings.Cut(name, "_")

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return fmt.Errorf("error parsing version %q: %w", versionStr, err)
		}

		// This works because `fs.WalkDir` guarantees lexical order, so all 001*
		// files always appear before all 002* files, etc.
		if lastBundle == nil || lastBundle.Version != version {
			lastBundle = &migrationBundle{Version: version}
			bundles = append(bundles, lastBundle)
		}

		file, err := migrationFS.Open(path)
		if err != nil {
			return fmt.Errorf("error opening file %q: %w", path, err)
		}

		contents, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("error reading file %q: %w", path, err)
		}

		switch {
		case strings.HasSuffix(name, ".down.sql"):
			lastBundle.Down = string(contents)
		case strings.HasSuffix(name, ".up.sql"):
			lastBundle.Up = string(contents)
		default:
			return fmt.Errorf("file %q should end with either '.down.sql' or '.up.sql'", name)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return bundles, nil
}

// Same as the above, but for convenience, panics on an error.
func mustMigrationsFromFS(migrationFS fs.FS) []*migrationBundle {
	bundles, err := migrationsFromFS(migrationFS)
	if err != nil {
		panic(err)
	}
	return bundles
}

// Validates and fully initializes a set of migrations to reduce the probability
// of configuration problems as new migrations are introduced. e.g. Checks for
// missing fields or accidentally duplicated version numbers from copy/pasta
// problems.
func validateAndInit(versions []*migrationBundle) map[int]*migrationBundle {
	lastVersion := 0
	migrations := make(map[int]*migrationBundle, len(versions))

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
