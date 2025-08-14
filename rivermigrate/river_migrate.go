// Package rivermigrate provides a Go API for running migrations as alternative
// to migrating via the bundled CLI.
package rivermigrate

import (
	"cmp"
	"context"
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

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/levenshtein"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
)

// The migrate version where the `line` column was added. Meaningful in that the
// migrator has to behave a little differently depending on whether it's working
// with versions before or after this boundary.
const migrateVersionLineColumnAdded = 5

// Migration is a bundled migration containing a version (e.g. 1, 2, 3), and SQL
// for up and down directions.
type Migration struct {
	// Name is a human-friendly name for the migration derived from its
	// filename.
	Name string

	// SQLDown is the s SQL for the migration's down direction.
	SQLDown string

	// SQLUp is the s SQL for the migration's up direction.
	SQLUp string

	// Version is the integer version number of this migration.
	Version int
}

// Config contains configuration for Migrator.
type Config struct {
	// Line is the migration line to use. Most drivers will only have a single
	// line, which is `main`.
	//
	// Defaults to `main`.
	Line string

	// Logger is the structured logger to use for logging purposes. If none is
	// specified, logs will be emitted to STDOUT with messages at warn level
	// or higher.
	Logger *slog.Logger

	// Schema is the target schema to migrate.
	//
	// Defaults to empty, which means that no schema is explicitly targeted. In
	// Postgres a schema will be selected based on what's set in `search_path`.
	Schema string
}

// Migrator is a database migration tool for River which can run up or down
// migrations in order to establish the schema that the queue needs to run.
type Migrator[TTx any] struct {
	baseservice.BaseService

	driver     riverdriver.Driver[TTx]
	line       string
	migrations map[int]Migration // allows us to inject test migrations
	replacer   sqlctemplate.Replacer
	schema     string
}

// New returns a new migrator with the given database driver and configuration.
// The config parameter may be omitted as nil.
//
// Two drivers are supported for migrations, one for Pgx v5 and one for the
// built-in database/sql package for use with migration frameworks like Goose.
// See packages riverpgxv5 and riverdatabasesql respectively.
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
func New[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Migrator[TTx], error) {
	if config == nil {
		config = &Config{}
	}

	line := cmp.Or(config.Line, riverdriver.MigrationLineMain)

	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	archetype := &baseservice.Archetype{
		Logger: logger,
		Time:   &baseservice.UnStubbableTimeGenerator{},
	}

	if !slices.Contains(driver.GetMigrationLines(), line) {
		const minLevenshteinDistance = 2

		var suggestedLines []string
		for _, existingLine := range driver.GetMigrationLines() {
			if distance := levenshtein.ComputeDistance(existingLine, line); distance <= minLevenshteinDistance {
				suggestedLines = append(suggestedLines, "`"+existingLine+"`")
			}
		}

		errorStr := "migration line does not exist: " + line
		switch {
		case len(suggestedLines) == 1:
			errorStr += fmt.Sprintf(" (did you mean %s?)", suggestedLines[0])
		case len(suggestedLines) > 1:
			errorStr += fmt.Sprintf(" (did you mean one of %v?)", strings.Join(suggestedLines, ", "))
		}

		return nil, errors.New(errorStr)
	}

	riverMigrations, err := migrationsFromFS(driver.GetMigrationFS(line), line)
	if err != nil {
		// If there's ever a problem here, it's a very fundamental internal
		// River one, so it's okay to panic.
		panic(err)
	}

	return baseservice.Init(archetype, &Migrator[TTx]{
		driver:     driver,
		line:       line,
		migrations: validateAndInit(riverMigrations),
		schema:     config.Schema,
	}), nil
}

// ExistingVersions gets the existing set of versions that have been migrated in
// the database, ordered by version.
func (m *Migrator[TTx]) ExistingVersions(ctx context.Context) ([]Migration, error) {
	migrations, err := m.existingMigrations(ctx, m.driver.GetExecutor())
	if err != nil {
		return nil, err
	}

	versions, err := m.versionsFromDriver(migrations)
	if err != nil {
		return nil, err
	}

	return versions, nil
}

// ExistingVersions gets the existing set of versions that have been migrated in
// the database, ordered by version.
//
// This variant checks for existing versions in a transaction.
func (m *Migrator[TTx]) ExistingVersionsTx(ctx context.Context, tx TTx) ([]Migration, error) {
	migrations, err := m.existingMigrations(ctx, m.driver.UnwrapExecutor(tx))
	if err != nil {
		return nil, err
	}

	versions, err := m.versionsFromDriver(migrations)
	if err != nil {
		return nil, err
	}

	return versions, nil
}

func (m *Migrator[TTx]) versionsFromDriver(migrations []*riverdriver.Migration) ([]Migration, error) {
	versions := make([]Migration, len(migrations))
	for i, existingMigration := range migrations {
		migration, ok := m.migrations[existingMigration.Version]
		if !ok {
			return nil, fmt.Errorf("migration %d not found in migrator bundle", existingMigration.Version)
		}
		versions[i] = migration
	}
	return versions, nil
}

// MigrateOpts are options for a migrate operation.
type MigrateOpts struct {
	DryRun bool

	// MaxSteps is the maximum number of migrations to apply either up or down.
	// When migrating in the up direction, migrates an unlimited number of steps
	// by default. When migrating in the down direction, migrates only a single
	// step by default (set TargetVersion to -1 to apply unlimited steps down).
	// Set to -1 to apply no migrations (for testing/checking purposes).
	MaxSteps int

	// TargetVersion is a specific migration version to apply migrations to. The
	// version must exist and it must be in the possible list of migrations to
	// apply. e.g. If requesting an up migration with version 3, version 3 must
	// not already be applied.
	//
	// When applying migrations up, migrations are applied including the target
	// version, so when starting at version 0 and requesting version 3, versions
	// 1, 2, and 3 would be applied. When applying migrations down, down
	// migrations are applied excluding the target version, so when starting at
	// version 5 and requesting version 3, down migrations for versions 5 and 4
	// would be applied, leaving the final schema at version 3.
	//
	// When migrating down, TargetVersion can be set to the special value of -1
	// to apply all down migrations (i.e. River schema is removed completely).
	TargetVersion int
}

// MigrateResult is the result of a migrate operation.
type MigrateResult struct {
	// Direction is the direction that migration occurred (up or down).
	Direction Direction

	// Versions are migration versions that were added (for up migrations) or
	// removed (for down migrations) for this run.
	Versions []MigrateVersion
}

// MigrateVersion is the result for a single applied migration.
type MigrateVersion struct {
	// Duration is the amount of time it took to apply the migration.
	Duration time.Duration

	// Name is a human-friendly name for the migration derived from its
	// filename.
	Name string

	// SQL is the SQL that was applied along with the migration.
	SQL string

	// Version is the version of the migration applied.
	Version int
}

func migrateVersionToInt(version MigrateVersion) int { return version.Version }

type Direction string

const (
	DirectionDown Direction = "down"
	DirectionUp   Direction = "up"
)

// AllVersions gets information on all known migration versions.
func (m *Migrator[TTx]) AllVersions() []Migration {
	migrations := maputil.Values(m.migrations)
	slices.SortFunc(migrations, func(v1, v2 Migration) int { return v1.Version - v2.Version })
	return migrations
}

// GetVersion gets information about a specific migration version. An error is
// returned if a versions is requested that doesn't exist.
func (m *Migrator[TTx]) GetVersion(version int) (Migration, error) {
	migration, ok := m.migrations[version]
	if !ok {
		availableVersions := maputil.Keys(m.migrations)
		slices.Sort(availableVersions)
		return Migration{}, fmt.Errorf("migration %d not found (available versions: %v)", version, availableVersions)
	}

	return migration, nil
}

// Migrate migrates the database in the given direction (up or down). The opts
// parameter may be omitted for convenience.
//
// By default, applies all outstanding migrations when moving in the up
// direction, but for safety, only one step when moving in the down direction.
// To migrate more than one step down, MigrateOpts.MaxSteps or
// MigrateOpts.TargetVersion are available. Setting MigrateOpts.TargetVersion to
// -1 will apply every available downstep so that River's schema is removed
// completely.
//
//	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
//	if err != nil {
//		// handle error
//	}
func (m *Migrator[TTx]) Migrate(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	exec := m.driver.GetExecutor()
	switch direction {
	case DirectionDown:
		return m.migrateDown(ctx, exec, direction, opts)
	case DirectionUp:
		return m.migrateUp(ctx, exec, direction, opts)
	}

	panic("invalid direction: " + direction)
}

// Migrate migrates the database in the given direction (up or down). The opts
// parameter may be omitted for convenience.
//
// By default, applies all outstanding migrations when moving in the up
// direction, but for safety, only one step when moving in the down direction.
// To migrate more than one step down, MigrateOpts.MaxSteps or
// MigrateOpts.TargetVersion are available. Setting MigrateOpts.TargetVersion to
// -1 will apply every available downstep so that River's schema is removed
// completely.
//
//	res, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, nil)
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller run migrations within a transaction. Postgres DDL
// is transactional, so migration changes aren't visible until the transaction
// commits, and are rolled back if the transaction rolls back.
//
// Deprecated: Use Migrate instead. Certain migrations cannot be batched together
// in a single transaction, so this method is not recommended.
func (m *Migrator[TTx]) MigrateTx(ctx context.Context, tx TTx, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	switch direction {
	case DirectionDown:
		return m.migrateDown(ctx, m.driver.UnwrapExecutor(tx), direction, opts)
	case DirectionUp:
		return m.migrateUp(ctx, m.driver.UnwrapExecutor(tx), direction, opts)
	}

	panic("invalid direction: " + direction)
}

// ValidateResult is the result of a validation operation.
type ValidateResult struct {
	// Messages contain informational messages of what wasn't valid in case of a
	// failed validation. Always empty if OK is true.
	Messages []string

	// OK is true if validation completed with no problems.
	OK bool
}

// Validate validates the current state of migrations, returning an unsuccessful
// validation and usable message in case there are migrations that haven't yet
// been applied.
func (m *Migrator[TTx]) Validate(ctx context.Context) (*ValidateResult, error) {
	return dbutil.WithTxV(ctx, m.driver.GetExecutor(), func(ctx context.Context, tx riverdriver.ExecutorTx) (*ValidateResult, error) {
		return m.validate(ctx, tx)
	})
}

// Validate validates the current state of migrations, returning an unsuccessful
// validation and usable message in case there are migrations that haven't yet
// been applied.
//
// This variant lets a caller validate within a transaction.
func (m *Migrator[TTx]) ValidateTx(ctx context.Context, tx TTx) (*ValidateResult, error) {
	return m.validate(ctx, m.driver.UnwrapExecutor(tx))
}

// migrateDown runs down migrations.
func (m *Migrator[TTx]) migrateDown(ctx context.Context, exec riverdriver.Executor, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx, exec)
	if err != nil {
		return nil, err
	}
	existingMigrationsMap := sliceutil.KeyBy(existingMigrations,
		func(m *riverdriver.Migration) (int, struct{}) { return m.Version, struct{}{} })

	targetMigrations := maps.Clone(m.migrations)
	for version := range targetMigrations {
		if _, ok := existingMigrationsMap[version]; !ok {
			delete(targetMigrations, version)
		}
	}

	sortedTargetMigrations := maputil.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b Migration) int { return b.Version - a.Version }) // reverse order

	res, err := m.applyMigrations(ctx, exec, direction, opts, sortedTargetMigrations)
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

	if !opts.DryRun && len(res.Versions) > 0 {
		versions := sliceutil.Map(res.Versions, migrateVersionToInt)

		// Version 005 is hard-coded here because that's the version in which
		// the migration `line` comes in. If migration to a point equal or above
		// 005, we can remove migrations with a line included, but otherwise we
		// must omit the `line` column from queries because it doesn't exist.
		if m.line == riverdriver.MigrationLineMain && slices.Min(versions) <= migrateVersionLineColumnAdded {
			if _, err := exec.MigrationDeleteAssumingMainMany(ctx, &riverdriver.MigrationDeleteAssumingMainManyParams{
				Versions: versions,
				Schema:   m.schema,
			}); err != nil {
				return nil, fmt.Errorf("error inserting migration rows for versions %+v assuming main: %w", res.Versions, err)
			}
		} else {
			if _, err := exec.MigrationDeleteByLineAndVersionMany(ctx, &riverdriver.MigrationDeleteByLineAndVersionManyParams{
				Line:     m.line,
				Schema:   m.schema,
				Versions: versions,
			}); err != nil {
				return nil, fmt.Errorf("error deleting migration rows for versions %+v on line %q: %w", res.Versions, m.line, err)
			}
		}
	}

	return res, nil
}

// migrateUp runs up migrations.
func (m *Migrator[TTx]) migrateUp(ctx context.Context, exec riverdriver.Executor, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx, exec)
	if err != nil {
		return nil, err
	}

	targetMigrations := maps.Clone(m.migrations)
	for _, migrateRow := range existingMigrations {
		delete(targetMigrations, migrateRow.Version)
	}

	sortedTargetMigrations := maputil.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b Migration) int { return a.Version - b.Version })

	res, err := m.applyMigrations(ctx, exec, direction, opts, sortedTargetMigrations)
	if err != nil {
		return nil, err
	}

	if (opts == nil || !opts.DryRun) && len(res.Versions) > 0 {
		versions := sliceutil.Map(res.Versions, migrateVersionToInt)

		// Version 005 is hard-coded here because that's the version in which
		// the migration `line` comes in. If migration to a point equal or above
		// 005, we can insert migrations with a line included, but otherwise we
		// must omit the `line` column from queries because it doesn't exist.
		if m.line == riverdriver.MigrationLineMain && slices.Max(versions) < migrateVersionLineColumnAdded {
			if _, err := exec.MigrationInsertManyAssumingMain(ctx, &riverdriver.MigrationInsertManyAssumingMainParams{
				Schema:   m.schema,
				Versions: versions,
			}); err != nil {
				return nil, fmt.Errorf("error inserting migration rows for versions %+v assuming main: %w", res.Versions, err)
			}
		} else {
			if _, err := exec.MigrationInsertMany(ctx, &riverdriver.MigrationInsertManyParams{
				Line:     m.line,
				Schema:   m.schema,
				Versions: versions,
			}); err != nil {
				return nil, fmt.Errorf("error inserting migration rows for versions %+v on line %q: %w", res.Versions, m.line, err)
			}
		}
	}

	return res, nil
}

// validate validates current migration state.
func (m *Migrator[TTx]) validate(ctx context.Context, exec riverdriver.Executor) (*ValidateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx, exec)
	if err != nil {
		return nil, err
	}

	targetMigrations := maps.Clone(m.migrations)
	for _, migrateRow := range existingMigrations {
		delete(targetMigrations, migrateRow.Version)
	}

	notOKWithMessage := func(message string) *ValidateResult {
		m.Logger.InfoContext(ctx, m.Name+": "+message)
		return &ValidateResult{Messages: []string{message}}
	}

	if len(targetMigrations) > 0 {
		sortedTargetMigrations := maputil.Keys(targetMigrations)
		slices.Sort(sortedTargetMigrations)

		return notOKWithMessage(fmt.Sprintf("Unapplied migrations: %v", sortedTargetMigrations)), nil
	}

	return &ValidateResult{OK: true}, nil
}

// Common code shared between the up and down migration directions that walks
// through each target migration and applies it, logging appropriately.
func (m *Migrator[TTx]) applyMigrations(ctx context.Context, exec riverdriver.Executor, direction Direction, opts *MigrateOpts, sortedTargetMigrations []Migration) (*MigrateResult, error) {
	if opts == nil {
		opts = &MigrateOpts{}
	}

	var maxSteps int
	switch {
	case opts.MaxSteps != 0:
		maxSteps = opts.MaxSteps
	case direction == DirectionDown && opts.TargetVersion == 0:
		maxSteps = 1
	}

	switch {
	case maxSteps < 0:
		sortedTargetMigrations = []Migration{}
	case maxSteps > 0:
		sortedTargetMigrations = sortedTargetMigrations[0:min(maxSteps, len(sortedTargetMigrations))]
	}

	if opts.TargetVersion > 0 {
		if _, ok := m.migrations[opts.TargetVersion]; !ok {
			return nil, fmt.Errorf("version %d is not a valid River migration version", opts.TargetVersion)
		}

		targetIndex := slices.IndexFunc(sortedTargetMigrations, func(b Migration) bool { return b.Version == opts.TargetVersion })
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

	res := &MigrateResult{Direction: direction, Versions: make([]MigrateVersion, 0, len(sortedTargetMigrations))}

	// Short circuit early if there's nothing to do.
	if len(sortedTargetMigrations) < 1 {
		m.Logger.InfoContext(ctx, m.Name+": No migrations to apply")
		return res, nil
	}

	var schema string
	if m.schema != "" {
		schema = m.schema + "."
	}
	schemaReplacement := map[string]sqlctemplate.Replacement{
		"schema": {Value: schema},
	}

	for _, versionBundle := range sortedTargetMigrations {
		var sql string
		switch direction {
		case DirectionDown:
			sql = versionBundle.SQLDown
		case DirectionUp:
			sql = versionBundle.SQLUp
		}

		// Most migrations contain schema in their SQL by necessity, but some of
		// the test ones do not because they only run trivial operations.
		if strings.Contains(sql, "/* TEMPLATE: schema */") {
			ctx := sqlctemplate.WithReplacements(ctx, schemaReplacement, nil)
			sql, _ = m.replacer.Run(ctx, m.driver.ArgPlaceholder(), sql, nil)
		}

		var duration time.Duration

		if !opts.DryRun {
			start := time.Now()

			// Similar to ActiveRecord migrations, we wrap each individual migration
			// in its own transaction.  Without this, certain migrations that require
			// a commit on a preexisting operation (such as adding an enum value to be
			// used in an immutable function) cannot succeed.
			err := dbutil.WithTx(ctx, exec, func(ctx context.Context, exec riverdriver.ExecutorTx) error {
				if err := exec.Exec(ctx, sql); err != nil {
					return fmt.Errorf("error applying version %03d [%s]: %w",
						versionBundle.Version, strings.ToUpper(string(direction)), err)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			duration = time.Since(start)
		}

		m.Logger.InfoContext(ctx, m.Name+": Applied migration",
			slog.String("direction", string(direction)),
			slog.Bool("dry_run", opts.DryRun),
			slog.Duration("duration", duration),
			slog.Int("version", versionBundle.Version),
		)

		res.Versions = append(res.Versions, MigrateVersion{Duration: duration, Name: versionBundle.Name, SQL: sql, Version: versionBundle.Version})
	}

	return res, nil
}

// Get existing migrations that've already been run in the database. This is
// encapsulated to run a check in a subtransaction and the handle the case of
// the `river_migration` table not existing yet. (The subtransaction is needed
// because otherwise the existing transaction would become aborted on an
// unsuccessful `river_migration` check.)
func (m *Migrator[TTx]) existingMigrations(ctx context.Context, exec riverdriver.Executor) ([]*riverdriver.Migration, error) {
	migrateTableExists, err := exec.TableExists(ctx, &riverdriver.TableExistsParams{
		Schema: m.schema,
		Table:  "river_migration",
	})
	if err != nil {
		return nil, fmt.Errorf("error checking if `%s` exists: %w", "river_migration", err)
	}
	if !migrateTableExists {
		if m.line != riverdriver.MigrationLineMain {
			return nil, errors.New("can't add a non-main migration line until `river_migration` is raised; fully migrate the main migration line and try again")
		}

		return nil, nil
	}

	lineColumnExists, err := exec.ColumnExists(ctx, &riverdriver.ColumnExistsParams{
		Column: "line",
		Schema: m.schema,
		Table:  "river_migration",
	})
	if err != nil {
		return nil, fmt.Errorf("error checking if `%s.%s` exists: %w", "river_migration", "line", err)
	}

	if !lineColumnExists {
		if m.line != riverdriver.MigrationLineMain {
			return nil, errors.New("can't add a non-main migration line until `river_migration.line` is raised; fully migrate the main migration line and try again")
		}

		migrations, err := exec.MigrationGetAllAssumingMain(ctx, &riverdriver.MigrationGetAllAssumingMainParams{
			Schema: m.schema,
		})
		if err != nil {
			return nil, fmt.Errorf("error getting existing migrations: %w", err)
		}

		return migrations, nil
	}

	migrations, err := exec.MigrationGetByLine(ctx, &riverdriver.MigrationGetByLineParams{
		Line:   m.line,
		Schema: m.schema,
	})
	if err != nil {
		return nil, fmt.Errorf("error getting existing migrations for line %q: %w", m.line, err)
	}

	return migrations, nil
}

// Reads a series of migration bundles from a file system, which practically
// speaking will always be the embedded FS read from the contents of the
// `migration/<line>/` subdirectory.
func migrationsFromFS(migrationFS fs.FS, line string) ([]Migration, error) {
	const subdir = "migration"

	var (
		lastBundle *Migration
		migrations []Migration
	)

	err := fs.WalkDir(migrationFS, subdir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error walking FS: %w", err)
		}

		// The WalkDir callback is invoked for each embedded subdirectory and
		// file. For our purposes here, we're only interested in files.
		if entry.IsDir() {
			return nil
		}

		filename := path

		// Invoked with the full path name. Strip `migration/` from the front so
		// we have a name that we can parse with.
		if !strings.HasPrefix(filename, subdir) {
			return fmt.Errorf("expected path %q to start with subdir %q", path, subdir)
		}
		filename = filename[len(subdir)+1:]

		// Ignore any migrations that don't belong to the line we're reading.
		if !strings.HasPrefix(filename, line) {
			return nil
		}
		filename = filename[len(line)+1:]

		versionStr, name, ok := strings.Cut(filename, "_")
		if !ok {
			return fmt.Errorf("expected name to start with version string like '001_': %q", filename)
		}

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return fmt.Errorf("error parsing version %q: %w", versionStr, err)
		}

		// Non-version name for the migration. So for `002_initial_schema` it
		// would be `initial schema`.
		name, _, _ = strings.Cut(name, ".")
		name = strings.ReplaceAll(name, "_", " ")

		// This works because `fs.WalkDir` guarantees lexical order, so all 001*
		// files always appear before all 002* files, etc.
		if lastBundle == nil || lastBundle.Version != version {
			migrations = append(migrations, Migration{Name: name, Version: version})
			lastBundle = &migrations[len(migrations)-1]
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
		case strings.HasSuffix(filename, ".down.sql"):
			lastBundle.SQLDown = string(contents)
		case strings.HasSuffix(filename, ".up.sql"):
			lastBundle.SQLUp = string(contents)
		default:
			return fmt.Errorf("file %q should end with either '.down.sql' or '.up.sql'", filename)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(migrations) < 1 {
		return nil, fmt.Errorf("no migrations found for line: %q", line)
	}

	return migrations, nil
}

// Validates and fully initializes a set of migrations to reduce the probability
// of configuration problems as new migrations are introduced. e.g. Checks for
// missing fields or accidentally duplicated version numbers from copy/pasta
// problems.
func validateAndInit(versions []Migration) map[int]Migration {
	lastVersion := 0
	migrations := make(map[int]Migration, len(versions))

	for _, versionBundle := range versions {
		if versionBundle.SQLDown == "" {
			panic(fmt.Sprintf("version bundle should specify Down: %+v", versionBundle))
		}
		if versionBundle.SQLUp == "" {
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
