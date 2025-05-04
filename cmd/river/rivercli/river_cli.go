// Package rivercli provides an implementation for the River CLI.
//
// This package is largely for internal use and doesn't provide the same API
// guarantees as the main River modules. Breaking API changes will be made
// without warning.
package rivercli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivermigrate"
)

type Config struct {
	// DriverProcurer provides a way of procuring drivers for various supported
	// databases.
	DriverProcurer DriverProcurer

	// Name is the human-friendly named of the executable, used while showing
	// version output. Usually this is just "River", but it could be "River
	// Pro".
	Name string
}

// DriverProcurer is an interface that provides a way of procuring drivers for
// various supported databases.
type DriverProcurer interface {
	ProcurePgxV5(pool *pgxpool.Pool) riverdriver.Driver[pgx.Tx]
}

// CLI provides a common base of commands for the River CLI.
type CLI struct {
	driverProcurer DriverProcurer
	name           string
	out            io.Writer
}

func NewCLI(config *Config) *CLI {
	return &CLI{
		driverProcurer: config.DriverProcurer,
		name:           config.Name,
		out:            os.Stdout,
	}
}

// BaseCommandSet provides a base River CLI command set which may be further
// augmented with additional commands.
func (c *CLI) BaseCommandSet() *cobra.Command {
	ctx := context.Background()

	var globalOpts struct {
		Debug   bool
		Verbose bool
	}

	makeLogger := func() *slog.Logger {
		switch {
		case globalOpts.Debug:
			return slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug}))
		case globalOpts.Verbose:
			return slog.New(tint.NewHandler(os.Stdout, nil))
		default:
			return slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelWarn}))
		}
	}

	// Make a bundle for RunCommand. Takes a database URL pointer because not every command is required to take a database URL.
	makeCommandBundle := func(databaseURL *string, schema string) *RunCommandBundle {
		return &RunCommandBundle{
			DatabaseURL:    databaseURL,
			DriverProcurer: c.driverProcurer,
			Logger:         makeLogger(),
			OutStd:         c.out,
			Schema:         schema,
		}
	}

	var rootCmd *cobra.Command
	{
		var rootOpts struct {
			Version bool
		}

		rootCmd = &cobra.Command{
			Use:   "river",
			Short: "Provides command line facilities for the River job queue",
			Long: strings.TrimSpace(`
Provides command line facilities for the River job queue.

Commands that need database access will take a --database-url argument, but can
also accept Postgres configuration through the standard set of libpq environment
variables like PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, and PGSSLMODE,
with a minimum of PGDATABASE required. --database-url will take precedence of
PG* vars if it's been specified.
		`),
			RunE: func(cmd *cobra.Command, args []string) error {
				if rootOpts.Version {
					return RunCommand(ctx, makeCommandBundle(nil, ""), &version{}, &versionOpts{Name: c.name})
				}

				_ = cmd.Usage()
				return nil
			},
		}
		rootCmd.SetOut(c.out)

		rootCmd.PersistentFlags().BoolVar(&globalOpts.Debug, "debug", false, "output maximum logging verbosity (debug level)")
		rootCmd.PersistentFlags().BoolVarP(&globalOpts.Verbose, "verbose", "v", false, "output additional logging verbosity (info level)")
		rootCmd.MarkFlagsMutuallyExclusive("debug", "verbose")

		rootCmd.Flags().BoolVar(&rootOpts.Version, "version", false, "print version information")
	}

	addDatabaseURLFlag := func(cmd *cobra.Command, databaseURL *string) {
		cmd.Flags().StringVar(databaseURL, "database-url", "", "URL of the database (should look like `postgres://...)`")
	}
	addLineFlag := func(cmd *cobra.Command, line *string) {
		cmd.Flags().StringVar(line, "line", "", "migration line to operate on (default: main)")
	}
	addSchemaFlag := func(cmd *cobra.Command, schema *string) {
		cmd.Flags().StringVar(schema, "schema", "", "name of non-default database schema where River tables are located")
	}

	// bench
	{
		var opts benchOpts

		cmd := &cobra.Command{
			Use:   "bench",
			Short: "Run River benchmark",
			Long: strings.TrimSpace(`
Run a River benchmark which inserts and works jobs continually, giving a rough
idea of jobs per second and time to work a single job.

By default, the benchmark will continuously insert and work jobs in perpetuity
until interrupted by SIGINT (Ctrl^C). It can alternatively take a maximum run
duration with --duration, which takes a Go-style duration string like 1m.
Lastly, it can take --num-total-jobs, which inserts the given number of jobs
before starting the client, and works until all jobs are finished.

The database in --database-url will have its jobs table truncated, so make sure
to use a development database only.
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(&opts.DatabaseURL, opts.Schema), &bench{}, &opts)
			},
		}
		addDatabaseURLFlag(cmd, &opts.DatabaseURL)
		addSchemaFlag(cmd, &opts.Schema)
		cmd.Flags().DurationVar(&opts.Duration, "duration", 0, "duration after which to stop benchmark, accepting Go-style durations like 1m, 5m30s")
		cmd.Flags().IntVarP(&opts.NumTotalJobs, "num-total-jobs", "n", 0, "number of jobs to insert before starting and which are worked down until finish")
		rootCmd.AddCommand(cmd)
	}

	// migrate-down and migrate-up share a set of options, so this is a way of
	// plugging in all the right flags to both so options and docstrings stay
	// consistent.
	addMigrateFlags := func(cmd *cobra.Command, opts *migrateOpts) {
		addDatabaseURLFlag(cmd, &opts.DatabaseURL)
		addSchemaFlag(cmd, &opts.Schema)
		cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "print information on migrations, but don't apply them")
		cmd.Flags().StringVar(&opts.Line, "line", "", "migration line to operate on (default: main)")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 0, "maximum number of steps to migrate")
		cmd.Flags().BoolVar(&opts.ShowSQL, "show-sql", false, "show SQL of each migration")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "target version to migrate to (final state includes this version, but none after it)")
	}

	// migrate-down
	{
		var opts migrateOpts

		cmd := &cobra.Command{
			Use:   "migrate-down",
			Short: "Run River schema down migrations",
			Long: strings.TrimSpace(`
Run down migrations to reverse the River database schema changes.

Defaults to running a single down migration. This behavior can be changed with
--max-steps or --target-version.

SQL being run can be output using --show-sql, and executing real database
operations can be prevented with --dry-run. Combine --show-sql and --dry-run to
dump prospective migrations that would be applied to stdout.
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(&opts.DatabaseURL, opts.Schema), &migrateDown{}, &opts)
			},
		}
		addMigrateFlags(cmd, &opts)
		rootCmd.AddCommand(cmd)
	}

	// migrate-get
	{
		var opts migrateGetOpts

		cmd := &cobra.Command{
			Use:   "migrate-get",
			Short: "Get SQL for specific River migration",
			Long: strings.TrimSpace(`
Retrieve SQL for a single migration version. This command is aimed at cases
where using River's internal migration framework isn't desirable by allowing
migration SQL to be dumped for use elsewhere.

Specify a version with --version, and one of --down or --up:

    river migrate-get --version 3 --up > river3.up.sql
    river migrate-get --version 3 --down > river3.down.sql

Can also take multiple versions by separating them with commas or passing
--version multiple times:

    river migrate-get --version 1,2,3 --up > river.up.sql
    river migrate-get --version 3,2,1 --down > river.down.sql

Or use --all to print all known migrations in either direction. Often used in
conjunction with --exclude-version 1 to exclude the tables for River's migration
framework, which aren't necessary if using an external framework:

    river migrate-get --all --exclude-version 1 --up > river_all.up.sql
    river migrate-get --all --exclude-version 1 --down > river_all.down.sql
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(nil, ""), &migrateGet{}, &opts)
			},
		}
		cmd.Flags().BoolVar(&opts.All, "all", false, "print all migrations; down migrations are printed in descending order")
		cmd.Flags().BoolVar(&opts.Down, "down", false, "print down migration")
		cmd.Flags().IntSliceVar(&opts.ExcludeVersion, "exclude-version", nil, "exclude version(s), usually version 1, containing River's migration tables")
		addLineFlag(cmd, &opts.Line)
		cmd.Flags().BoolVar(&opts.Up, "up", false, "print up migration")
		cmd.Flags().IntSliceVar(&opts.Version, "version", nil, "version(s) to print (can be multiple versions)")
		cmd.MarkFlagsMutuallyExclusive("all", "version")
		cmd.MarkFlagsOneRequired("all", "version")
		cmd.MarkFlagsMutuallyExclusive("down", "up")
		cmd.MarkFlagsOneRequired("down", "up")
		rootCmd.AddCommand(cmd)
	}

	// migrate-list
	{
		var opts migrateListOpts

		cmd := &cobra.Command{
			Use:   "migrate-list",
			Short: "List River schema migrations",
			Long: strings.TrimSpace(`
TODO
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(&opts.DatabaseURL, opts.Schema), &migrateList{}, &opts)
			},
		}
		addDatabaseURLFlag(cmd, &opts.DatabaseURL)
		addSchemaFlag(cmd, &opts.Schema)
		cmd.Flags().StringVar(&opts.Line, "line", "", "migration line to operate on (default: main)")
		rootCmd.AddCommand(cmd)
	}

	// migrate-up
	{
		var opts migrateOpts

		cmd := &cobra.Command{
			Use:   "migrate-up",
			Short: "Run River schema up migrations",
			Long: strings.TrimSpace(`
Run up migrations to raise the database schema necessary to run River.

Defaults to running all up migrations that aren't yet run. This behavior can be
restricted with --max-steps or --target-version.

SQL being run can be output using --show-sql, and executing real database
operations can be prevented with --dry-run. Combine --show-sql and --dry-run to
dump prospective migrations that would be applied to stdout.
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(&opts.DatabaseURL, opts.Schema), &migrateUp{}, &opts)
			},
		}
		addMigrateFlags(cmd, &opts)
		rootCmd.AddCommand(cmd)
	}

	// validate
	{
		var opts validateOpts

		cmd := &cobra.Command{
			Use:   "validate",
			Short: "Validate River schema",
			Long: strings.TrimSpace(`
Validates the current River schema, exiting with a non-zero status in case there
are outstanding migrations that still need to be run.

Can be paired with river migrate-up --dry-run --show-sql to dump information on
migrations that need to be run, but without running them.
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(&opts.DatabaseURL, opts.Schema), &validate{}, &opts)
			},
		}
		addDatabaseURLFlag(cmd, &opts.DatabaseURL)
		addSchemaFlag(cmd, &opts.Schema)
		cmd.Flags().StringVar(&opts.Line, "line", "", "migration line to operate on (default: main)")
		rootCmd.AddCommand(cmd)
	}

	// version
	{
		cmd := &cobra.Command{
			Use:   "version",
			Short: "Print version information",
			Long: strings.TrimSpace(`
Print River and Go version information.
	`),
			RunE: func(cmd *cobra.Command, args []string) error {
				return RunCommand(ctx, makeCommandBundle(nil, ""), &version{}, &versionOpts{Name: c.name})
			},
		}
		rootCmd.AddCommand(cmd)
	}

	return rootCmd
}

// SetOut sets standard output. Should be called before BaseCommandSet.
func (c *CLI) SetOut(out io.Writer) { c.out = out }

type benchOpts struct {
	DatabaseURL  string
	Debug        bool
	Duration     time.Duration
	NumTotalJobs int
	Schema       string
	Verbose      bool
}

func (o *benchOpts) Validate() error {
	if o.DatabaseURL == "" && !pgEnvConfigured() {
		return errors.New("either PG* env vars or --database-url must be set")
	}

	return nil
}

type bench struct {
	CommandBase
}

func (c *bench) Run(ctx context.Context, opts *benchOpts) (bool, error) {
	if err := c.GetBenchmarker().Run(ctx, opts.Duration, opts.NumTotalJobs); err != nil {
		return false, err
	}
	return true, nil
}

type migrateOpts struct {
	DatabaseURL   string
	DryRun        bool
	Line          string
	ShowSQL       bool
	MaxSteps      int
	Schema        string
	TargetVersion int
}

func (o *migrateOpts) Validate() error {
	if o.DatabaseURL == "" && !pgEnvConfigured() {
		return errors.New("either PG* env vars or --database-url must be set")
	}

	return nil
}

type migrateDown struct {
	CommandBase
}

func (c *migrateDown) Run(ctx context.Context, opts *migrateOpts) (bool, error) {
	migrator, err := c.GetMigrator(&rivermigrate.Config{Line: opts.Line, Logger: c.Logger, Schema: c.Schema})
	if err != nil {
		return false, err
	}

	res, err := migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		DryRun:        opts.DryRun,
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

	migratePrintResult(c.Out, opts, res, rivermigrate.DirectionDown)

	return true, nil
}

// Rounds a duration so that it doesn't show so much cluttered and not useful
// precision in printf output.
func roundDuration(duration time.Duration) time.Duration {
	switch {
	case duration > 1*time.Second:
		return duration.Truncate(10 * time.Millisecond)
	case duration < 1*time.Millisecond:
		return duration.Truncate(10 * time.Nanosecond)
	default:
		return duration.Truncate(10 * time.Microsecond)
	}
}

func migratePrintResult(out io.Writer, opts *migrateOpts, res *rivermigrate.MigrateResult, direction rivermigrate.Direction) {
	if len(res.Versions) < 1 {
		fmt.Fprintf(out, "no migrations to apply\n")
		return
	}

	versionWithLongestName := slices.MaxFunc(res.Versions,
		func(v1, v2 rivermigrate.MigrateVersion) int { return len(v1.Name) - len(v2.Name) })

	for _, migrateVersion := range res.Versions {
		if opts.DryRun {
			fmt.Fprintf(out, "migration %03d [%s] [DRY RUN]\n", migrateVersion.Version, direction)
		} else {
			fmt.Fprintf(out, "applied migration %03d [%s] %-*s [%s]\n", migrateVersion.Version, direction, len(versionWithLongestName.Name), migrateVersion.Name, roundDuration(migrateVersion.Duration))
		}

		if opts.ShowSQL {
			fmt.Fprintf(out, "%s\n", strings.Repeat("-", 80))
			fmt.Fprintf(out, "%s\n", migrationComment(opts.Line, migrateVersion.Version, direction))
			fmt.Fprintf(out, "%s\n\n", strings.TrimSpace(migrateVersion.SQL))
		}
	}

	// Only prints if more steps than available were requested.
	if opts.MaxSteps > 0 && len(res.Versions) < opts.MaxSteps {
		fmt.Fprintf(out, "no more migrations to apply\n")
	}
}

// An informational comment that's tagged on top of any migration's SQL to help
// attribute what it is for when it's copied elsewhere like other migration
// frameworks.
func migrationComment(line string, version int, direction rivermigrate.Direction) string {
	return fmt.Sprintf("-- River %s migration %03d [%s]", line, version, direction)
}

type migrateGetOpts struct {
	All            bool
	Down           bool
	ExcludeVersion []int
	Line           string
	Up             bool
	Version        []int
}

func (o *migrateGetOpts) Validate() error { return nil }

type migrateGet struct {
	CommandBase
}

func (c *migrateGet) Run(_ context.Context, opts *migrateGetOpts) (bool, error) {
	// We'll need to have a way of using an alternate driver if support for
	// other databases is added in the future. Unlike other migrate commands,
	// this one doesn't take a `--database-url`, so we'd need a way of
	// detecting the database type.
	migrator, err := rivermigrate.New(c.DriverProcurer.ProcurePgxV5(nil), &rivermigrate.Config{Line: opts.Line, Logger: c.Logger, Schema: ""})
	if err != nil {
		return false, err
	}

	var migrations []rivermigrate.Migration
	if opts.All {
		migrations = migrator.AllVersions()
		if opts.Down {
			slices.Reverse(migrations)
		}
	} else {
		for _, version := range opts.Version {
			migration, err := migrator.GetVersion(version)
			if err != nil {
				return false, err
			}

			migrations = append(migrations, migration)
		}
	}

	var printedOne bool

	for _, migration := range migrations {
		if slices.Contains(opts.ExcludeVersion, migration.Version) {
			continue
		}

		// print newlines between multiple versions
		if printedOne {
			fmt.Fprintf(c.Out, "\n")
		}

		var (
			direction rivermigrate.Direction
			sql       string
		)
		switch {
		case opts.Down:
			direction = rivermigrate.DirectionDown
			sql = migration.SQLDown
		case opts.Up:
			direction = rivermigrate.DirectionUp
			sql = migration.SQLUp
		}

		printedOne = true
		fmt.Fprintf(c.Out, "%s\n", migrationComment(opts.Line, migration.Version, direction))
		fmt.Fprintf(c.Out, "%s\n", strings.TrimSpace(sql))
	}

	return true, nil
}

type migrateListOpts struct {
	DatabaseURL string
	Line        string
	Schema      string
}

func (o *migrateListOpts) Validate() error { return nil }

type migrateList struct {
	CommandBase
}

func (c *migrateList) Run(ctx context.Context, opts *migrateListOpts) (bool, error) {
	migrator, err := c.GetMigrator(&rivermigrate.Config{Line: opts.Line, Logger: c.Logger, Schema: c.Schema})
	if err != nil {
		return false, err
	}

	allMigrations := migrator.AllVersions()

	existingMigrations, err := migrator.ExistingVersions(ctx)
	if err != nil {
		return false, err
	}

	var maxExistingVersion int
	if len(existingMigrations) > 0 {
		maxExistingVersion = existingMigrations[len(existingMigrations)-1].Version
	}

	for _, migration := range allMigrations {
		var currentVersionPrefix string
		switch {
		case migration.Version == maxExistingVersion:
			currentVersionPrefix = "* "
		case maxExistingVersion > 0:
			currentVersionPrefix = "  "
		}

		fmt.Fprintf(c.Out, "%s%03d %s\n", currentVersionPrefix, migration.Version, migration.Name)
	}

	return true, nil
}

type migrateUp struct {
	CommandBase
}

func (c *migrateUp) Run(ctx context.Context, opts *migrateOpts) (bool, error) {
	migrator, err := c.GetMigrator(&rivermigrate.Config{Line: opts.Line, Logger: c.Logger, Schema: c.Schema})
	if err != nil {
		return false, err
	}

	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		DryRun:        opts.DryRun,
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

	migratePrintResult(c.Out, opts, res, rivermigrate.DirectionUp)

	return true, nil
}

type validateOpts struct {
	DatabaseURL string
	Line        string
	Schema      string
}

func (o *validateOpts) Validate() error {
	if o.DatabaseURL == "" && !pgEnvConfigured() {
		return errors.New("either PG* env vars or --database-url must be set")
	}

	return nil
}

type validate struct {
	CommandBase
}

func (c *validate) Run(ctx context.Context, opts *validateOpts) (bool, error) {
	migrator, err := c.GetMigrator(&rivermigrate.Config{Line: opts.Line, Logger: c.Logger, Schema: c.Schema})
	if err != nil {
		return false, err
	}

	res, err := migrator.Validate(ctx)
	if err != nil {
		return false, err
	}

	return res.OK, nil
}

type versionOpts struct {
	Name string
}

func (o *versionOpts) Validate() error {
	if o.Name == "" {
		return errors.New("name should be set")
	}

	return nil
}

type version struct {
	CommandBase
}

func (c *version) Run(ctx context.Context, opts *versionOpts) (bool, error) {
	buildInfo, _ := debug.ReadBuildInfo()

	// Go 1.24 appears to have changed the build version to "(devel)" even when
	// using a release version of Go. This is a workaround to print "(unknown)"
	// in that case to match previous versions. We could relax the test instead
	// but it's unclear if this is a permanent change or not.
	buildVersion := buildInfo.Main.Version
	if strings.HasPrefix(buildInfo.GoVersion, "go1.24.") && buildVersion == "(devel)" {
		buildVersion = ""
	}

	fmt.Fprintf(c.Out, "%s version %s\n", opts.Name, cmp.Or(buildVersion, "(unknown)"))
	fmt.Fprintf(c.Out, "Built with %s\n", buildInfo.GoVersion)

	return true, nil
}
