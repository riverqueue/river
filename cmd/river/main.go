package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"

	"github.com/riverqueue/river/cmd/river/riverbench"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func main() {
	var rootOpts struct {
		Debug   bool
		Verbose bool
	}

	rootCmd := &cobra.Command{
		Use:   "river",
		Short: "Provides command line facilities for the River job queue",
		Long: strings.TrimSpace(`
Provides command line facilities for the River job queue.
		`),
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Usage()
		},
	}
	rootCmd.PersistentFlags().BoolVar(&rootOpts.Debug, "debug", false, "output maximum logging verbosity (debug level)")
	rootCmd.PersistentFlags().BoolVarP(&rootOpts.Verbose, "verbose", "v", false, "output additional logging verbosity (info level)")
	rootCmd.MarkFlagsMutuallyExclusive("debug", "verbose")

	ctx := context.Background()

	execHandlingError := func(f func() (bool, error)) {
		ok, err := f()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed: %s\n", err)
		}
		if err != nil || !ok {
			os.Exit(1)
		}
	}

	makeLogger := func() *slog.Logger {
		switch {
		case rootOpts.Debug:
			return slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug}))
		case rootOpts.Verbose:
			return slog.New(tint.NewHandler(os.Stdout, nil))
		default:
			return slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelWarn}))
		}
	}

	mustMarkFlagRequired := func(cmd *cobra.Command, name string) {
		// We just panic here because this will never happen outside of an error
		// in development.
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(err)
		}
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
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return bench(ctx, makeLogger(), os.Stdout, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to benchmark (should look like `postgres://...`")
		cmd.Flags().DurationVar(&opts.Duration, "duration", 0, "duration after which to stop benchmark, accepting Go-style durations like 1m, 5m30s")
		cmd.Flags().IntVarP(&opts.NumTotalJobs, "num-total-jobs", "n", 0, "number of jobs to insert before starting and which are worked down until finish")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	// migrate-down and migrate-up share a set of options, so this is a way of
	// plugging in all the right flags to both so options and docstrings stay
	// consistent.
	addMigrateFlags := func(cmd *cobra.Command, opts *migrateOpts) {
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to migrate (should look like `postgres://...`")
		cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "print information on migrations, but don't apply them")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 0, "maximum number of steps to migrate")
		cmd.Flags().BoolVar(&opts.ShowSQL, "show-sql", false, "show SQL of each migration")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "target version to migrate to (final state includes this version, but none after it)")
		mustMarkFlagRequired(cmd, "database-url")
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
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return migrateDown(ctx, makeLogger(), os.Stdout, &opts) })
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
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return migrateGet(ctx, makeLogger(), os.Stdout, &opts) })
			},
		}
		cmd.Flags().BoolVar(&opts.All, "all", false, "print all migrations; down migrations are printed in descending order")
		cmd.Flags().BoolVar(&opts.Down, "down", false, "print down migration")
		cmd.Flags().IntSliceVar(&opts.ExcludeVersion, "exclude-version", nil, "exclude version(s), usually version 1, containing River's migration tables")
		cmd.Flags().BoolVar(&opts.Up, "up", false, "print up migration")
		cmd.Flags().IntSliceVar(&opts.Version, "version", nil, "version(s) to print (can be multiple versions)")
		cmd.MarkFlagsMutuallyExclusive("all", "version")
		cmd.MarkFlagsOneRequired("all", "version")
		cmd.MarkFlagsMutuallyExclusive("down", "up")
		cmd.MarkFlagsOneRequired("down", "up")
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
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return migrateUp(ctx, makeLogger(), os.Stdout, &opts) })
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
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return validate(ctx, makeLogger(), os.Stdout, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to validate (should look like `postgres://...`")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	// Cobra will already print an error on an uknown command, and there aren't
	// really any other important top-level error cases to worry about as far as
	// I can tell, so ignore a returned error here so we don't double print it.
	_ = rootCmd.Execute()
}

func openDBPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	const (
		defaultIdleInTransactionSessionTimeout = 11 * time.Second // should be greater than statement timeout because statements count towards idle-in-transaction
		defaultStatementTimeout                = 10 * time.Second
	)

	pgxConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing database URL: %w", err)
	}

	setParamIfUnset(pgxConfig.ConnConfig.RuntimeParams, "application_name", "river CLI")
	setParamIfUnset(pgxConfig.ConnConfig.RuntimeParams, "idle_in_transaction_session_timeout", strconv.Itoa(int(defaultIdleInTransactionSessionTimeout.Milliseconds())))
	setParamIfUnset(pgxConfig.ConnConfig.RuntimeParams, "statement_timeout", strconv.Itoa(int(defaultStatementTimeout.Milliseconds())))

	dbPool, err := pgxpool.NewWithConfig(ctx, pgxConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	return dbPool, nil
}

// Sets a parameter in a parameter map (aimed at a Postgres connection
// configuration map), but only if that parameter wasn't already set.
func setParamIfUnset(runtimeParams map[string]string, name, val string) {
	if currentVal := runtimeParams[name]; currentVal != "" {
		return
	}

	runtimeParams[name] = val
}

type benchOpts struct {
	DatabaseURL  string
	Debug        bool
	Duration     time.Duration
	NumTotalJobs int
	Verbose      bool
}

func (o *benchOpts) validate() error {
	if o.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}

	return nil
}

func bench(ctx context.Context, logger *slog.Logger, _ io.Writer, opts *benchOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	benchmarker := riverbench.NewBenchmarker(riverpgxv5.New(dbPool), logger, opts.Duration, opts.NumTotalJobs)

	if err := benchmarker.Run(ctx); err != nil {
		return false, err
	}

	return true, nil
}

type migrateOpts struct {
	DatabaseURL   string
	DryRun        bool
	ShowSQL       bool
	MaxSteps      int
	TargetVersion int
}

func (o *migrateOpts) validate() error {
	if o.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}

	return nil
}

func migrateDown(ctx context.Context, logger *slog.Logger, out io.Writer, opts *migrateOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	// Default to applying only one migration maximum on the down direction.
	if opts.MaxSteps == 0 && opts.TargetVersion == 0 {
		opts.MaxSteps = 1
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), &rivermigrate.Config{Logger: logger})

	res, err := migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		DryRun:        opts.DryRun,
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

	migratePrintResult(out, opts, res, rivermigrate.DirectionDown)

	return true, nil
}

func migratePrintResult(out io.Writer, opts *migrateOpts, res *rivermigrate.MigrateResult, direction rivermigrate.Direction) {
	if len(res.Versions) < 1 {
		fmt.Fprintf(out, "no migrations to apply\n")
		return
	}

	for _, migrateVersion := range res.Versions {
		if opts.DryRun {
			fmt.Fprintf(out, "migration %03d [%s] [DRY RUN]\n", migrateVersion.Version, direction)
		} else {
			fmt.Fprintf(out, "applied migration %03d [%s] [%s]\n", migrateVersion.Version, direction, migrateVersion.Duration)
		}

		if opts.ShowSQL {
			fmt.Fprintf(out, "%s\n", strings.Repeat("-", 80))
			fmt.Fprintf(out, "%s\n", migrationComment(migrateVersion.Version, direction))
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
func migrationComment(version int, direction rivermigrate.Direction) string {
	return fmt.Sprintf("-- River migration %03d [%s]", version, direction)
}

type migrateGetOpts struct {
	All            bool
	Down           bool
	ExcludeVersion []int
	Up             bool
	Version        []int
}

func migrateGet(_ context.Context, logger *slog.Logger, out io.Writer, opts *migrateGetOpts) (bool, error) {
	migrator := rivermigrate.New(riverpgxv5.New(nil), &rivermigrate.Config{Logger: logger})

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
			fmt.Fprintf(out, "\n")
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
		fmt.Fprintf(out, "%s\n", migrationComment(migration.Version, direction))
		fmt.Fprintf(out, "%s\n", strings.TrimSpace(sql))
	}

	return true, nil
}

func migrateUp(ctx context.Context, logger *slog.Logger, out io.Writer, opts *migrateOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), &rivermigrate.Config{Logger: logger})

	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		DryRun:        opts.DryRun,
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

	migratePrintResult(out, opts, res, rivermigrate.DirectionUp)

	return true, nil
}

type validateOpts struct {
	DatabaseURL string
}

func (o *validateOpts) validate() error {
	if o.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}

	return nil
}

func validate(ctx context.Context, logger *slog.Logger, _ io.Writer, opts *validateOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), &rivermigrate.Config{Logger: logger})

	res, err := migrator.Validate(ctx)
	if err != nil {
		return false, err
	}

	return res.OK, nil
}
