package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmittmann/tint"
	"github.com/spf13/cobra"

	"github.com/riverqueue/river/cmd/river/riverbench"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "river",
		Short: "Provides command line facilities for the River job queue",
		Long: `
Provides command line facilities for the River job queue.
		`,
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Usage()
		},
	}

	ctx := context.Background()

	execHandlingError := func(f func() (bool, error)) {
		ok, err := f()
		if err != nil {
			fmt.Printf("failed: %s\n", err)
		}
		if err != nil || !ok {
			os.Exit(1)
		}
	}

	mustMarkFlagRequired := func(cmd *cobra.Command, name string) { //nolint:unparam
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
			Long: `
Run a River benchmark which inserts and works jobs continually, giving a rough
idea of jobs per second and time to work a single job.

By default, the benchmark will continuously insert and work jobs in perpetuity
until interrupted by SIGINT (Ctrl^C). It can alternatively take a maximum run
duration with --duration, which takes a Go-style duration string like 1m.
Lastly, it can take --num-total-jobs, which inserts the given number of jobs
before starting the client, and works until all jobs are finished.

The database in --database-url will have its jobs table truncated, so make sure
to use a development database only.
	`,
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return bench(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to benchmark (should look like `postgres://...`")
		cmd.Flags().BoolVar(&opts.Debug, "debug", false, "output maximum logging verbosity (debug level)")
		cmd.Flags().DurationVar(&opts.Duration, "duration", 0, "duration after which to stop benchmark, accepting Go-style durations like 1m, 5m30s")
		cmd.Flags().IntVarP(&opts.NumTotalJobs, "num-total-jobs", "n", 0, "number of jobs to insert before starting and which are worked down until finish")
		cmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", false, "output additional logging verbosity (info level)")
		mustMarkFlagRequired(cmd, "database-url")
		cmd.MarkFlagsMutuallyExclusive("debug", "verbose")
		rootCmd.AddCommand(cmd)
	}

	// migrate-down
	{
		var opts migrateDownOpts

		cmd := &cobra.Command{
			Use:   "migrate-down",
			Short: "Run River schema down migrations",
			Long: `
Run down migrations to reverse the River database schema changes.

Defaults to running a single down migration. This behavior can be changed with
--max-steps or --target-version.
	`,
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return migrateDown(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to migrate (should look like `postgres://...`")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 1, "maximum number of steps to migrate")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "target version to migrate to (final state includes this version, but none after it)")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	// migrate-up
	{
		var opts migrateUpOpts

		cmd := &cobra.Command{
			Use:   "migrate-up",
			Short: "Run River schema up migrations",
			Long: `
Run up migrations to raise the database schema necessary to run River.

Defaults to running all up migrations that aren't yet run. This behavior can be
restricted with --max-steps or --target-version.
	`,
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return migrateUp(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to migrate (should look like `postgres://...`")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 0, "maximum number of steps to migrate")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "target version to migrate to (final state includes this version)")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	// validate
	{
		var opts validateOpts

		cmd := &cobra.Command{
			Use:   "validate",
			Short: "Validate River schema",
			Long: `
Validates the current River schema, exiting with a non-zero status in case there
are outstanding migrations that still need to be run.
	`,
			Run: func(cmd *cobra.Command, args []string) {
				execHandlingError(func() (bool, error) { return validate(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to validate (should look like `postgres://...`")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	execHandlingError(func() (bool, error) { return true, rootCmd.Execute() })
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

func bench(ctx context.Context, opts *benchOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	var logger *slog.Logger
	switch {
	case opts.Debug:
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelDebug}))
	case opts.Verbose:
		logger = slog.New(tint.NewHandler(os.Stdout, nil))
	default:
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{Level: slog.LevelWarn}))
	}

	benchmarker := riverbench.NewBenchmarker(riverpgxv5.New(dbPool), logger, opts.Duration, opts.NumTotalJobs)

	if err := benchmarker.Run(ctx); err != nil {
		return false, err
	}

	return true, nil
}

type migrateDownOpts struct {
	DatabaseURL   string
	MaxSteps      int
	TargetVersion int
}

func (o *migrateDownOpts) validate() error {
	if o.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}

	return nil
}

func migrateDown(ctx context.Context, opts *migrateDownOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

type migrateUpOpts struct {
	DatabaseURL   string
	MaxSteps      int
	TargetVersion int
}

func (o *migrateUpOpts) validate() error {
	if o.DatabaseURL == "" {
		return errors.New("database URL cannot be empty")
	}

	return nil
}

func migrateUp(ctx context.Context, opts *migrateUpOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	if err != nil {
		return false, err
	}

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

func validate(ctx context.Context, opts *validateOpts) (bool, error) {
	if err := opts.validate(); err != nil {
		return false, err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return false, err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

	res, err := migrator.Validate(ctx)
	if err != nil {
		return false, err
	}

	return res.OK, nil
}
