package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"weavelab.xyz/river/riverdriver/riverpgxv5"
	"weavelab.xyz/river/rivermigrate"
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

	execHandlingError := func(f func() error) {
		err := f()
		if err != nil {
			fmt.Printf("failed: %s\n", err)
			os.Exit(1)
		}
	}

	mustMarkFlagRequired := func(cmd *cobra.Command, name string) {
		// We just panic here because this will never happen outside of an error
		// in development.
		if err := cmd.MarkFlagRequired(name); err != nil {
			panic(err)
		}
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
				execHandlingError(func() error { return migrateDown(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to migrate (should look like `postgres://...`")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 1, "Maximum number of steps to migrate")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "Target version to migrate to (final state includes this version, but none after it)")
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
				execHandlingError(func() error { return migrateUp(ctx, &opts) })
			},
		}
		cmd.Flags().StringVar(&opts.DatabaseURL, "database-url", "", "URL of the database to migrate (should look like `postgres://...`")
		cmd.Flags().IntVar(&opts.MaxSteps, "max-steps", 0, "Maximum number of steps to migrate")
		cmd.Flags().IntVar(&opts.TargetVersion, "target-version", 0, "Target version to migrate to (final state includes this version)")
		mustMarkFlagRequired(cmd, "database-url")
		rootCmd.AddCommand(cmd)
	}

	execHandlingError(rootCmd.Execute)
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

func migrateDown(ctx context.Context, opts *migrateDownOpts) error {
	if err := opts.validate(); err != nil {
		return err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	return err
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

func migrateUp(ctx context.Context, opts *migrateUpOpts) error {
	if err := opts.validate(); err != nil {
		return err
	}

	dbPool, err := openDBPool(ctx, opts.DatabaseURL)
	if err != nil {
		return err
	}
	defer dbPool.Close()

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		MaxSteps:      opts.MaxSteps,
		TargetVersion: opts.TargetVersion,
	})
	return err
}
