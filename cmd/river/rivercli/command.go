package rivercli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/cmd/river/riverbench"
	"github.com/riverqueue/river/rivermigrate"
)

const (
	uriScheme      = "postgresql://"
	uriSchemeAlias = "postgres://"
)

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
	GetVersion(version int) (rivermigrate.Migration, error)
	Migrate(ctx context.Context, direction rivermigrate.Direction, opts *rivermigrate.MigrateOpts) (*rivermigrate.MigrateResult, error)
	Validate(ctx context.Context) (*rivermigrate.ValidateResult, error)
}

// Command is an interface to a River CLI subcommand. Commands generally only
// implement a Run function, and get the rest of the implementation by embedding
// CommandBase.
type Command[TOpts CommandOpts] interface {
	Run(ctx context.Context, opts TOpts) (bool, error)
	SetCommandBase(b *CommandBase)
}

// CommandBase provides common facilities for a River CLI command. It's
// generally embedded on the struct of a command.
type CommandBase struct {
	DriverProcurer DriverProcurer
	Logger         *slog.Logger
	Out            io.Writer

	GetBenchmarker func() BenchmarkerInterface
	GetMigrator    func(config *rivermigrate.Config) MigratorInterface
}

func (b *CommandBase) SetCommandBase(base *CommandBase) {
	*b = *base
}

// CommandOpts are options for a command options. It makes sure that options
// provide a way of validating themselves.
type CommandOpts interface {
	Validate() error
}

// RunCommandBundle is a bundle of utilities for RunCommand.
type RunCommandBundle struct {
	DatabaseURL    *string
	DriverProcurer DriverProcurer
	Logger         *slog.Logger
}

// RunCommand bootstraps and runs a River CLI subcommand.
func RunCommand[TOpts CommandOpts](ctx context.Context, bundle *RunCommandBundle, command Command[TOpts], opts TOpts) {
	procureAndRun := func() (bool, error) {
		if err := opts.Validate(); err != nil {
			return false, err
		}

		commandBase := &CommandBase{
			DriverProcurer: bundle.DriverProcurer,
			Logger:         bundle.Logger,
			Out:            os.Stdout,
		}

		switch {
		// If database URL is still nil after Validate check, then assume this
		// command doesn't take one.
		case bundle.DatabaseURL == nil:
			commandBase.GetBenchmarker = func() BenchmarkerInterface { panic("databaseURL was not set") }
			commandBase.GetMigrator = func(config *rivermigrate.Config) MigratorInterface { panic("databaseURL was not set") }

		case strings.HasPrefix(*bundle.DatabaseURL, uriScheme) ||
			strings.HasPrefix(*bundle.DatabaseURL, uriSchemeAlias):
			dbPool, err := openPgxV5DBPool(ctx, *bundle.DatabaseURL)
			if err != nil {
				return false, err
			}
			defer dbPool.Close()

			driver := bundle.DriverProcurer.ProcurePgxV5(dbPool)

			commandBase.GetBenchmarker = func() BenchmarkerInterface { return riverbench.NewBenchmarker(driver, commandBase.Logger) }
			commandBase.GetMigrator = func(config *rivermigrate.Config) MigratorInterface { return rivermigrate.New(driver, config) }

		default:
			return false, fmt.Errorf(
				"unsupported database URL (`%s`); try one with a `%s` or `%s` scheme/prefix",
				*bundle.DatabaseURL,
				uriSchemeAlias,
				uriScheme,
			)
		}

		command.SetCommandBase(commandBase)

		return command.Run(ctx, opts)
	}

	ok, err := procureAndRun()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed: %s\n", err)
	}
	if err != nil || !ok {
		os.Exit(1)
	}
}

func openPgxV5DBPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	const (
		defaultIdleInTransactionSessionTimeout = 11 * time.Second // should be greater than statement timeout because statements count towards idle-in-transaction
		defaultStatementTimeout                = 10 * time.Second
	)

	pgxConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing database URL: %w", err)
	}

	// Sets a parameter in a parameter map (aimed at a Postgres connection
	// configuration map), but only if that parameter wasn't already set.
	setParamIfUnset := func(runtimeParams map[string]string, name, val string) {
		if currentVal := runtimeParams[name]; currentVal != "" {
			return
		}

		runtimeParams[name] = val
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
