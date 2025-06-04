package rivercli

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	"github.com/riverqueue/river/rivershared/util/ptrutil"
)

// Command is an interface to a River CLI subcommand. Commands generally only
// implement a Run function, and get the rest of the implementation by embedding
// CommandBase.
type Command[TOpts CommandOpts] interface {
	Run(ctx context.Context, opts TOpts) (bool, error)
	GetCommandBase() *CommandBase
	SetCommandBase(b *CommandBase)
}

// CommandBase provides common facilities for a River CLI command. It's
// generally embedded on the struct of a command.
type CommandBase struct {
	DriverProcurer DriverProcurer
	Logger         *slog.Logger
	Out            io.Writer
	Schema         string
}

func (b *CommandBase) GetCommandBase() *CommandBase     { return b }
func (b *CommandBase) SetCommandBase(base *CommandBase) { *b = *base }

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
	OutStd         io.Writer
	Schema         string
}

// RunCommand bootstraps and runs a River CLI subcommand.
func RunCommand[TOpts CommandOpts](ctx context.Context, bundle *RunCommandBundle, command Command[TOpts], opts TOpts) error {
	procureAndRun := func() (bool, error) {
		if err := opts.Validate(); err != nil {
			return false, err
		}

		var (
			databaseURL        *string
			protocol           string
			urlWithoutProtocol string
		)
		if pgEnvConfigured() {
			databaseURL = ptrutil.Ptr("")
			protocol = "postgres"
		} else if bundle.DatabaseURL != nil {
			databaseURL = bundle.DatabaseURL
			var ok bool
			protocol, urlWithoutProtocol, ok = strings.Cut(*databaseURL, "://")
			if !ok {
				return false, fmt.Errorf("expected database URL (`%s`) to be formatted like `postgres://...`", *bundle.DatabaseURL)
			}
		}

		driverProcurer := bundle.DriverProcurer
		if databaseURL != nil {
			switch protocol {
			case "postgres", "postgresql":
				dbPool, err := openPgxV5DBPool(ctx, *databaseURL)
				if err != nil {
					return false, err
				}
				defer dbPool.Close()

				driverProcurerPgxV5, isPgxV5Procurer := driverProcurer.(DriverProcurerPgxV5)
				if driverProcurer != nil && isPgxV5Procurer {
					driverProcurerPgxV5.InitPgxV5(dbPool)
				} else {
					driverProcurer = &pgxV5DriverProcurer{dbPool: dbPool}
				}

			case "sqlite":
				dbPool, err := openSQLitePool(protocol, urlWithoutProtocol)
				if err != nil {
					return false, err
				}
				defer dbPool.Close()

				driverProcurer = &sqliteDriverProcurer{dbPool: dbPool}

			default:
				return false, fmt.Errorf("unsupported database URL (`%s`); try one with a `postgres://`, `postgresql://`, or `sqlite://` scheme/prefix", *bundle.DatabaseURL)
			}
		}

		command.SetCommandBase(&CommandBase{
			DriverProcurer: driverProcurer,
			Logger:         bundle.Logger,
			Out:            bundle.OutStd,
			Schema:         bundle.Schema,
		})

		return command.Run(ctx, opts)
	}

	ok, err := procureAndRun()
	if err != nil {
		return err
	}
	if !ok {
		os.Exit(1)
	}
	return nil
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
		return nil, fmt.Errorf("error connecting to Postgres database: %w", err)
	}

	return dbPool, nil
}

func openSQLitePool(protocol, urlWithoutProtocol string) (*sql.DB, error) {
	dbPool, err := sql.Open(protocol, urlWithoutProtocol)
	if err != nil {
		return nil, fmt.Errorf("error connecting to SQLite database: %w", err)
	}

	// See notes on this in `riversharedtest.DBPoolSQLite`.
	dbPool.SetMaxOpenConns(1)

	return dbPool, nil
}

// Determines if there's a minimum number of `PG*` env vars configured to
// consider that configurable path viable. A `--database-url` parameter will
// take precedence.
func pgEnvConfigured() bool {
	return os.Getenv("PGDATABASE") != ""
}
