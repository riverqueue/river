// testdbman is a command-line tool for managing the test databases used by
// parallel tests and the sample applications.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/util/maputil"
)

func main() {
	commandBundle := NewCommandBundle(
		"testdbman",
		"testdbman manages test databases",
		`
A small program to create and manage test databases. River currently requires a
number of different of test databases loaded with its schema for it to be able
to run the full test suite in parallel.

Run "testdbman create" to raise all required test databases and prepare for a
test run.
		`,
	)

	// create
	{
		commandBundle.AddCommand(
			"create",
			"Create test databases",
			`
Creates the test databases used by parallel tests and the sample applications.
Each is migrated with River's current schema.

The sample application DB is named river_test, while the DBs for parallel
tests are named river_test_0, river_test_1, etc. up to the larger of 4 or
runtime.NumCPU() (a choice that comes from pgx's default connection pool size).
`,
			createTestDatabases,
		)
	}

	// drop
	{
		commandBundle.AddCommand(
			"drop",
			"Drop test databases",
			`
Drops all test databases. Any test database matching the base name
(river_test) or the base name with an underscore followed by any other token
(river_test_example, river_test_0, river_test_1, etc.) will be dropped.
`,
			dropTestDatabases,
		)
	}

	// reset
	{
		commandBundle.AddCommand(
			"reset",
			"Drop and recreate test databases",
			`
Reset the test databases, dropping the existing database(s) if they exist, and
recreating them with the most up to date schema. Equivalent to running "drop"
followed by "create".
`,
			resetTestDatabases,
		)
	}

	ctx := context.Background()

	if err := commandBundle.Exec(ctx, os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "failed: %s\n", err)
		os.Exit(1)
	}
}

//
// Commands
//

const managementDatabaseURL = "postgres:///postgres"

//
// Helpers
//

func createTestDatabases(ctx context.Context, out io.Writer) error {
	mgmtConn, err := pgx.Connect(ctx, managementDatabaseURL)
	if err != nil {
		return fmt.Errorf("error opening management connection: %w", err)
	}
	defer mgmtConn.Close(ctx)

	createDBAndMigrate := func(dbName string) error {
		if _, err := mgmtConn.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
			return fmt.Errorf("error crating database %q: %w", dbName, err)
		}
		fmt.Fprintf(out, "created: %-20s", dbName)

		// Defer printing a newline, which will be either added to the end of a
		// successful invocation of this command (after the string "[and
		// migrated]" has been printed to the current line), or printed before
		// returning an error so that in either case output looks right.
		defer fmt.Fprintf(out, "\n")

		dbURL := "postgres:///" + dbName

		dbPool, err := pgxpool.New(ctx, dbURL)
		if err != nil {
			return fmt.Errorf("error creating connection pool to %q: %w", dbURL, err)
		}
		defer dbPool.Close()

		migrator, err := rivermigrate.New(riverpgxv5.New(dbPool), nil)
		if err != nil {
			return err
		}

		if _, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{}); err != nil {
			return err
		}
		fmt.Fprintf(out, " [and migrated]")

		return nil
	}

	// Allow up to one database per concurrent test, plus two for overhead:
	maxTestDBs := runtime.GOMAXPROCS(0) + 2
	dbNames := generateTestDBNames(maxTestDBs)
	for _, dbName := range dbNames {
		if err := createDBAndMigrate(dbName); err != nil {
			return err
		}
	}

	return nil
}

func generateTestDBNames(numDBs int) []string {
	dbNames := []string{
		"river_test",
		"river_test_example",
	}

	for i := range numDBs {
		dbNames = append(dbNames, fmt.Sprintf("river_test_%d", i))
	}

	return dbNames
}

func dropTestDatabases(ctx context.Context, out io.Writer) error {
	mgmtConn, err := pgx.Connect(ctx, managementDatabaseURL)
	if err != nil {
		return fmt.Errorf("error opening management connection: %w", err)
	}
	defer mgmtConn.Close(ctx)

	rows, err := mgmtConn.Query(ctx, "SELECT datname FROM pg_database")
	if err != nil {
		return fmt.Errorf("error listing databases: %w", err)
	}
	defer rows.Close()

	allDBNames := make([]string, 0)
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			return fmt.Errorf("error scanning database name: %w", err)
		}
		allDBNames = append(allDBNames, dbName)
	}
	rows.Close()

	for _, dbName := range allDBNames {
		if strings.HasPrefix(dbName, "river_test") {
			if _, err := mgmtConn.Exec(ctx, "DROP DATABASE "+dbName); err != nil {
				return fmt.Errorf("error dropping database %q: %w", dbName, err)
			}
			fmt.Fprintf(out, "dropped: %s\n", dbName)
		}
	}

	return nil
}

func resetTestDatabases(ctx context.Context, out io.Writer) error {
	if err := dropTestDatabases(ctx, out); err != nil {
		return err
	}

	if err := createTestDatabases(ctx, out); err != nil {
		return err
	}

	return nil
}

//
// Command bundle framework
//

// CommandBundle is a basic CLI command framework similar to Cobra, but with far
// reduced capabilities. I know it seems crazy to write one when Cobra is
// available, but the test manager's interface is quite simple, and not using
// Cobra lets us drop its dependency in the main River package.
type CommandBundle struct {
	commands map[string]*commandBundleCommand
	long     string
	out      io.Writer
	short    string
	use      string
}

func NewCommandBundle(use, short, long string) *CommandBundle {
	if use == "" {
		panic("use is required")
	}
	if short == "" {
		panic("short is required")
	}
	if long == "" {
		panic("long is required")
	}

	return &CommandBundle{
		commands: make(map[string]*commandBundleCommand),
		long:     long,
		out:      os.Stdout,
		short:    short,
		use:      use,
	}
}

func (b *CommandBundle) AddCommand(use, short, long string, execFunc func(ctx context.Context, out io.Writer) error) {
	if use == "" {
		panic("use is required")
	}
	if short == "" {
		panic("short is required")
	}
	if long == "" {
		panic("long is required")
	}
	if execFunc == nil {
		panic("execFunc is required")
	}

	if _, ok := b.commands[use]; ok {
		panic("command already registered: " + use)
	}

	b.commands[use] = &commandBundleCommand{
		execFunc: execFunc,
		long:     long,
		short:    short,
		use:      use,
	}
}

const helpUse = "help"

func (b *CommandBundle) Exec(ctx context.Context, args []string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var (
		flagSet flag.FlagSet
		help    bool
	)
	flagSet.BoolVar(&help, "help", false, "help for program or command")

	args = args[1:] // drop program name

	var commandUse string
	if len(args) > 0 && args[0][0] != '-' {
		commandUse = args[0]
		args = args[1:]
	}

	if err := flagSet.Parse(args); err != nil {
		return err
	}

	args = flagSet.Args()

	// Try extracting a command again after flags are parsed and we didn't get
	// one on the first pass.
	if commandUse == "" && len(args) > 0 {
		commandUse = args[0]
		args = args[1:]
	}

	if commandUse != "" && commandUse != helpUse && len(args) > 0 || len(args) > 1 {
		return errors.New("expected exactly one command")
	}

	if commandUse == "" || commandUse == helpUse && len(args) < 1 {
		fmt.Fprintf(b.out, "%s\n", b.usage(&flagSet))
		return nil
	}

	if commandUse == "help" {
		commandUse = args[0]
		help = true
	}

	command, ok := b.commands[commandUse]
	if !ok {
		return errors.New("unknown command: " + commandUse)
	}

	if help {
		fmt.Fprintf(b.out, "%s\n", command.printUsage(b.use, &flagSet))
		return nil
	}

	return command.execFunc(ctx, b.out)
}

func (b *CommandBundle) usage(flagSet *flag.FlagSet) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`
%s

Usage:
  %s [command] [flags]

Available Commands:
`, strings.TrimSpace(b.long), b.use))

	var longestUse int
	for use := range b.commands {
		if len(use) > longestUse {
			longestUse = len(use)
		}
	}

	// Go's maps are unordered of course. Alphabetize.
	sortedCommandUses := maputil.Keys(b.commands)
	slices.Sort(sortedCommandUses)

	for _, use := range sortedCommandUses {
		command := b.commands[use]
		sb.WriteString(fmt.Sprintf("  %-*s  %s\n", longestUse, use, command.short))
	}

	sb.WriteString("\nFlags:\n")
	flagSet.SetOutput(&sb)
	flagSet.PrintDefaults()

	sb.WriteString(fmt.Sprintf(`
Use "%s [command] -help" for more information about a command.
  `, b.use))

	// Go's flag module loves tabs of course. Kill them in favor of spaces,
	// which are easier to test against.
	return strings.TrimSpace(strings.ReplaceAll(sb.String(), "\t", "    "))
}

type commandBundleCommand struct {
	execFunc func(ctx context.Context, out io.Writer) error
	long     string
	short    string
	use      string
}

func (b *commandBundleCommand) printUsage(bundleUse string, flagSet *flag.FlagSet) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf(`
%s

Usage:
  %s %s [flags]
`, strings.TrimSpace(b.long), bundleUse, b.use))

	sb.WriteString("\nFlags:\n")
	flagSet.SetOutput(&sb)
	flagSet.PrintDefaults()

	// Go's flag module loves tabs of course. Kill them in favor of spaces,
	// which are easier to test against.
	return strings.TrimSpace(strings.ReplaceAll(sb.String(), "\t", "    "))
}
