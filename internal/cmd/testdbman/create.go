package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/riverqueue/river/internal/baseservice"
	"github.com/riverqueue/river/internal/dbmigrate"
)

func init() { //nolint:gochecknoinits
	rootCmd.AddCommand(createCmd)
}

var createCmd = &cobra.Command{ //nolint:gochecknoglobals
	Use:   "create",
	Short: "Create the test databases",
	Long: `
Creates the test databases used by parallel tests and the sample applications.
Each is loaded with the schema from river.Schema.

The sample application DB is named river_testdb, while the DBs for parallel
tests are named river_testdb_0, river_testdb_1, etc. up to the larger of 4 or
runtime.NumCPU() (a choice that comes from pgx's default connection pool size).
	`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		mgmtConn, err := pgx.Connect(ctx, dbURL(""))
		if err != nil {
			fmt.Printf("Unable to create connection: %v\n", err)
			os.Exit(1)
		}
		defer mgmtConn.Close(ctx)

		createDBAndLoadSchema := func(ctx context.Context, dbName string) error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			if _, err := mgmtConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
				return fmt.Errorf("error running `CREATE DATABASE %s`: %w", dbName, err)
			}
			fmt.Printf("Created database %s.\n", dbName)

			dbPool, err := pgxpool.New(ctx, dbURL(dbName))
			if err != nil {
				return fmt.Errorf("error creating connection pool to %q: %w", dbURL(dbName), err)
			}
			defer dbPool.Close()

			archetype := &baseservice.Archetype{
				Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
					Level: slog.LevelWarn,
				})),
				TimeNowUTC: func() time.Time { return time.Now().UTC() },
			}

			migrator := dbmigrate.NewMigrator(archetype)

			if _, err = migrator.Up(ctx, dbPool, &dbmigrate.MigrateOptions{}); err != nil {
				return err
			}
			fmt.Printf("Loaded schema in %s.\n", dbName)

			return nil
		}

		mustCreateDBAndLoadSchema := func(ctx context.Context, dbName string) {
			if err := createDBAndLoadSchema(ctx, dbName); err != nil {
				fmt.Fprintf(os.Stderr, "%s\n", err)
				os.Exit(1)
			}
		}

		mustCreateDBAndLoadSchema(ctx, "river_testdb")
		mustCreateDBAndLoadSchema(ctx, "river_testdb_example")

		// This is the same default as pgxpool's maximum number of connections
		// when not specified -- either 4 or the number of CPUs, whichever is
		// greater. If changing this number, also change the similar value in
		// `riverinternaltest` where it's duplicated.
		numDBs := max(4, runtime.NumCPU())

		for i := 0; i < numDBs; i++ {
			mustCreateDBAndLoadSchema(ctx, fmt.Sprintf("river_testdb_%d", i))
		}
	},
}

// TODO: smarter way to figure out db url, dynamic, etc.
func dbURL(dbName string) string {
	if dbName == "" {
		return "postgres:///postgres"
	}
	return fmt.Sprintf("postgres:///%s", dbName)
}
