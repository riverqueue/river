package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

func init() { //nolint:gochecknoinits
	rootCmd.AddCommand(dropCmd)
}

var ignoreNonExistent bool //nolint:gochecknoglobals

var dropCmd = &cobra.Command{ //nolint:gochecknoglobals
	Use:   "drop",
	Short: "Drop the test database",
	Long: `
Drops all test databases. Any test database matching the base name
(river_testdb) or the base name with an underscore and a number (river_testdb_0,
river_testdb_1, etc.) will be dropped.
	`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := drop(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "failed: %s", err.Error())
			os.Exit(1)
		}
	},
}

func attemptDropDB(ctx context.Context, mgmtConn *pgx.Conn, dbNameToDelete string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := mgmtConn.Exec(ctx, "DROP DATABASE "+dbNameToDelete); err != nil {
		if ignoreNonExistent && strings.Contains(err.Error(), fmt.Sprintf("database %q does not exist", dbNameToDelete)) {
			fmt.Printf("Database %s does not exist, ignoring reset\n", dbNameToDelete)
			return nil
		}

		return fmt.Errorf("dropping database %q failed: %w", dbNameToDelete, err)
	}
	fmt.Printf("Dropped database %s\n", dbNameToDelete)

	return nil
}

func drop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// List all databases:
	mgmtConn, err := getMgmtConn(ctx)
	if err != nil {
		return fmt.Errorf("error getting management connection: %w", err)
	}
	defer mgmtConn.Close(ctx)

	rows, err := mgmtConn.Query(ctx, "SELECT datname FROM pg_database")
	if err != nil {
		return fmt.Errorf("error listing databases: %w", err)
	}
	defer rows.Close()

	allDBs := make([]string, 0)
	for rows.Next() {
		var dbName string
		err := rows.Scan(&dbName)
		if err != nil {
			return fmt.Errorf("error scanning DB name: %w", err)
		}
		allDBs = append(allDBs, dbName)
	}
	rows.Close()

	for _, dbName := range allDBs {
		if strings.HasPrefix(dbName, "river_testdb") {
			if err := attemptDropDB(ctx, mgmtConn, dbName); err != nil {
				return err
			}
		}
	}

	return nil
}

func getMgmtConn(ctx context.Context) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	mgmtConn, err := pgx.Connect(ctx, dbURL(""))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to management DB: %w", err)
	}

	return mgmtConn, nil
}
