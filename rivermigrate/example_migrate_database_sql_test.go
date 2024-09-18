package rivermigrate_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
)

// Example_migrateDatabaseSQL demonstrates the use of River's Go migration API
// through Go's built-in database/sql package.
func Example_migrateDatabaseSQL() {
	ctx := context.Background()

	// Use a dedicated Postgres schema for this example so we can migrate and drop it at will:
	schemaName := "migration_example_dbsql"
	url := riverinternaltest.DatabaseURL("river_test_example") + "&search_path=" + schemaName
	dbPool, err := sql.Open("pgx", url)
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	driver := riverdatabasesql.New(dbPool)
	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		panic(err)
	}

	// Create the schema used for this example. Drop it when we're done.
	// This isn't necessary outside this test.
	if _, err := dbPool.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName); err != nil {
		panic(err)
	}
	defer dropRiverSchema(ctx, driver, schemaName)

	printVersions := func(res *rivermigrate.MigrateResult) {
		for _, version := range res.Versions {
			fmt.Printf("Migrated [%s] version %d\n", strings.ToUpper(string(res.Direction)), version.Version)
		}
	}

	// Migrate to version 3. An actual call may want to omit all MigrateOpts,
	// which will default to applying all available up migrations.
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		TargetVersion: 3,
	})
	if err != nil {
		panic(err)
	}
	printVersions(res)

	// Migrate down by three steps. Down migrating defaults to running only one
	// step unless overridden by an option like MaxSteps or TargetVersion.
	res, err = migrator.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		MaxSteps: 3,
	})
	if err != nil {
		panic(err)
	}
	printVersions(res)

	// Output:
	// Migrated [UP] version 1
	// Migrated [UP] version 2
	// Migrated [UP] version 3
	// Migrated [DOWN] version 3
	// Migrated [DOWN] version 2
	// Migrated [DOWN] version 1
}
