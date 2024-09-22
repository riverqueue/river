package rivermigrate_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// Example_migrate demonstrates the use of River's Go migration API by migrating
// up and down.
func Example_migrate() {
	ctx := context.Background()

	// Use a dedicated Postgres schema for this example so we can migrate and drop it at will:
	schemaName := "migration_example"
	poolConfig := riverinternaltest.DatabaseConfig("river_test_example")
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schemaName

	dbPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	driver := riverpgxv5.New(dbPool)
	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		panic(err)
	}

	// Create the schema used for this example. Drop it when we're done.
	// This isn't necessary outside this test.
	if _, err := dbPool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaName); err != nil {
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

func dropRiverSchema[TTx any](ctx context.Context, driver riverdriver.Driver[TTx], schemaName string) {
	_, err := driver.GetExecutor().Exec(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE;")
	if err != nil {
		panic(err)
	}
}
