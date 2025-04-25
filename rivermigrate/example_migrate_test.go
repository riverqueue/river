package rivermigrate_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/util/testutil"
)

// Example_migrate demonstrates the use of River's Go migration API by migrating
// up and down.
func Example_migrate() {
	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, riversharedtest.TestDatabaseURL())
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	driver := riverpgxv5.New(dbPool)
	migrator, err := rivermigrate.New(driver, &rivermigrate.Config{
		// Test schema with no migrations for purposes of this test.
		Schema: riverdbtest.TestSchema(ctx, testutil.PanicTB(), driver, &riverdbtest.TestSchemaOpts{Lines: []string{}}),
	})
	if err != nil {
		panic(err)
	}

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
