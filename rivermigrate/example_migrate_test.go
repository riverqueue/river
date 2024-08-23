package rivermigrate_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// Example_migrate demonstrates the use of River's Go migration API by migrating
// up and down.
func Example_migrate() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_test_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	migrator, err := rivermigrate.New(riverpgxv5.New(dbPool), nil)
	if err != nil {
		panic(err)
	}

	// Our test database starts with a full River schema. Drop it so that we can
	// demonstrate working migrations. This isn't necessary outside this test.
	dropRiverSchema(ctx, migrator, tx)

	printVersions := func(res *rivermigrate.MigrateResult) {
		for _, version := range res.Versions {
			fmt.Printf("Migrated [%s] version %d\n", strings.ToUpper(string(res.Direction)), version.Version)
		}
	}

	// Migrate to version 3. An actual call may want to omit all MigrateOpts,
	// which will default to applying all available up migrations.
	res, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{
		TargetVersion: 3,
	})
	if err != nil {
		panic(err)
	}
	printVersions(res)

	// Migrate down by three steps. Down migrating defaults to running only one
	// step unless overridden by an option like MaxSteps or TargetVersion.
	res, err = migrator.MigrateTx(ctx, tx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
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

func dropRiverSchema[TTx any](ctx context.Context, migrator *rivermigrate.Migrator[TTx], tx TTx) {
	_, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		TargetVersion: -1,
	})
	if err != nil {
		panic(err)
	}
}
