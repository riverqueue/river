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

	dbPool, err := sql.Open("pgx", riverinternaltest.DatabaseURL("river_test_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	tx, err := dbPool.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	migrator, err := rivermigrate.New(riverdatabasesql.New(dbPool), nil)
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
