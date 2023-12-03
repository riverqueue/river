package rivermigrate_test

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/riverinternaltest"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type SortArgs struct {
	// Strings is a slice of strings to sort.
	Strings []string `json:"strings"`
}

func (SortArgs) Kind() string { return "sort" }

type SortWorker struct {
	river.WorkerDefaults[SortArgs]
}

func (w *SortWorker) Work(ctx context.Context, job *river.Job[SortArgs]) error {
	sort.Strings(job.Args.Strings)
	fmt.Printf("Sorted strings: %+v\n", job.Args.Strings)
	return nil
}

// Example_migrate demonstrates the use of River's Go migration API by migrating
// up and down.
func Example_migrate() {
	ctx := context.Background()

	dbPool, err := pgxpool.NewWithConfig(ctx, riverinternaltest.DatabaseConfig("river_testdb_example"))
	if err != nil {
		panic(err)
	}
	defer dbPool.Close()

	tx, err := dbPool.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

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

	// Roll back all changes applied so our test database is left unaffected.
	if err := tx.Rollback(ctx); err != nil {
		panic(err)
	}

	// Output:
	// Migrated [UP] version 1
	// Migrated [UP] version 2
	// Migrated [UP] version 3
	// Migrated [DOWN] version 3
	// Migrated [DOWN] version 2
	// Migrated [DOWN] version 1
}

func dropRiverSchema(ctx context.Context, migrator *rivermigrate.Migrator[pgx.Tx], tx pgx.Tx) {
	_, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{
		TargetVersion: -1,
	})
	if err != nil {
		panic(err)
	}
}
