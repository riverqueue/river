package riverdrivertest_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "turso.tech/database/tursogo"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riversqlite"
)

// Example_turso demonstrates use of River's SQLite driver with a local Turso
// database.
func Example_turso() {
	ctx := context.Background()

	tempDir, err := os.MkdirTemp("", "river-example-turso-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	dbPool, err := sql.Open("turso", filepath.Join(tempDir, "example_turso.db"))
	if err != nil {
		panic(err)
	}
	dbPool.SetMaxOpenConns(1)
	defer dbPool.Close()

	driver := riversqlite.New(dbPool)

	if err := migrateDB(ctx, driver); err != nil {
		panic(err)
	}

	riverClient, err := river.NewClient(driver, initTestConfig(ctx, nil, &river.Config{}))
	if err != nil {
		panic(err)
	}

	insertResult, err := riverClient.Insert(ctx, SortArgs{
		Strings: []string{
			"whale", "tiger", "bear",
		},
	}, nil)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Inserted job kind: %s\n", insertResult.Job.Kind)

	// Output:
	// Inserted job kind: sort
}
