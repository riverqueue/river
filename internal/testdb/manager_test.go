package testdb_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"weavelab.xyz/river/internal/testdb"
)

func getTestDatabaseURL() string {
	if envURL := os.Getenv("TEST_DATABASE_URL"); envURL != "" {
		return envURL
	}
	return "postgres:///river_testdb?sslmode=disable"
}

func testConfig(t *testing.T) *pgxpool.Config {
	t.Helper()

	config, err := pgxpool.ParseConfig(getTestDatabaseURL())
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestManager_AcquireMultiple(t *testing.T) {
	t.Parallel()

	manager, err := testdb.NewManager(testConfig(t), 10, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool0, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool0.Release()

	checkDBNameForPool(ctx, t, pool0, "river_testdb_")

	pool1, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool1.Release()

	checkDBNameForPool(ctx, t, pool1, "river_testdb_")
	pool0.Release()

	//  ensure we get db 0 back on subsequent acquire since it was released to the pool:
	pool0Again, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool0Again.Release()

	checkDBNameForPool(ctx, t, pool0Again, "river_testdb_")
	pool0Again.Release()
	pool1.Release()

	manager.Close()
}

func TestManager_ReleaseTwice(t *testing.T) {
	t.Parallel()

	manager, err := testdb.NewManager(testConfig(t), 10, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tdb0, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tdb0.Release()

	selectOne(ctx, t, tdb0)

	// explicitly close p0's pgxpool.Pool before Release to ensure it can be fully
	// reused after release:
	tdb0.Pool().Close()
	t.Log("RELEASING P0")
	// tdb0.Release()
	// Calling release twice should be a no-op:
	t.Log("RELEASING P0 AGAIN")
	tdb0.Release()

	//  ensure we get db 0 back on subsequent acquire since it was released to the pool:
	t.Log("REACQUIRING P0")
	tdb1, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tdb1.Release()

	selectOne(ctx, t, tdb1)
	tdb1.Release()

	t.Log("CALLING MANAGER CLOSE MANUALLY")
	manager.Close()
}

func checkDBNameForPool(ctx context.Context, t *testing.T, p *testdb.DBWithPool, expectedPrefix string) {
	t.Helper()

	var currentDBName string
	if err := p.Pool().QueryRow(ctx, "SELECT current_database()").Scan(&currentDBName); err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(currentDBName, expectedPrefix) {
		t.Errorf("expected database name to begin with %q, got %q", expectedPrefix, currentDBName)
	}
}

func selectOne(ctx context.Context, t *testing.T, p *testdb.DBWithPool) {
	t.Helper()

	var one int
	if err := p.Pool().QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}

	if one != 1 {
		t.Errorf("expected 1, got %d", one)
	}
}
