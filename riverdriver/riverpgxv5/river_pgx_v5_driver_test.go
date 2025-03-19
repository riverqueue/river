package riverpgxv5

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[pgx.Tx] = New(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		dbPool := &pgxpool.Pool{}
		driver := New(dbPool)
		require.Equal(t, dbPool, driver.dbPool)
	})

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		driver := New(nil)
		require.Nil(t, driver.dbPool)
	})
}

func TestListener_Close(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("NoOpWithoutConn", func(t *testing.T) {
		t.Parallel()

		listener := &Listener{dbPool: testPool(ctx, t, nil)}
		require.Nil(t, listener.conn)
		require.NoError(t, listener.Close(ctx))
	})

	t.Run("UnsetsConnEvenOnError", func(t *testing.T) {
		t.Parallel()

		var connStub *connStub

		config := testPoolConfig()
		config.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
			// Dialer settings come from pgx's default internal one (not exported unfortunately).
			conn, err := (&net.Dialer{KeepAlive: 5 * time.Minute}).Dial(network, addr)
			if err != nil {
				return nil, err
			}

			connStub = newConnStub(conn)
			return connStub, nil
		}

		listener := &Listener{dbPool: testPool(ctx, t, config)}

		require.NoError(t, listener.Connect(ctx))
		require.NotNil(t, listener.conn)

		expectedErr := errors.New("conn close error")
		connStub.closeFunc = func() error {
			t.Logf("Close invoked; returning error")
			return expectedErr
		}

		require.ErrorIs(t, listener.Close(ctx), expectedErr)

		// Despite error, internal connection still unset.
		require.Nil(t, listener.conn)
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(puddle.ErrClosedPool), riverdriver.ErrClosedPool)
	require.ErrorIs(t, interpretError(pgx.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

// connStub implements net.Conn and allows us to stub particular functions like
// Close that are otherwise nigh impossible to test.
type connStub struct {
	net.Conn

	closeFunc func() error
}

func newConnStub(conn net.Conn) *connStub {
	return &connStub{
		Conn: conn,

		closeFunc: conn.Close,
	}
}

func (c *connStub) Close() error {
	return c.closeFunc()
}

func testPoolConfig() *pgxpool.Config {
	databaseURL := "postgres://localhost/river_test?sslmode=disable"
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		databaseURL = url
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		panic(fmt.Sprintf("error parsing database URL: %v", err))
	}
	config.ConnConfig.ConnectTimeout = 10 * time.Second
	config.ConnConfig.RuntimeParams["timezone"] = "UTC"

	return config
}

func testPool(ctx context.Context, t *testing.T, config *pgxpool.Config) *pgxpool.Pool {
	t.Helper()

	if config == nil {
		config = testPoolConfig()
	}

	dbPool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(dbPool.Close)
	return dbPool
}

func TestSchemaTemplateParam(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct{}

	setup := func(t *testing.T) (*sqlctemplate.Replacer, *testBundle) { //nolint:unparam
		t.Helper()

		return &sqlctemplate.Replacer{}, &testBundle{}
	}

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		replacer, _ := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "custom_schema"),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM custom_schema.river_job", updatedSQL)
	})
}
