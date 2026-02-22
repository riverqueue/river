package riverpgxv5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

func TestListener_Connect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("SuccessfulConnect", func(t *testing.T) {
		t.Parallel()

		pool := testPool(ctx, t, nil)
		listener := &Listener{dbPool: pool}

		require.NoError(t, listener.Connect(ctx))
		require.NotNil(t, listener.conn)

		require.NoError(t, listener.Close(ctx))
	})

	t.Run("ReleasesPoolConnOnAfterConnectExecError", func(t *testing.T) {
		t.Parallel()

		config := testPoolConfig()
		config.MaxConns = 1 // only one connection in pool

		pool := testPool(ctx, t, config)

		listener := &Listener{dbPool: pool}
		listener.SetAfterConnectExec("INVALID SQL THAT WILL FAIL")

		// Connect should fail because of invalid afterConnectExec SQL.
		err := listener.Connect(ctx)
		require.Error(t, err)

		// The pool connection must have been released back to the pool.
		// With MaxConns=1, if it leaked, this Acquire would hang forever.
		acquireCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		conn, err := pool.Acquire(acquireCtx)
		require.NoError(t, err, "pool connection was leaked: Acquire timed out because the connection was not released")
		conn.Release()
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

	type testBundle struct {
		driver *Driver
	}

	setup := func(t *testing.T) (*sqlctemplate.Replacer, *testBundle) {
		t.Helper()

		return &sqlctemplate.Replacer{}, &testBundle{
			driver: New(nil),
		}
	}

	t.Run("NoSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL)
	})

	t.Run("WithSchema", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		updatedSQL, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "custom_schema"),
			bundle.driver.ArgPlaceholder(),
			"SELECT 1 FROM /* TEMPLATE: schema */river_job",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM custom_schema.river_job", updatedSQL)
	})
}

type panicConnDBTX struct{}

func (panicConnDBTX) Begin(context.Context) (pgx.Tx, error) { panic("unused") }
func (panicConnDBTX) Conn() *pgx.Conn                       { panic("not implemented") }
func (panicConnDBTX) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unused")
}
func (panicConnDBTX) Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error) {
	panic("unused")
}
func (panicConnDBTX) Query(context.Context, string, ...interface{}) (pgx.Rows, error) {
	panic("unused")
}
func (panicConnDBTX) QueryRow(context.Context, string, ...interface{}) pgx.Row { panic("unused") }

func TestTemplateReplaceWrapper_DefaultQueryExecMode(t *testing.T) {
	t.Parallel()

	t.Run("FallsBackToCacheStatementIfConnPanics", func(t *testing.T) {
		t.Parallel()

		wrapper := templateReplaceWrapper{
			dbtx:     panicConnDBTX{},
			replacer: &sqlctemplate.Replacer{},
		}

		require.Equal(t, pgx.QueryExecModeCacheStatement, wrapper.defaultQueryExecMode())
	})
}

func TestAdaptArgsForJSONTextModes(t *testing.T) {
	t.Parallel()

	t.Run("ConvertsOnlyJSONArgsSimpleProtocol", func(t *testing.T) {
		t.Parallel()

		args := []any{
			[]byte(`{"a":1}`),
			[]byte{0x01, 0x02},
			[][]byte{[]byte(`{"b":2}`), []byte(`{"c":3}`)},
		}
		updatedArgs := adaptArgsForJSONTextModes(pgx.QueryExecModeSimpleProtocol, "SELECT $1::jsonb, $2::bytea, $3::jsonb[]", args)

		require.IsType(t, json.RawMessage{}, updatedArgs[0])
		require.Equal(t, json.RawMessage([]byte(`{"a":1}`)), updatedArgs[0])
		require.IsType(t, []byte{}, updatedArgs[1])
		require.Equal(t, []byte{0x01, 0x02}, updatedArgs[1])

		jsonArray, ok := updatedArgs[2].([]json.RawMessage)
		require.True(t, ok)
		require.Equal(t, []json.RawMessage{
			json.RawMessage(`{"b":2}`),
			json.RawMessage(`{"c":3}`),
		}, jsonArray)
	})

	t.Run("ConvertsUncastByteSlicesExceptExplicitBytea", func(t *testing.T) {
		t.Parallel()

		args := []any{
			[]byte(`{"a":1}`),                                  // uncast json-ish arg
			[][]byte{[]byte(`{"b":2}`), []byte(`{"c":3}`)},    // uncast json-ish array arg
			[][]byte{[]byte{0x00, 0x01}, []byte{0x02, 0x03}}, // explicit bytea[] arg
		}
		updatedArgs := adaptArgsForJSONTextModes(
			pgx.QueryExecModeSimpleProtocol,
			"INSERT INTO river_job(args, errors, unique_key) VALUES ($1, $2, unnest($3::bytea[]))",
			args,
		)

		require.IsType(t, json.RawMessage{}, updatedArgs[0])
		require.IsType(t, []json.RawMessage{}, updatedArgs[1])
		require.IsType(t, [][]byte{}, updatedArgs[2])
	})

	t.Run("PreservesNilForConvertedByteSliceArrays", func(t *testing.T) {
		t.Parallel()

		var errors [][]byte
		updatedArgs := adaptArgsForJSONTextModes(pgx.QueryExecModeSimpleProtocol, "INSERT INTO river_job(errors) VALUES ($1)", []any{errors})

		converted, ok := updatedArgs[0].([]json.RawMessage)
		require.True(t, ok)
		require.Nil(t, converted)
	})

	t.Run("ConvertsJSONArgsInExecMode", func(t *testing.T) {
		t.Parallel()

		args := []any{[]byte(`{"x":1}`)}
		updatedArgs := adaptArgsForJSONTextModes(pgx.QueryExecModeExec, "SELECT $1::jsonb", args)

		require.IsType(t, json.RawMessage{}, updatedArgs[0])
		require.Equal(t, json.RawMessage(`{"x":1}`), updatedArgs[0])
	})

	t.Run("DoesNotConvertArgsInCacheStatementMode", func(t *testing.T) {
		t.Parallel()

		args := []any{[]byte(`{"x":1}`), [][]byte{[]byte(`{"y":2}`)}}
		updatedArgs := adaptArgsForJSONTextModes(pgx.QueryExecModeCacheStatement, "SELECT $1::jsonb, $2::jsonb[]", args)

		require.IsType(t, []byte{}, updatedArgs[0])
		require.IsType(t, [][]byte{}, updatedArgs[1])
	})

	t.Run("RespectsQueryOptionArgOffset", func(t *testing.T) {
		t.Parallel()

		args := []any{
			pgx.QueryExecModeSimpleProtocol,
			[]byte(`{"x":1}`),
			[][]byte{[]byte(`{"y":2}`)},
		}
		updatedArgs := adaptArgsForJSONTextModes(pgx.QueryExecModeCacheStatement, "SELECT $1::jsonb, $2::jsonb[]", args)

		require.Equal(t, pgx.QueryExecModeSimpleProtocol, updatedArgs[0])
		require.IsType(t, json.RawMessage{}, updatedArgs[1])
		require.IsType(t, []json.RawMessage{}, updatedArgs[2])
	})

}
