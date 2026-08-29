package riverdatabasesql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("DatabasePool", func(t *testing.T) {
		t.Parallel()

		dbPool := &sql.DB{}
		driver := New(dbPool)
		require.Equal(t, dbPool, driver.dbPool)
	})

	t.Run("NoDatabasePool", func(t *testing.T) {
		t.Parallel()

		driver := New(nil)
		require.Nil(t, driver.dbPool)
	})

	t.Run("PollOnly", func(t *testing.T) {
		t.Parallel()

		driver := New(&sql.DB{})
		require.Nil(t, driver.listenerDriver)
		require.False(t, driver.SupportsListener())
		require.True(t, driver.SupportsListenNotify())
		require.PanicsWithValue(t, riverdriver.ErrNotImplemented, func() {
			driver.GetListener(&riverdriver.GetListenenerParams{})
		})
	})
}

func TestNewWithPgxListener(t *testing.T) {
	t.Parallel()

	t.Run("PanicsOnNilListenerPool", func(t *testing.T) {
		t.Parallel()

		require.PanicsWithValue(t, "riverdatabasesql: listener pool must not be nil", func() {
			NewWithPgxListener(&sql.DB{}, nil)
		})
	})

	t.Run("UsesSeparateListenerPool", func(t *testing.T) {
		t.Parallel()

		dbPool := &sql.DB{}
		listenerPool := &pgxpool.Pool{}
		driver := NewWithPgxListener(dbPool, listenerPool)

		require.Equal(t, dbPool, driver.dbPool)
		require.NotNil(t, driver.listenerDriver)
		require.True(t, driver.SupportsListener())
		require.Equal(t, dbPool, driver.GetExecutor().(*Executor).dbPool) //nolint:forcetypeassert

		listener1 := driver.GetListener(&riverdriver.GetListenenerParams{Schema: "schema_one"})
		listener2 := driver.GetListener(&riverdriver.GetListenenerParams{Schema: "schema_two"})
		require.NotSame(t, listener1, listener2)
		require.Equal(t, "schema_one", listener1.Schema())
		require.Equal(t, "schema_two", listener2.Schema())
	})
}

func TestBitIntegerToBits(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0b0000_0000, bitIntegerToBits(0, 8))
	require.Equal(t, 0b0000_0001, bitIntegerToBits(1, 8))
	require.Equal(t, 0b0000_0011, bitIntegerToBits(11, 8))
	require.Equal(t, 0b0000_0111, bitIntegerToBits(111, 8))
	require.Equal(t, 0b0000_1111, bitIntegerToBits(1111, 8))
	require.Equal(t, 0b0001_1111, bitIntegerToBits(1_1111, 8))
	require.Equal(t, 0b0011_1111, bitIntegerToBits(11_1111, 8))
	require.Equal(t, 0b0111_1111, bitIntegerToBits(111_1111, 8))
	require.Equal(t, 0b1111_1111, bitIntegerToBits(1111_1111, 8))
	require.Equal(t, 0b0000_0010, bitIntegerToBits(10, 8))
	require.Equal(t, 0b0000_0100, bitIntegerToBits(100, 8))
	require.Equal(t, 0b0000_1000, bitIntegerToBits(1000, 8))
	require.Equal(t, 0b0001_0000, bitIntegerToBits(1_0000, 8))
	require.Equal(t, 0b0010_0000, bitIntegerToBits(10_0000, 8))
	require.Equal(t, 0b0100_0000, bitIntegerToBits(100_0000, 8))
	require.Equal(t, 0b1000_0000, bitIntegerToBits(1000_0000, 8))
	require.Equal(t, 0b1010_1010, bitIntegerToBits(1010_1010, 8))
	require.Equal(t, 0b101_0101, bitIntegerToBits(101_0101, 8))

	// Extra values past numBits.
	require.Equal(t, 0b1010, bitIntegerToBits(1010_1010, 4))
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
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
		require.Equal(t, `SELECT 1 FROM "custom_schema".river_job`, updatedSQL)
	})

	t.Run("SchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, `SELECT 1 FROM "my_schema".river_job`, updatedSQL1)

		// Second call with same SQL + same schema produces identical result.
		// Because schema is marked Stable, the Replacer caches the output
		// after the first call and short-circuits regex on subsequent calls.
		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, "my_schema"),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})

	t.Run("EmptySchemaReplacementIsStable", func(t *testing.T) {
		t.Parallel()

		replacer, bundle := setup(t)

		const sql = "SELECT 1 FROM /* TEMPLATE: schema */river_job"

		updatedSQL1, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, "SELECT 1 FROM river_job", updatedSQL1)

		updatedSQL2, _, err := replacer.RunSafely(
			schemaTemplateParam(ctx, ""),
			bundle.driver.ArgPlaceholder(),
			sql,
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, updatedSQL1, updatedSQL2)
	})
}
