package riverdatabasesql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/sqlctemplate"
	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var _ riverdriver.Driver[*sql.Tx] = New(nil)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		dbPool := &sql.DB{}
		driver := New(dbPool)
		require.Equal(t, dbPool, driver.dbPool)
	})

	t.Run("AllowsNilDatabasePool", func(t *testing.T) {
		t.Parallel()

		driver := New(nil)
		require.Nil(t, driver.dbPool)
	})
}

func TestInterpretError(t *testing.T) {
	t.Parallel()

	require.EqualError(t, interpretError(errors.New("an error")), "an error")
	require.ErrorIs(t, interpretError(sql.ErrNoRows), rivertype.ErrNotFound)
	require.NoError(t, interpretError(nil))
}

func TestReplaceNamed(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Desc        string
		ExpectedSQL string
		InputSQL    string
		InputArgs   map[string]any
	}{
		{Desc: "Boolean", ExpectedSQL: "SELECT true", InputSQL: "SELECT @bool", InputArgs: map[string]any{"bool": true}},
		{Desc: "Float32", ExpectedSQL: "SELECT 1.23", InputSQL: "SELECT @float32", InputArgs: map[string]any{"float32": float32(1.23)}},
		{Desc: "Float64", ExpectedSQL: "SELECT 1.23", InputSQL: "SELECT @float64", InputArgs: map[string]any{"float64": float64(1.23)}},
		{Desc: "Int", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @int", InputArgs: map[string]any{"int": 123}},
		{Desc: "Int16", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @int16", InputArgs: map[string]any{"int16": int16(123)}},
		{Desc: "Int32", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @int32", InputArgs: map[string]any{"int32": int32(123)}},
		{Desc: "Int64", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @int64", InputArgs: map[string]any{"int64": int64(123)}},
		{Desc: "String", ExpectedSQL: "SELECT 'string value'", InputSQL: "SELECT @string", InputArgs: map[string]any{"string": "string value"}},
		{Desc: "StringWithQuote", ExpectedSQL: "SELECT 'string value with '' quote'", InputSQL: "SELECT @string", InputArgs: map[string]any{"string": "string value with ' quote"}},
		{Desc: "Uint", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @uint", InputArgs: map[string]any{"uint": uint(123)}},
		{Desc: "Uint16", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @uint16", InputArgs: map[string]any{"uint16": uint16(123)}},
		{Desc: "Uint32", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @uint32", InputArgs: map[string]any{"uint32": uint32(123)}},
		{Desc: "Uint64", ExpectedSQL: "SELECT 123", InputSQL: "SELECT @uint64", InputArgs: map[string]any{"uint64": uint64(123)}},

		{Desc: "SliceBoolean", ExpectedSQL: "SELECT ARRAY[false,true]", InputSQL: "SELECT @slice_bool", InputArgs: map[string]any{"slice_bool": []bool{false, true}}},
		{Desc: "SliceFloat32", ExpectedSQL: "SELECT ARRAY[1.23,1.24]", InputSQL: "SELECT @slice_float32", InputArgs: map[string]any{"slice_float32": []float32{1.23, 1.24}}},
		{Desc: "SliceFloat64", ExpectedSQL: "SELECT ARRAY[1.23,1.24]", InputSQL: "SELECT @slice_float64", InputArgs: map[string]any{"slice_float64": []float64{1.23, 1.24}}},
		{Desc: "SliceInt", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_int", InputArgs: map[string]any{"slice_int": []int{123, 124}}},
		{Desc: "SliceInt16", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_int16", InputArgs: map[string]any{"slice_int16": []int16{123, 124}}},
		{Desc: "SliceInt32", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_int32", InputArgs: map[string]any{"slice_int32": []int32{123, 124}}},
		{Desc: "SliceInt64", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_int64", InputArgs: map[string]any{"slice_int64": []int64{123, 124}}},
		{Desc: "SliceString", ExpectedSQL: "SELECT ARRAY['string 1','string 2']", InputSQL: "SELECT @slice_string", InputArgs: map[string]any{"slice_string": []string{"string 1", "string 2"}}},
		{Desc: "SliceUint", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_uint", InputArgs: map[string]any{"slice_uint": []uint{123, 124}}},
		{Desc: "SliceUint16", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_uint16", InputArgs: map[string]any{"slice_uint16": []uint16{123, 124}}},
		{Desc: "SliceUint32", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_uint32", InputArgs: map[string]any{"slice_uint32": []uint32{123, 124}}},
		{Desc: "SliceUint64", ExpectedSQL: "SELECT ARRAY[123,124]", InputSQL: "SELECT @slice_uint64", InputArgs: map[string]any{"slice_uint64": []uint64{123, 124}}},
	}
	for _, tt := range testCases {
		t.Run(tt.Desc, func(t *testing.T) {
			t.Parallel()

			actualSQL, err := replaceNamed(tt.InputSQL, tt.InputArgs)
			require.NoError(t, err)
			require.Equal(t, tt.ExpectedSQL, actualSQL)
		})
	}
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
