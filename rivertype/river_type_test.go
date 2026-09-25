package rivertype_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestAttemptError_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	attemptAt := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)

	t.Run("InvalidJSON", func(t *testing.T) {
		t.Parallel()

		var attemptErr rivertype.AttemptError
		require.EqualError(t, attemptErr.UnmarshalJSON([]byte(`{"at":`)), "attempt error is not valid JSON")
	})

	t.Run("Lenient", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			expected rivertype.AttemptError
			json     string
			name     string
		}{
			{name: "AtInvalid", json: `{"at":"not a time","attempt":2,"error":"err"}`, expected: rivertype.AttemptError{Attempt: 2, Error: "err"}},
			{name: "AtNoOffset", json: `{"at":"2024-01-02T03:04:05.123456","attempt":2}`, expected: rivertype.AttemptError{At: attemptAt, Attempt: 2}},
			{name: "AtNumber", json: `{"at":1704164645,"attempt":2}`, expected: rivertype.AttemptError{Attempt: 2}},
			{name: "AtPostgresText", json: `{"at":"2024-01-02 03:04:05.123456+00","attempt":2}`, expected: rivertype.AttemptError{At: attemptAt, Attempt: 2}},
			{name: "AtSpaceNoOffset", json: `{"at":"2024-01-02 03:04:05.123456","attempt":2}`, expected: rivertype.AttemptError{At: attemptAt, Attempt: 2}},
			{name: "AttemptFloat", json: `{"attempt":3.0,"error":"err"}`, expected: rivertype.AttemptError{Attempt: 3, Error: "err"}},
			{name: "AttemptFractional", json: `{"attempt":3.5,"error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "AttemptObject", json: `{"attempt":{},"error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "AttemptString", json: `{"attempt":" 3 ","error":"err"}`, expected: rivertype.AttemptError{Attempt: 3, Error: "err"}},
			{name: "AttemptStringInvalid", json: `{"attempt":"three","error":"err"}`, expected: rivertype.AttemptError{Error: "err"}},
			{name: "ElementArray", json: `[1, "two"]`, expected: rivertype.AttemptError{Error: `[1,"two"]`}},
			{name: "ElementNumber", json: `123`, expected: rivertype.AttemptError{Error: "123"}},
			{name: "ElementString", json: `"job failed"`, expected: rivertype.AttemptError{Error: "job failed"}},
			{name: "ErrorObject", json: `{"attempt":1,"error":{"message": "boom", "code": 7}}`, expected: rivertype.AttemptError{Attempt: 1, Error: `{"message":"boom","code":7}`}},
			{name: "TraceArray", json: `{"attempt":1,"error":"err","trace":["frame1", "frame2"]}`, expected: rivertype.AttemptError{Attempt: 1, Error: "err", Trace: `["frame1","frame2"]`}},
			{name: "TraceNullWithInvalidField", json: `{"attempt":"x","error":null,"trace":null}`, expected: rivertype.AttemptError{}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				var attemptErr rivertype.AttemptError
				require.NoError(t, json.Unmarshal([]byte(test.json), &attemptErr))
				require.True(t, test.expected.At.Equal(attemptErr.At), "expected at %s, got %s", test.expected.At, attemptErr.At)
				attemptErr.At = test.expected.At
				require.Equal(t, test.expected, attemptErr)
			})
		}
	})

	t.Run("RoundTrip", func(t *testing.T) {
		t.Parallel()

		attemptErr := rivertype.AttemptError{
			At:      attemptAt,
			Attempt: 3,
			Error:   "job failed",
			Trace:   "goroutine 1 [running]:",
		}
		data, err := json.Marshal(attemptErr)
		require.NoError(t, err)
		require.JSONEq(t, `{"at":"2024-01-02T03:04:05.123456Z","attempt":3,"error":"job failed","trace":"goroutine 1 [running]:"}`, string(data))

		var decoded rivertype.AttemptError
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, attemptErr, decoded)
	})

	t.Run("Slice", func(t *testing.T) {
		t.Parallel()

		// One unexpected element doesn't prevent decoding the others.
		var attemptErrs []rivertype.AttemptError
		require.NoError(t, json.Unmarshal([]byte(`[{"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err1","trace":""},"err2"]`), &attemptErrs))
		require.Equal(t, []rivertype.AttemptError{
			{At: attemptAt, Attempt: 1, Error: "err1"},
			{Error: "err2"},
		}, attemptErrs)
	})

	t.Run("StrictShapeUnchanged", func(t *testing.T) {
		t.Parallel()

		// Missing fields and nulls decode the same as with encoding/json's
		// defaults.
		var attemptErr rivertype.AttemptError
		require.NoError(t, json.Unmarshal([]byte(`{"attempt":2,"error":null}`), &attemptErr))
		require.Equal(t, rivertype.AttemptError{Attempt: 2}, attemptErr)

		attemptErr = rivertype.AttemptError{Error: "previous"}
		require.NoError(t, json.Unmarshal([]byte(`null`), &attemptErr))
		require.Equal(t, rivertype.AttemptError{Error: "previous"}, attemptErr)
	})
}

func TestJobRow_Output(t *testing.T) {
	t.Parallel()

	t.Run("SimpleStringOutput", func(t *testing.T) {
		t.Parallel()

		jobRow := &rivertype.JobRow{
			Metadata: []byte(`{"output": "test"}`),
		}
		require.Equal(t, []byte(`"test"`), jobRow.Output())
	})

	t.Run("ComplexObjectOutput", func(t *testing.T) {
		t.Parallel()
		jobRow := &rivertype.JobRow{
			Metadata: []byte(`{"output": {"foo": {"bar": "baz"}}}`),
		}
		require.JSONEq(t, `{"foo": {"bar": "baz"}}`, string(jobRow.Output()))
	})

	t.Run("NoOutput", func(t *testing.T) {
		t.Parallel()
		jobRow := &rivertype.JobRow{
			Metadata: []byte(`{}`),
		}
		require.Nil(t, jobRow.Output())
	})

	t.Run("InvalidMetadata", func(t *testing.T) {
		t.Parallel()

		jobRow := &rivertype.JobRow{
			Metadata: []byte(`not-json`),
		}
		require.Nil(t, jobRow.Output())
	})
}

func TestJobStates(t *testing.T) {
	t.Parallel()

	jobStates := rivertype.JobStates()

	// One easy check that doesn't require the source file reading below.
	require.Contains(t, jobStates, rivertype.JobStateAvailable)

	// Get all job state names from the corresponding source file and make sure
	// they're included in JobStates. Helps check that we didn't add a new value
	// but forgot to add it to the full list of constant values.
	for _, nameAndValue := range allValuesForStringConstantType(t, "river_type.go", "JobState") {
		t.Logf("Checking for job state: %s / %s", nameAndValue.Name, nameAndValue.Value)
		require.Contains(t, jobStates, rivertype.JobState(nameAndValue.Value))
	}
}

// stringConstantNameAndValue is a name and value for a string constant like
// `JobStateAvailable` + `available`.
type stringConstantNameAndValue struct{ Name, Value string }

// allValuesForStringConstantType reads a Go source file and looks for all
// values for the named string constant.
func allValuesForStringConstantType(t *testing.T, srcFile, typeName string) []stringConstantNameAndValue {
	t.Helper()

	fset := token.NewFileSet()

	src, err := os.ReadFile(srcFile)
	require.NoError(t, err)

	f, err := parser.ParseFile(fset, srcFile, src, parser.ParseComments)
	require.NoError(t, err)

	var valueNames []stringConstantNameAndValue

	for _, decl := range f.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.CONST {
			for _, spec := range gen.Specs {
				// Always ast.ValueSpec for token.CONST.
				valueSpec := spec.(*ast.ValueSpec) //nolint:forcetypeassert

				typeIdent, ok := valueSpec.Type.(*ast.Ident)
				if !ok || typeIdent.Name != typeName {
					continue
				}

				for i, nameIdent := range valueSpec.Names {
					// Force type assert because we expect one of our constants
					// to be defined as a basic type literal like this.
					basicLitExpr := valueSpec.Values[i].(*ast.BasicLit) //nolint:forcetypeassert

					valueNames = append(valueNames, stringConstantNameAndValue{
						Name:  nameIdent.Name,
						Value: basicLitExpr.Value[1 : len(basicLitExpr.Value)-1], // strip quote on either side
					})
				}
			}
		}
	}

	if len(valueNames) < 1 {
		require.FailNow(t, "No values found", "No values found for source file and constant type: %s / %s", srcFile, typeName)
	}

	return valueNames
}
