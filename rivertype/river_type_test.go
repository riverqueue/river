package rivertype_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

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

func TestMetadataGetters(t *testing.T) {
	metaValid := "{ \"foo\": \"bar\" }"
	metaInvalid := "{ \"foo\": \"bar\""
	want := map[string]interface{}{"foo": "bar"}

	t.Run("JobRowGet", func(t *testing.T) {
		jobRow := rivertype.JobRow{
			Metadata: []byte(metaValid),
		}

		got, err := jobRow.GetMetadata()
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("JobRowGetInvalid", func(t *testing.T) {
		jobRow := rivertype.JobRow{
			Metadata: []byte(metaInvalid),
		}

		_, err := jobRow.GetMetadata()
		require.Error(t, err)
	})
	t.Run("JobRowMustGet", func(t *testing.T) {
		jobRow := rivertype.JobRow{
			Metadata: []byte(metaValid),
		}

		got := jobRow.MustGetMetadata()
		require.Equal(t, want, got)
	})
	t.Run("JobRowMustGetInvalid", func(t *testing.T) {
		jobRow := rivertype.JobRow{
			Metadata: []byte(metaInvalid),
		}

		require.Panics(t, func() {
			jobRow.MustGetMetadata()
		})
	})

	t.Run("JobInsertParamsGet", func(t *testing.T) {
		jobInsertParams := rivertype.JobInsertParams{
			Metadata: []byte(metaValid),
		}

		got, err := jobInsertParams.GetMetadata()
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("JobInsertParamsGetInvalid", func(t *testing.T) {
		jobInsertParams := rivertype.JobInsertParams{
			Metadata: []byte(metaInvalid),
		}

		_, err := jobInsertParams.GetMetadata()
		require.Error(t, err)
	})
	t.Run("JobInsertParamsMustGet", func(t *testing.T) {
		jobInsertParams := rivertype.JobInsertParams{
			Metadata: []byte(metaValid),
		}

		got := jobInsertParams.MustGetMetadata()
		require.Equal(t, want, got)
	})
	t.Run("JobInsertParamsMustGetInvalid", func(t *testing.T) {
		jobInsertParams := rivertype.JobInsertParams{
			Metadata: []byte(metaInvalid),
		}

		require.Panics(t, func() {
			jobInsertParams.MustGetMetadata()
		})
	})

	t.Run("QueueGet", func(t *testing.T) {
		queue := rivertype.Queue{
			Metadata: []byte(metaValid),
		}

		got, err := queue.GetMetadata()
		require.NoError(t, err)
		require.Equal(t, want, got)
	})
	t.Run("QueueGetInvalid", func(t *testing.T) {
		queue := rivertype.Queue{
			Metadata: []byte(metaInvalid),
		}

		_, err := queue.GetMetadata()
		require.Error(t, err)
	})
	t.Run("QueueMustGet", func(t *testing.T) {
		queue := rivertype.Queue{
			Metadata: []byte(metaValid),
		}

		got := queue.MustGetMetadata()
		require.Equal(t, want, got)
	})
	t.Run("QueueMustGetInvalid", func(t *testing.T) {
		queue := rivertype.Queue{
			Metadata: []byte(metaInvalid),
		}

		require.Panics(t, func() {
			queue.MustGetMetadata()
		})
	})
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
