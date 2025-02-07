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
