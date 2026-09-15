package riverdatabasesql

import (
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYugabyteCompatibility(t *testing.T) {
	t.Parallel()

	// YugabyteDB doesn't expose PostgreSQL's transaction-related system columns:
	// https://docs.yugabyte.com/stable/yugabyte-voyager/known-issues/postgresql/#system-columns-is-not-yet-supported
	// The unique insert query may use xmax only because the entire expression is
	// replaced when the driver detects YugabyteDB.
	var (
		uniqueInsertModeTemplateRE = regexp.MustCompile(`(?s)/\*\s*TEMPLATE_BEGIN: unique_skipped_as_duplicate\s*\*/.*?/\*\s*TEMPLATE_END\s*\*/`)
		unsupportedSystemColumnRE  = regexp.MustCompile(`(?i)\b(?:cmax|cmin|ctid|xmax|xmin)\b`)
	)

	sourceRoot, err := os.OpenRoot(".")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sourceRoot.Close()) })

	var violations []string
	err = fs.WalkDir(sourceRoot.FS(), ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || (!strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, ".sql")) || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		contents, err := sourceRoot.ReadFile(path)
		if err != nil {
			return err
		}
		contents = uniqueInsertModeTemplateRE.ReplaceAll(contents, nil)

		for lineNum, line := range strings.Split(string(contents), "\n") {
			for _, column := range unsupportedSystemColumnRE.FindAllString(line, -1) {
				violations = append(violations, fmt.Sprintf("%s:%d: %s", path, lineNum+1, column))
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.Empty(t, violations,
		"YugabyteDB-incompatible PostgreSQL system columns must only appear inside SQL templates that replace them for YugabyteDB",
	)
}
