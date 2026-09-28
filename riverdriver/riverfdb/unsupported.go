//go:build foundationdb

package riverfdb

import (
	"context"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivertype"
)

// These SQL-specific operations are deliberately unsupported. Keep concrete
// methods so a newly added driver operation is caught by the interface check.
type unsupportedExecutor struct{}

func (e *unsupportedExecutor) ColumnExists(ctx context.Context, params *riverdriver.ColumnExistsParams) (bool, error) {
	return false, unsupported("ColumnExists")
}

func (e *unsupportedExecutor) Exec(ctx context.Context, sql string, args ...any) error {
	return unsupported("Exec")
}

func (e *unsupportedExecutor) IndexDropIfExists(ctx context.Context, params *riverdriver.IndexDropIfExistsParams) error {
	return unsupported("IndexDropIfExists")
}

func (e *unsupportedExecutor) IndexExists(ctx context.Context, params *riverdriver.IndexExistsParams) (bool, error) {
	return false, unsupported("IndexExists")
}

func (e *unsupportedExecutor) IndexesExist(ctx context.Context, params *riverdriver.IndexesExistParams) (map[string]bool, error) {
	return nil, unsupported("IndexesExist")
}

func (e *unsupportedExecutor) IndexReindex(ctx context.Context, params *riverdriver.IndexReindexParams) error {
	return unsupported("IndexReindex")
}

func (e *unsupportedExecutor) IndexReindexArtifacts(ctx context.Context, params *riverdriver.IndexReindexArtifactsParams) ([]string, error) {
	return nil, unsupported("IndexReindexArtifacts")
}

func (e *unsupportedExecutor) JobDeleteMany(ctx context.Context, params *riverdriver.JobDeleteManyParams) ([]*rivertype.JobRow, error) {
	return nil, unsupported("JobDeleteMany")
}

func (e *unsupportedExecutor) JobList(ctx context.Context, params *riverdriver.JobListParams) ([]*rivertype.JobRow, error) {
	return nil, unsupported("JobList")
}

func (e *unsupportedExecutor) MigrationDeleteAssumingMainMany(ctx context.Context, params *riverdriver.MigrationDeleteAssumingMainManyParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationDeleteAssumingMainMany")
}

func (e *unsupportedExecutor) MigrationDeleteByLineAndVersionMany(ctx context.Context, params *riverdriver.MigrationDeleteByLineAndVersionManyParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationDeleteByLineAndVersionMany")
}

func (e *unsupportedExecutor) MigrationGetAllAssumingMain(ctx context.Context, params *riverdriver.MigrationGetAllAssumingMainParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationGetAllAssumingMain")
}

func (e *unsupportedExecutor) MigrationGetByLine(ctx context.Context, params *riverdriver.MigrationGetByLineParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationGetByLine")
}

func (e *unsupportedExecutor) MigrationInsertMany(ctx context.Context, params *riverdriver.MigrationInsertManyParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationInsertMany")
}

func (e *unsupportedExecutor) MigrationInsertManyAssumingMain(ctx context.Context, params *riverdriver.MigrationInsertManyAssumingMainParams) ([]*riverdriver.Migration, error) {
	return nil, unsupported("MigrationInsertManyAssumingMain")
}

func (e *unsupportedExecutor) PGAdvisoryXactLock(ctx context.Context, key int64) (*struct{}, error) {
	return nil, unsupported("PGAdvisoryXactLock")
}

func (e *unsupportedExecutor) QueryRow(ctx context.Context, sql string, args ...any) riverdriver.Row {
	return unsupportedRow{}
}

func (e *unsupportedExecutor) SchemaCreate(ctx context.Context, params *riverdriver.SchemaCreateParams) error {
	return unsupported("SchemaCreate")
}

func (e *unsupportedExecutor) SchemaDrop(ctx context.Context, params *riverdriver.SchemaDropParams) error {
	return unsupported("SchemaDrop")
}

func (e *unsupportedExecutor) SchemaGetExpired(ctx context.Context, params *riverdriver.SchemaGetExpiredParams) ([]string, error) {
	return nil, unsupported("SchemaGetExpired")
}

func (e *unsupportedExecutor) TableExists(ctx context.Context, params *riverdriver.TableExistsParams) (bool, error) {
	return false, unsupported("TableExists")
}

func (e *unsupportedExecutor) TableTruncate(ctx context.Context, params *riverdriver.TableTruncateParams) error {
	return unsupported("TableTruncate")
}

type unsupportedRow struct{}

func (unsupportedRow) Scan(...any) error { return unsupported("QueryRow") }
