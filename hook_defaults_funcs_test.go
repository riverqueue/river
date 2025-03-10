package river

import (
	"context"

	"github.com/riverqueue/river/rivertype"
)

// Verify interface compliance.
var (
	_ rivertype.Hook            = HookInsertBeginFunc(func(ctx context.Context, params *rivertype.JobInsertParams) error { return nil })
	_ rivertype.HookInsertBegin = HookInsertBeginFunc(func(ctx context.Context, params *rivertype.JobInsertParams) error { return nil })

	_ rivertype.Hook          = HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error { return nil })
	_ rivertype.HookWorkBegin = HookWorkBeginFunc(func(ctx context.Context, job *rivertype.JobRow) error { return nil })
)
