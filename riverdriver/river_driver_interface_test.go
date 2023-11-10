package riverdriver

import (
	"github.com/jackc/pgx/v5"

	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Verify interface compliance.
var _ Driver[pgx.Tx] = &riverpgxv5.Driver{}
