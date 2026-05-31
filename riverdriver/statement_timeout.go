package riverdriver

import (
	"strconv"
	"time"
)

// PostgresStatementTimeoutValue formats a duration for Postgres'
// statement_timeout setting.
//
// Postgres accepts statement_timeout values as whole milliseconds. Round
// positive sub-millisecond values up so they don't truncate to 0ms, which would
// disable the timeout.
func PostgresStatementTimeoutValue(timeout time.Duration) string {
	milliseconds := timeout / time.Millisecond
	if timeout > 0 && timeout%time.Millisecond != 0 {
		milliseconds++
	}

	return strconv.FormatInt(int64(milliseconds), 10) + "ms"
}
