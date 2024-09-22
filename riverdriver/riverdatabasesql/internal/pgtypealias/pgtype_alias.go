// package pgtypealias exists to work aronud sqlc bugs with being able to
// reference v5 the pgtype package from within a dbsql package.
package pgtypealias

import "github.com/jackc/pgx/v5/pgtype"

type Bits struct {
	pgtype.Bits
}
