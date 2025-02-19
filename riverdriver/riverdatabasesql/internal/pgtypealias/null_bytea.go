package pgtypealias

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
)

// NullBytea is a custom type for Postgres bytea that returns SQL NULL when
// the underlying slice is nil or empty. This override takes over for the base
// type `bytea`, so that when sqlc generates code for arrays of bytea, each
// element is a NullBytea and properly handles nil values. This is in contrast
// to the default behavior of pq.Array in this scenario.
//
// See https://github.com/riverqueue/river/issues/650 for more information.
type NullBytea []byte //nolint:recvcheck

// Value implements the driver.Valuer interface. It returns nil when the
// underlying slice is nil or empty, ensuring that missing values are sent as
// SQL NULL.
func (nb NullBytea) Value() (driver.Value, error) {
	if len(nb) == 0 {
		return nil, nil //nolint:nilnil
	}

	// Encode the byte slice as a hex format string with \x prefix:
	result := make([]byte, 2+hex.EncodedLen(len(nb)))
	result[0] = '\\'
	result[1] = 'x'
	hex.Encode(result[2:], nb)
	return result, nil
}

// Scan implements the sql.Scanner interface.
func (nb *NullBytea) Scan(src interface{}) error {
	if src == nil {
		*nb = nil
		return nil
	}
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("nullBytea.Scan: got %T, expected []byte", src)
	}
	*nb = append((*nb)[0:0], b...)
	return nil
}
