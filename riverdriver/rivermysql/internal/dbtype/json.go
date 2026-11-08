// Package dbtype provides MySQL-specific SQL parameter types.
package dbtype

import (
	"bytes"
	"database/sql/driver"
	"fmt"
)

// JSON is a JSON document bound as text. go-sql-driver/mysql interpolates a
// []byte parameter as a binary string, which MySQL rejects as JSON. Binding
// text works with both prepared statements and interpolateParams=true, while
// retaining []byte's scanning and decoding behavior.
type JSON []byte //nolint:recvcheck // Scan must mutate a pointer; Value must also work on non-pointer parameters.

// Scan copies a document out of the driver's reusable result buffer.
func (j *JSON) Scan(value any) error {
	switch value := value.(type) {
	case nil:
		*j = nil
	case []byte:
		*j = bytes.Clone(value)
	case string:
		*j = []byte(value)
	default:
		return fmt.Errorf("cannot scan %T into JSON", value)
	}
	return nil
}

// Value preserves SQL NULL and otherwise binds the document as text.
func (j JSON) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil //nolint:nilnil // A nil driver.Value represents SQL NULL.
	}
	return string(j), nil
}
