package jsonutil

import (
	"encoding/json"
	"reflect"
)

// Equal compares two JSON byte slices for semantic equality by parsing them into
// maps and doing a deep comparison. This handles cases where the JSON is
// equivalent but formatted differently (whitespace, field order, etc).
func Equal(a, b []byte) bool {
	var m1, m2 map[string]any
	if err := json.Unmarshal(a, &m1); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &m2); err != nil {
		return false
	}
	return reflect.DeepEqual(m1, m2)
}
