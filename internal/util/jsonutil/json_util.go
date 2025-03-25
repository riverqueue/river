package jsonutil

import (
	"encoding/json"
	"reflect"
)

// Equal compares two JSON byte slices for semantic equality by parsing them into
// maps and doing a deep comparison. This handles cases where the JSON is
// equivalent but formatted differently (whitespace, field order, etc).
func Equal(a, b []byte) bool {
	var unmarshaledA, unmarshaledB map[string]any
	if err := json.Unmarshal(a, &unmarshaledA); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &unmarshaledB); err != nil {
		return false
	}
	return reflect.DeepEqual(unmarshaledA, unmarshaledB)
}
