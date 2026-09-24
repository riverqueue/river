//go:build riverconformance

package harness_test

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	// `go test -run` exits successfully when a pattern matches nothing. A
	// required CI run must execute at least one conformance test.
	if code == 0 && conformanceRequired() && conformanceTestsStarted.Load() == 0 {
		fmt.Fprintln(os.Stderr, "RIVER_CONFORMANCE_REQUIRED=1: no conformance test ran; check the -run pattern")
		code = 1
	}
	os.Exit(code)
}
