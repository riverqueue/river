//go:build riverconformance

package harness_test

import (
	"flag"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// scenarioTracker ties registered scenario IDs to the subtests that execute
// them, so one ID can never be credited by another scenario's assertions.
type scenarioTracker struct {
	adapters  []*adapter
	completed map[string]bool
	owner     string
	t         *testing.T
}

func newScenarioTracker(t *testing.T, owner string) *scenarioTracker {
	t.Helper()

	conformanceTestsStarted.Add(1)
	tracker := &scenarioTracker{completed: make(map[string]bool), owner: owner, t: t}
	t.Cleanup(tracker.verify)
	return tracker
}

// attach registers adapters that must be returned to a clean state after a
// failed scenario so later scenarios in the same owner still run
// independently.
func (tracker *scenarioTracker) attach(adapters ...*adapter) {
	tracker.adapters = append(tracker.adapters, adapters...)
}

// pass records scenarios verified inline by an owner whose whole body is one
// scenario. Owners with several scenarios use a subtest per scenario and
// record instead.
func (tracker *scenarioTracker) pass(names ...string) {
	tracker.t.Helper()

	for _, name := range names {
		tracker.requireOwned(name)
		tracker.completed[name] = true
	}
}

func (tracker *scenarioTracker) requireOwned(name string) {
	tracker.t.Helper()

	binding, ok := scenarioRegistry[name]
	require.True(tracker.t, ok, "unregistered conformance scenario %q", name)
	require.Equal(tracker.t, tracker.owner, binding.owner, "scenario %q is owned by another test", name)
	require.False(tracker.t, tracker.completed[name], "conformance scenario %q completed more than once", name)
}

// record marks the calling scenario subtest as passed. Owners run each
// scenario with t.Run using the scenario ID as the subtest name and defer
// record as the subtest's first statement, so the ID is credited only when
// that subtest's own assertions completed without failing or skipping. A
// failed scenario returns the owner's adapters to a clean state so later
// scenarios still run independently.
func (tracker *scenarioTracker) record(t *testing.T) {
	t.Helper()

	name := path.Base(t.Name())
	switch {
	case t.Skipped():
		t.Errorf("conformance scenario %q skipped; scenarios must pass or fail", name)
	case t.Failed():
		for _, current := range tracker.adapters {
			current.recover()
		}
	default:
		tracker.requireOwned(name)
		tracker.completed[name] = true
	}
}

func (tracker *scenarioTracker) verify() {
	tracker.t.Helper()

	if tracker.t.Failed() || tracker.t.Skipped() {
		return
	}
	var missing []string
	for name, binding := range scenarioRegistry {
		if binding.owner == tracker.owner && !tracker.completed[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return
	}
	slices.Sort(missing)
	// A -run pattern that selects subtests can exclude scenarios; that is
	// only acceptable for local debugging.
	if runPattern := flag.Lookup("test.run").Value.String(); strings.Contains(runPattern, "/") && !conformanceRequired() {
		tracker.t.Logf("-run %q excluded registered scenarios, so %s is not a complete result: %v", runPattern, tracker.owner, missing)
		return
	}
	tracker.t.Errorf("%s did not run registered scenarios: %v", tracker.owner, missing)
}

// conformanceRequired reports whether a conformance run must not skip. CI sets
// RIVER_CONFORMANCE_REQUIRED=1 so a missing database URL or opt-in variable
// fails instead of passing with skipped tests.
func conformanceRequired() bool {
	return os.Getenv("RIVER_CONFORMANCE_REQUIRED") == "1"
}

// requireEnv returns a required environment variable. When it is unset the
// test is skipped for local runs and fails when RIVER_CONFORMANCE_REQUIRED=1.
func requireEnv(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	if value == "" {
		if conformanceRequired() {
			t.Fatalf("%s is required when RIVER_CONFORMANCE_REQUIRED=1", name)
		}
		t.Skipf("%s is required", name)
	}
	return value
}

// requireOptIn skips a long-running tier unless its variable is "1". A
// required run that selects the tier without enabling it fails instead.
func requireOptIn(t *testing.T, name string) {
	t.Helper()

	if os.Getenv(name) != "1" {
		if conformanceRequired() {
			t.Fatalf("%s=1 is required when RIVER_CONFORMANCE_REQUIRED=1 selects this tier", name)
		}
		t.Skipf("%s=1 is required", name)
	}
}
