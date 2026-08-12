//go:build riverconformance

package harness_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

type scenarioTracker struct {
	completed map[string]bool
	owner     string
	t         *testing.T
}

func newScenarioTracker(t *testing.T, owner string) *scenarioTracker {
	t.Helper()
	tracker := &scenarioTracker{completed: make(map[string]bool), owner: owner, t: t}
	t.Cleanup(tracker.verify)
	return tracker
}

func (tracker *scenarioTracker) pass(names ...string) {
	tracker.t.Helper()
	for _, name := range names {
		binding, ok := scenarioRegistry[name]
		require.True(tracker.t, ok, "unregistered conformance scenario %q", name)
		require.Equal(tracker.t, tracker.owner, binding.owner, "scenario %q is owned by another test", name)
		require.False(tracker.t, tracker.completed[name], "conformance scenario %q completed more than once", name)
		tracker.completed[name] = true
	}
}

func (tracker *scenarioTracker) verify() {
	tracker.t.Helper()
	if tracker.t.Failed() {
		return
	}
	want := make([]string, 0)
	for name, binding := range scenarioRegistry {
		if binding.owner == tracker.owner {
			want = append(want, name)
		}
	}
	slices.Sort(want)
	got := make([]string, 0, len(tracker.completed))
	for name := range tracker.completed {
		got = append(got, name)
	}
	slices.Sort(got)
	require.Equal(tracker.t, want, got, "%s did not complete its exact registered scenario set", tracker.owner)
}
