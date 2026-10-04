//go:build riverconformance

package harness_test

import (
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Kinds the adapters' `worker_kinds` start parameter registers the built-in
// worker under. The renamed kind keeps the echo kind as an alias.
const (
	echoKind    = "conformance_echo"
	peerKind    = "conformance_echo_peer"
	renamedKind = "conformance_echo_renamed"
)

// insertJobOfKind inserts a built-in job of kind through actor. The raw
// insertion doesn't check the kind against the actor's running client,
// which may not know it.
func insertJobOfKind(t *testing.T, actor *adapter, kind string, params map[string]any) normalizedJob {
	t.Helper()

	request := map[string]any{"kind": kind}
	maps.Copy(request, params)
	var job normalizedJob
	actor.call(t, "raw_insert_no_notify", request, &job)
	require.Equal(t, kind, job.Kind)
	return job
}

// requireWorkedOnceBy requires that job completed in a single attempt made
// by clientID, without errors, and kept kind.
func requireWorkedOnceBy(t *testing.T, job normalizedJob, kind, clientID string) {
	t.Helper()

	require.Equal(t, "completed", job.State, "job %d (%s)", job.ID, kind)
	require.Equal(t, 1, job.Attempt, "job %d (%s)", job.ID, kind)
	require.Equal(t, []string{clientID}, job.AttemptedBy, "job %d (%s)", job.ID, kind)
	require.Empty(t, job.Errors, "job %d (%s)", job.ID, kind)
	require.Equal(t, kind, job.Kind, "job %d", job.ID)
}

// requireUnclaimed requires that the job with id is still available and that
// no client has used one of its attempts.
func requireUnclaimed(t *testing.T, observer *adapter, id int64, kind string) {
	t.Helper()

	var job normalizedJob
	observer.call(t, "get", map[string]any{"id": id}, &job)
	require.Equal(t, "available", job.State, "job %d (%s)", id, kind)
	require.Zero(t, job.Attempt, "job %d (%s)", id, kind)
	require.Empty(t, job.AttemptedBy, "job %d (%s)", id, kind)
	require.Empty(t, job.Errors, "job %d (%s)", id, kind)
}

// verifyKindAliasRename checks a safe kind rename across implementations,
// as Go's `JobArgsWithKindAliases` supports it: one implementation inserts
// jobs under the old kind and the new one, and the other works both with a
// worker registered under the new kind that keeps the old one as an alias.
// It does so first with an ordinary client and then with one that fetches
// only known kinds, whose claim filter must include the alias, while a job
// of a kind it doesn't know stays untouched.
func verifyKindAliasRename(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	for _, pair := range []struct {
		inserter *adapter
		worker   *adapter
	}{
		{inserter: goAdapter, worker: candidateAdapter},
		{inserter: candidateAdapter, worker: goAdapter},
	} {
		for _, fetchOnlyKnownKinds := range []bool{false, true} {
			pair.inserter.call(t, "reset", map[string]any{}, nil)
			description := fmt.Sprintf("%s renamed worker (fetch_only_known_kinds %t)", pair.worker.name, fetchOnlyKnownKinds)
			oldKindJob := insertJobOfKind(t, pair.inserter, echoKind, map[string]any{"message": "kind alias old kind"})
			newKindJob := insertJobOfKind(t, pair.inserter, renamedKind, map[string]any{"message": "kind alias new kind"})
			var unknownJob normalizedJob
			if fetchOnlyKnownKinds {
				unknownJob = insertJobOfKind(t, pair.inserter, peerKind, map[string]any{"message": "kind alias unknown kind"})
			}

			clientID := pair.worker.name + "-renamed-worker"
			pair.worker.call(t, "start", map[string]any{
				"client_id": clientID, "fetch_only_known_kinds": fetchOnlyKnownKinds, "max_workers": 2,
				"worker_kinds": []string{renamedKind},
			}, nil)
			for _, job := range []normalizedJob{oldKindJob, newKindJob} {
				worked := waitForJobStateWithin(t, pair.worker, job.ID, []string{"completed", "discarded", "retryable"}, 30*time.Second)
				require.Equal(t, "completed", worked.State, "%s: %+v", description, worked)
				requireWorkedOnceBy(t, worked, job.Kind, clientID)
			}
			if fetchOnlyKnownKinds {
				requireUnclaimed(t, pair.inserter, unknownJob.ID, peerKind)
			}
			pair.worker.call(t, "stop", map[string]any{}, nil)
		}
	}
}

// verifyHeterogeneousFleet checks clients that share a queue while each
// knows only its own kind, the deployment Go's `FetchOnlyKnownKinds`
// exists for. In each direction, the first client starts alone with jobs of
// the other's kind ahead of its own in claim order, works its own, and must
// leave the others available with no attempt used. The second then starts
// and works the rest, and jobs of both kinds inserted while both run are
// each worked by the client that knows their kind.
func verifyHeterogeneousFleet(t *testing.T, goAdapter, candidateAdapter *adapter) {
	t.Helper()

	const jobsPerKind = 3
	for _, pair := range []struct {
		first  *adapter
		second *adapter
	}{
		{first: candidateAdapter, second: goAdapter},
		{first: goAdapter, second: candidateAdapter},
	} {
		pair.first.call(t, "reset", map[string]any{}, nil)
		firstKind, secondKind := peerKind, echoKind
		clientIDs := map[string]string{
			firstKind:  pair.first.name + "-fleet-" + firstKind,
			secondKind: pair.second.name + "-fleet-" + secondKind,
		}
		jobs := map[string][]normalizedJob{}
		insert := func(actor *adapter, kind string) {
			for index := range jobsPerKind {
				jobs[kind] = append(jobs[kind], insertJobOfKind(t, actor, kind, map[string]any{"message": fmt.Sprintf("fleet %s %d", kind, index)}))
			}
		}
		// Lower IDs claim first, so a client that ignores the kind filter
		// would claim the other kind's jobs before its own.
		insert(pair.first, secondKind)
		insert(pair.second, firstKind)

		pair.first.call(t, "start", map[string]any{
			"client_id": clientIDs[firstKind], "fetch_only_known_kinds": true, "max_workers": 1,
			"worker_kinds": []string{firstKind},
		}, nil)
		for _, job := range jobs[firstKind] {
			worked := waitForJobStateWithin(t, pair.first, job.ID, []string{"completed", "discarded", "retryable"}, 30*time.Second)
			requireWorkedOnceBy(t, worked, firstKind, clientIDs[firstKind])
		}
		for _, job := range jobs[secondKind] {
			requireUnclaimed(t, pair.second, job.ID, secondKind)
		}

		pair.second.call(t, "start", map[string]any{
			"client_id": clientIDs[secondKind], "fetch_only_known_kinds": true, "max_workers": 1,
			"worker_kinds": []string{secondKind},
		}, nil)
		insert(pair.first, firstKind)
		insert(pair.second, secondKind)
		for kind, kindJobs := range jobs {
			for _, job := range kindJobs {
				worked := waitForJobStateWithin(t, pair.second, job.ID, []string{"completed", "discarded", "retryable"}, 30*time.Second)
				requireWorkedOnceBy(t, worked, kind, clientIDs[kind])
			}
		}
		pair.first.call(t, "stop", map[string]any{}, nil)
		pair.second.call(t, "stop", map[string]any{}, nil)
	}
}

// rescueOutcome is what a rescuer did to one abandoned job, without the
// values that differ between runs.
type rescueOutcome struct {
	AttemptedBy []string
	Attempt     int
	Errors      []rescueOutcomeError
	Finalized   bool
	Kind        string
	MaxAttempts int
	RescueCount any
	// RetryDelay is the delay from the rescue to the job's new scheduled_at,
	// to the second, or zero when the rescue left scheduled_at unchanged.
	RetryDelay time.Duration
	State      string
}

type rescueOutcomeError struct {
	Attempt int
	Error   string
	Trace   string
}

// verifyRescuerUnknownKind checks how a leader that knows only some kinds
// rescues jobs abandoned by a client that knew others, as happens when
// implementations with disjoint workers share a database. A Go process
// that works both kinds dies holding one job of each, and each
// implementation in turn leads with a worker for one kind only. Like Go's
// rescuer, it must retry the job of the kind it knows on its retry policy
// and discard the one it doesn't, and both must end up as Go leaves them.
func verifyRescuerUnknownKind(t *testing.T, goAdapter, candidateAdapter *adapter, newCrasher func(t *testing.T, name string) *adapter) {
	t.Helper()

	const (
		queue       = "rescuer_kinds"
		rescueAfter = time.Second
		retryDelay  = time.Minute
	)
	outcomes := make(map[string]map[string]rescueOutcome)
	for _, leader := range []*adapter{goAdapter, candidateAdapter} {
		goAdapter.call(t, "reset", map[string]any{}, nil)
		inserted := make(map[string]normalizedJob)
		for _, kind := range []string{echoKind, peerKind} {
			// Go runs no client here, so it inserts either kind.
			var job normalizedJob
			goAdapter.call(t, "insert", map[string]any{
				"behavior": "sleep", "duration_ms": 60_000, "message": "rescuer kinds " + kind,
				"opts": map[string]any{"max_attempts": 3, "queue": queue},
			}, &job)
			if kind != echoKind {
				goAdapter.call(t, "raw_set_kind", map[string]any{"id": job.ID, "kind": kind}, &job)
			}
			inserted[kind] = job
		}

		const crasherID = "rescuer-kinds-crasher"
		crasher := newCrasher(t, "go-rescuer-kinds-crasher-"+leader.name)
		crasher.call(t, "start", map[string]any{
			"client_id": crasherID, "leader_election_disabled": true, "max_workers": 2, "queue": queue,
			"worker_kinds": []string{echoKind, peerKind},
		}, nil)
		running := make(map[string]normalizedJob)
		for kind, job := range inserted {
			running[kind] = waitForJobStateWithin(t, goAdapter, job.ID, []string{"running"}, 30*time.Second)
		}
		crasher.kill(t)
		for _, job := range running {
			waitUntilRescuable(t, job, rescueAfter)
		}

		// The leader knows only the peer kind, so the echo kind is unknown
		// to it.
		leader.startWithTuning(t, map[string]any{
			"client_id": "rescuer-kinds-leader", "job_timeout_ms": rescueAfter.Milliseconds(), "max_workers": 1,
			"rescue_after_ms": rescueAfter.Milliseconds(), "retry_delay_ms": retryDelay.Milliseconds(),
			"worker_kinds": []string{peerKind},
		}, map[string]any{"elect_interval_ms": 20, "rescuer_interval_ms": 20, "scheduler_interval_ms": 20})
		rescued := map[string]normalizedJob{
			echoKind: waitForJobStateWithin(t, goAdapter, inserted[echoKind].ID, []string{"discarded"}, 30*time.Second),
			peerKind: waitForJobStateWithin(t, goAdapter, inserted[peerKind].ID, []string{"retryable"}, 30*time.Second),
		}
		leader.call(t, "stop", map[string]any{}, nil)

		outcomes[leader.name] = make(map[string]rescueOutcome)
		for kind, job := range rescued {
			outcome := rescueOutcome{
				AttemptedBy: job.AttemptedBy,
				Attempt:     job.Attempt,
				Finalized:   job.FinalizedAt != nil,
				Kind:        job.Kind,
				MaxAttempts: job.MaxAttempts,
				RescueCount: job.Metadata["river:rescue_count"],
				State:       job.State,
			}
			for _, attemptError := range job.Errors {
				outcome.Errors = append(outcome.Errors, rescueOutcomeError{
					Attempt: attemptError.Attempt, Error: attemptError.Error, Trace: attemptError.Trace,
				})
			}
			require.Len(t, job.Errors, 1, "%s rescue of %s", leader.name, kind)
			if job.ScheduledAt != running[kind].ScheduledAt {
				outcome.RetryDelay = parseTime(t, job.ScheduledAt).Sub(parseTime(t, job.Errors[0].At)).Round(time.Second)
			}
			outcomes[leader.name][kind] = outcome
		}
	}

	reference := outcomes[goAdapter.name]
	require.Equal(t, "discarded", reference[echoKind].State)
	require.True(t, reference[echoKind].Finalized)
	require.Zero(t, reference[echoKind].RetryDelay)
	require.Equal(t, "retryable", reference[peerKind].State)
	require.False(t, reference[peerKind].Finalized)
	require.Equal(t, retryDelay, reference[peerKind].RetryDelay)
	require.Equal(t, reference, outcomes[candidateAdapter.name],
		"%s's rescuer and Go's left abandoned jobs differently", candidateAdapter.name)
}
