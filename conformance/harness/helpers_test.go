package harness

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

func TestMain(m *testing.M) {
	Main(m)
}

const (
	// errorCancelledRemotely is the attempt error River records for a job
	// cancelled while running.
	errorCancelledRemotely = "JobCancelError: job cancelled remotely"

	// errorUnknownKind is the attempt error River records for a job whose
	// kind has no worker.
	errorUnknownKind = "job kind is not registered in the client's Workers bundle: "

	// maintenanceWait bounds waits on maintenance an implementation runs on
	// its own schedule: River Go elects every five seconds and schedules
	// jobs every five seconds.
	maintenanceWait = 45 * time.Second

	// workWait bounds waits on ordinary work.
	workWait = 15 * time.Second
)

// fastTuning are the maintenance intervals scenarios ask for, which only
// implementations that expose them apply.
var fastTuning = &protocol.Tuning{ElectIntervalMS: 20, RescuerIntervalMS: 20, SchedulerIntervalMS: 20} //nolint:gochecknoglobals // constant

// echo is an insertable job with a behavior.
func echo(message, behavior string) protocol.InsertJob {
	return protocol.InsertJob{Args: protocol.Args{Behavior: behavior, Message: message}}
}

// withOpts returns job with opts.
func withOpts(job protocol.InsertJob, opts protocol.InsertOpts) protocol.InsertJob {
	job.Opts = &opts
	return job
}

// withDuration returns job with a duration.
func withDuration(job protocol.InsertJob, duration time.Duration) protocol.InsertJob {
	job.DurationMS = duration.Milliseconds()
	return job
}

func listedIDs(jobs []protocol.Job) []int64 {
	result := make([]int64, len(jobs))
	for i, job := range jobs {
		result[i] = job.ID
	}
	return result
}

// listOne lists the job with id through adapter, or returns nil.
func listOne(t *testing.T, adapter *Adapter, id int64) *protocol.Job {
	t.Helper()

	jobs := adapter.List(t, protocol.ListParams{IDs: []int64{id}}).Jobs
	if len(jobs) == 0 {
		return nil
	}
	require.Len(t, jobs, 1)
	return &jobs[0]
}

// workOne starts adapter's client on the default queue, waits for the job to
// be finalized, stops the client, and returns the job.
func workOne(t *testing.T, env *Env, adapter *Adapter, clientID string, id int64) *protocol.Job {
	t.Helper()

	adapter.Start(t, protocol.StartParams{ClientID: clientID, MaxWorkers: 1})
	job := env.DB.WaitJob(t, id, workWait)
	adapter.Stop(t, protocol.StopParams{})
	return job
}

// requireWorkedOnceBy requires that job completed in one attempt by clientID.
func requireWorkedOnceBy(t *testing.T, job *protocol.Job, clientID string) {
	t.Helper()

	require.Equal(t, "completed", job.State, "job %d (%s)", job.ID, job.Kind)
	require.Equal(t, 1, job.Attempt, "job %d (%s)", job.ID, job.Kind)
	require.Equal(t, []string{clientID}, job.AttemptedBy, "job %d (%s)", job.ID, job.Kind)
	require.Empty(t, job.Errors, "job %d (%s)", job.ID, job.Kind)
}

// metadata encodes metadata for insert options.
func metadata(t *testing.T, value map[string]any) json.RawMessage {
	t.Helper()

	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return encoded
}

// periodicJobs returns the jobs a periodic job inserted.
func periodicJobs(t *testing.T, env *Env, periodicJobID string) []*protocol.Job {
	t.Helper()

	var periodic []*protocol.Job
	for _, job := range env.DB.Jobs(t, "kind = $1", protocol.KindEcho) {
		if job.Metadata["river:periodic_job_id"] == periodicJobID {
			periodic = append(periodic, job)
		}
	}
	return periodic
}

// waitPeriodicJobs waits for count jobs a periodic job inserted.
func waitPeriodicJobs(t *testing.T, env *Env, periodicJobID string, count int) []*protocol.Job {
	t.Helper()

	var periodic []*protocol.Job
	WaitFor(t, "periodic jobs", maintenanceWait, func() bool {
		periodic = periodicJobs(t, env, periodicJobID)
		return len(periodic) >= count
	})
	require.Len(t, periodic, count)
	return periodic
}

// waitUntilRescuable waits until a running attempt is older than the rescue
// horizon, so the next rescuer run must rescue it.
func waitUntilRescuable(t *testing.T, job *protocol.Job, rescueAfter time.Duration) {
	t.Helper()

	require.NotNil(t, job.AttemptedAt)
	time.Sleep(time.Until(job.AttemptedAt.Add(rescueAfter + 100*time.Millisecond)))
}

var allStates = []string{"available", "cancelled", "completed", "discarded", "pending", "retryable", "running", "scheduled"} //nolint:gochecknoglobals // constant
