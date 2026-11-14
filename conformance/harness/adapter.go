package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/conformance/protocol"
)

const (
	// adapterExitTimeout bounds how long an adapter may take to exit after
	// its stdin closes.
	adapterExitTimeout = 30 * time.Second

	// adapterRequestTimeout bounds every request, so a wedged adapter fails
	// its scenario instead of hanging the run.
	adapterRequestTimeout = 2 * time.Minute
)

// Adapter is a running adapter process: one implementation connected to the
// scenario's database. Its methods are the contract's, typed. Each requires
// success and fails the test otherwise; Call returns errors instead, for
// requests that are meant to fail or that run off the test goroutine.
type Adapter struct {
	// ApplicationName is the Postgres application_name of the adapter's
	// connections, unique to the process.
	ApplicationName string

	// Implementation is the implementation behind the adapter.
	Implementation *Implementation

	// Label names the adapter in failure messages.
	Label string

	cmd     *exec.Cmd
	exited  chan struct{}
	killed  atomic.Bool
	lines   chan []byte
	mu      sync.Mutex
	nextID  int64
	stderr  *lockedBuffer
	stdin   io.WriteCloser
	waitErr error
}

// startAdapter starts implementation's adapter against database, through
// databaseURL if it's set.
func startAdapter(t *testing.T, implementation *Implementation, database *Database, databaseURL, label string) *Adapter {
	t.Helper()

	command, err := implementation.command()
	require.NoError(t, err, "error building the %s adapter", implementation.Name)

	applicationName := fmt.Sprintf("river-conformance-%s-%d", implementation.Name, applicationNameSequence.Add(1))

	// Not t.Context(): it's cancelled before cleanups run, and the adapter
	// should get a chance to exit gracefully first.
	cmd := exec.CommandContext(context.Background(), command[0], command[1:]...) //nolint:gosec // the harness's own adapter commands
	cmd.Dir = implementation.dir
	cmd.Env = append(os.Environ(),
		"RIVER_CONFORMANCE_APPLICATION_NAME="+applicationName,
		"RIVER_CONFORMANCE_DATABASE_URL="+cmpOr(databaseURL, database.adapterURL),
		"RIVER_CONFORMANCE_DRIVER="+database.Driver,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	require.NoError(t, cmd.Start(), "error starting the %s adapter", implementation.Name)

	adapter := &Adapter{
		ApplicationName: applicationName,
		Implementation:  implementation,
		Label:           label,
		cmd:             cmd,
		exited:          make(chan struct{}),
		lines:           make(chan []byte),
		stderr:          stderr,
		stdin:           stdin,
	}
	go adapter.readLines(stdout)
	go func() {
		adapter.waitErr = cmd.Wait()
		close(adapter.exited)
	}()
	t.Cleanup(func() { adapter.close(t) })

	return adapter
}

var applicationNameSequence atomic.Int64 //nolint:gochecknoglobals // unique names across the test process

func (a *Adapter) readLines(stdout io.Reader) {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		a.lines <- bytes.Clone(scanner.Bytes())
	}
	close(a.lines)
}

// close shuts the adapter down when its test ends. An adapter that doesn't
// exit once its stdin closes is killed and fails the test.
func (a *Adapter) close(t *testing.T) {
	t.Helper()

	_ = a.stdin.Close()
	defer func() {
		if t.Failed() && a.stderr.String() != "" {
			t.Logf("%s stderr:\n%s", a.Label, a.stderr.String())
		}
	}()
	select {
	case <-a.exited:
		if a.waitErr != nil && !a.killed.Load() {
			t.Errorf("%s exited with an error: %v\nstderr:\n%s", a.Label, a.waitErr, a.stderr.String())
		}
	case <-time.After(adapterExitTimeout):
		_ = a.cmd.Process.Kill()
		<-a.exited
		t.Errorf("%s didn't exit within %s of its stdin closing\nstderr:\n%s", a.Label, adapterExitTimeout, a.stderr.String())
	}
}

// Kill kills the adapter's process, as a crash would, and waits for it to
// exit.
func (a *Adapter) Kill(t *testing.T) {
	t.Helper()

	a.killed.Store(true)
	require.NoError(t, a.cmd.Process.Kill())
	<-a.exited
}

// Call sends a request and decodes its result into result, which may be nil.
// A failed request returns a *protocol.Error. It's safe to call off the test
// goroutine; requests to one adapter are serialized.
func (a *Adapter) Call(method string, params, result any) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.nextID++
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("error encoding %s params: %w", method, err)
	}
	request, err := json.Marshal(&protocol.Request{ID: a.nextID, JSONRPC: "2.0", Method: method, Params: encodedParams})
	if err != nil {
		return fmt.Errorf("error encoding %s request: %w", method, err)
	}
	if _, err := a.stdin.Write(append(request, '\n')); err != nil {
		return fmt.Errorf("error writing %s request to %s: %w\nstderr:\n%s", method, a.Label, err, a.stderr.String())
	}

	var line []byte
	select {
	case received, ok := <-a.lines:
		if !ok {
			return fmt.Errorf("%s exited during %s\nstderr:\n%s", a.Label, method, a.stderr.String())
		}
		line = received
	case <-time.After(adapterRequestTimeout):
		return fmt.Errorf("%s didn't answer %s within %s\nstderr:\n%s", a.Label, method, adapterRequestTimeout, a.stderr.String())
	}

	var response protocol.Response
	if err := json.Unmarshal(line, &response); err != nil {
		return fmt.Errorf("error decoding %s response from %s: %w: %s", method, a.Label, err, line)
	}
	if response.ID != a.nextID {
		return fmt.Errorf("%s answered %s with ID %d, expected %d", a.Label, method, response.ID, a.nextID)
	}
	if response.Error != nil {
		return response.Error
	}
	if result != nil {
		if err := json.Unmarshal(response.Result, result); err != nil {
			return fmt.Errorf("error decoding %s result from %s: %w: %s", method, a.Label, err, response.Result)
		}
	}
	return nil
}

// RequireErrorCode requires err to be a protocol error with code.
func RequireErrorCode(t *testing.T, err error, code int) {
	t.Helper()

	var protocolErr *protocol.Error
	require.ErrorAs(t, err, &protocolErr)
	require.Equal(t, code, protocolErr.Code, "error: %s", protocolErr.Message)
}

func (a *Adapter) mustCall(t *testing.T, method string, params, result any) {
	t.Helper()

	require.NoError(t, a.Call(method, params, result), "%s %s", a.Label, method)
}

// Cancel cancels a job.
func (a *Adapter) Cancel(t *testing.T, params protocol.JobParams) *protocol.Job {
	t.Helper()

	var job protocol.Job
	a.mustCall(t, protocol.MethodCancel, &params, &job)
	return &job
}

// Handshake identifies the adapter.
func (a *Adapter) Handshake(t *testing.T) *protocol.HandshakeResult {
	t.Helper()

	var result protocol.HandshakeResult
	a.mustCall(t, protocol.MethodHandshake, struct{}{}, &result)
	return &result
}

// Insert inserts a batch of jobs and returns the results in input order.
func (a *Adapter) Insert(t *testing.T, params protocol.InsertParams) []protocol.JobInsertResult {
	t.Helper()

	var result protocol.InsertResult
	a.mustCall(t, protocol.MethodInsert, &params, &result)
	require.Len(t, result.Results, len(params.Jobs), "%s insert results", a.Label)
	return result.Results
}

// InsertJob inserts one job.
func (a *Adapter) InsertJob(t *testing.T, job protocol.InsertJob) *protocol.Job {
	t.Helper()

	return &a.Insert(t, protocol.InsertParams{Jobs: []protocol.InsertJob{job}})[0].Job
}

// List lists jobs.
func (a *Adapter) List(t *testing.T, params protocol.ListParams) *protocol.ListResult {
	t.Helper()

	var result protocol.ListResult
	a.mustCall(t, protocol.MethodList, &params, &result)
	return &result
}

// Migrate migrates and returns the versions it applied.
func (a *Adapter) Migrate(t *testing.T, params protocol.MigrateParams) []int {
	t.Helper()

	var result protocol.MigrateResult
	a.mustCall(t, protocol.MethodMigrate, &params, &result)
	return result.Versions
}

// Queue pauses, resumes, or updates a queue.
func (a *Adapter) Queue(t *testing.T, params protocol.QueueParams) {
	t.Helper()

	a.mustCall(t, protocol.MethodQueue, &params, nil)
}

// Release releases a barrier.
func (a *Adapter) Release(t *testing.T, name string) {
	t.Helper()

	a.mustCall(t, protocol.MethodRelease, &protocol.ReleaseParams{Name: name}, nil)
}

// RequestResign asks the current leader to resign.
func (a *Adapter) RequestResign(t *testing.T, params protocol.RequestResignParams) {
	t.Helper()

	a.mustCall(t, protocol.MethodRequestResign, &params, nil)
}

// Retry retries a job.
func (a *Adapter) Retry(t *testing.T, params protocol.JobParams) *protocol.Job {
	t.Helper()

	var job protocol.Job
	a.mustCall(t, protocol.MethodRetry, &params, &job)
	return &job
}

// Start starts the adapter's worker client.
func (a *Adapter) Start(t *testing.T, params protocol.StartParams) {
	t.Helper()

	a.mustCall(t, protocol.MethodStart, &params, nil)
}

// Stats returns what the running client observed.
func (a *Adapter) Stats(t *testing.T) *protocol.StatsResult {
	t.Helper()

	var result protocol.StatsResult
	a.mustCall(t, protocol.MethodStats, struct{}{}, &result)
	return &result
}

// Stop stops the running client.
func (a *Adapter) Stop(t *testing.T, params protocol.StopParams) {
	t.Helper()

	a.mustCall(t, protocol.MethodStop, &params, nil)
}

// TxBegin opens a named transaction.
func (a *Adapter) TxBegin(t *testing.T, tx string) {
	t.Helper()

	a.mustCall(t, protocol.MethodTxBegin, &protocol.TxParams{Tx: tx}, nil)
}

// TxEnd commits or rolls back a named transaction.
func (a *Adapter) TxEnd(t *testing.T, tx string, commit bool) {
	t.Helper()

	a.mustCall(t, protocol.MethodTxEnd, &protocol.TxEndParams{Commit: commit, Tx: tx}, nil)
}

// WaitStats polls the running client's stats until done returns true.
func (a *Adapter) WaitStats(t *testing.T, description string, done func(stats *protocol.StatsResult) bool) *protocol.StatsResult {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for {
		stats := a.Stats(t)
		if done(stats) {
			return stats
		}
		if time.Now().After(deadline) {
			require.FailNowf(t, "timed out", "waited for %s stats: %s; last stats: %+v", a.Label, description, stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// CountEvents counts events of kind.
func CountEvents(stats *protocol.StatsResult, kind string) int {
	count := 0
	for _, event := range stats.Events {
		if event == kind {
			count++
		}
	}
	return count
}

// lockedBuffer is a buffer safe for concurrent writes and reads, holding at
// most the last 64 KiB written.
type lockedBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	const limit = 64 * 1024
	n, err := b.buf.Write(p)
	if b.buf.Len() > limit {
		b.buf.Next(b.buf.Len() - limit)
	}
	return n, err
}

// WaitFor polls condition until it returns true, failing the test after
// timeout.
func WaitFor(t *testing.T, description string, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			require.FailNowf(t, "timed out", "waited %s for %s", timeout, description)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var errUnknownImplementation = errors.New("unknown implementation")
