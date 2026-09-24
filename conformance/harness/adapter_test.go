//go:build riverconformance

package harness_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// adapter is one running conformance adapter process. Requests are strictly
// sequential within a process: a request is written, then its response is
// read before the next request is sent.
type adapter struct {
	// applicationName is the PostgreSQL application_name the adapter uses,
	// when known. Harness-side observations such as lock waits use it.
	applicationName   string
	command           *exec.Cmd
	expectedExitError bool
	input             io.WriteCloser
	name              string
	nextID            int
	openHandles       map[string]bool
	output            *bufio.Scanner
	running           bool
	stderr            lockedBuffer
}

type adapterSpec struct {
	ApplicationName string   `json:"application_name"`
	Command         []string `json:"command"`
	Implementation  string   `json:"implementation"`
	ReleaseCommand  []string `json:"release_command"`
	RestartCommand  []string `json:"restart_command"`
	Version         string   `json:"version"`
}

type adapterHandshake struct {
	AdapterVersion        int            `json:"adapter_version"`
	Backend               string         `json:"backend"`
	Capabilities          []string       `json:"capabilities"`
	Implementation        string         `json:"implementation"`
	ImplementationVersion string         `json:"implementation_version"`
	Methods               []string       `json:"methods"`
	MigrationLines        map[string]int `json:"migration_lines"`
	Profile               string         `json:"profile"`
	ProtocolRevision      int            `json:"protocol_revision"`
}

type adapterProfile struct {
	Backend          string   `json:"backend"`
	Capabilities     []string `json:"capabilities"`
	Methods          []string `json:"methods"`
	Name             string   `json:"name"`
	ProtocolRevision int      `json:"protocol_revision"`
}

type lockedBuffer struct {
	buffer bytes.Buffer
	mu     sync.Mutex
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	return buffer.buffer.String()
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	return buffer.buffer.Write(data)
}

type rpcResponse struct {
	Error  *rpcError       `json:"error"`
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type normalizedJob struct {
	Args         map[string]any           `json:"args"`
	Attempt      int                      `json:"attempt"`
	AttemptedAt  *string                  `json:"attempted_at"`
	AttemptedBy  []string                 `json:"attempted_by"`
	CreatedAt    string                   `json:"created_at"`
	Errors       []normalizedAttemptError `json:"errors"`
	FinalizedAt  *string                  `json:"finalized_at"`
	ID           int64                    `json:"id"`
	Kind         string                   `json:"kind"`
	MaxAttempts  int                      `json:"max_attempts"`
	Metadata     map[string]any           `json:"metadata"`
	Priority     int                      `json:"priority"`
	Queue        string                   `json:"queue"`
	ScheduledAt  string                   `json:"scheduled_at"`
	State        string                   `json:"state"`
	Tags         []string                 `json:"tags"`
	UniqueKey    *string                  `json:"unique_key"`
	UniqueStates []string                 `json:"unique_states"`
}

type normalizedAttemptError struct {
	At      string `json:"at"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error"`
	Trace   string `json:"trace"`
}

type normalizedInsertResult struct {
	Job                      normalizedJob `json:"job"`
	UniqueSkippedAsDuplicate bool          `json:"unique_skipped_as_duplicate"`
}

type normalizedQueue struct {
	CreatedAt string         `json:"created_at"`
	Metadata  map[string]any `json:"metadata"`
	Name      string         `json:"name"`
	PausedAt  *string        `json:"paused_at"`
	UpdatedAt string         `json:"updated_at"`
}

// call performs a request that must succeed and decodes its result.
func (adapter *adapter) call(t *testing.T, method string, params any, result any) {
	t.Helper()

	response, err := adapter.roundTrip(method, params)
	require.NoErrorf(t, err, "%s adapter stderr: %s", adapter.name, adapter.stderr.String())
	if response.Error != nil {
		t.Fatalf("%s adapter %s failed (%d): %s\nstderr: %s", adapter.name, method, response.Error.Code, response.Error.Message, adapter.stderr.String())
	}
	if result != nil {
		require.NoError(t, decodeAdapterResult(response.Result, result))
	}
}

// callWithoutTest performs a serialized adapter call without invoking testing.T
// methods, so a deliberately blocking request can run in a helper goroutine.
func (adapter *adapter) callWithoutTest(method string, params any, result any) error {
	response, err := adapter.roundTrip(method, params)
	if err != nil {
		return err
	}
	if response.Error != nil {
		return fmt.Errorf("%s adapter %s failed (%d): %s", adapter.name, method, response.Error.Code, response.Error.Message)
	}
	if result != nil {
		if err := decodeAdapterResult(response.Result, result); err != nil {
			return err
		}
	}
	return nil
}

func (adapter *adapter) kill(t *testing.T) {
	t.Helper()

	adapter.expectedExitError = true
	require.NoError(t, adapter.command.Process.Kill())
	adapter.running = false
	adapter.openHandles = nil
}

func (adapter *adapter) callError(t *testing.T, method string, params any) string {
	t.Helper()

	response := adapter.callResponse(t, method, params)
	require.NotNil(t, response.Error, "%s adapter %s unexpectedly succeeded", adapter.name, method)
	return response.Error.Message
}

func (adapter *adapter) callResponse(t *testing.T, method string, params any) rpcResponse {
	t.Helper()

	response, err := adapter.roundTrip(method, params)
	require.NoErrorf(t, err, "%s adapter stderr: %s", adapter.name, adapter.stderr.String())
	return response
}

// recover returns an adapter to a state where the next scenario can reset
// the database after an earlier scenario failed midway: it stops a running
// client and rolls back transactions the harness opened. Errors are ignored
// because the process may already be unusable, in which case the following
// scenario reports the failure.
func (adapter *adapter) recover() {
	if adapter.expectedExitError {
		return
	}
	if adapter.running {
		_, _ = adapter.roundTrip("stop", map[string]any{"cancel": true})
	}
	handles := mapKeys(adapter.openHandles)
	slices.Sort(handles)
	for _, handle := range handles {
		_, _ = adapter.roundTrip("tx_rollback", map[string]any{"handle": handle})
	}
}

// roundTrip writes one request and reads its response. It also tracks which
// runtime client and transaction handles the adapter holds so recover can
// release them.
func (adapter *adapter) roundTrip(method string, params any) (rpcResponse, error) {
	adapter.nextID++
	requestID := adapter.nextID
	encoded, err := json.Marshal(map[string]any{
		"id":      requestID,
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return rpcResponse{}, err
	}
	if _, err := adapter.input.Write(append(encoded, '\n')); err != nil {
		return rpcResponse{}, fmt.Errorf("write %s adapter request: %w", adapter.name, err)
	}
	if !adapter.output.Scan() {
		return rpcResponse{}, fmt.Errorf("%s adapter stopped: %s", adapter.name, adapter.stderr.String())
	}

	var response rpcResponse
	if err := json.Unmarshal(adapter.output.Bytes(), &response); err != nil {
		return rpcResponse{}, fmt.Errorf("decode %s adapter response: %w", adapter.name, err)
	}
	if response.ID != requestID {
		return rpcResponse{}, fmt.Errorf("%s adapter response ID %d, expected %d", adapter.name, response.ID, requestID)
	}
	// Commit and rollback consume a handle even when they report an error.
	if response.Error == nil || method == "tx_commit" || method == "tx_rollback" {
		adapter.trackState(method, params)
	}
	return response, nil
}

func (adapter *adapter) trackState(method string, params any) {
	switch method {
	case "start":
		adapter.running = true
	case "stop":
		adapter.running = false
	case "tx_begin", "tx_commit", "tx_rollback":
		encoded, err := json.Marshal(params)
		if err != nil {
			return
		}
		var decoded struct {
			Handle string `json:"handle"`
		}
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.Handle == "" {
			return
		}
		if adapter.openHandles == nil {
			adapter.openHandles = make(map[string]bool)
		}
		if method == "tx_begin" {
			adapter.openHandles[decoded.Handle] = true
		} else {
			delete(adapter.openHandles, decoded.Handle)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func conformanceCandidateSpec(t *testing.T, root string, release bool) adapterSpec {
	t.Helper()

	encoded := os.Getenv("RIVER_CONFORMANCE_CANDIDATE")
	descriptorPath := os.Getenv("RIVER_CONFORMANCE_CANDIDATE_FILE")
	require.False(t, encoded != "" && descriptorPath != "",
		"set only one of RIVER_CONFORMANCE_CANDIDATE or RIVER_CONFORMANCE_CANDIDATE_FILE")

	var descriptor []byte
	if encoded != "" {
		descriptor = []byte(encoded)
	} else {
		if descriptorPath == "" {
			descriptorPath = "conformance/adapter/candidates/rust.json"
		}
		if !filepath.IsAbs(descriptorPath) {
			descriptorPath = filepath.Join(root, descriptorPath)
		}
		var err error
		//nolint:gosec // The caller explicitly selects a local candidate descriptor.
		descriptor, err = os.ReadFile(descriptorPath)
		require.NoError(t, err)
	}

	var spec adapterSpec
	require.NoError(t, json.Unmarshal(descriptor, &spec))
	require.NotEmpty(t, spec.ApplicationName)
	require.NotEmpty(t, spec.Command)
	require.NotEmpty(t, spec.Implementation)
	if release && len(spec.ReleaseCommand) > 0 {
		spec.Command = slices.Clone(spec.ReleaseCommand)
	}
	if len(spec.RestartCommand) == 0 {
		spec.RestartCommand = slices.Clone(spec.Command)
	}
	return spec
}

// startCandidateAdapter starts a candidate adapter from its descriptor on
// PostgreSQL and records its application name for harness observations.
func startCandidateAdapter(t *testing.T, root, databaseURL, name string, spec adapterSpec, command []string) *adapter {
	t.Helper()

	started := startAdapterCommand(t, root, databaseURL, name, command)
	started.applicationName = spec.ApplicationName
	return started
}

// startReferenceAdapter starts the Go reference adapter on PostgreSQL.
func startReferenceAdapter(t *testing.T, root, databaseURL, name string) *adapter {
	t.Helper()

	started := startAdapter(t, root, databaseURL, name, "go", "run", "./internal/cmd/riverconformanceadapter")
	started.applicationName = referenceApplicationName
	return started
}

func startAdapterCommand(t *testing.T, root, databaseURL, name string, command []string) *adapter {
	t.Helper()

	return startAdapterCommandForBackend(t, root, databaseURL, "postgres", name, command)
}

func startAdapterCommandForBackend(
	t *testing.T,
	root, databaseURL, databaseKind, name string,
	command []string,
) *adapter {
	t.Helper()

	require.NotEmpty(t, command)
	return startAdapterForProfile(t, root, databaseURL, databaseKind, "", name, command[0], command[1:]...)
}

func startAdapterCommandForProfile(
	t *testing.T,
	root, databaseURL, databaseKind, profile, name string,
	command []string,
) *adapter {
	t.Helper()

	require.NotEmpty(t, command)
	return startAdapterForProfile(
		t, root, databaseURL, databaseKind, profile, name, command[0], command[1:]...,
	)
}

func startAdapter(t *testing.T, root, databaseURL, name, executable string, args ...string) *adapter {
	t.Helper()

	return startAdapterForBackend(t, root, databaseURL, "postgres", name, executable, args...)
}

func startAdapterForBackend(
	t *testing.T,
	root, databaseURL, databaseKind, name, executable string,
	args ...string,
) *adapter {
	t.Helper()
	return startAdapterForProfile(t, root, databaseURL, databaseKind, "", name, executable, args...)
}

func startAdapterForProfile(
	t *testing.T,
	root, databaseURL, databaseKind, profile, name, executable string,
	args ...string,
) *adapter {
	t.Helper()

	// Keep cancellation after the adapter's graceful cleanup (LIFO), rather
	// than using t.Context(), which is cancelled before cleanup begins.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, executable, args...)
	command.Dir = root
	command.Env = append(
		os.Environ(),
		"RIVER_CONFORMANCE_DATABASE_KIND="+databaseKind,
		"RIVER_CONFORMANCE_DATABASE_URL="+databaseURL,
	)
	if profile != "" {
		command.Env = append(command.Env, "RIVER_CONFORMANCE_PROFILE="+profile)
	}
	input, err := command.StdinPipe()
	require.NoError(t, err)
	output, err := command.StdoutPipe()
	require.NoError(t, err)
	adapter := &adapter{
		command: command,
		input:   input,
		name:    name,
		output:  bufio.NewScanner(output),
	}
	adapter.output.Buffer(make([]byte, 64*1024), 4*1024*1024)
	command.Stderr = &adapter.stderr
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		if err := adapter.input.Close(); err != nil && !adapter.expectedExitError {
			t.Errorf("%s adapter stdin close: %v", name, err)
		}
		if err := adapter.command.Wait(); err != nil && !adapter.expectedExitError {
			t.Errorf("%s adapter exit: %v\nstderr: %s", name, err, adapter.stderr.String())
		}
	})
	return adapter
}

func Example_protocolRequest() {
	fmt.Println(`{"id":1,"jsonrpc":"2.0","method":"handshake","params":{}}`)
	// Output: {"id":1,"jsonrpc":"2.0","method":"handshake","params":{}}
}
