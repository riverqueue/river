package harness_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// adapterExitTimeout bounds how long an adapter may take to exit after
	// the harness closes its stdin. Adapters stop a running client on the
	// way out, which the reference bounds at ten seconds.
	adapterExitTimeout = 30 * time.Second

	// adapterKillTimeout bounds how long a killed adapter may take to be
	// reaped, including adapterPipeCloseDelay.
	adapterKillTimeout = 15 * time.Second

	// adapterPipeCloseDelay bounds how long the harness waits for an exited
	// adapter's output pipes to close. A descendant process that inherited
	// them, such as the adapter under a wrapper command, would otherwise keep
	// the wait open indefinitely.
	adapterPipeCloseDelay = 5 * time.Second
)

// errAdapterExitTimeout reports an adapter that had to be killed because it
// didn't exit within its time bound.
var errAdapterExitTimeout = errors.New("adapter did not exit")

// adapterProcess is one running adapter child process and its protocol
// pipes. Its exit status is collected at most once, by whichever of kill or
// shutdown first waits for it.
type adapterProcess struct {
	command  *exec.Cmd
	exitErr  error
	exited   chan struct{}
	input    io.WriteCloser
	output   *bufio.Scanner
	stderr   lockedBuffer
	waitOnce sync.Once
}

// startAdapterProcess starts command with its stdin and stdout connected to
// the harness and its stderr captured.
func startAdapterProcess(command *exec.Cmd) (*adapterProcess, error) {
	input, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	output, err := command.StdoutPipe()
	if err != nil {
		return nil, err
	}
	process := &adapterProcess{
		command: command,
		exited:  make(chan struct{}),
		input:   input,
		output:  bufio.NewScanner(output),
	}
	process.output.Buffer(make([]byte, 64*1024), 4*1024*1024)
	command.Stderr = &process.stderr
	command.WaitDelay = adapterPipeCloseDelay
	if err := command.Start(); err != nil {
		return nil, err
	}
	return process, nil
}

// kill kills the process and waits up to timeout for it to be reaped.
func (process *adapterProcess) kill(timeout time.Duration) error {
	if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill adapter: %w", err)
	}
	if !process.waitForExit(timeout) {
		return fmt.Errorf("%w within %s of being killed", errAdapterExitTimeout, timeout)
	}
	return nil
}

// shutdown closes the process's stdin, which asks an adapter to exit, and
// waits up to exitTimeout for it to do so. An adapter still running after
// that is killed and reported with errAdapterExitTimeout, so one wedged
// adapter fails its test instead of hanging the whole run. Otherwise
// shutdown returns any error closing stdin joined with the exit error.
func (process *adapterProcess) shutdown(exitTimeout time.Duration) error {
	closeErr := process.input.Close()
	if !process.waitForExit(exitTimeout) {
		return errors.Join(
			fmt.Errorf("%w within %s of closing its stdin and was killed", errAdapterExitTimeout, exitTimeout),
			process.kill(adapterKillTimeout),
		)
	}
	return errors.Join(closeErr, process.exitErr)
}

// waitForExit waits up to timeout for the process to exit and its pipes to
// close, and reports whether it did. The exit status is recorded in exitErr.
func (process *adapterProcess) waitForExit(timeout time.Duration) bool {
	process.waitOnce.Do(func() {
		go func() {
			process.exitErr = process.command.Wait()
			close(process.exited)
		}()
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.exited:
		return true
	case <-timer.C:
		return false
	}
}

// lockedBuffer collects an adapter's stderr, which the process writes while
// the harness reads it for failure messages.
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

func TestAdapterProcess(t *testing.T) {
	t.Parallel()

	// start runs this test binary as a fake adapter with the given
	// behavior; see TestAdapterProcessFake.
	start := func(t *testing.T, behavior string) *adapterProcess {
		t.Helper()

		//nolint:gosec // Reruns this test binary with fixed arguments.
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestAdapterProcessFake$")
		command.Env = append(os.Environ(), "RIVER_CONFORMANCE_FAKE_ADAPTER="+behavior)
		process, err := startAdapterProcess(command)
		require.NoError(t, err)
		return process
	}

	t.Run("KillReapsProcess", func(t *testing.T) {
		t.Parallel()

		process := start(t, "ignore_eof")

		require.NoError(t, process.kill(adapterKillTimeout))
		require.NotNil(t, process.command.ProcessState)
		require.False(t, process.command.ProcessState.Success())
	})

	t.Run("ShutdownKillsWedgedAdapter", func(t *testing.T) {
		t.Parallel()

		process := start(t, "ignore_eof")

		err := process.shutdown(100 * time.Millisecond)
		require.ErrorIs(t, err, errAdapterExitTimeout)
		require.ErrorContains(t, err, "within 100ms of closing its stdin and was killed")
		require.NotNil(t, process.command.ProcessState, "shutdown must reap the killed adapter")
	})

	t.Run("ShutdownReportsExitError", func(t *testing.T) {
		t.Parallel()

		process := start(t, "fail_on_eof")

		err := process.shutdown(adapterExitTimeout)
		require.Error(t, err)
		require.NotErrorIs(t, err, errAdapterExitTimeout)
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr)
		require.Equal(t, 3, exitErr.ExitCode())
	})

	t.Run("ShutdownWaitsForGracefulExit", func(t *testing.T) {
		t.Parallel()

		process := start(t, "exit_on_eof")

		require.NoError(t, process.shutdown(adapterExitTimeout))
		require.True(t, process.command.ProcessState.Success())
	})
}

// TestAdapterProcessFake is not a test on its own. TestAdapterProcess runs
// the test binary with RIVER_CONFORMANCE_FAKE_ADAPTER set to make this
// function behave like an adapter that exits, fails, or wedges once its
// stdin closes.
func TestAdapterProcessFake(t *testing.T) {
	t.Parallel()

	behavior := os.Getenv("RIVER_CONFORMANCE_FAKE_ADAPTER")
	if behavior == "" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	switch behavior {
	case "exit_on_eof":
		os.Exit(0)
	case "fail_on_eof":
		os.Exit(3)
	case "ignore_eof":
		time.Sleep(time.Minute)
	}
	os.Exit(2)
}
