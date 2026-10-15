package cli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// idleChildEnv makes this test binary act as `taskforge-cli idle` when it is
// re-executed by TestIdle_*: a real process receiving a real signal, without
// building a separate binary.
const idleChildEnv = "TASKFORGE_CLI_IDLE_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(idleChildEnv) == "1" {
		// No API address at all, and one that would refuse a connection if
		// anything dialed it: idle must not try.
		os.Setenv(APIURLEnv, "http://127.0.0.1:1")
		os.Exit(Run(context.Background(), []string{"idle"}, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// idleWindow is how long a child must stay running with no signal: long
// enough that an idle which returned at once, or after any short delay, is
// caught, short enough for a unit test.
const idleWindow = 1500 * time.Millisecond

func startIdleChild(t *testing.T) (*exec.Cmd, chan error, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), idleChildEnv+"=1")
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	return cmd, done, &output
}

// TestIdle_StaysRunningAndExitsZeroOnEachSignal proves `idle` keeps running
// with no signal for the whole check window, and exits 0, promptly and silently,
// on SIGTERM and on SIGINT.
func TestIdle_StaysRunningAndExitsZeroOnEachSignal(t *testing.T) {
	for name, sig := range map[string]syscall.Signal{"SIGTERM": syscall.SIGTERM, "SIGINT": syscall.SIGINT} {
		t.Run(name, func(t *testing.T) {
			cmd, done, output := startIdleChild(t)

			select {
			case err := <-done:
				t.Fatalf("idle exited within %s with no signal: %v\n%s", idleWindow, err, output)
			case <-time.After(idleWindow):
			}

			require.NoError(t, cmd.Process.Signal(sig))
			select {
			case err := <-done:
				require.NoError(t, err, "idle must exit 0 on %s\n%s", name, output)
				require.Equal(t, 0, cmd.ProcessState.ExitCode())
				require.Empty(t, output.String(), "idle prints nothing")
			case <-time.After(5 * time.Second):
				t.Fatalf("idle did not exit within 5s of %s", name)
			}
		})
	}
}

// TestIdle_ReturnsSuccessWhenItsContextEnds covers the in-process path: a
// cancelled context ends idle with ExitSuccess.
func TestIdle_ReturnsSuccessWhenItsContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan int, 1)
	go func() { got <- Run(ctx, []string{"idle"}, &bytes.Buffer{}, &bytes.Buffer{}) }()
	cancel()
	select {
	case code := <-got:
		require.Equal(t, ExitSuccess, code)
	case <-time.After(5 * time.Second):
		t.Fatal("idle did not return after its context ended")
	}
}

// TestIdle_TakesNoArguments proves "idle" is exactly the one word: anything
// after it is the ordinary usage error, not a silent idle.
func TestIdle_TakesNoArguments(t *testing.T) {
	var stderr bytes.Buffer
	require.Equal(t, ExitUsageError, Run(context.Background(), []string{"idle", "now"}, &bytes.Buffer{}, &stderr))
}
