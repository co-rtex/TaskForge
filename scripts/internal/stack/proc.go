package stack

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Which phase of cleanup stops a process: workers first, so the run's keys can
// be revoked while the API is still up, then everything else.
const (
	KindService = "service"
	KindWorker  = "worker"
)

// stopGrace is how long a process gets to exit after SIGTERM before it is
// killed. The workers' own shutdown timeout is shorter. A variable only so a
// test of the kill fallback need not wait it out.
var stopGrace = 8 * time.Second

// Proc is one real TaskForge binary running as its own operating-system
// process, in its own process group, so that signalling it cannot reach this
// program or a sibling, and so nothing it might have started outlives it.
type Proc struct {
	Label string
	Kind  string
	Addr  string // the address its health server listens on
	Log   string // the file its output goes to
	PID   int

	exited  chan struct{} // closed once Wait has returned
	waitErr error         // valid only after exited is closed
}

// Start launches one binary from ./bin with exactly the environment given. Its
// working directory is empty, so no .env can influence it, and its output goes
// to a file under the run's log directory.
func (s *Stack) Start(label, binary, addr, kind string, env map[string]string) (*Proc, error) {
	logPath := filepath.Join(s.LogDir, label+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open the log for %s: %w", label, err)
	}
	// The child holds its own descriptors after Start; this one is only ours.
	defer logFile.Close()

	cmd := exec.Command(filepath.Join(s.BinDir, binary))
	cmd.Dir = s.WorkDir
	cmd.Env = sortedEnv(env)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", label, err)
	}

	p := &Proc{
		Label: label, Kind: kind, Addr: addr, Log: logPath,
		PID: cmd.Process.Pid, exited: make(chan struct{}),
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()

	s.mu.Lock()
	s.procs = append(s.procs, p)
	s.mu.Unlock()
	return p, nil
}

// Running reports whether the process has not yet exited.
func (p *Proc) Running() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// Signal sends sig to the process's whole group. A process that has already
// gone is not an error.
func (p *Proc) Signal(sig syscall.Signal) error {
	if err := syscall.Kill(-p.PID, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// WaitExit reports whether the process exited within timeout.
func (p *Proc) WaitExit(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.exited:
		return true
	case <-timer.C:
		return false
	}
}

// DiedFromSignal reports whether the process ended because sig killed it, as
// opposed to exiting on its own. Valid once the process has exited.
func (p *Proc) DiedFromSignal(sig syscall.Signal) bool {
	var exitErr *exec.ExitError
	if !errors.As(p.waitErr, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == sig
}

// Stop ends a process politely and, failing that, forcibly. SIGCONT goes first:
// a frozen process cannot act on SIGTERM until it is continued.
func (p *Proc) Stop() {
	if !p.Running() {
		return
	}
	_ = p.Signal(syscall.SIGCONT)
	_ = p.Signal(syscall.SIGTERM)
	if p.WaitExit(stopGrace) {
		return
	}
	_ = p.Signal(syscall.SIGKILL)
	p.WaitExit(5 * time.Second)
}

// StopProcs stops every started process the filter selects, concurrently.
func (s *Stack) StopProcs(filter func(*Proc) bool) {
	s.mu.Lock()
	procs := append([]*Proc(nil), s.procs...)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range procs {
		if !filter(p) || !p.Running() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Stop()
		}()
	}
	wg.Wait()
}

// WaitReady blocks until the process answers its own readiness probe, it exits,
// or the timeout passes.
func (p *Proc) WaitReady(ctx context.Context, timeout time.Duration) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		response, err := client.Get("http://" + p.Addr + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-p.exited:
			return fmt.Errorf("%s exited before it was ready; see %s", p.Label, p.Log)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("%s was not ready within %s; see %s", p.Label, timeout, p.Log)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// WorkerName is the name label gives a worker in this run: unique to the run, and
// the same every time it is asked for, so a worker that is killed and started
// again is the same logical worker and not a new one.
func (s *Stack) WorkerName(label string) string {
	return fmt.Sprintf("%s-%s-%s", s.Prefix, s.RunID, label)
}

// StartWorker runs one worker under WorkerName(label) and waits for it to report
// ready, which includes having registered its session.
func (s *Stack) StartWorker(ctx context.Context, label string, concurrency int) (*Proc, string, error) {
	name := s.WorkerName(label)
	p, err := s.StartWorkerNamed(ctx, label, name, concurrency)
	if err != nil {
		return nil, "", err
	}
	return p, name, nil
}

// StartWorkerNamed runs one worker under exactly the name given and waits for it
// to report ready. Starting it again under the same name after the first process
// has died is a process boot of the same logical worker: registration replaces
// the prior boot's session and leaves its leases to expire (internal/workers,
// Register). The process's output is appended to the same log file.
func (s *Stack) StartWorkerNamed(ctx context.Context, label, name string, concurrency int) (*Proc, error) {
	addr, err := FreeLoopbackAddr()
	if err != nil {
		return nil, err
	}
	env, err := s.Env("taskforge-worker", s.WorkerEnv(name, addr, concurrency))
	if err != nil {
		return nil, err
	}
	p, err := s.Start("worker-"+label, "taskforge-worker", addr, KindWorker, env)
	if err != nil {
		return nil, err
	}
	if err := p.WaitReady(ctx, 60*time.Second); err != nil {
		return nil, err
	}
	return p, nil
}
