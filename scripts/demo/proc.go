package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Which phase of cleanup stops a process: workers first, so the run's keys can
// be revoked while the API is still up, then everything else.
const (
	kindService = "service"
	kindWorker  = "worker"
)

// stopGrace is how long a process gets to exit after SIGTERM before it is
// killed. The workers' own shutdown timeout in this demo is shorter. A variable
// only so a test of the kill fallback need not wait it out.
var stopGrace = 8 * time.Second

// proc is one real TaskForge binary running as its own operating-system
// process, in its own process group, so that signalling it cannot reach this
// program or a sibling, and so nothing it might have started outlives it.
type proc struct {
	label string
	kind  string
	addr  string // the address its health server listens on
	log   string // the file its output goes to
	pid   int

	exited  chan struct{} // closed once Wait has returned
	waitErr error         // valid only after exited is closed
}

// start launches one binary from ./bin with exactly the environment given. Its
// working directory is empty, so no .env can influence it, and its output goes
// to a file under the run's log directory.
func (d *demo) start(label, binary, addr, kind string, env map[string]string) (*proc, error) {
	logPath := filepath.Join(d.logDir, label+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open the log for %s: %w", label, err)
	}
	// The child holds its own descriptors after Start; this one is only ours.
	defer logFile.Close()

	cmd := exec.Command(filepath.Join(d.binDir, binary))
	cmd.Dir = d.workDir
	cmd.Env = sortedEnv(env)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", label, err)
	}

	p := &proc{
		label: label, kind: kind, addr: addr, log: logPath,
		pid: cmd.Process.Pid, exited: make(chan struct{}),
	}
	go func() {
		p.waitErr = cmd.Wait()
		close(p.exited)
	}()

	d.mu.Lock()
	d.procs = append(d.procs, p)
	d.mu.Unlock()
	return p, nil
}

func (p *proc) running() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

// signal sends sig to the process's whole group. A process that has already
// gone is not an error.
func (p *proc) signal(sig syscall.Signal) error {
	if err := syscall.Kill(-p.pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

// waitExit reports whether the process exited within timeout.
func (p *proc) waitExit(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.exited:
		return true
	case <-timer.C:
		return false
	}
}

// diedFromSignal reports whether the process ended because sig killed it, as
// opposed to exiting on its own. Valid once the process has exited.
func (p *proc) diedFromSignal(sig syscall.Signal) bool {
	var exitErr *exec.ExitError
	if !errors.As(p.waitErr, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == sig
}

// stop ends a process politely and, failing that, forcibly. SIGCONT goes first:
// a frozen process cannot act on SIGTERM until it is continued.
func (p *proc) stop() {
	if !p.running() {
		return
	}
	_ = p.signal(syscall.SIGCONT)
	_ = p.signal(syscall.SIGTERM)
	if p.waitExit(stopGrace) {
		return
	}
	_ = p.signal(syscall.SIGKILL)
	p.waitExit(5 * time.Second)
}

// stopProcs stops every started process the filter selects, concurrently.
func (d *demo) stopProcs(filter func(*proc) bool) {
	d.mu.Lock()
	procs := append([]*proc(nil), d.procs...)
	d.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range procs {
		if !filter(p) || !p.running() {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.stop()
		}()
	}
	wg.Wait()
}

// waitReady blocks until the process answers its own readiness probe, it exits,
// or the timeout passes.
func (p *proc) waitReady(ctx context.Context, timeout time.Duration) error {
	client := &http.Client{Timeout: time.Second}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		response, err := client.Get("http://" + p.addr + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-p.exited:
			return fmt.Errorf("%s exited before it was ready; see %s", p.label, p.log)
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("%s was not ready within %s; see %s", p.label, timeout, p.log)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// startWorker runs one worker under a name unique to this run and waits for it
// to report ready, which includes having registered its session.
func (d *demo) startWorker(ctx context.Context, label string, concurrency int) (*proc, string, error) {
	addr, err := freeLoopbackAddr()
	if err != nil {
		return nil, "", err
	}
	name := fmt.Sprintf("demo-%s-%s", d.runID, label)
	env, err := d.env("taskforge-worker", d.workerEnv(name, addr, concurrency))
	if err != nil {
		return nil, "", err
	}
	p, err := d.start("worker-"+label, "taskforge-worker", addr, kindWorker, env)
	if err != nil {
		return nil, "", err
	}
	if err := p.waitReady(ctx, 60*time.Second); err != nil {
		return nil, "", err
	}
	return p, name, nil
}

// --- the run's own broker queue ------------------------------------------------

var queueURLPattern = regexp.MustCompile(`<QueueUrl>([^<]+)</QueueUrl>`)

// sqsCall issues one unsigned SQS query-protocol request. ElasticMQ, the local
// broker (ADR-0005), accepts it; the AWS SDK clients the services use are not
// needed to create and delete a queue.
func sqsCall(ctx context.Context, endpoint string, form url.Values) (string, error) {
	form.Set("Version", "2012-11-05")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	if err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: HTTP %d: %s", form.Get("Action"), response.StatusCode, body)
	}
	return string(body), nil
}

// createQueue creates the queue named name and returns its URL.
func createQueue(ctx context.Context, endpoint, name string) (string, error) {
	body, err := sqsCall(ctx, endpoint, url.Values{"Action": {"CreateQueue"}, "QueueName": {name}})
	if err != nil {
		return "", err
	}
	match := queueURLPattern.FindStringSubmatch(body)
	if len(match) != 2 {
		return "", fmt.Errorf("CreateQueue returned no queue URL: %s", body)
	}
	return match[1], nil
}

// deleteQueue deletes a queue this run created.
func deleteQueue(ctx context.Context, endpoint, queueURL string) error {
	_, err := sqsCall(ctx, endpoint, url.Values{"Action": {"DeleteQueue"}, "QueueUrl": {queueURL}})
	return err
}
