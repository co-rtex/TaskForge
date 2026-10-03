package stack

import (
	"strconv"
	"time"
)

// Timings is every duration a run hands the services. Each profile is checked
// against internal/config's own validation by a test, so a value here that the
// services would refuse fails `make test-unit` rather than a run.
type Timings struct {
	Lease, Heartbeat, Stale, Renew time.Duration
	WorkerRequest                  time.Duration

	// Shared by both demonstration profiles.
	PollInterval  time.Duration // reconciler and scheduler
	OutboxPoll    time.Duration
	OutboxClaim   time.Duration
	RenotifyAfter time.Duration
	WorkerPollWait,
	WorkerShutdown,
	APIRequest time.Duration

	RetryBase, RetryMax time.Duration
	RetryMultiplier     float64
	RetryJitter         float64
}

// commonTimings are the settings both demonstrations share. They exist to make a
// run fast, not to change what it shows, and each satisfies the relationship
// internal/config validates for it.
func commonTimings() Timings {
	return Timings{
		// Fast scans, so a promoted retry or a recovered job is seen within a
		// fraction of a second instead of a poll interval of seconds.
		PollInterval: 250 * time.Millisecond,
		OutboxPoll:   200 * time.Millisecond,
		// At least the outbox poll interval, which config requires.
		OutboxClaim: time.Second,
		// At least 3x the scheduler poll interval (750ms) and at least the outbox
		// claim timeout (1s): both are config rules. It is only a repair for a
		// lost notification, so it should rarely matter in a demonstration.
		RenotifyAfter: 5 * time.Second,
		// The minimum config allows: the broker request carries whole seconds.
		WorkerPollWait: time.Second,
		WorkerShutdown: 2 * time.Second,
		APIRequest:     5 * time.Second,

		// Retry settings, so two retries take about a second and a half rather
		// than minutes. The multiplier and jitter are the defaults; only the base
		// and the cap are shortened (cap >= base, whole milliseconds).
		RetryBase:       500 * time.Millisecond,
		RetryMax:        5 * time.Second,
		RetryMultiplier: 2.0,
		RetryJitter:     0.2,
	}
}

// SuccessTimings keep the shipped liveness and lease settings. Nothing in the
// success demonstration depends on a worker dying, so there is no reason to run
// it with a lease short enough to be sensitive to a busy machine.
func SuccessTimings() Timings {
	t := commonTimings()
	t.Lease = 30 * time.Second
	t.Heartbeat = 5 * time.Second
	t.Stale = 15 * time.Second        // >= 3x heartbeat
	t.Renew = 10 * time.Second        // 3x renew <= lease
	t.WorkerRequest = 5 * time.Second // <= stale and <= renew
	return t
}

// FailureTimings are short, so a killed worker's lease lapses and a frozen
// worker's session goes stale within seconds. Each value is as short as its
// relationship allows:
//
//	heartbeat      1s     the interval a worker proves it is alive
//	stale          3s     3x heartbeat: the minimum config accepts, so one lost
//	                      heartbeat cannot look like a dead process
//	lease          5s     the window an attempt owns without renewing
//	renew          1.5s   3x renew = 4.5s <= lease, so two renewals can fail and
//	                      a third still land inside the lease
//	workerRequest  1s     <= stale and <= renew, so one hung control call cannot
//	                      consume a whole window
func FailureTimings() Timings {
	t := commonTimings()
	t.Lease = 5 * time.Second
	t.Heartbeat = time.Second
	t.Stale = 3 * time.Second
	t.Renew = 1500 * time.Millisecond
	t.WorkerRequest = time.Second
	return t
}

// ShippedTimings is every value the services run with when nothing overrides it:
// internal/config's defaults, spelled out. The benchmark's headline run uses it,
// so that "shipped defaults" is a list the record can print rather than a claim.
// A test loads the services' own configuration with an empty environment and
// fails if any value here differs, so the list cannot go stale.
func ShippedTimings() Timings {
	return Timings{
		Lease:         30 * time.Second,
		Heartbeat:     5 * time.Second,
		Stale:         15 * time.Second,
		Renew:         10 * time.Second,
		WorkerRequest: 10 * time.Second,

		PollInterval:   2 * time.Second, // reconciler and scheduler
		OutboxPoll:     time.Second,
		OutboxClaim:    30 * time.Second,
		RenotifyAfter:  60 * time.Second,
		WorkerPollWait: 10 * time.Second,
		WorkerShutdown: 15 * time.Second,
		APIRequest:     25 * time.Second,

		RetryBase:       time.Second,
		RetryMax:        5 * time.Minute,
		RetryMultiplier: 2.0,
		RetryJitter:     0.2,
	}
}

// TunedTimings is the one labelled alternative to the shipped defaults: a shorter
// lease and tighter liveness windows, and faster scan intervals. It exists to show
// what a deployment could buy by changing them, and it never replaces a headline
// run. Nothing outside these nine values differs from ShippedTimings, which a test
// pins. Each is as short as the services' own validation allows for its neighbours:
//
//	lease          10s     a killed worker's attempt is abandoned when this lapses
//	heartbeat      2s      the interval a worker proves it is alive
//	stale          6s      3x heartbeat, the minimum config accepts
//	renew          3s      3x renew = 9s <= lease, so two renewals can fail and a
//	                       third still land inside the lease
//	workerRequest  2s      <= stale and <= renew
//	pollInterval   250ms   reconciler and scheduler scans
//	outboxPoll     200ms   how soon a committed notification is published
//	outboxClaim    1s      >= outboxPoll
//	renotifyAfter  5s      >= 3x pollInterval and >= outboxClaim
func TunedTimings() Timings {
	t := ShippedTimings()
	t.Lease = 10 * time.Second
	t.Heartbeat = 2 * time.Second
	t.Stale = 6 * time.Second
	t.Renew = 3 * time.Second
	t.WorkerRequest = 2 * time.Second
	t.PollInterval = 250 * time.Millisecond
	t.OutboxPoll = 200 * time.Millisecond
	t.OutboxClaim = time.Second
	t.RenotifyAfter = 5 * time.Second
	return t
}

// serviceEnv is the timing settings the four background services read.
func (t Timings) serviceEnv() map[string]string {
	return map[string]string{
		"TASKFORGE_API_REQUEST_TIMEOUT":  t.APIRequest.String(),
		"TASKFORGE_LEASE_DURATION":       t.Lease.String(),
		"TASKFORGE_HEARTBEAT_INTERVAL":   t.Heartbeat.String(),
		"TASKFORGE_SESSION_STALE_AFTER":  t.Stale.String(),
		"TASKFORGE_LEASE_RENEW_INTERVAL": t.Renew.String(),

		"TASKFORGE_OUTBOX_POLL_INTERVAL":     t.OutboxPoll.String(),
		"TASKFORGE_OUTBOX_CLAIM_TIMEOUT":     t.OutboxClaim.String(),
		"TASKFORGE_RECONCILER_POLL_INTERVAL": t.PollInterval.String(),
		"TASKFORGE_SCHEDULER_POLL_INTERVAL":  t.PollInterval.String(),
		"TASKFORGE_SCHEDULER_RENOTIFY_AFTER": t.RenotifyAfter.String(),

		"TASKFORGE_JOB_RETRY_BASE":       t.RetryBase.String(),
		"TASKFORGE_JOB_RETRY_MAX":        t.RetryMax.String(),
		"TASKFORGE_JOB_RETRY_MULTIPLIER": strconv.FormatFloat(t.RetryMultiplier, 'f', -1, 64),
		"TASKFORGE_JOB_RETRY_JITTER":     strconv.FormatFloat(t.RetryJitter, 'f', -1, 64),
	}
}

// workerEnv is the timing settings a worker reads.
func (t Timings) workerEnv() map[string]string {
	return map[string]string{
		"TASKFORGE_WORKER_POLL_WAIT":        t.WorkerPollWait.String(),
		"TASKFORGE_WORKER_REQUEST_TIMEOUT":  t.WorkerRequest.String(),
		"TASKFORGE_WORKER_SHUTDOWN_TIMEOUT": t.WorkerShutdown.String(),
	}
}

// Env is every TASKFORGE_* timing variable a service or a worker is started
// with. BaseEnv and WorkerEnv build from the same two maps, so this is the list
// the processes were handed and not a second rendering of it.
func (t Timings) Env() map[string]string {
	out := t.serviceEnv()
	for name, value := range t.workerEnv() {
		out[name] = value
	}
	return out
}
