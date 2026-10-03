package stack

import "time"

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
