package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// maxInFlight bounds the submissions outstanding at once. At the target rate with
// a healthy API there are one or two; the bound is only so a stalled API cannot
// grow this process without limit. If it is ever reached, the pacer's lateness
// shows it in the record.
const maxInFlight = 256

// runThroughput offers o.rate jobs a minute for a warm-up and then a steady
// window, stops, lets the system drain, and reads the results back from
// PostgreSQL.
//
// Every instant that bounds a measurement is PostgreSQL's clock_timestamp()
// sampled at the moment this process crosses the boundary: the start of pacing,
// the opening and the closing of the steady window, the end of the drain. This
// process's own clock only paces the offered load and bounds the waits.
func runThroughput(parent context.Context, o options, out io.Writer) (res ThroughputResult, err error) {
	fl, err := startFleet(parent, o, out)
	if err != nil {
		return ThroughputResult{}, err
	}
	defer fl.close()
	ctx, stop := fl.watch(parent)
	defer stop(nil)
	defer func() {
		if cause := failureCause(ctx); cause != nil {
			err = cause
		}
	}()
	st := fl.st
	db := st.DB
	clk := realClock{}

	submit := newSubmitter(st.APIURL, st.APIKey, st.RunID)

	pacingStartedPG, err := readdb.ClockNow(ctx, db)
	if err != nil {
		return ThroughputResult{}, err
	}
	start := time.Now()
	pacer, err := NewPacer(start, o.rate)
	if err != nil {
		return ThroughputResult{}, err
	}
	st.Say("Offering %.0f jobs/minute (one every %s): warm-up %s, steady window %s.",
		o.rate, pacer.Interval(), o.warmup, o.window)

	pacing, stopPacing := context.WithCancel(ctx)
	defer stopPacing()
	var inflight sync.WaitGroup
	slots := make(chan struct{}, maxInFlight)
	paced := make(chan PaceStats, 1)
	go func() {
		paced <- pacer.Run(pacing, clk, -1, func(i int) {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			inflight.Add(1)
			go func() {
				defer inflight.Done()
				defer func() { <-slots }()
				// A failure is counted by the submitter and shown in the record.
				_ = submit.submit(ctx, i)
			}()
		})
	}()

	// Progress, so a five minute window is not five silent minutes.
	progressDone := make(chan struct{})
	defer close(progressDone)
	go func() {
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				s := submit.stats()
				st.Say("%d jobs submitted so far (%d failed).", s.OK, s.Failed)
			}
		}
	}()

	if err := clk.SleepUntil(ctx, start.Add(o.warmup)); err != nil {
		return ThroughputResult{}, err
	}
	windowOpen, err := readdb.ClockNow(ctx, db)
	if err != nil {
		return ThroughputResult{}, err
	}
	openedMono := time.Now()
	st.Say("Steady window opens at PostgreSQL time %s.", windowOpen.UTC().Format("15:04:05.000"))

	if err := clk.SleepUntil(ctx, openedMono.Add(o.window)); err != nil {
		return ThroughputResult{}, err
	}
	windowClose, err := readdb.ClockNow(ctx, db)
	if err != nil {
		return ThroughputResult{}, err
	}
	closedMono := time.Now()
	stopPacing()
	paceStats := <-paced
	inflight.Wait()
	st.Say("Steady window closed at PostgreSQL time %s. Draining.", windowClose.UTC().Format("15:04:05.000"))

	endedBy, err := waitTerminal(ctx, clk, db, st.Scope, closedMono.Add(o.drain))
	if err != nil {
		return ThroughputResult{}, err
	}
	drainEnd, err := readdb.ClockNow(ctx, db)
	if err != nil {
		return ThroughputResult{}, err
	}
	st.Say("Drain ended (%s).", endedBy)

	window := Window{Start: windowOpen, End: windowClose}
	dispatches, err := readdb.Dispatches(ctx, db, st.Scope, window.Start, window.End)
	if err != nil {
		return ThroughputResult{}, err
	}
	finishes, err := readdb.SucceededFinishes(ctx, db, st.Scope)
	if err != nil {
		return ThroughputResult{}, err
	}
	statuses, nonTerminal, err := statusesAndTerminal(ctx, db, st.Scope)
	if err != nil {
		return ThroughputResult{}, err
	}
	dlq, err := readdb.DLQReasons(ctx, db, st.Scope)
	if err != nil {
		return ThroughputResult{}, err
	}

	sent := submit.stats()
	res = analyzeThroughput(throughputInputs{
		profile: o.profile, workers: o.workers, concurrency: o.concurrency, targetRate: o.rate,
		warmup: windowOpen.Sub(pacingStartedPG), drain: drainEnd.Sub(windowClose), window: window,
		pacer: paceStats, submitted: sent.OK, submitErrors: sent.Failed, retries: sent.Retries,
		dispatches: dispatches, finishes: finishes, statuses: statuses, dlq: dlq, nonTerminal: nonTerminal,
		monotonicWindow: closedMono.Sub(openedMono),
		observed:        fl.exitedOnTheirOwn(),
	})
	st.Say("Throughput %.2f jobs/min from %d completions (offered %.2f/min); dispatch p95 %s over %d jobs.",
		floorTo(res.ThroughputPerMin, 2), res.CompletedInWindow, floorTo(res.OfferedPerMin, 2), fmtDur(res.Dispatch.P95), res.Dispatch.N)
	if !res.Valid {
		return res, fmt.Errorf("the throughput run is not valid: %v", res.Problems)
	}
	return res, nil
}
