package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// What the recovery probe prints, and the pure functions behind it.

// segmentNames are the segments in order, as readdb.Segments names them.
var segmentNames = []string{"S1", "S2", "S3", "S4", "S5"}

func segmentValues(s readdb.Segments) []time.Duration {
	return []time.Duration{s.S1, s.S2, s.S3, s.S4, s.S5}
}

// fmtMS prints a duration in whole milliseconds, rounded toward zero. Only the
// printing rounds: the identity is checked on the exact values.
func fmtMS(d time.Duration) string { return fmt.Sprintf("%d ms", d.Milliseconds()) }

func short(id uuid.UUID) string { return id.String()[:8] }

// claim is one POST /internal/v1/claims the api served, read from its log.
type claim struct {
	at        time.Time     // the log line's time: this machine's clock, at the end of the request
	took      time.Duration // the request's duration, as the api logged it
	requestID string
	status    int
	jobID     string // the job it claimed, from the api's "job claimed" line; empty if none
}

// parseClaims reads every claim request out of an api log. The api logs one
// "http request" line per request (method, path, status, request_id) and, for a
// claim that took a job, a "job claimed" line with the same request_id and the
// job. A claim request with no "job claimed" line took nothing: its disposition
// was QUEUE_EMPTY, NO_ELIGIBLE_JOB, CAPACITY_EXHAUSTED or DUPLICATE_NOTIFICATION,
// which the api does not log, or it failed. Lines that are not JSON are skipped.
func parseClaims(raw []byte) []claim {
	type line struct {
		Time      time.Time `json:"time"`
		Msg       string    `json:"msg"`
		RequestID string    `json:"request_id"`
		Method    string    `json:"method"`
		Path      string    `json:"path"`
		Status    int       `json:"status"`
		Duration  int64     `json:"duration"` // slog writes a time.Duration as nanoseconds
		JobID     string    `json:"job_id"`
	}
	var requests []claim
	claimed := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		var l line
		if err := json.Unmarshal(scanner.Bytes(), &l); err != nil {
			continue
		}
		switch {
		case l.Msg == "http request" && l.Method == "POST" && l.Path == "/internal/v1/claims":
			requests = append(requests, claim{at: l.Time, took: time.Duration(l.Duration), requestID: l.RequestID, status: l.Status})
		case l.Msg == "job claimed":
			claimed[l.RequestID] = l.JobID
		}
	}
	for i := range requests {
		requests[i].jobID = claimed[requests[i].requestID]
	}
	return requests
}

// claimWindow is what the api served between two instants.
type claimWindow struct {
	requests int
	tookJob  int
	tookThis int // claims of the job being followed
	failed   int // non-200
}

// claimsBetween counts the claim requests that ended in (from, to], both on this
// machine's clock, and how many took a job, and how many took jobID.
func claimsBetween(claims []claim, from, to time.Time, jobID uuid.UUID) claimWindow {
	var w claimWindow
	for _, c := range claims {
		if !c.at.After(from) || c.at.After(to) {
			continue
		}
		w.requests++
		if c.status != 200 {
			w.failed++
		}
		if c.jobID != "" {
			w.tookJob++
			if c.jobID == jobID.String() {
				w.tookThis++
			}
		}
	}
	return w
}

// overThreshold reports whether a hop's recovery exceeded lease + slack, or never
// completed at all.
func overThreshold(h hopResult, lease time.Duration) bool {
	return !h.complete || h.recovery > lease+probeOverThresholdSlack
}

// excessSegment names the segment holding a slow hop's excess: the one furthest
// above that segment's baseline median. The baseline is the condition's other
// hops; see printSummary.
func excessSegment(s readdb.Segments, baseline []time.Duration) (string, time.Duration) {
	best, bestBy := "", time.Duration(0)
	for i, v := range segmentValues(s) {
		if by := v - baseline[i]; best == "" || by > bestBy {
			best, bestBy = segmentNames[i], by
		}
	}
	return best, bestBy
}

// segmentMedians is the nearest-rank median of each segment over complete hops.
func segmentMedians(hops []hopResult) []time.Duration {
	out := make([]time.Duration, len(segmentNames))
	for i := range segmentNames {
		var vs []time.Duration
		for _, h := range hops {
			if h.complete {
				vs = append(vs, segmentValues(h.segments)[i])
			}
		}
		slices.Sort(vs)
		if len(vs) > 0 {
			out[i] = NearestRank(vs, 50)
		}
	}
	return out
}

// printTrial writes one trial's block.
func printTrial(out io.Writer, tr trialResult, lease time.Duration) {
	fmt.Fprintf(out, "=== %s trial %d ===\n", tr.cond, tr.n)
	if tr.missed {
		fmt.Fprintf(out, "MISSED, not counted: %s\n", tr.missReason)
		if tr.worker != "" {
			fmt.Fprintf(out, "kill: PostgreSQL %s, worker %s, session %s\n", stamp(tr.killedAt), tr.worker, tr.session)
		}
		fmt.Fprintf(out, "logs: %s\n\n", tr.logDir)
		return
	}
	fmt.Fprintf(out, "kill: PostgreSQL %s, worker %s, killed session %s; it held %d attempt(s) when dead; jobs sleep %s\n",
		stamp(tr.killedAt), tr.worker, tr.session, tr.held, tr.jobDuration)
	fmt.Fprintf(out, "queue VisibilityTimeout %s s; broker at the kill: %s\n", tr.visibility, tr.atKill)
	if !tr.submissionEndPG.IsZero() {
		fmt.Fprintf(out, "last submission returned at PostgreSQL %s, %s after the kill\n",
			stamp(tr.submissionEndPG), fmtMS(tr.submissionEndPG.Sub(tr.killedAt)))
	}
	for i, h := range tr.hops {
		fmt.Fprintf(out, "hop %d: job %s, abandoned attempt %d\n", i+1, short(h.hop.JobID), h.hop.AbandonedAttempt)
		if !h.complete {
			fmt.Fprintf(out, "  INCOMPLETE: released %v, event published %v, replacement %v\n",
				h.hop.LeaseReleasedAt != nil, h.hop.EventPublishedAt != nil, h.hop.ReplacementCreatedAt != nil)
		} else {
			parts := make([]string, 0, len(segmentNames))
			for j, v := range segmentValues(h.segments) {
				parts = append(parts, fmt.Sprintf("%s %s", segmentNames[j], fmtMS(v)))
			}
			verdict := "HOLDS"
			if !h.identity {
				verdict = fmt.Sprintf("FAILS (sum %s)", h.segments.Sum())
			}
			fmt.Fprintf(out, "  %s; recovery %s; identity S1+..+S5 = recovery: %s\n",
				strings.Join(parts, ", "), fmtMS(h.recovery), verdict)
		}
		attempts, lastError := "?", "none"
		if h.hop.EventAttempts != nil {
			attempts = fmt.Sprint(*h.hop.EventAttempts)
		}
		if h.hop.EventLastError != nil {
			lastError = *h.hop.EventLastError
		}
		fmt.Fprintf(out, "  recovery event %s: outbox attempts %s, last_error %s\n", optID(h.hop.EventID), attempts, lastError)
		if h.hop.ReplacementSessionID != nil {
			whose := "another worker's session"
			if *h.hop.ReplacementSessionID == tr.restartedSession {
				whose = "the restarted victim's session"
			}
			fmt.Fprintf(out, "  replacement on session %s (%s)\n", *h.hop.ReplacementSessionID, whose)
		}
		fmt.Fprintf(out, "  replacement claimed by: %s\n", claimedBy(h.hop))
		fmt.Fprintf(out, "  the recovery event's own claim took: %s\n", eventTook(h.hop))
		if h.hop.EventPublishedAt != nil {
			lag := "?"
			if !h.published.at.IsZero() {
				lag = "+" + fmtMS(h.published.at.Sub(*h.hop.EventPublishedAt))
			}
			fmt.Fprintf(out, "  broker right after publish (%s): %s\n", lag, h.published)
		}
		if h.complete {
			fmt.Fprintf(out, "  claims the api served during S5 (+%s): %d, %d took a job (%d took this one), %d took nothing, %d failed\n",
				probeClaimLogSlack, h.claims.requests, h.claims.tookJob, h.claims.tookThis, h.claims.requests-h.claims.tookJob, h.claims.failed)
		}
		if overThreshold(h, lease) {
			fmt.Fprintf(out, "  OVER THRESHOLD (recovery > %s)\n", lease+probeOverThresholdSlack)
		}
	}
	fmt.Fprintf(out, "claims that took nothing from the kill to the last replacement: %d\n", len(tr.empty))
	for _, e := range tr.empty {
		fmt.Fprintf(out, "  at PostgreSQL ~%s (%s after the kill): %s\n", stamp(e.at), fmtMS(e.at.Sub(tr.killedAt)), e.capacity)
	}
	for _, n := range tr.notes {
		fmt.Fprintf(out, "note: %s\n", n)
	}
	fmt.Fprintf(out, "logs (every process of this trial): %s\n\n", tr.logDir)
}

func stamp(t time.Time) string { return t.UTC().Format("15:04:05.000") + " UTC" }

func optID(id *uuid.UUID) string {
	if id == nil {
		return "(none matched)"
	}
	return short(*id)
}

func claimedBy(h readdb.RecoveryHop) string {
	switch {
	case h.ReplacementEventID == nil:
		return "(no replacement)"
	case h.EventID != nil && *h.ReplacementEventID == *h.EventID:
		return "the recovery event's own notification"
	}
	s := fmt.Sprintf("ANOTHER event, %s", short(*h.ReplacementEventID))
	if h.ReplacementEventJobID != nil {
		s += fmt.Sprintf(" of job %s", short(*h.ReplacementEventJobID))
	}
	if h.ReplacementEventCreatedAt != nil && h.ReplacementEventPublishedAt != nil && h.ReplacementCreatedAt != nil {
		s += fmt.Sprintf(", created %s, published %s, which is %s before this claim",
			stamp(*h.ReplacementEventCreatedAt), stamp(*h.ReplacementEventPublishedAt),
			fmtMS(h.ReplacementCreatedAt.Sub(*h.ReplacementEventPublishedAt)))
	}
	return s
}

func eventTook(h readdb.RecoveryHop) string {
	switch {
	case h.EventClaimedJobID == nil:
		return "nothing (no lease carries its id)"
	case *h.EventClaimedJobID == h.JobID:
		return "this job"
	default:
		return fmt.Sprintf("ANOTHER job, %s", short(*h.EventClaimedJobID))
	}
}

// printSummary writes one condition's summary: count, min, median and max of
// each segment and of the recovery, and every hop over the threshold with the
// segment that held its excess.
func printSummary(out io.Writer, cond string, trials []trialResult, lease time.Duration) {
	var hops, normal []hopResult
	identityHeld, identityChecked := 0, 0
	for _, tr := range trials {
		for _, h := range tr.hops {
			hops = append(hops, h)
			if h.complete {
				identityChecked++
				if h.identity {
					identityHeld++
				}
				if !overThreshold(h, lease) {
					normal = append(normal, h)
				}
			}
		}
	}
	fmt.Fprintf(out, "=== summary: condition %s, %s ===\n", cond, conditionNames[cond])
	fmt.Fprintf(out, "%d trials, %d abandoned attempts, %d complete; identity held in %d of %d\n",
		len(trials), len(hops), identityChecked, identityHeld, identityChecked)
	fmt.Fprintf(out, "%-9s %6s %10s %10s %10s\n", "segment", "count", "min", "median", "max")
	columns := append(slices.Clone(segmentNames), "recovery")
	for i, name := range columns {
		var vs []time.Duration
		for _, h := range hops {
			if !h.complete {
				continue
			}
			if i < len(segmentNames) {
				vs = append(vs, segmentValues(h.segments)[i])
			} else {
				vs = append(vs, h.recovery)
			}
		}
		slices.Sort(vs)
		if len(vs) == 0 {
			fmt.Fprintf(out, "%-9s %6d\n", name, 0)
			continue
		}
		fmt.Fprintf(out, "%-9s %6d %10s %10s %10s\n", name, len(vs), fmtMS(vs[0]), fmtMS(NearestRank(vs, 50)), fmtMS(vs[len(vs)-1]))
	}
	baseline := segmentMedians(normal)
	if len(normal) == 0 {
		baseline = segmentMedians(hops)
	}
	over := 0
	for _, tr := range trials {
		for _, h := range tr.hops {
			if !overThreshold(h, lease) {
				continue
			}
			over++
			if !h.complete {
				fmt.Fprintf(out, "OVER: %s trial %d job %s never completed its recovery\n", cond, tr.n, short(h.hop.JobID))
				continue
			}
			name, by := excessSegment(h.segments, baseline)
			fmt.Fprintf(out, "OVER: %s trial %d job %s recovery %s: excess in %s (%s above its baseline median); replacement claimed by %s\n",
				cond, tr.n, short(h.hop.JobID), fmtMS(h.recovery), name, fmtMS(by), claimedBy(h.hop))
		}
	}
	if over == 0 {
		fmt.Fprintf(out, "no recovery over %s\n", lease+probeOverThresholdSlack)
	}
	fmt.Fprintln(out)
}

// copyLogs copies a trial's log files (not its empty working directory) into dst.
func copyLogs(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), raw, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// emptyClaim is a claim that took no job, placed on PostgreSQL's clock, with the
// logical workers that were at their limit at that instant.
type emptyClaim struct {
	at       time.Time
	capacity string
}

// describeCapacity renders WorkersAtCapacity's answer.
func describeCapacity(at []readdb.AtCapacity, err error) string {
	if err != nil {
		return "capacity unavailable: " + err.Error()
	}
	if len(at) == 0 {
		return "no worker was at its limit (so not CAPACITY_EXHAUSTED by this reconstruction)"
	}
	parts := make([]string, 0, len(at))
	for _, c := range at {
		parts = append(parts, fmt.Sprintf("%s held %d of %d (%d on a dead boot)", c.Worker[strings.LastIndex(c.Worker, "-")+1:], c.Active, c.Limit, c.DeadBoots))
	}
	return "at their limit: " + strings.Join(parts, "; ")
}

// midpoint is the middle of a logged request, on this machine's clock.
func (c claim) midpoint() time.Time { return c.at.Add(-c.took / 2) }
