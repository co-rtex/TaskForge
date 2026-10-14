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

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
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
// above its nominal value (see nominalSegments).
func excessSegment(s readdb.Segments, baseline []time.Duration) (string, time.Duration) {
	best, bestBy := "", time.Duration(0)
	for i, v := range segmentValues(s) {
		if by := v - baseline[i]; best == "" || by > bestBy {
			best, bestBy = segmentNames[i], by
		}
	}
	return best, bestBy
}

// nominalSegments is what each segment is expected to take with nothing going
// wrong, from the profile alone: S1 the lease, S2 a reconciler poll interval, S3
// nothing, S4 an outbox poll interval, S5 nothing. They are what a slow hop's
// segments are compared with to say where its excess was. A baseline taken from
// the condition's own hops would not do: when most hops are slow, the "normal"
// hops are few, and the comparison says more about them than about the slow one.
func nominalSegments(t stack.Timings) []time.Duration {
	return []time.Duration{t.Lease, t.PollInterval, 0, t.OutboxPoll, 0}
}

// printTrial writes one trial's block.
func printTrial(out io.Writer, tr trialResult, lease time.Duration) {
	fmt.Fprintf(out, "=== %s trial %d ===\n", tr.cond, tr.n)
	if tr.missed {
		fmt.Fprintf(out, "MISSED, not counted: %s\n", tr.missReason)
		if tr.worker != "" {
			fmt.Fprintf(out, "kill: PostgreSQL %s, worker %s, session %s\n", stamp(tr.killedAt), tr.worker, tr.session)
			printOutcomes(out, tr)
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
	printOutcomes(out, tr)
	fmt.Fprintf(out, "claims that took nothing from the kill to the last replacement: %d\n", len(tr.empty))
	for _, e := range tr.empty {
		fmt.Fprintf(out, "  ended at PostgreSQL ~%s (%s after the kill): %s\n", stamp(e.at), fmtMS(e.at.Sub(tr.killedAt)), e.capacity)
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
func printSummary(out io.Writer, cond string, trials []trialResult, timings stack.Timings) {
	lease := timings.Lease
	var hops []hopResult
	identityHeld, identityChecked := 0, 0
	for _, tr := range trials {
		for _, h := range tr.hops {
			hops = append(hops, h)
			if h.complete {
				identityChecked++
				if h.identity {
					identityHeld++
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
	baseline := nominalSegments(timings)
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
			fmt.Fprintf(out, "OVER: %s trial %d job %s recovery %s: excess in %s (%s above its nominal value); replacement claimed by %s\n",
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
		return "no worker reached its limit during the claim (so not CAPACITY_EXHAUSTED by this reconstruction)"
	}
	parts := make([]string, 0, len(at))
	for _, c := range at {
		parts = append(parts, fmt.Sprintf("%s held %d of %d (%d on a dead boot) at %s", c.Worker[strings.LastIndex(c.Worker, "-")+1:], c.Active, c.Limit, c.DeadBoots, stamp(c.At)))
	}
	return "at their limit: " + strings.Join(parts, "; ")
}

// precedenceIdentity checks one resolved attempt against the reconciler's
// precedence. Both sweeps decide on one post-lock clock sample, and the outcome
// transaction writes that sample to the lease's released_at; the decision is
// TIMED_OUT exactly when that sample is at or after timeout_at (fenceState.timedOut),
// and ABANDONED otherwise. So for an attempt that ended ABANDONED or TIMED_OUT,
// TIMED_OUT must equal released_at >= timeout_at. applies is false for any other
// outcome, which a cancellation, a report or nothing at all decided.
func precedenceIdentity(o readdb.KillOutcome) (applies, holds bool) {
	if (o.Status != "ABANDONED" && o.Status != "TIMED_OUT") || o.LeaseReleasedAt == nil {
		return false, false
	}
	due := o.TimeoutAt != nil && !o.LeaseReleasedAt.Before(*o.TimeoutAt)
	return true, (o.Status == "TIMED_OUT") == due
}

// takenBy names the sweep that resolved an attempt, read from its lease: a
// timeout found while the lease was live leaves it RELEASED (the timeout sweep);
// the expired-lease path leaves it EXPIRED.
func takenBy(o readdb.KillOutcome) string {
	switch o.LeaseStatus {
	case "RELEASED":
		return "the timeout sweep, lease live"
	case "EXPIRED":
		return "the expired-lease path"
	default:
		return "lease " + o.LeaseStatus
	}
}

// printOutcomes writes what became of every attempt the kill caught.
func printOutcomes(out io.Writer, tr trialResult) {
	if tr.outcomeErr != nil {
		fmt.Fprintf(out, "outcomes: UNAVAILABLE: %v\n", tr.outcomeErr)
		return
	}
	states := map[string]int{}
	for _, o := range tr.outcomes {
		states[o.StateAtKill]++
	}
	chosen := "not recorded"
	if !tr.chosenAt.IsZero() {
		chosen = fmt.Sprintf("PostgreSQL %s, %s before the kill", stamp(tr.chosenAt), fmtMS(tr.killedAt.Sub(tr.chosenAt)))
	}
	fmt.Fprintf(out, "outcomes (timeout_seconds %d): victim chosen at %s; %d attempt(s) unfinished at the kill (%s); %d finished between the choice and the kill\n",
		tr.timeoutSeconds, chosen, len(tr.outcomes), countsLine(states, heldOrder), len(tr.finishedBeforeKill))
	fmt.Fprintf(out, "held when dead %d, outcomes %d: %s\n", tr.held, len(tr.outcomes), heldAgreement(tr))
	for _, f := range tr.finishedBeforeKill {
		fmt.Fprintf(out, "  finished before the kill: job %s attempt %d %s at %s, %s before the kill\n",
			short(f.JobID), f.AttemptNumber, f.Status, stamp(f.FinishedAt), fmtMS(tr.killedAt.Sub(f.FinishedAt)))
	}
	for _, o := range tr.outcomes {
		parts := []string{fmt.Sprintf("job %s attempt %d: %s at the kill -> %s (job now %s",
			short(o.JobID), o.AttemptNumber, o.StateAtKill, o.Outcome(), o.JobStatus)}
		if o.DLQReason != nil {
			parts[0] += ", " + *o.DLQReason
		}
		parts[0] += ")"
		if o.ErrorCode != nil {
			parts = append(parts, "code "+*o.ErrorCode)
		}
		parts = append(parts, fmt.Sprintf("lease %s, renewed %dx", o.LeaseStatus, o.RenewalVersion))
		if d, ok := o.ClaimToStart(); ok {
			parts = append(parts, "claim->start "+fmtMS(d))
		}
		if o.TimeoutAt != nil {
			parts = append(parts, "timeout_at - expires_at "+fmtMS(o.TimeoutAt.Sub(o.LeaseExpiresAt)))
		}
		if o.LeaseReleasedAt != nil {
			parts = append(parts, "resolved "+fmtMS(o.LeaseReleasedAt.Sub(o.LeaseExpiresAt))+" after expires_at by "+takenBy(o))
		}
		if o.RetryAt != nil && o.FinishedAt != nil {
			parts = append(parts, "retry_at = finished + "+fmtMS(o.RetryAt.Sub(*o.FinishedAt)))
		}
		if d, ok := o.ToReplacement(tr.killedAt); ok {
			parts = append(parts, "kill->replacement "+fmtMS(d))
		} else {
			parts = append(parts, "no replacement")
		}
		if applies, holds := precedenceIdentity(o); applies {
			verdict := "HOLDS"
			if !holds {
				verdict = "FAILS"
			}
			parts = append(parts, "precedence identity "+verdict)
		}
		fmt.Fprintf(out, "  %s\n", strings.Join(parts, "; "))
	}
}

// noAbandoned classifies a trial in which no attempt ended ABANDONED: what its
// kill hit instead. "" means the trial had an ABANDONED attempt.
//
//   - "finished before the signal": every attempt it was aimed at finished
//     anyway: at least one attempt of the session finished between the choice and
//     the kill, or a caught attempt ended SUCCEEDED after the kill instant (its
//     report was committing as the signal was sent), and none ended any other
//     way (M8A's explanation);
//   - "TIMED_OUT": at least one caught attempt ended TIMED_OUT, with or without
//     dead-lettering (M8D3's);
//   - "no victim": a miss that never chose a victim, because no worker held an
//     attempt it could be aimed at; it caught nothing because nothing was aimed;
//   - "other": anything else, named in the trial's own block.
func noAbandoned(tr trialResult) string {
	if tr.worker == "" {
		return noVictim
	}
	for _, o := range tr.outcomes {
		if b := o.Outcome(); b == readdb.OutcomeAbandoned || b == readdb.OutcomeAbandonedDeadLetter {
			return ""
		}
	}
	for _, o := range tr.outcomes {
		if o.Outcome() == readdb.OutcomeTimedOut || o.Outcome() == readdb.OutcomeTimedOutDeadLetter {
			return "TIMED_OUT"
		}
	}
	succeeded := 0
	for _, o := range tr.outcomes {
		if o.Status != "SUCCEEDED" {
			return "other"
		}
		succeeded++
	}
	if succeeded > 0 || len(tr.finishedBeforeKill) > 0 {
		return "finished before the signal"
	}
	return "other"
}

// noVictim is noAbandoned's category for a miss that chose no victim.
const noVictim = "no victim"

// heldOrder is the order the states at a kill are printed in.
var heldOrder = []string{readdb.HeldLeased, readdb.HeldRunning, readdb.HeldCreatedAfterKill}

// heldAgreement compares the attempts the victim's session held once it was dead
// (readdb.HeldBySession, read after the process exited) with the attempts
// KillOutcomes caught. Every attempt held when dead is unfinished at the kill, so
// fewer outcomes than held attempts means the selection dropped one.
func heldAgreement(tr trialResult) string {
	switch {
	case len(tr.outcomes) < tr.held:
		return "MISMATCH: fewer outcomes than attempts held when dead"
	case len(tr.outcomes) > tr.held:
		return "more outcomes than held when dead (an attempt finished between the kill stamp and the process's exit)"
	default:
		return "agree"
	}
}

// outcomeSummary is one condition's outcomes, with every denominator.
type outcomeSummary struct {
	trials, missed, withHeld int
	withVictim               int
	heldAgree, heldMismatch  int
	held                     map[string]int            // by state at kill
	final                    map[string]int            // by outcome bucket
	byState                  map[string]map[string]int // state at kill -> outcome -> count
	takenBy                  map[string]int            // TIMED_OUT attempts by the sweep that took them
	identityChecked          int
	identityHeld             int
	finishedBeforeKill       int
	trialsWithFinished       int
	claimToStart             []time.Duration // RUNNING at kill
	abandonedToReplacement   []time.Duration // the recovery figure
	timedOutToReplacement    []time.Duration // its own figure
	noAbandoned              map[string]int
	noAbandonedTrials        int
	outcomeErrors            int
}

func summarizeOutcomes(trials []trialResult) outcomeSummary {
	s := outcomeSummary{
		held: map[string]int{}, final: map[string]int{}, byState: map[string]map[string]int{},
		takenBy: map[string]int{}, noAbandoned: map[string]int{},
	}
	for _, tr := range trials {
		s.trials++
		if tr.missed {
			s.missed++
		}
		if tr.outcomeErr != nil {
			s.outcomeErrors++
			continue
		}
		if tr.worker != "" {
			s.withVictim++
			if len(tr.outcomes) < tr.held {
				s.heldMismatch++
			} else {
				s.heldAgree++
			}
		}
		if len(tr.outcomes) > 0 {
			s.withHeld++
		}
		s.finishedBeforeKill += len(tr.finishedBeforeKill)
		if len(tr.finishedBeforeKill) > 0 {
			s.trialsWithFinished++
		}
		switch kind := noAbandoned(tr); kind {
		case "":
		case noVictim:
			s.noAbandoned[kind]++
		default:
			s.noAbandonedTrials++
			s.noAbandoned[kind]++
		}
		for _, o := range tr.outcomes {
			bucket := o.Outcome()
			s.held[o.StateAtKill]++
			s.final[bucket]++
			if s.byState[o.StateAtKill] == nil {
				s.byState[o.StateAtKill] = map[string]int{}
			}
			s.byState[o.StateAtKill][bucket]++
			if o.Status == "TIMED_OUT" {
				s.takenBy[takenBy(o)]++
			}
			if applies, holds := precedenceIdentity(o); applies {
				s.identityChecked++
				if holds {
					s.identityHeld++
				}
			}
			if d, ok := o.ClaimToStart(); ok && o.StateAtKill == readdb.HeldRunning {
				s.claimToStart = append(s.claimToStart, d)
			}
			if d, ok := o.ToReplacement(tr.killedAt); ok {
				switch bucket {
				case readdb.OutcomeAbandoned:
					s.abandonedToReplacement = append(s.abandonedToReplacement, d)
				case readdb.OutcomeTimedOut:
					s.timedOutToReplacement = append(s.timedOutToReplacement, d)
				}
			}
		}
	}
	return s
}

// countsLine renders a map of counts in the given key order, then any other key.
func countsLine(m map[string]int, order []string) string {
	var parts []string
	seen := map[string]bool{}
	for _, k := range order {
		seen[k] = true
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	var rest []string
	for k := range m {
		if !seen[k] {
			rest = append(rest, k)
		}
	}
	slices.Sort(rest)
	for _, k := range rest {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

var outcomeOrder = []string{readdb.OutcomeAbandoned, readdb.OutcomeAbandonedDeadLetter, readdb.OutcomeTimedOut, readdb.OutcomeTimedOutDeadLetter,
	readdb.OutcomeCanceled, readdb.OutcomeUnresolved, readdb.OutcomeOther}

// spread renders count, min, median and max.
func spread(vs []time.Duration) string {
	if len(vs) == 0 {
		return "none"
	}
	sorted := slices.Clone(vs)
	slices.Sort(sorted)
	return fmt.Sprintf("%d; min %s, median %s, max %s", len(sorted), fmtMS(sorted[0]), fmtMS(NearestRank(sorted, 50)), fmtMS(sorted[len(sorted)-1]))
}

// printOutcomeSummary writes one condition's outcome summary over every trial
// that ran, counted or missed.
func printOutcomeSummary(out io.Writer, cond string, trials []trialResult) {
	s := summarizeOutcomes(trials)
	total := 0
	for _, n := range s.held {
		total += n
	}
	fmt.Fprintf(out, "=== outcomes: condition %s, %s ===\n", cond, conditionNames[cond])
	fmt.Fprintf(out, "trials run %d (%d counted, %d missed and run again); trials with a victim: %d; trials with >=1 attempt unfinished at the kill: %d\n",
		s.trials, s.trials-s.missed, s.missed, s.withVictim, s.withHeld)
	fmt.Fprintf(out, "held when dead vs outcomes: agree (outcomes >= held) in %d of %d trials with a victim", s.heldAgree, s.withVictim)
	if s.heldMismatch > 0 {
		fmt.Fprintf(out, "; MISMATCH in %d", s.heldMismatch)
	}
	fmt.Fprintln(out)
	if s.outcomeErrors > 0 {
		fmt.Fprintf(out, "OUTCOMES UNAVAILABLE in %d trial(s); they are left out of every figure below\n", s.outcomeErrors)
	}
	fmt.Fprintf(out, "attempts unfinished at the kill: %d (%s)\n", total, countsLine(s.held, heldOrder))
	fmt.Fprintf(out, "final, of %d: %s\n", total, countsLine(s.final, outcomeOrder))
	for _, state := range heldOrder {
		fmt.Fprintf(out, "  %s at the kill, of %d: %s\n", state, s.held[state], countsLine(s.byState[state], outcomeOrder))
	}
	fmt.Fprintf(out, "TIMED_OUT attempts taken by: %s\n", countsLine(s.takenBy, nil))
	fmt.Fprintf(out, "precedence identity (TIMED_OUT exactly when released_at >= timeout_at): held in %d of %d\n", s.identityHeld, s.identityChecked)
	fmt.Fprintf(out, "finished between the choice and the kill: %d attempt(s), in %d of %d trials\n", s.finishedBeforeKill, s.trialsWithFinished, s.trials)
	fmt.Fprintf(out, "claim->start (started_at - acquired_at), RUNNING at the kill: %s\n", spread(s.claimToStart))
	fmt.Fprintf(out, "kill->replacement, ABANDONED (the recovery figure): %s\n", spread(s.abandonedToReplacement))
	fmt.Fprintf(out, "kill->replacement, TIMED_OUT (its own figure, not a recovery): %s\n", spread(s.timedOutToReplacement))
	fmt.Fprintf(out, "trials with no ABANDONED attempt: %d of %d with a victim (%s); misses with no victim: %d\n\n",
		s.noAbandonedTrials, s.withVictim,
		countsLine(map[string]int{
			"finished before the signal": s.noAbandoned["finished before the signal"],
			"TIMED_OUT":                  s.noAbandoned["TIMED_OUT"],
			"other":                      s.noAbandoned["other"],
		}, []string{"finished before the signal", "TIMED_OUT", "other"}),
		s.noAbandoned[noVictim])
}
