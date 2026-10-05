package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// killObservation is everything the harness knows about one kill when it
// finishes: when it was scheduled, the database's clock reading taken
// immediately before the SIGKILL, who was killed, and (read back from the
// database once recovery has run) the attempts it left ABANDONED.
type killObservation struct {
	Index           int
	ScheduledOffset time.Duration
	// KilledAt is `SELECT clock_timestamp()` read immediately before the SIGKILL.
	KilledAt time.Time
	Worker   string
	Session  uuid.UUID
	// HeldAtSelection is how many attempts the victim was holding when it was
	// chosen, and ChoseOccupied whether it was chosen because it held any.
	HeldAtSelection int
	ChoseOccupied   bool
	RestartMS       float64
	RestartError    string
	Abandoned       []readdb.Recovery
}

// KillRecord is one kill as the summary records it.
type KillRecord struct {
	Index           int       `json:"index"`
	ScheduledOffset float64   `json:"scheduled_offset_seconds"`
	KilledAt        time.Time `json:"killed_at_postgresql_time"`
	Worker          string    `json:"worker"`
	Session         string    `json:"killed_session"`
	HeldAtSelection int       `json:"attempts_held_when_chosen"`
	ChoseOccupied   bool      `json:"chosen_because_it_held_an_attempt"`
	Affected        int       `json:"abandoned_attempts"`
	Unrecovered     int       `json:"abandoned_attempts_never_replaced"`
	RecoverySeconds []float64 `json:"recovery_seconds"`
	RestartMS       float64   `json:"restart_ms"`
	RestartError    string    `json:"restart_error,omitempty"`
}

// faultsInputs is what the fault run's figures are computed from.
type faultsInputs struct {
	profile     string
	seed        int64
	workers     int
	concurrency int
	targetRate  float64

	jobsTarget     int
	submitted      int // accepted by the API
	submitErrors   int
	retries        int
	jobsInDatabase int // every job in the run's scope

	kills          []killObservation
	scheduledKills int

	submissionDuration time.Duration
	deadline           time.Duration
	deadlineAt         time.Time
	endedBy            string

	statuses map[string]int
	dlq      map[string]int

	// observed are problems the runner saw while the run was going, such as a
	// worker that exited on its own. Any one makes the run invalid.
	observed []string
}

// FaultsResult is the fault run's figures, and the JSON the summary records.
type FaultsResult struct {
	Profile     string  `json:"profile"`
	Seed        int64   `json:"seed"`
	Workers     int     `json:"workers"`
	Concurrency int     `json:"concurrency_per_worker"`
	TargetRate  float64 `json:"target_jobs_per_minute"`

	JobsTarget     int `json:"jobs_target"`
	JobsSubmitted  int `json:"jobs_accepted_by_the_api"`
	SubmitErrors   int `json:"submit_errors"`
	SubmitRetries  int `json:"submit_retries"`
	JobsInDatabase int `json:"jobs_in_database"`

	KillsScheduled int          `json:"kills_scheduled"`
	KillsDone      int          `json:"kills_done"`
	Kills          []KillRecord `json:"kills"`

	SubmissionSeconds         float64   `json:"submission_seconds"`
	CompletionDeadlineSeconds float64   `json:"completion_deadline_seconds_after_last_submission"`
	DeadlineAt                time.Time `json:"completion_deadline_at_postgresql_time"`
	EndedBy                   string    `json:"ended_by"`

	Succeeded         int            `json:"jobs_succeeded"`
	CompletionPercent float64        `json:"completion_percent"`
	FinalStatuses     map[string]int `json:"final_statuses"`
	DLQReasons        map[string]int `json:"dlq_reasons"`

	Affected    int       `json:"abandoned_attempts_total"`
	Unrecovered int       `json:"abandoned_attempts_never_replaced"`
	Recovery    Summary   `json:"-"`
	RecoveryMS  summaryMS `json:"recovery_ms"`

	Valid    bool     `json:"valid"`
	Problems []string `json:"problems"`
}

// analyzeFaults computes the fault run's figures. It is pure, and tested against
// observations whose answers are known.
func analyzeFaults(in faultsInputs) FaultsResult {
	res := FaultsResult{
		Profile: in.profile, Seed: in.seed, Workers: in.workers, Concurrency: in.concurrency, TargetRate: in.targetRate,
		JobsTarget: in.jobsTarget, JobsSubmitted: in.submitted, SubmitErrors: in.submitErrors, SubmitRetries: in.retries,
		JobsInDatabase: in.jobsInDatabase,
		KillsScheduled: in.scheduledKills, KillsDone: len(in.kills),
		SubmissionSeconds: in.submissionDuration.Seconds(), CompletionDeadlineSeconds: in.deadline.Seconds(),
		DeadlineAt: in.deadlineAt, EndedBy: in.endedBy,
		FinalStatuses: in.statuses, DLQReasons: in.dlq,
		Succeeded: in.statuses["SUCCEEDED"],
		Valid:     true, Problems: []string{},
	}
	if in.jobsInDatabase > 0 {
		res.CompletionPercent = 100 * float64(res.Succeeded) / float64(in.jobsInDatabase)
	}
	problem := func(format string, args ...any) {
		res.Valid = false
		res.Problems = append(res.Problems, fmt.Sprintf(format, args...))
	}

	for _, o := range in.observed {
		problem("%s", o)
	}

	var all []time.Duration
	for _, k := range in.kills {
		rec := recoveryTimes(k.KilledAt, k.Abandoned)
		all = append(all, rec.Durations...)
		res.Affected += rec.Affected
		res.Unrecovered += rec.Unrecovered
		for _, p := range rec.Problems {
			problem("kill %d: %s", k.Index, p)
		}
		seconds := make([]float64, len(rec.Durations))
		for i, d := range rec.Durations {
			seconds[i] = d.Seconds()
		}
		res.Kills = append(res.Kills, KillRecord{
			Index: k.Index, ScheduledOffset: k.ScheduledOffset.Seconds(), KilledAt: k.KilledAt,
			Worker: k.Worker, Session: k.Session.String(),
			HeldAtSelection: k.HeldAtSelection, ChoseOccupied: k.ChoseOccupied,
			Affected: rec.Affected, Unrecovered: rec.Unrecovered, RecoverySeconds: seconds,
			RestartMS: k.RestartMS, RestartError: k.RestartError,
		})
		if k.RestartError != "" {
			problem("kill %d: worker %s could not be restarted (%s), so the run lost capacity it was meant to keep", k.Index, k.Worker, k.RestartError)
		}
	}
	res.Recovery = Summarize(all)
	res.RecoveryMS = toMS(res.Recovery)

	if len(in.kills) < in.scheduledKills {
		problem("only %d of the %d scheduled kills were made", len(in.kills), in.scheduledKills)
	}
	if in.jobsInDatabase != in.submitted {
		problem("the API accepted %d submissions and the database holds %d jobs in this run's scope", in.submitted, in.jobsInDatabase)
	}
	return res
}
