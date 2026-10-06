package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/co-rtex/TaskForge/scripts/internal/stack"
	"github.com/co-rtex/TaskForge/scripts/readdb"
)

// fixtureRecord is a full run with a known mix of verdicts: throughput, dispatch,
// volume and completion are met, and recovery is missed at 31s against a 30s
// target, as the shipped lease predicts.
func fixtureRecord(t *testing.T, profile string) Record {
	t.Helper()
	timings := stack.ShippedTimings()
	if profile == profileTuned {
		timings = stack.TunedTimings()
	}

	in := steadyInputs(180*time.Millisecond, 55*time.Millisecond)
	in.profile = profile
	th := analyzeThroughput(in)

	var kills []killObservation
	for i := 0; i < 24; i++ {
		offset := 20*time.Second + time.Duration(i)*25*time.Second
		k := killAt(i, offset, "w01", 1)
		if i%6 == 0 {
			k.Abandoned = []readdb.Recovery{{JobID: uuid.New(), AbandonedAttempt: 1, ReplacementClaimedAt: claimedAt(offset + 31*time.Second)}}
		}
		kills = append(kills, k)
	}
	fi := faultInputs(kills...)
	fi.profile = profile
	fi.seed = 20261002
	f := analyzeFaults(fi)

	return Record{
		SchemaVersion: 1,
		RecordedAt:    time.Date(2026, time.October, 3, 1, 2, 3, 0, time.UTC),
		Commit:        "99044203187854f9cffaf76658a094223061ab77",
		TreeClean:     true, BinariesChecked: true,
		Command: "go run ./scripts/bench throughput faults --record",
		Profile: profile, Seed: 20261002,
		Timing: timings.Env(),
		Environment: Environment{
			CPUModel: "Apple M2", LogicalCores: 8, MemoryBytes: 17179869184, OS: "macOS 26.0 (arm64)",
			PowerSource: "AC power", LowPowerMode: "off", GoVersion: "go1.27.0 darwin/arm64", DockerClient: "28.5.2", DockerServer: "28.5.2",
			DockerVMCPUs: 8, DockerVMMemory: 4109217792, PostgresVersion: "16.4",
			ComposeImages: []string{"localstack/localstack:4.14.0", "postgres:16-alpine"},
		},
		Throughput: &th, Faults: &f,
		Targets: evaluateTargets(&th, &f, timings.Env(), 12, 4),
	}
}

func TestEvaluateTargets_MarksEachAgainstItsTargetAndNamesTheSettingsThatGovernIt(t *testing.T) {
	r := fixtureRecord(t, profileShipped)
	byName := map[string]TargetRow{}
	for _, row := range r.Targets {
		byName[row.Name] = row
	}
	require.Len(t, r.Targets, 5, "one row for each PROJECT_SPEC section 7 target")

	require.Equal(t, "Met", byName["Sustained throughput"].Verdict)
	require.Equal(t, "Met", byName["Dispatch latency"].Verdict)
	require.Equal(t, "Met", byName["Fault-injection volume"].Verdict)
	require.Equal(t, "Met", byName["Completion under fault injection"].Verdict)
	recovery := byName["Worker-failure recovery"]
	require.Equal(t, "MISSED", recovery.Verdict, "31s against < 30s")
	require.Contains(t, recovery.Measured, "31", "the worst observed recovery is printed with the verdict")

	// The configuration that decides the recovery sits beside the verdict.
	var governing []string
	for _, s := range recovery.GovernedBy {
		governing = append(governing, s.Name+"="+s.Value)
	}
	require.Contains(t, governing, "TASKFORGE_LEASE_DURATION=30s")
	require.Contains(t, governing, "TASKFORGE_RECONCILER_POLL_INTERVAL=2s")
}

func TestEvaluateTargets_ARunWithOnlyOnePartDoesNotInventTheOther(t *testing.T) {
	r := fixtureRecord(t, profileShipped)
	rows := evaluateTargets(r.Throughput, nil, r.Timing, 12, 4)
	require.Len(t, rows, 2, "throughput and dispatch only")
	rows = evaluateTargets(nil, r.Faults, r.Timing, 12, 4)
	require.Len(t, rows, 3, "volume, completion and recovery only")
}

func TestRenderMarkdown_SaysWhatWasMeasuredHowAndOnWhat(t *testing.T) {
	md := renderMarkdown(fixtureRecord(t, profileShipped))

	for _, want := range []string{
		"99044203187854f9cffaf76658a094223061ab77",
		"go run ./scripts/bench throughput faults --record",
		"working tree clean", "yes",
		"20261002",
		"Apple M2", "go1.27.0", "28.5.2", "16.4", "AC power", "Low Power Mode",
		"shipped defaults",
		"| Met |", "**MISSED**",
		"## Limitations",
		"one machine", "ElasticMQ", "PostgreSQL", "single run", "load generator",
		"adr/0020-benchmark-methodology.md",
	} {
		require.Contains(t, md, want)
	}
	// Every timing value in effect is listed.
	for name, value := range stack.ShippedTimings().Env() {
		require.Contains(t, md, name)
		require.Contains(t, md, value)
	}
	// One timeline row per kill.
	require.Equal(t, 24, strings.Count(md, "| w01 |"))
}

func TestRenderMarkdown_ATunedRunIsLabelledTunedAndNeverCalledShipped(t *testing.T) {
	md := renderMarkdown(fixtureRecord(t, profileTuned))

	require.Contains(t, md, "TUNED")
	require.Contains(t, md, "not the shipped defaults")
	require.NotContains(t, md, "shipped defaults)", "the headline profile's label must not appear on a tuned record")
}

func TestRenderMarkdown_AMissedTargetIsNotSoftenedAnywhere(t *testing.T) {
	md := renderMarkdown(fixtureRecord(t, profileShipped))
	require.Equal(t, 1, strings.Count(md, "**MISSED**")-strings.Count(md, "**MISSED** ("), "exactly one row is missed")
	require.NotContains(t, strings.ToLower(md), "nearly met")
	require.NotContains(t, strings.ToLower(md), "approximately met")
}

func TestRecord_JSONIsASummaryNotAPerJobDump(t *testing.T) {
	r := fixtureRecord(t, profileShipped)
	raw, err := json.Marshal(r)
	require.NoError(t, err)

	require.Less(t, len(raw), 64*1024, "aggregates and the fault timeline, not 15,000 job rows")
	var back map[string]any
	require.NoError(t, json.Unmarshal(raw, &back))
	for _, key := range []string{"commit", "command", "profile", "seed", "environment", "throughput", "faults", "targets", "taskforge_timing_settings"} {
		require.Contains(t, back, key)
	}
	faults := back["faults"].(map[string]any)
	require.Len(t, faults["kills"], 24)
	require.NotContains(t, faults, "jobs", "no per-job rows")
}

func TestRecordPaths(t *testing.T) {
	when := time.Date(2026, time.October, 3, 1, 2, 3, 0, time.UTC)
	md, js := recordPaths("docs/benchmarks", when, "9904420", profileShipped)
	require.Equal(t, "docs/benchmarks/2026-10-03-9904420.md", md)
	require.Equal(t, "docs/benchmarks/2026-10-03-9904420.json", js)

	md, js = recordPaths("docs/benchmarks", when, "9904420", profileTuned)
	require.Equal(t, "docs/benchmarks/2026-10-03-9904420-tuned.md", md)
	require.Equal(t, "docs/benchmarks/2026-10-03-9904420-tuned.json", js)
}

// requireOneTrailingNewline asserts text ends in a non-empty line and exactly one
// "\n" after it: no missing newline, and no blank line at the end.
func requireOneTrailingNewline(t *testing.T, text, what string) {
	t.Helper()
	require.NotEmpty(t, text, what)
	require.Truef(t, strings.HasSuffix(text, "\n"), "%s must end in a newline", what)
	require.Falsef(t, strings.HasSuffix(text, "\n\n"), "%s must not end in a blank line", what)
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	require.NotEmptyf(t, strings.TrimSpace(lines[len(lines)-1]), "%s: the last line must have text in it", what)
}

// TestRenderMarkdown_EndsWithExactlyOneNewline proves the renderer's output ends
// with the last Limitations line and exactly one "\n": no trailing blank line.
//
// A record is written to disk exactly as rendered, so a trailing blank line here
// is a trailing blank line in every file a run commits. The two records already in
// docs/benchmarks were trimmed by hand to avoid it.
func TestRenderMarkdown_EndsWithExactlyOneNewline(t *testing.T) {
	for _, profile := range []string{profileShipped, profileTuned} {
		md := renderMarkdown(fixtureRecord(t, profile))
		requireOneTrailingNewline(t, md, profile+" record")

		lines := strings.Split(strings.TrimSuffix(md, "\n"), "\n")
		require.Contains(t, lines[len(lines)-1], "Power and sleep",
			"the output ends with the last Limitations line, not with something after it")
	}
}

// TestCommittedRecords_EndInExactlyOneNewline proves every committed record's
// Markdown ends in exactly one "\n". It guards the two hand-trimmed files and
// every record a future run commits alike, so a renderer that regressed would be
// caught the first time its output was committed.
func TestCommittedRecords_EndInExactlyOneNewline(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "docs", "benchmarks", "*.md"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(paths), 2, "the two committed records must be found; an empty glob would prove nothing")
	for _, path := range paths {
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		requireOneTrailingNewline(t, string(content), path)
	}
}
