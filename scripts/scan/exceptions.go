package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// The scanners, by the names an exception uses for them.
const (
	toolGovulncheck = "govulncheck"
	toolGitleaks    = "gitleaks"
	toolPipAudit    = "pip-audit"
	toolNpmAudit    = "npm-audit"
)

var knownTools = []string{toolGovulncheck, toolGitleaks, toolPipAudit, toolNpmAudit}

// Exception is one accepted finding in security/scan-exceptions.yaml. Every field
// is required: an exception is a decision someone made, with a reason, until a
// date, and it lapses on that date instead of becoming permanent by neglect.
type Exception struct {
	// ID is an identifier the tool reports for the finding: an OSV id or a CVE
	// for govulncheck, a fingerprint for gitleaks, a PYSEC or GHSA id for
	// pip-audit and npm-audit.
	ID   string `yaml:"id"`
	Tool string `yaml:"tool"`
	// Reason says why the finding is acceptable, not merely that it is accepted.
	Reason     string `yaml:"reason"`
	AcceptedBy string `yaml:"accepted_by"`
	// Expires is an ISO date (YYYY-MM-DD). The exception holds through the end of
	// that day, in UTC, and fails the build from the day after.
	Expires string `yaml:"expires"`
}

type exceptionsFile struct {
	Exceptions []Exception `yaml:"exceptions"`
}

const dateLayout = "2006-01-02"

// parseExceptions reads the exceptions file and returns the entries that are valid
// today, together with a message for every problem it found. A problem is not an
// entry silently dropped: the caller fails the run on any, and an entry with a
// problem excuses nothing.
//
// Unknown keys are refused, so that a misspelled `expires` cannot leave an
// exception that never lapses.
func parseExceptions(data []byte, today time.Time) ([]Exception, []string) {
	var file exceptionsFile
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return nil, []string{fmt.Sprintf("the exceptions file is not valid: %v", err)}
	}

	// Compare dates, not instants: an exception holds through the whole of its
	// last day.
	y, m, d := today.UTC().Date()
	startOfToday := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	var valid []Exception
	var problems []string
	seen := map[string]bool{}
	for i, e := range file.Exceptions {
		label := e.ID
		if strings.TrimSpace(label) == "" {
			label = fmt.Sprintf("#%d", i+1)
		}
		bad := func(format string, args ...any) {
			problems = append(problems, fmt.Sprintf("%s (%s) %s", label, firstNonEmpty(e.Tool, "no tool"), fmt.Sprintf(format, args...)))
		}
		before := len(problems)

		for field, value := range map[string]string{"id": e.ID, "tool": e.Tool, "reason": e.Reason, "accepted_by": e.AcceptedBy, "expires": e.Expires} {
			if strings.TrimSpace(value) == "" {
				bad("is missing the required field %q", field)
			}
		}
		if e.Tool != "" && !slices.Contains(knownTools, e.Tool) {
			bad("names an unknown tool %q (known: %s)", e.Tool, strings.Join(knownTools, ", "))
		}
		if strings.TrimSpace(e.Expires) != "" {
			expires, err := time.Parse(dateLayout, strings.TrimSpace(e.Expires))
			switch {
			case err != nil:
				bad("has an expires value %q that is not an ISO date (YYYY-MM-DD)", e.Expires)
			case expires.Before(startOfToday):
				bad("expired on %s: renew it with a fresh reason and date, or remove it", e.Expires)
			}
		}
		if key := e.Tool + "\x00" + e.ID; e.ID != "" {
			if seen[key] {
				bad("is a duplicate exception for %s", e.ID)
			}
			seen[key] = true
		}
		if len(problems) == before {
			valid = append(valid, e)
		}
	}
	return valid, problems
}

// matchException finds a valid exception for a finding: the same tool, naming the
// finding's own identifier or one of its aliases. An exception for one tool never
// excuses another tool's finding.
func matchException(f Finding, list []Exception) (Exception, bool) {
	for _, e := range list {
		if e.Tool != f.Tool {
			continue
		}
		if e.ID == f.ID || slices.Contains(f.Aliases, e.ID) {
			return e, true
		}
	}
	return Exception{}, false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
