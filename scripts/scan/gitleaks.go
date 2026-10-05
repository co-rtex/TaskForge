package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// parseGitleaks reads a gitleaks JSON report.
//
// It reads the rule, the file, the line and the commit, and never the secret or
// the matched text: a scanner that printed what it found would put the secret in
// the CI log. (The driver also runs gitleaks with --redact, so the report should
// not carry one; this is the second lock.)
//
// The finding's ID is its fingerprint, "commit:file:rule:line", so an exception is
// as narrow as the finding is: one place in history.
func parseGitleaks(data []byte) (scanResult, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return scanResult{}, errors.New("gitleaks wrote no report: an empty file is not a clean scan")
	}
	var report []struct {
		RuleID      string `json:"RuleID"`
		Description string `json:"Description"`
		File        string `json:"File"`
		StartLine   int    `json:"StartLine"`
		Commit      string `json:"Commit"`
		Fingerprint string `json:"Fingerprint"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return scanResult{}, fmt.Errorf("gitleaks report is not a JSON array: %w", err)
	}

	result := scanResult{Note: "full git history, all refs"}
	for _, r := range report {
		id := firstNonEmpty(r.Fingerprint, fmt.Sprintf("%s:%s:%s:%d", r.Commit, r.File, r.RuleID, r.StartLine))
		commit := r.Commit
		if len(commit) > 8 {
			commit = commit[:8]
		}
		result.Findings = append(result.Findings, Finding{
			Tool: toolGitleaks, ID: id,
			Location: fmt.Sprintf("%s:%d (commit %s)", r.File, r.StartLine, commit),
			Detail:   fmt.Sprintf("%s [rule %s]", firstNonEmpty(r.Description, "possible secret"), r.RuleID),
		})
	}
	return result, nil
}
