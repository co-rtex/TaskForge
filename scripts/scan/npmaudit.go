package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"slices"
	"sort"
	"strings"
)

// severityRank orders npm's severities. The owner's threshold is high: anything
// at or above it blocks, and anything below is not a finding.
var severityRank = map[string]int{"info": 0, "low": 1, "moderate": 2, "high": 3, "critical": 4}

const npmThreshold = "high"

// parseNpmAudit reads `npm audit --json` (report version 2).
//
// Advisories live in the `via` list of the package they are about, as objects. A
// package that is vulnerable only because a dependency is lists that dependency's
// name as a string, so it adds no advisory of its own and no duplicate finding.
func parseNpmAudit(data []byte) (scanResult, error) {
	var report struct {
		AuditReportVersion int `json:"auditReportVersion"`
		Error              *struct {
			Code    string `json:"code"`
			Summary string `json:"summary"`
			Detail  string `json:"detail"`
		} `json:"error"`
		Vulnerabilities map[string]struct {
			Name  string            `json:"name"`
			Range string            `json:"range"`
			Via   []json.RawMessage `json:"via"`
		} `json:"vulnerabilities"`
		Metadata struct {
			Dependencies struct {
				Prod int `json:"prod"`
			} `json:"dependencies"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return scanResult{}, fmt.Errorf("npm audit output is not JSON: %w", err)
	}
	if report.Error != nil {
		return scanResult{}, fmt.Errorf("npm audit failed: %s: %s", report.Error.Code, firstNonEmpty(report.Error.Summary, report.Error.Detail))
	}
	if report.AuditReportVersion == 0 {
		return scanResult{}, errors.New(`npm audit output has no "auditReportVersion": it is not an audit report`)
	}

	names := make([]string, 0, len(report.Vulnerabilities))
	for name := range report.Vulnerabilities {
		names = append(names, name)
	}
	sort.Strings(names)

	result := scanResult{Note: fmt.Sprintf("%d production packages, severity %s or above", report.Metadata.Dependencies.Prod, npmThreshold)}
	seen := map[string]bool{}
	for _, name := range names {
		vuln := report.Vulnerabilities[name]
		for _, raw := range vuln.Via {
			var advisory struct {
				Source   int    `json:"source"`
				Name     string `json:"name"`
				Title    string `json:"title"`
				URL      string `json:"url"`
				Severity string `json:"severity"`
				Range    string `json:"range"`
			}
			if json.Unmarshal(raw, &advisory) != nil || advisory.Source == 0 && advisory.URL == "" {
				continue // a string: a dependency this package is vulnerable through
			}
			if severityRank[advisory.Severity] < severityRank[npmThreshold] {
				continue
			}
			id := path.Base(advisory.URL)
			if !strings.HasPrefix(id, "GHSA-") {
				id = fmt.Sprintf("npm:%d", advisory.Source)
			}
			if seen[id] {
				continue
			}
			seen[id] = true
			pkg := firstNonEmpty(advisory.Name, name)
			result.Findings = append(result.Findings, Finding{
				Tool: toolNpmAudit, ID: id, Location: fmt.Sprintf("%s %s", pkg, firstNonEmpty(advisory.Range, vuln.Range)),
				Detail: fmt.Sprintf("%s (%s)", firstNonEmpty(advisory.Title, "advisory"), advisory.Severity),
			})
		}
	}
	slices.SortFunc(result.Findings, func(a, b Finding) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}
