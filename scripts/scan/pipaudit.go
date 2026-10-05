package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// parsePipAudit reads `pip-audit --format json`.
//
// A dependency pip-audit could not audit (it carries a skip_reason) is reported as
// a finding of its own. A run that silently skipped a package has not shown that
// package is clean.
func parsePipAudit(data []byte) (scanResult, error) {
	var report struct {
		Dependencies *[]struct {
			Name       string `json:"name"`
			Version    string `json:"version"`
			SkipReason string `json:"skip_reason"`
			Vulns      []struct {
				ID          string   `json:"id"`
				FixVersions []string `json:"fix_versions"`
				Aliases     []string `json:"aliases"`
				Description string   `json:"description"`
			} `json:"vulns"`
		} `json:"dependencies"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return scanResult{}, fmt.Errorf("pip-audit output is not a JSON report: %w", err)
	}
	if report.Dependencies == nil {
		return scanResult{}, errors.New(`pip-audit output has no "dependencies" list: it is not an audit report`)
	}

	result := scanResult{Note: fmt.Sprintf("%d packages in the SDK's resolved runtime tree", len(*report.Dependencies))}
	for _, dep := range *report.Dependencies {
		if dep.SkipReason != "" {
			result.Findings = append(result.Findings, Finding{
				Tool: toolPipAudit, ID: "pip-audit-skipped:" + dep.Name, Location: dep.Name,
				Detail: "not audited: " + dep.SkipReason,
			})
		}
		for _, v := range dep.Vulns {
			detail := truncate(firstNonEmpty(v.Description, "known vulnerability"), 120)
			if len(v.FixVersions) > 0 {
				detail += " (fixed in " + strings.Join(v.FixVersions, ", ") + ")"
			}
			result.Findings = append(result.Findings, Finding{
				Tool: toolPipAudit, ID: v.ID, Aliases: v.Aliases,
				Location: dep.Name + "==" + dep.Version, Detail: detail,
			})
		}
	}
	return result, nil
}
