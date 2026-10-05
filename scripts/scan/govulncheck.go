package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

type vulnFrame struct {
	Module   string `json:"module"`
	Version  string `json:"version"`
	Package  string `json:"package"`
	Function string `json:"function"`
	Receiver string `json:"receiver"`
	Position *struct {
		Filename string `json:"filename"`
		Line     int    `json:"line"`
	} `json:"position"`
}

type vulnMessage struct {
	Config *struct {
		ScannerVersion string `json:"scanner_version"`
		GoVersion      string `json:"go_version"`
		DBLastModified string `json:"db_last_modified"`
	} `json:"config"`
	OSV *struct {
		ID      string   `json:"id"`
		Aliases []string `json:"aliases"`
		Summary string   `json:"summary"`
	} `json:"osv"`
	Finding *struct {
		OSV          string      `json:"osv"`
		FixedVersion string      `json:"fixed_version"`
		Trace        []vulnFrame `json:"trace"`
	} `json:"finding"`
	Error json.RawMessage `json:"error"`
}

// parseGovulncheck reads `govulncheck -json` output: a stream of JSON messages.
//
// A finding is reachable only if its trace begins with a called function. A trace
// that names only a module or a package is a vulnerability in something the build
// imports but never calls, and is not a finding here. That is the owner's scope for
// this scanner.
//
// Output without a config message is not govulncheck's output, and an empty stream
// is not a clean scan: both are errors, so that a tool which failed to run cannot
// read as one that found nothing.
func parseGovulncheck(data []byte) (scanResult, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var config *vulnMessage
	aliases := map[string][]string{}
	summaries := map[string]string{}
	best := map[string][]vulnFrame{}
	fixed := map[string]string{}
	var order []string

	for {
		var m vulnMessage
		if err := decoder.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return scanResult{}, fmt.Errorf("govulncheck output is not a JSON stream: %w", err)
		}
		switch {
		case m.Error != nil:
			return scanResult{}, fmt.Errorf("govulncheck reported an error: %s", truncate(string(m.Error), 300))
		case m.Config != nil:
			config = &m
		case m.OSV != nil:
			for _, a := range m.OSV.Aliases {
				if !slices.Contains(aliases[m.OSV.ID], a) {
					aliases[m.OSV.ID] = append(aliases[m.OSV.ID], a)
				}
			}
			summaries[m.OSV.ID] = m.OSV.Summary
		case m.Finding != nil:
			trace := m.Finding.Trace
			if len(trace) == 0 || trace[0].Function == "" {
				continue // imported or required, never called
			}
			id := m.Finding.OSV
			if current, seen := best[id]; !seen {
				order = append(order, id)
				best[id], fixed[id] = trace, m.Finding.FixedVersion
			} else if len(trace) < len(current) {
				best[id], fixed[id] = trace, m.Finding.FixedVersion
			}
		}
	}
	if config == nil {
		return scanResult{}, errors.New("no govulncheck config message in the output: it is empty or not govulncheck's")
	}

	result := scanResult{Note: fmt.Sprintf("%s, scanner %s, database modified %s",
		config.Config.GoVersion, config.Config.ScannerVersion, config.Config.DBLastModified)}
	for _, id := range order {
		trace := best[id]
		vulnerable, caller := trace[0], trace[len(trace)-1]
		detail := firstNonEmpty(summaries[id], "reachable vulnerability")
		if fixed[id] != "" {
			detail += " (fixed in " + fixed[id] + ")"
		}
		result.Findings = append(result.Findings, Finding{
			Tool: toolGovulncheck, ID: id, Aliases: aliases[id],
			Location: fmt.Sprintf("%s, called from %s", describeSymbol(vulnerable), describeCaller(caller)),
			Detail:   detail,
		})
	}
	return result, nil
}

func describeSymbol(f vulnFrame) string {
	name := f.Function
	if f.Receiver != "" {
		name = strings.TrimPrefix(f.Receiver, "*") + "." + name
	}
	return fmt.Sprintf("%s %s.%s@%s", f.Module, f.Package, name, f.Version)
}

func describeCaller(f vulnFrame) string {
	if f.Position == nil {
		return f.Function
	}
	return fmt.Sprintf("%s:%d (%s)", relPath(f.Position.Filename), f.Position.Line, f.Function)
}

// relPath shortens an absolute path under the working directory to a relative one.
func relPath(path string) string {
	if wd, err := os.Getwd(); err == nil {
		if rel, ok := strings.CutPrefix(path, wd+string(os.PathSeparator)); ok {
			return rel
		}
	}
	return path
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
