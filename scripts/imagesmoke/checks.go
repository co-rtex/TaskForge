package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Everything in this file is a judgment about something the smoke observed, as a
// pure function of what it observed. That is what makes each judgment testable
// without a Docker daemon, and what lets a test hand it a broken image's
// observations and see it refuse. The observing is in docker.go and main.go.

const (
	labelSource   = "org.opencontainers.image.source"
	labelRevision = "org.opencontainers.image.revision"
	repoURL       = "https://github.com/co-rtex/TaskForge"
)

// userProblem judges an image's configured user. An image with no USER runs as
// root, so "" is a failure and not a default: the Dockerfile's whole claim is that
// every service runs as the base's non-root user.
func userProblem(user string) string {
	u := strings.TrimSpace(user)
	if u == "" {
		return "the image sets no USER, so it runs as root"
	}
	name, _, _ := strings.Cut(u, ":")
	name = strings.TrimSpace(name)
	if name == "root" {
		return fmt.Sprintf("the image runs as root (%q)", user)
	}
	if uid, err := strconv.Atoi(name); err == nil && uid == 0 {
		return fmt.Sprintf("the image runs as uid 0 (%q)", user)
	}
	return ""
}

func labelProblems(labels map[string]string, wantRevision string) []string {
	var problems []string
	if got := labels[labelSource]; got != repoURL {
		problems = append(problems, fmt.Sprintf("label %s is %q, want %q", labelSource, got, repoURL))
	}
	if got := labels[labelRevision]; got != wantRevision {
		problems = append(problems, fmt.Sprintf("label %s is %q, want the commit %q", labelRevision, got, wantRevision))
	}
	return problems
}

func entrypointProblem(entrypoint []string, service string) string {
	want := "/taskforge-" + service
	if len(entrypoint) != 1 || entrypoint[0] != want {
		return fmt.Sprintf("entrypoint is %q, want exactly [%q] in exec form", entrypoint, want)
	}
	return ""
}

// rejectionProblem judges a run of an image whose environment configuration
// validation must refuse. It needs the process to have actually run and said why:
// 125 to 127 are Docker failing to start it (the daemon, not executable, not
// found), which is not the service refusing its configuration.
func rejectionProblem(exit int, output, want string) string {
	switch {
	case want == "":
		return "no expected message was given, so this check would prove nothing"
	case exit == 0:
		return "the container exited 0 with an invalid configuration"
	case exit >= 125 && exit <= 127:
		return fmt.Sprintf("exit %d is Docker failing to run the image, not the service refusing its configuration", exit)
	case !strings.Contains(output, want):
		return fmt.Sprintf("exit %d, but the output does not contain %q:\n%s", exit, want, truncate(output, 600))
	}
	return ""
}

type outcomeKind int

const (
	outcomeApplied outcomeKind = iota + 1
	outcomeCurrent
)

type migrationResult struct {
	kind    outcomeKind
	applied int
}

// migrationOutcome reads the migrate command's own log. It succeeded if it said
// "migrations complete" (and how many it applied) or "schema already up to date",
// and logged no error. Anything else, including silence, is a failure.
func migrationOutcome(output string) (migrationResult, error) {
	var result migrationResult
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		var line struct {
			Level   string `json:"level"`
			Msg     string `json:"msg"`
			Applied int    `json:"applied"`
			Error   string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) != nil {
			continue
		}
		if strings.EqualFold(line.Level, "ERROR") {
			return migrationResult{}, fmt.Errorf("the migrate command logged an error: %s %s", line.Msg, line.Error)
		}
		switch line.Msg {
		case "migrations complete":
			result = migrationResult{kind: outcomeApplied, applied: line.Applied}
		case "schema already up to date":
			result = migrationResult{kind: outcomeCurrent}
		}
	}
	if result.kind == 0 {
		return migrationResult{}, errors.New(`the migrate command logged neither "migrations complete" nor "schema already up to date"`)
	}
	return result, nil
}

var migrationFile = regexp.MustCompile(`^(\d{4})_.+\.sql$`)

// expectedSchema counts the migration files in dir and finds the highest version.
// These are the files the migrate image embeds, so they are what the database must
// end up recording.
func expectedSchema(dir string) (count, highest int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		m := migrationFile.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		version, _ := strconv.Atoi(m[1])
		count++
		highest = max(highest, version)
	}
	if count == 0 {
		return 0, 0, fmt.Errorf("no NNNN_*.sql migration in %s: an empty expectation would make any schema look right", dir)
	}
	return count, highest, nil
}

func schemaProblem(gotCount, gotHighest, wantCount, wantHighest int) string {
	if gotCount != wantCount || gotHighest != wantHighest {
		return fmt.Sprintf("schema_migrations records %d migrations up to version %d; the image embeds %d up to version %d",
			gotCount, gotHighest, wantCount, wantHighest)
	}
	return ""
}

// binaryProblems judges the file list of an image (as `tar -t` prints an exported
// container) for one service. The base brings files of its own; the check is on
// the taskforge-* names and on there being no shell.
func binaryProblems(listing []string, service string) []string {
	own := "taskforge-" + service
	var problems []string
	found := false
	for _, raw := range listing {
		entry := strings.TrimPrefix(strings.TrimSuffix(raw, "/"), "./")
		base := path.Base(entry)
		switch {
		case entry == own:
			found = true
		case strings.HasPrefix(base, "taskforge-"):
			problems = append(problems, fmt.Sprintf("%q is a TaskForge binary that is not this image's own", entry))
		case (base == "sh" || base == "bash" || base == "ash" || base == "dash") && strings.Contains(entry, "bin/"):
			problems = append(problems, fmt.Sprintf("%q: the image has a shell", entry))
		}
	}
	if !found {
		problems = append(problems, fmt.Sprintf("/%s is not in the image", own))
	}
	sort.Strings(problems)
	return problems
}

var (
	builtMarker       = regexp.MustCompile(`<meta\s+name="taskforge-dashboard"\s+content="built"`)
	placeholderMarker = regexp.MustCompile(`content="placeholder"`)
	scriptSrc         = regexp.MustCompile(`<script[^>]*\ssrc="(/dashboard/assets/[^"]+\.js)"`)
	styleHref         = regexp.MustCompile(`<link[^>]*\shref="(/dashboard/assets/[^"]+\.css)"`)
)

// assetPaths lists the built dashboard's hashed script and stylesheet paths as the
// index page references them.
func assetPaths(html string) (scripts, styles []string) {
	for _, m := range scriptSrc.FindAllStringSubmatch(html, -1) {
		scripts = append(scripts, m[1])
	}
	for _, m := range styleHref.FindAllStringSubmatch(html, -1) {
		styles = append(styles, m[1])
	}
	return scripts, styles
}

// dashboardProblem judges the page the api serves at /dashboard/. The binary embeds
// either the real build or a placeholder notice, and the two mark themselves with
// a <meta name="taskforge-dashboard"> element. The marker alone is not enough: a
// real build also references at least one content-hashed script, which a notice
// page never does.
func dashboardProblem(html string) string {
	switch {
	case placeholderMarker.MatchString(html):
		return "the api serves the placeholder notice: the image was built without the dashboard"
	case !builtMarker.MatchString(html):
		return `the page does not carry <meta name="taskforge-dashboard" content="built">`
	}
	if scripts, _ := assetPaths(html); len(scripts) == 0 {
		return "the page claims to be the build but references no /dashboard/assets/*.js"
	}
	return ""
}

var loopbackHosts = map[string]bool{"127.0.0.1": true, "localhost": true, "::1": true}

// rewriteForDesktop returns env with every loopback host in a *_URL or *_ENDPOINT
// value replaced by host.docker.internal. On Docker Desktop a container's
// 127.0.0.1 is the container, not the machine the infrastructure's ports are
// published on. The listen addresses are not touched: the service must still bind
// its own loopback. The input is not modified.
func rewriteForDesktop(env map[string]string) map[string]string {
	out := make(map[string]string, len(env))
	for k, v := range env {
		out[k] = v
		if !strings.HasSuffix(k, "_URL") && !strings.HasSuffix(k, "_ENDPOINT") {
			continue
		}
		u, err := url.Parse(v)
		if err != nil || u.Host == "" || !loopbackHosts[u.Hostname()] {
			continue
		}
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort("host.docker.internal", port)
		} else {
			u.Host = "host.docker.internal"
		}
		out[k] = u.String()
	}
	return out
}

var builderFrom = regexp.MustCompile(`(?im)^FROM\s+(\S+)\s+AS\s+builder\s*$`)

// builderImage is the Go builder image the Dockerfile pins. The smoke reuses it to
// run curl beside a container, so that no second image is pinned for the purpose.
func builderImage(dockerfile string) (string, error) {
	m := builderFrom.FindStringSubmatch(dockerfile)
	if m == nil {
		return "", errors.New("the Dockerfile has no `FROM <image> AS builder`")
	}
	return m[1], nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
