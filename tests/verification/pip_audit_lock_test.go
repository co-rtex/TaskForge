package verification

// security/pip-audit.requirements.txt is the hash-locked tree of the SCANNER, pip-audit
// itself, installed with `pip install --require-hashes --no-deps -r`. pip would refuse
// an unhashed line under --require-hashes anyway; this holds the shape in review, with
// a message that says which line, before a scan run has to fail to say so.
//
// The pure function below judges the file's text. It is tested on examples that are not
// the real file, each invalid variant being one the lock must never contain, and then
// applied to the real file.

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const pipAuditLockPath = "security/pip-audit.requirements.txt"

var (
	lockRequirement = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*)==([A-Za-z0-9][A-Za-z0-9._+!-]*)(\s*;\s*\S.*)?$`)
	lockHash        = regexp.MustCompile(`--hash=sha256:[0-9a-f]{64}`)
)

// logicalLines joins pip's backslash continuations and drops comments and blank lines.
func logicalLines(text string) []string {
	var out []string
	var pending string
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if pending == "" && (line == "" || strings.HasPrefix(line, "#")) {
			continue
		}
		if strings.HasSuffix(line, `\`) {
			pending += strings.TrimSpace(strings.TrimSuffix(line, `\`)) + " "
			continue
		}
		out = append(out, strings.TrimSpace(pending+line))
		pending = ""
	}
	if strings.TrimSpace(pending) != "" {
		out = append(out, strings.TrimSpace(pending))
	}
	return out
}

// lockProblems judges a hash-locked requirements file: every requirement is
// name==version (with at most an environment marker) and carries at least one
// --hash=sha256 digest, and nothing else may appear on a line: no URL, no editable
// install, no option, no range.
func lockProblems(text string) []string {
	var problems []string
	lines := logicalLines(text)
	if len(lines) == 0 {
		return []string{"the lock holds no requirements"}
	}
	foundPipAudit := false
	for _, line := range lines {
		hashes := lockHash.FindAllString(line, -1)
		head := strings.TrimSpace(lockHash.ReplaceAllString(line, ""))
		m := lockRequirement.FindStringSubmatch(head)
		switch {
		case m == nil:
			problems = append(problems, fmt.Sprintf("%q is not name==version (with at most an environment marker) followed by --hash options", truncateTo(line, 60)))
		case len(hashes) == 0:
			problems = append(problems, fmt.Sprintf("%s==%s carries no --hash=sha256:<digest>", m[1], m[2]))
		default:
			if strings.EqualFold(m[1], "pip-audit") {
				foundPipAudit = true
			}
		}
	}
	if !foundPipAudit {
		problems = append(problems, "pip-audit itself is not pinned in its own lock")
	}
	return problems
}

const (
	hashA = "--hash=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	hashB = "--hash=sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

func TestPipAuditLock_AWellFormedLockIsAccepted(t *testing.T) {
	text := "# header\n\n" +
		"pip-audit==2.10.1 \\\n    " + hashA + " \\\n    " + hashB + "\n" +
		"requests==2.34.2 \\\n    " + hashA + "\n" +
		"typing-extensions==4.16.0 ; python_full_version < '3.13' \\\n    " + hashA + "\n"

	require.Empty(t, lockProblems(text))
}

func TestPipAuditLock_EveryKindOfLooseLineIsRefused(t *testing.T) {
	const ok = "pip-audit==2.10.1 \\\n    " + hashA + "\n"
	for name, tc := range map[string]struct {
		text string
		want string
	}{
		"an unpinned requirement":         {ok + "requests \\\n    " + hashA + "\n", "requests"},
		"a range":                         {ok + "requests>=2.0 \\\n    " + hashA + "\n", "requests"},
		"a compatible-release pin":        {ok + "requests~=2.34 \\\n    " + hashA + "\n", "requests"},
		"a pin with no hash":              {ok + "requests==2.34.2\n", "carries no --hash"},
		"a pin whose hash is too short":   {ok + "requests==2.34.2 --hash=sha256:abc123\n", "requests"},
		"a pin whose hash is not sha256":  {ok + "requests==2.34.2 --hash=md5:0123456789abcdef0123456789abcdef\n", "requests"},
		"an upper-case digest":            {ok + "requests==2.34.2 --hash=sha256:" + strings.ToUpper(strings.TrimPrefix(hashA, "--hash=sha256:")) + "\n", "requests"},
		"a URL requirement":               {ok + "requests @ https://example.invalid/requests.whl \\\n    " + hashA + "\n", "requests"},
		"an editable install":             {ok + "-e . \\\n    " + hashA + "\n", "-e"},
		"an index option":                 {ok + "--index-url https://example.invalid/simple\n", "index-url"},
		"an extra index option":           {ok + "--extra-index-url https://example.invalid/simple\n", "extra-index-url"},
		"a find-links option":             {ok + "--find-links https://example.invalid/\n", "find-links"},
		"a nested requirements include":   {ok + "-r other.txt\n", "-r"},
		"pip-audit missing from its lock": {"requests==2.34.2 \\\n    " + hashA + "\n", "pip-audit itself"},
		"an empty lock":                   {"# only a comment\n", "no requirements"},
		"trailing words after a hash":     {ok + "requests==2.34.2 " + hashA + " --no-binary :all:\n", "requests"},
	} {
		problems := lockProblems(tc.text)
		require.NotEmptyf(t, problems, "%s must be refused", name)
		require.Containsf(t, strings.Join(problems, "\n"), tc.want, "%s\n%v", name, problems)
	}
}

func TestPipAuditLock_TheCommittedLockIsWellFormed(t *testing.T) {
	text := readRepoFile(t, pipAuditLockPath)

	require.Empty(t, lockProblems(text))
	require.Greater(t, len(logicalLines(text)), 10, "pip-audit's whole transitive tree, not only pip-audit")
}
