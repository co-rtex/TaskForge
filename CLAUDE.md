# CLAUDE.md — Session Operating Guide

Read this first in every TaskForge session. It tells you how to start, how to stay
in scope, and how to hand off. It is deliberately short: **the repository, not this
file and not chat history, is the source of truth for project state.**

---

## Canonical ownership of information

Do not duplicate these. Link to the owner instead.

| Information | Canonical file |
| --- | --- |
| Stable engineering rules, toolchain, commands | [AGENTS.md](AGENTS.md) |
| Session startup and handoff routine | this file |
| Product requirements, V1 scope, guarantees, non-goals | [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md) |
| Architecture, invariants, failure semantics | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) + accepted ADRs |
| Milestone sequence and future work | [docs/ROADMAP.md](docs/ROADMAP.md) |
| What is actually implemented and verified | [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md) |

---

## Startup routine

1. **Preflight.** Inspect branch, default branch, `git status`, staged and untracked
   files, recent history, remotes, and upstream. Confirm `gh auth status` shows
   `co-rtex` with push permission. Confirm `git var GIT_AUTHOR_IDENT` and
   `git var GIT_COMMITTER_IDENT` both resolve to Christian Cortez with an email
   verified for `co-rtex`. Capture the base commit SHA. Change nothing yet.
2. **Read context in order:** `AGENTS.md` → `docs/PROJECT_SPEC.md` →
   `docs/ARCHITECTURE.md` → `docs/ROADMAP.md` → `docs/CURRENT_STATE.md` →
   `docs/adr/README.md` and any relevant accepted ADR.
3. **Read the code, tests, and migrations** that the current task touches.
4. **Reconcile** the handoff you were given against repository truth. If they
   materially conflict, stop and report the conflict — do not silently pick a side.
5. **Execute only the current bounded milestone** named in `docs/ROADMAP.md`.

## While working

- Make routine implementation decisions yourself; document the ones that matter.
- Follow every rule in [AGENTS.md](AGENTS.md) — especially §7 testing, §9
  verification honesty, and §11 Git.
- Stop and ask a single concise question only for a genuine blocker: unavailable
  verified Git identity or push permission, an unresolved repository target, a
  material conflict with canonical documentation, overlapping user changes that
  cannot be preserved, missing credentials, paid infrastructure, a destructive
  operation, a public-API or data-model change outside the accepted architecture, a
  security change that moves a trust boundary, or a change to V1 scope. A blocking
  question states the decision needed, why it blocks, the recommended option, and
  the consequence of each alternative.

## Finishing

1. Run every applicable verification command from [AGENTS.md](AGENTS.md) §4.
   Fix failures and rerun. Do not weaken tests to pass.
2. Update repository truth: always `docs/CURRENT_STATE.md`; plus
   `docs/ROADMAP.md` status, `docs/ARCHITECTURE.md`, ADRs, `AGENTS.md`, or
   `README.md` **only** where implementation changed what is true.
3. Commit coherently, push, and verify the remote head SHA.
4. Open or update the **draft** pull request. Do not merge it.
5. Stop. Do not start the next milestone. Return the handoff below.

## Required reviewer handoff

Your final response must use exactly this structure, with real command output —
never a summary like "all tests passed".

```
TASK COMPLETED
- <bounded milestone actually completed>

BRANCH
- <branch name>

BASE COMMIT
- <full base SHA>

HEAD COMMIT
- <full head SHA>

COMMITS
- <hash> <exact commit message>

PULL REQUEST
- <URL, or precise blocker if creation failed>

FILES CHANGED AND WHY
- <path>: <reason>

PR DIFF STAT
<complete output of git diff --stat <base>...<head>>

PR NAME STATUS
<complete output of git diff --name-status <base>...<head>>

ACTUAL DIFF ACCESS
- PR Files Changed URL or PR URL
- Exact local diff command
- Complete diff included: yes/no
- Uncommitted patch path, size, and checksum when required

VERIFICATION
- <exact command>
  - Result: PASS/FAIL/NOT RUN
  - Summary: <concise real output>

RELEVANT FAILURE LOGS
- <remaining failure output, secrets redacted — or None>

ACCEPTANCE CRITERIA COMPLETED
- ...

ACCEPTANCE CRITERIA INCOMPLETE
- ...

DOCUMENTATION AND CONTEXT UPDATED
- AGENTS.md: yes/no and what changed
- CLAUDE.md: yes/no and what changed
- PROJECT_SPEC.md: yes/no and what changed
- ARCHITECTURE.md: yes/no and what changed
- ROADMAP.md: yes/no and what changed
- CURRENT_STATE.md: yes/no and what changed
- ADR index/records: yes/no and what changed

KNOWN RISKS AND LIMITATIONS
- ...

RECOMMENDED NEXT BOUNDED MILESTONE
- <one milestone only>
- Objective: ...
- Suggested acceptance criteria: ...
- Explicitly not started: yes
```

**Diff access rule.** Always give the PR URL, base and head SHAs, the full
`git diff --stat` and `--name-status` for the PR range, and the exact command to
inspect the diff. Include the complete diff inline when it is roughly 500 changed
non-generated lines or fewer. Otherwise write a binary-capable patch
(`git diff --binary <base>...<head>`) to a path **outside** the repository, never
staged or committed, and report its path, size, and checksum.
