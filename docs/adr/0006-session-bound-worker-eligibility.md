# ADR-0006: Session-bound worker eligibility

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

The pull-based claim decision needs a worker's group, concurrency limit,
capabilities, and trusted handler types. ADR-0003 described a worker presenting
capabilities on each claim. Treating those mutable request fields as authority would
let one process change identity between claims, overstate its abilities, or bypass
the capacity declared when it registered. It would also make an HTTP retry depend on
caller reconstruction rather than durable session state.

TaskForge also needs to distinguish a stable logical worker from one operating-system
process lifetime. A restarted process must not inherit the old process's leases.

## Decision

This decision supersedes ADR-0003's caller-presented capability clause while leaving
its pull-based architecture intact.

Registration creates or reuses a stable logical `worker_id` by scope and name, then
creates one client-generated `worker_session_id` for that process boot. The session
stores immutable worker group, concurrency limit, normalized capabilities, and the
trusted job types compiled into that binary.

A claim carries only worker id, session id, queue, and durable notification event id.
The control plane locks the current session and loads its eligibility fields from
PostgreSQL. A new boot for the same logical worker marks the prior current session
offline. Old leases remain bound to their original session and continue consuming
logical-worker capacity until reconciliation; they are never transferred to the new
boot.

## Alternatives considered

**Present capabilities and limits on every claim.** Rejected because caller-supplied
mutable authority can drift from registration and complicates idempotent replay.

**Use one worker id as both logical and process identity.** Rejected because a restart
could accidentally renew or complete work owned by the dead process.

**Copy old leases to the replacement session.** Rejected because process replacement
would become an authority-escalation path and stale completion could no longer be
fenced reliably.

## Consequences

**Positive.** Eligibility and capacity are auditable durable state; claim requests
stay small; replacement boots are fenced; worker restart cannot erase active
capacity; the trusted-handler boundary is enforced before assignment.

**Negative.** Registration data is immutable for a process lifetime. Changing group,
capabilities, handler set, or concurrency requires a new session. Until M3 heartbeat
and reconciliation, an old active lease can intentionally keep replacement capacity
reserved.
