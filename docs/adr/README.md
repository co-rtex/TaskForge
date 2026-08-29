# Architecture Decision Records

One record per decision that constrains future work or carries a real tradeoff.
Small coding choices do not get an ADR.

Each record contains: **Context**, **Decision**, **Alternatives considered**,
**Consequences**, and **Status** (`Proposed` / `Accepted` / `Superseded`).

Records are immutable once accepted. To change a decision, add a new ADR that
supersedes the old one and update the old one's status.

| # | Title | Status | Date | Purpose |
| --- | --- | --- | --- | --- |
| [0001](0001-postgresql-as-authoritative-state.md) | PostgreSQL as authoritative state | Accepted | 2026-08-29 | All control-plane state lives in PostgreSQL, not the broker or memory. |
| [0002](0002-at-least-once-execution-semantics.md) | At-least-once execution semantics | Accepted | 2026-08-29 | TaskForge promises at-least-once, never exactly-once. |
| [0003](0003-pull-based-claim-with-broker-notification.md) | Pull-based claim with broker notification | Accepted | 2026-08-29 | The broker signals availability; SQL decides who gets which job. |
| [0004](0004-transactional-outbox.md) | Transactional outbox for broker delivery | Accepted | 2026-08-29 | State changes and their notifications commit atomically. |
| [0005](0005-elasticmq-for-local-broker.md) | ElasticMQ as the local SQS-compatible broker | Accepted | 2026-08-29 | Real SQS semantics locally with no AWS account and no cost. |
