# ADR-0005: ElasticMQ as the local SQS-compatible broker

- **Status:** Accepted
- **Date:** 2026-08-29

## Context

TaskForge's cloud direction is AWS SQS Standard. Local development must exercise
**real** broker semantics — long polling, visibility timeouts, at-least-once delivery,
no ordering guarantee — because those semantics are exactly what the outbox publisher
and claim path must tolerate. Testing against an in-memory fake would prove nothing
about the behavior that matters.

Constraints: V1 must run entirely locally, with no AWS account and no cost, using only
Git, Go, Docker, Docker Compose, and Make.

## Decision

Use **ElasticMQ** as the local broker, running in Docker Compose and spoken to with the
standard AWS SQS SDK over a custom endpoint.

TaskForge code never imports ElasticMQ. All broker access goes through a
provider-neutral interface exposing only the capabilities actually needed:

- **publish**
- **long-poll receive**
- **acknowledge / delete**

The interface deliberately does **not** define a universal `Nack`, because SQS provides
no such primitive; redelivery is expressed through the visibility timeout. Inventing a
`Nack` would create an abstraction that cannot be honored in production.

Integration tests run against real ElasticMQ, not a mock.

## Alternatives considered

**LocalStack.** Broader AWS emulation and a reasonable choice. Rejected as the default:
it is substantially heavier to start and pull for the one service TaskForge needs, and
the fuller feature set invites accidental dependence on AWS services outside V1 scope.

**Real AWS SQS during development.** Rejected: requires an AWS account and credentials
to run tests, costs money, makes CI depend on a network service, and violates the
"clean clone runs locally" requirement.

**RabbitMQ, NATS, or Kafka.** Each is a fine broker with richer semantics than SQS.
Rejected: TaskForge's design is deliberately built to be correct under the *weakest*
useful delivery guarantee. Developing against a stronger broker risks accidentally
depending on ordering or true nack semantics that SQS will not provide in production.

**An in-memory Go fake as the only broker.** Rejected as the primary: it cannot prove
migrations, real network failure, restart behavior, or genuine duplicate delivery. A
fake may still be used to isolate unit-level domain logic, but never as the sole
evidence that delivery works.

**PostgreSQL-only, no broker.** Viable (see
[ADR-0003](0003-pull-based-claim-with-broker-notification.md)), but it removes the
outbox-to-broker path that this project exists to demonstrate.

## Consequences

**Positive.** Local development exercises production-like SQS semantics at zero cost.
The same AWS SDK code path runs locally and in AWS — only the endpoint changes. Broker
outage and recovery are testable by stopping and starting one container. The narrow
interface keeps the provider swappable.

**Negative.** ElasticMQ is not AWS SQS: it is a separate implementation, and behavioral
differences at the edges (throttling, quota errors, precise redrive behavior) will not
be caught locally. That gap is closed only by a real deployment smoke test, which is
M8 work.

**Constraint carried forward.** Because TaskForge targets SQS Standard, no design may
depend on FIFO ordering, message deduplication, or exactly-once delivery from the
broker. That constraint is already reflected in the pull-based claim design.
