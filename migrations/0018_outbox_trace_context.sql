-- 0018_outbox_trace_context
--
-- Milestone M6B: OpenTelemetry tracing across the full path. This migration
-- persists the W3C trace context of the transaction that WROTE an outbox
-- event, so the separate process that publishes it later can continue the same
-- trace rather than starting an unrelated one.
--
-- Why this has to be durable rather than in-process state. taskforge-outbox is
-- a different process, and ADR-0004's publish-before-mark window means it may
-- publish an event long after the API process that wrote it has exited. There
-- is no moment at which the publisher could ask the submitter for its trace
-- context: the only way the two halves of one submission can share a trace id
-- is if the first half commits it alongside the event, in the same transaction,
-- exactly as every other durable fact in this system is recorded.
--
-- Nullable, and deliberately NOT backfilled. An event written before this
-- migration has no trace context that could be reconstructed -- unlike
-- migration 0011's notification generations, which were derivable from the real
-- ordering of historical events, a trace id is not derivable from anything. A
-- NULL here means "start a new root span", which is a correct and complete
-- answer rather than a degraded one. This mirrors 0015's
-- worker_sessions.worker_key_id: a NULL is simply never treated as a value.
--
-- No index. Nothing queries by traceparent, and nothing should: these columns
-- are carried alongside an event, never used to find one. AGENTS.md section 6
-- requires a justifying query per index, and there is none.
--
-- Unlike M6A's 0017 this migration does add columns, so it is not index-only.
-- It still changes no existing row: ADD COLUMN with no DEFAULT is a catalog-only
-- operation in PostgreSQL 11+, so no table rewrite occurs and every existing
-- event keeps the exact bytes it had.

ALTER TABLE outbox_events
    -- The W3C traceparent, pinned to its exact wire shape:
    -- version(2) "-" trace-id(32) "-" parent-id(16) "-" trace-flags(2), all
    -- lowercase hex. The propagator already drops a malformed inbound value
    -- and starts a fresh root before anything reaches here, so in the normal
    -- path this constraint never fires. It exists as a backstop: a future
    -- caller that hand-built this column would be refused by the database
    -- rather than quietly publishing a corrupt trace header to the broker.
    ADD COLUMN traceparent TEXT
        CHECK (traceparent ~ '^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$'),

    -- tracestate is vendor-specific key-value data whose content this system
    -- neither produces nor interprets -- it only passes it through. It is
    -- bounded for the same reason job_attempts.error_message is: it originates
    -- in a caller-supplied header, and an unbounded string from outside must
    -- never be stored unbounded. 512 is comfortably above the W3C
    -- recommendation of 32 list members.
    ADD COLUMN tracestate TEXT
        CHECK (length(tracestate) <= 512);

COMMENT ON COLUMN outbox_events.traceparent IS
    'W3C traceparent of the transaction that wrote this event, so the separate '
    'publisher process continues that trace instead of starting a new one. '
    'NULL means "start a new root span" -- the value for every event written '
    'before milestone M6B, and for the server-initiated notifications (replay, '
    'scheduler promotion, abandonment requeue) that are not a continuation of '
    'any client request.';

COMMENT ON COLUMN outbox_events.tracestate IS
    'W3C tracestate accompanying traceparent. Passed through verbatim; this '
    'system never produces or interprets its contents. Bounded because it '
    'originates in a caller-supplied header.';
