-- 0008_document_renewal_identity_scope
--
-- Corrects an overstatement, forward-only.
--
-- Migration 0006's header says renewal identities are "globally unique". That
-- reads as a lifetime guarantee, and the index does not provide one: it is a
-- partial unique index over each lease's CURRENT last_renewal_request_id, so it
-- constrains the identities in force at any instant and nothing more. Once a
-- lease renews again its previous identity leaves the index and another lease may
-- reuse it.
--
-- 0006 has been applied, so it is immutable (AGENTS.md section 6) and the runner
-- verifies its checksum. The accurate statement is therefore attached to the
-- index itself, where an operator inspecting the schema will actually find it,
-- rather than left only in prose that the database does not carry.
--
-- No schema changes. Nothing about the guarantee changed; only its description.
-- See docs/adr/0008-fenced-idempotent-lease-renewal.md, "Scope note".

COMMENT ON INDEX leases_last_renewal_request_id_idx IS
    'No two leases may hold the same renewal identity AT THE SAME TIME. Only each '
    'lease''s current last_renewal_request_id is stored, so this constrains live '
    'identities, not every identity ever used: once a lease renews again its '
    'previous id leaves this index and another lease may reuse it. That is '
    'deliberate. An identity authorizes nothing by itself — extension is decided '
    'by the per-lease expected_renewal_version check under the full five-part '
    'fence — and a superseded id replayed against its original lease no longer '
    'matches that lease''s generation and is refused.';
