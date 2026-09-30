import { useCallback } from "react";
import { useClient } from "../api/ClientContext";
import { useResource } from "../api/useResource";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { Instant } from "../components/Instant";
import { LoadingState } from "../components/LoadingState";
import { Pager, useCursorStack } from "../components/Pager";
import { StatusBadge } from "../components/StatusBadge";
import { ViewHeader } from "../components/ViewHeader";
import { PAGE_SIZE } from "../format";
import { hrefFor, Link } from "../router";

/**
 * GET /v1/dlq: TaskForge's own logical dead-letter queue, newest first. Read
 * only. Replay is a write that requires a caller-chosen Idempotency-Key, and
 * this dashboard deliberately performs no writes -- see docs/adr/0017.
 */
export function DLQ() {
  const client = useClient();
  const pages = useCursorStack();
  const { cursor } = pages;
  const load = useCallback(
    (signal: AbortSignal) => client.listDLQ({ limit: PAGE_SIZE, cursor }, signal),
    [client, cursor],
  );
  const [resource, reload] = useResource(load);

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Dead-letter queue" onRefresh={reload} />
      <p className="note">
        Read-only. To replay an entry, use <code>taskforge-cli dlq replay</code> with an idempotency
        key.
      </p>
      {resource.state === "loading" && <LoadingState what="dead-lettered jobs" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" &&
        (resource.data.entries.length === 0 ? (
          <EmptyState title="The dead-letter queue is empty." />
        ) : (
          <div data-state="loaded">
            <table>
              <thead>
                <tr>
                  <th scope="col">Job</th>
                  <th scope="col">Queue</th>
                  <th scope="col">Type</th>
                  <th scope="col">Reason</th>
                  <th scope="col">Last attempt</th>
                  <th scope="col">Failure class</th>
                  <th scope="col">Error</th>
                  <th scope="col">Replays</th>
                  <th scope="col">Dead-lettered</th>
                </tr>
              </thead>
              <tbody>
                {resource.data.entries.map((entry) => (
                  <tr key={entry.id}>
                    <td>
                      <Link href={hrefFor({ name: "job", jobId: entry.job_id })}>
                        <code>{entry.job_id}</code>
                      </Link>
                    </td>
                    <td>{entry.queue}</td>
                    <td>{entry.job_type}</td>
                    <td>
                      <StatusBadge status={entry.reason} />
                    </td>
                    <td>
                      {entry.attempt_number == null
                        ? "—"
                        : `#${entry.attempt_number} of ${entry.max_attempts}`}
                      {entry.attempt_status != null && (
                        <>
                          {" "}
                          <StatusBadge status={entry.attempt_status} />
                        </>
                      )}
                    </td>
                    <td>{entry.failure_class ?? "—"}</td>
                    <td>
                      {entry.error_code != null && <code>{entry.error_code}</code>}
                      {entry.error_message != null && <div>{entry.error_message}</div>}
                      {entry.error_code == null && entry.error_message == null && "—"}
                    </td>
                    <td>{entry.replay_count}</td>
                    <td>
                      <Instant value={entry.created_at} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <Pager
              page={pages.page}
              nextCursor={resource.data.next_cursor}
              onNext={pages.next}
              onPrevious={pages.previous}
            />
          </div>
        ))}
    </section>
  );
}
