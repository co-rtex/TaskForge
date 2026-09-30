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
import { formatSeconds, PAGE_SIZE } from "../format";

/**
 * GET /v1/workers: every logical worker joined to its most recent session,
 * whatever that session's status. Every status is listed and labeled in text,
 * never filtered out. A crashed process appears UNHEALTHY -- the row an
 * operator most often opens this page to find. A worker whose boot was
 * replaced appears with its NEWEST session's status (HEALTHY in practice, as
 * M6A's integration test pins), not the OFFLINE session it replaced.
 *
 * Heartbeat age is shown as the API measured it and is not judged here: the
 * threshold that decides a worker is dead lives in the reconciler's
 * configuration, and the status column already reports its verdict.
 */
export function Workers() {
  const client = useClient();
  const pages = useCursorStack();
  const { cursor } = pages;
  const load = useCallback(
    (signal: AbortSignal) => client.listWorkers({ limit: PAGE_SIZE, cursor }, signal),
    [client, cursor],
  );
  const [resource, reload] = useResource(load);

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Workers" onRefresh={reload} />
      {resource.state === "loading" && <LoadingState what="workers" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" &&
        (resource.data.workers.length === 0 ? (
          <EmptyState title="No workers have registered.">
            <p>A worker appears here once taskforge-worker registers a session.</p>
          </EmptyState>
        ) : (
          <div data-state="loaded">
            <table>
              <thead>
                <tr>
                  <th scope="col">Worker</th>
                  <th scope="col">Status</th>
                  <th scope="col">Group</th>
                  <th scope="col">Host</th>
                  <th scope="col">Active leases / limit</th>
                  <th scope="col">Capabilities</th>
                  <th scope="col">Job types</th>
                  <th scope="col">Heartbeat age</th>
                  <th scope="col">Last heartbeat</th>
                  <th scope="col">Session ended</th>
                </tr>
              </thead>
              <tbody>
                {resource.data.workers.map((worker) => (
                  <tr key={worker.id} className={`worker-${worker.status.toLowerCase()}`}>
                    <td>
                      {worker.name}
                      <br />
                      <code className="muted">{worker.id}</code>
                    </td>
                    <td>
                      <StatusBadge status={worker.status} />
                    </td>
                    <td>{worker.worker_group}</td>
                    <td>{worker.hostname}</td>
                    <td>
                      {worker.active_leases} / {worker.concurrency_limit}
                    </td>
                    <td>{worker.capabilities.join(", ") || "None"}</td>
                    <td>{worker.supported_job_types.join(", ") || "None"}</td>
                    <td>{formatSeconds(worker.heartbeat_age_seconds)}</td>
                    <td>
                      <Instant value={worker.last_heartbeat_at} />
                    </td>
                    <td>
                      <Instant value={worker.ended_at} />
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
