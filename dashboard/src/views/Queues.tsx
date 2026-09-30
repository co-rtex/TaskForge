import { useCallback } from "react";
import { useClient } from "../api/ClientContext";
import { NON_TERMINAL_JOB_STATUSES } from "../api/types";
import { useResource } from "../api/useResource";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { StatusBadge } from "../components/StatusBadge";
import { ViewHeader } from "../components/ViewHeader";
import { total } from "../format";

/**
 * GET /v1/queues. Two numbers here look comparable and are not, and the table
 * keeps them apart on purpose: depth counts only this key's scope, while
 * max_concurrency is a queue-wide limit shared by every scope. Showing one as
 * a fraction of the other would state a utilization the API never reported.
 */
export function Queues() {
  const client = useClient();
  const load = useCallback((signal: AbortSignal) => client.listQueues(signal), [client]);
  const [resource, reload] = useResource(load);

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Queues" onRefresh={reload} />
      {resource.state === "loading" && <LoadingState what="queues" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" &&
        (resource.data.queues.length === 0 ? (
          <EmptyState title="No queues exist." />
        ) : (
          <div data-state="loaded">
            <table>
              <caption>
                Depth is this API key's non-terminal jobs only. Max concurrency is queue-wide,
                across every scope, and is not comparable with depth.
              </caption>
              <thead>
                <tr>
                  <th scope="col">Queue</th>
                  <th scope="col">Worker group</th>
                  {NON_TERMINAL_JOB_STATUSES.map((status) => (
                    <th scope="col" key={status}>
                      <StatusBadge status={status} />
                    </th>
                  ))}
                  <th scope="col">Depth (this key)</th>
                  <th scope="col">Max concurrency (queue-wide, all scopes)</th>
                </tr>
              </thead>
              <tbody>
                {resource.data.queues.map((queue) => (
                  <tr key={queue.name}>
                    <th scope="row">{queue.name}</th>
                    <td>{queue.worker_group}</td>
                    {NON_TERMINAL_JOB_STATUSES.map((status) => (
                      <td key={status}>{queue.depth[status]}</td>
                    ))}
                    <td>{total(queue.depth)}</td>
                    <td>{queue.max_concurrency}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ))}
    </section>
  );
}
