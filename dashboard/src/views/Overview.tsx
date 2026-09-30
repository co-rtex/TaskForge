import { useCallback } from "react";
import { useClient } from "../api/ClientContext";
import {
  type JobSummary,
  NON_TERMINAL_JOB_STATUSES,
  type Queue,
  WORKER_STATUSES,
  type Worker,
} from "../api/types";
import { useResource } from "../api/useResource";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { Instant } from "../components/Instant";
import { LoadingState } from "../components/LoadingState";
import { StatusBadge } from "../components/StatusBadge";
import { ViewHeader } from "../components/ViewHeader";
import { countBy, OVERVIEW_JOB_LIMIT, OVERVIEW_WORKER_LIMIT, total } from "../format";
import { hrefFor, Link } from "../router";

/**
 * A summary composed from three existing reads -- queues, the first page of
 * workers, and the most recent jobs. There is no overview endpoint, and this
 * view computes nothing the API could contradict: every figure is a count of
 * rows it was actually sent, and a truncated read says so.
 */
export function Overview() {
  const client = useClient();
  const load = useCallback(
    async (signal: AbortSignal) => {
      const [queues, workers, jobs] = await Promise.all([
        client.listQueues(signal),
        client.listWorkers({ limit: OVERVIEW_WORKER_LIMIT }, signal),
        client.listJobs({ limit: OVERVIEW_JOB_LIMIT }, signal),
      ]);
      return { queues, workers, jobs };
    },
    [client],
  );
  const [resource, reload] = useResource(load);

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Overview" onRefresh={reload} />
      {resource.state === "loading" && <LoadingState what="the overview" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" && (
        <div data-state="loaded" className="overview">
          <OverviewQueues queues={resource.data.queues.queues} />
          <OverviewWorkers
            workers={resource.data.workers.workers}
            truncated={resource.data.workers.next_cursor !== undefined}
          />
          <OverviewJobs jobs={resource.data.jobs.jobs} />
        </div>
      )}
    </section>
  );
}

function OverviewQueues({ queues }: { queues: readonly Queue[] }) {
  const depth = Object.fromEntries(
    NON_TERMINAL_JOB_STATUSES.map((status) => [
      status,
      queues.reduce((sum, queue) => sum + queue.depth[status], 0),
    ]),
  );
  return (
    <article className="panel" aria-labelledby="overview-queues">
      <h2 id="overview-queues">
        <Link href={hrefFor({ name: "queues" })}>Queues</Link>
      </h2>
      {queues.length === 0 ? (
        <EmptyState title="No queues exist." />
      ) : (
        <>
          <p>
            {queues.length} {queues.length === 1 ? "queue" : "queues"}, {total(depth)} non-terminal{" "}
            {total(depth) === 1 ? "job" : "jobs"} visible to this key.
          </p>
          <dl className="counts">
            {NON_TERMINAL_JOB_STATUSES.map((status) => (
              <div key={status}>
                <dt>
                  <StatusBadge status={status} />
                </dt>
                <dd>{depth[status]}</dd>
              </div>
            ))}
          </dl>
        </>
      )}
    </article>
  );
}

function OverviewWorkers({
  workers,
  truncated,
}: {
  workers: readonly Worker[];
  truncated: boolean;
}) {
  const byStatus = countBy(workers, WORKER_STATUSES, (worker) => worker.status);
  return (
    <article className="panel" aria-labelledby="overview-workers">
      <h2 id="overview-workers">
        <Link href={hrefFor({ name: "workers" })}>Workers</Link>
      </h2>
      {workers.length === 0 ? (
        <EmptyState title="No workers have registered." />
      ) : (
        <>
          <p>
            {truncated
              ? `The first ${workers.length} workers, by status. More exist; see Workers.`
              : `${workers.length} ${workers.length === 1 ? "worker" : "workers"}, by status.`}
          </p>
          <dl className="counts">
            {WORKER_STATUSES.map((status) => (
              <div key={status}>
                <dt>
                  <StatusBadge status={status} />
                </dt>
                <dd>{byStatus[status]}</dd>
              </div>
            ))}
          </dl>
        </>
      )}
    </article>
  );
}

function OverviewJobs({ jobs }: { jobs: readonly JobSummary[] }) {
  return (
    <article className="panel panel-wide" aria-labelledby="overview-jobs">
      <h2 id="overview-jobs">
        <Link href={hrefFor({ name: "jobs" })}>Most recent jobs</Link>
      </h2>
      {jobs.length === 0 ? (
        <EmptyState title="No jobs have been submitted." />
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">Job</th>
              <th scope="col">Queue</th>
              <th scope="col">Type</th>
              <th scope="col">Status</th>
              <th scope="col">Created</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr key={job.id}>
                <td>
                  <Link href={hrefFor({ name: "job", jobId: job.id })}>
                    <code>{job.id}</code>
                  </Link>
                </td>
                <td>{job.queue}</td>
                <td>{job.job_type}</td>
                <td>
                  <StatusBadge status={job.status} />
                </td>
                <td>
                  <Instant value={job.created_at} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </article>
  );
}
