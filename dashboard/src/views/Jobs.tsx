import { type FormEvent, useCallback, useState } from "react";
import { useClient } from "../api/ClientContext";
import { JOB_STATUSES, type JobStatus } from "../api/types";
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

interface Filters {
  status: JobStatus | undefined;
  queue: string | undefined;
}

/** GET /v1/jobs: newest first, keyset-paginated, filterable by status and queue. */
export function Jobs() {
  const client = useClient();
  const [filters, setFilters] = useState<Filters>({ status: undefined, queue: undefined });
  const pages = useCursorStack();
  const { cursor } = pages;

  const load = useCallback(
    (signal: AbortSignal) =>
      client.listJobs(
        { status: filters.status, queue: filters.queue, limit: PAGE_SIZE, cursor },
        signal,
      ),
    [client, filters, cursor],
  );
  const [resource, reload] = useResource(load);

  const applyFilters = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const status = String(form.get("status") ?? "");
    const queue = String(form.get("queue") ?? "").trim();
    setFilters({
      status: JOB_STATUSES.find((candidate) => candidate === status),
      queue: queue === "" ? undefined : queue,
    });
    // A cursor is a position in one particular listing; it is meaningless
    // under different filters.
    pages.reset();
  };
  const filtered = filters.status !== undefined || filters.queue !== undefined;

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Jobs" onRefresh={reload} />
      <form className="filters" onSubmit={applyFilters} aria-label="Filter jobs">
        <label>
          Status
          <select name="status" defaultValue="">
            <option value="">Any</option>
            {JOB_STATUSES.map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
          </select>
        </label>
        <label>
          Queue
          <input name="queue" type="text" placeholder="Any" autoComplete="off" />
        </label>
        <button type="submit">Apply</button>
      </form>
      {resource.state === "loading" && <LoadingState what="jobs" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" &&
        (resource.data.jobs.length === 0 ? (
          <EmptyState
            title={filtered ? "No jobs match these filters." : "No jobs have been submitted."}
          >
            {pages.page > 0 && (
              <button type="button" onClick={pages.reset}>
                Back to the first page
              </button>
            )}
          </EmptyState>
        ) : (
          <div data-state="loaded">
            <table>
              <thead>
                <tr>
                  <th scope="col">Job</th>
                  <th scope="col">Queue</th>
                  <th scope="col">Type</th>
                  <th scope="col">Status</th>
                  <th scope="col">Priority</th>
                  <th scope="col">Max attempts</th>
                  <th scope="col">Available at</th>
                  <th scope="col">Created</th>
                </tr>
              </thead>
              <tbody>
                {resource.data.jobs.map((job) => (
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
                    <td>{job.priority}</td>
                    <td>{job.max_attempts}</td>
                    <td>
                      <Instant value={job.available_at} />
                    </td>
                    <td>
                      <Instant value={job.created_at} />
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
