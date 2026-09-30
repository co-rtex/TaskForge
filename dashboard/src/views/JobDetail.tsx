import { useCallback } from "react";
import { useClient } from "../api/ClientContext";
import { useResource } from "../api/useResource";
import { AttemptTimeline } from "../components/AttemptTimeline";
import { EmptyState } from "../components/EmptyState";
import { ErrorState } from "../components/ErrorState";
import { Instant } from "../components/Instant";
import { LoadingState } from "../components/LoadingState";
import { StatusBadge } from "../components/StatusBadge";
import { ViewHeader } from "../components/ViewHeader";
import { formatPayload } from "../format";
import { hrefFor, Link } from "../router";

/**
 * GET /v1/jobs/{job_id} and GET /v1/jobs/{job_id}/attempts, read together. The
 * job is the only read in this dashboard that carries a payload, and it is
 * rendered as text: React escapes it, and nothing here interprets it.
 */
export function JobDetail({ jobId }: { jobId: string }) {
  const client = useClient();
  const load = useCallback(
    async (signal: AbortSignal) => {
      const [job, attempts] = await Promise.all([
        client.getJob(jobId, signal),
        client.listAttempts(jobId, signal),
      ]);
      return { job, attempts: attempts.attempts };
    },
    [client, jobId],
  );
  const [resource, reload] = useResource(load);

  return (
    <section aria-labelledby="view-title">
      <ViewHeader title="Job" onRefresh={reload}>
        <Link href={hrefFor({ name: "jobs" })}>All jobs</Link>
      </ViewHeader>
      <p>
        <code>{jobId}</code>
      </p>
      {resource.state === "loading" && <LoadingState what="the job and its attempts" />}
      {resource.state === "error" && <ErrorState error={resource.error} onRetry={reload} />}
      {resource.state === "loaded" && (
        <div data-state="loaded">
          <article className="panel" aria-labelledby="job-fields">
            <h2 id="job-fields">Job</h2>
            <dl className="fields">
              <dt>Status</dt>
              <dd>
                <StatusBadge status={resource.data.job.status} />
              </dd>
              <dt>Queue</dt>
              <dd>{resource.data.job.queue}</dd>
              <dt>Type</dt>
              <dd>{resource.data.job.job_type}</dd>
              <dt>Priority</dt>
              <dd>{resource.data.job.priority}</dd>
              <dt>Max attempts</dt>
              <dd>{resource.data.job.max_attempts}</dd>
              <dt>Timeout</dt>
              <dd>{resource.data.job.timeout_seconds}s per attempt</dd>
              <dt>Required capabilities</dt>
              <dd>
                {resource.data.job.required_capabilities.length === 0
                  ? "None"
                  : resource.data.job.required_capabilities.join(", ")}
              </dd>
              <dt>Scheduled at</dt>
              <dd>
                <Instant value={resource.data.job.scheduled_at} />
              </dd>
              <dt>Available at</dt>
              <dd>
                <Instant value={resource.data.job.available_at} />
              </dd>
              <dt>Cancel requested at</dt>
              <dd>
                <Instant value={resource.data.job.cancel_requested_at} />
              </dd>
              <dt>Replayed from</dt>
              <dd>
                {resource.data.job.replayed_from_job_id === null ? (
                  <span className="muted">—</span>
                ) : (
                  <Link
                    href={hrefFor({ name: "job", jobId: resource.data.job.replayed_from_job_id })}
                  >
                    <code>{resource.data.job.replayed_from_job_id}</code>
                  </Link>
                )}
              </dd>
              <dt>Created</dt>
              <dd>
                <Instant value={resource.data.job.created_at} />
              </dd>
              <dt>Updated</dt>
              <dd>
                <Instant value={resource.data.job.updated_at} />
              </dd>
            </dl>
            <h3>Payload</h3>
            <pre className="payload">{formatPayload(resource.data.job.payload)}</pre>
          </article>
          <article className="panel" aria-labelledby="job-attempts">
            <h2 id="job-attempts">Attempts</h2>
            {resource.data.attempts.length === 0 ? (
              <EmptyState title="No attempts yet.">
                <p>No worker has claimed this job.</p>
              </EmptyState>
            ) : (
              <AttemptTimeline attempts={resource.data.attempts} />
            )}
          </article>
        </div>
      )}
    </section>
  );
}
