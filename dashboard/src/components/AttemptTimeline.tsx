import type { Attempt } from "../api/types";
import { formatMillis } from "../format";
import { Instant } from "./Instant";
import { StatusBadge } from "./StatusBadge";

/**
 * A job's attempts in the order the API returns them: oldest first. Each entry
 * shows who ran it, when it started and ended, and -- when it did not succeed
 * -- how it ended and what the retry policy decided. An abandoned attempt (a
 * crashed worker's) is listed like any other; it consumed a place in the
 * attempt budget, and hiding it would misstate why a job was retried.
 */
export function AttemptTimeline({ attempts }: { attempts: readonly Attempt[] }) {
  return (
    <ol className="timeline" aria-label="Attempt timeline, oldest first">
      {attempts.map((attempt) => (
        <li key={attempt.id} className="timeline-entry">
          <div className="timeline-heading">
            <span className="timeline-number">Attempt {attempt.attempt_number}</span>
            <StatusBadge status={attempt.status} />
          </div>
          <dl className="fields">
            <dt>Worker</dt>
            <dd>
              {attempt.worker_name} <code className="muted">{attempt.worker_id}</code>
            </dd>
            <dt>Claimed</dt>
            <dd>
              <Instant value={attempt.created_at} />
            </dd>
            <dt>Started</dt>
            <dd>
              <Instant value={attempt.started_at} />
            </dd>
            <dt>Finished</dt>
            <dd>
              <Instant value={attempt.finished_at} />
            </dd>
            <dt>Execution deadline</dt>
            <dd>
              <Instant value={attempt.timeout_at} />
            </dd>
            {attempt.failure_class !== null && (
              <>
                <dt>Failure class</dt>
                <dd>
                  <StatusBadge status={attempt.failure_class} />
                </dd>
              </>
            )}
            {attempt.error_code !== null && (
              <>
                <dt>Error code</dt>
                <dd>
                  <code>{attempt.error_code}</code>
                </dd>
              </>
            )}
            {attempt.error_message !== null && (
              <>
                <dt>Error message</dt>
                <dd>{attempt.error_message}</dd>
              </>
            )}
            {attempt.retry_delay_ms !== null && (
              <>
                <dt>Retry delay</dt>
                <dd>{formatMillis(attempt.retry_delay_ms)}</dd>
              </>
            )}
            {attempt.retry_at !== null && (
              <>
                <dt>Retry at</dt>
                <dd>
                  <Instant value={attempt.retry_at} />
                </dd>
              </>
            )}
          </dl>
        </li>
      ))}
    </ol>
  );
}
