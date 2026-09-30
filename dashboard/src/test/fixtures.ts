// Response bodies for tests, shaped exactly like api/openapi.yaml's schemas.
//
// Every free-text value carries a "fixture" marker. hardcoded.test.ts asserts
// that no such value appears anywhere in non-test source, so a view that
// rendered a canned row instead of the API's would fail it.

import type {
  Attempt,
  DLQEntry,
  DLQPage,
  ErrorBody,
  Job,
  JobPage,
  JobSummary,
  QueueList,
  Worker,
  WorkerPage,
} from "../api/types";

export const TEST_API_KEY = "tf_fixture_key_0123456789";
export const FIXTURE_REQUEST_ID = "fixture-request-id-7f3a";

export const JOB_ID = "0b6f3c1e-8a52-4c9e-9d6b-3f1f0c2a7e11";
export const REPLAYED_FROM_JOB_ID = "5d0c9a7b-1e2f-4a3b-8c4d-9e8f7a6b5c4d";
export const WORKER_ID = "9a1b2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d";
export const CRASHED_WORKER_ID = "1f2e3d4c-5b6a-4978-8a9b-0c1d2e3f4a5b";

export const jobSummary: JobSummary = {
  id: JOB_ID,
  queue: "fixture-queue-emails",
  job_type: "fixture.send_email",
  status: "RETRY_WAIT",
  priority: 70,
  max_attempts: 4,
  timeout_seconds: 45,
  required_capabilities: ["fixture-cap-smtp"],
  scheduled_at: null,
  available_at: "2026-09-30T12:05:00Z",
  cancel_requested_at: null,
  replayed_from_job_id: REPLAYED_FROM_JOB_ID,
  created_at: "2026-09-30T12:00:00Z",
  updated_at: "2026-09-30T12:04:00Z",
};

export const job: Job = {
  ...jobSummary,
  payload: { fixture_recipient: "fixture-recipient@example.test" },
};

export const jobPage: JobPage = { jobs: [jobSummary], next_cursor: "fixture-cursor-jobs-2" };

export const failedAttempt: Attempt = {
  id: "7c6b5a49-3827-4165-9f4e-3d2c1b0a9f8e",
  attempt_number: 1,
  status: "ABANDONED",
  worker_id: CRASHED_WORKER_ID,
  worker_name: "fixture-worker-crashed",
  created_at: "2026-09-30T12:00:01Z",
  started_at: "2026-09-30T12:00:02Z",
  finished_at: "2026-09-30T12:01:00Z",
  timeout_at: "2026-09-30T12:00:47Z",
  failure_class: "ABANDONED",
  error_code: "fixture_lease_expired",
  error_message: "fixture: lease expired before completion",
  retry_delay_ms: 2500,
  retry_at: "2026-09-30T12:01:03Z",
};

export const runningAttempt: Attempt = {
  id: "2b3c4d5e-6f70-4812-9a3b-4c5d6e7f8091",
  attempt_number: 2,
  status: "RUNNING",
  worker_id: WORKER_ID,
  worker_name: "fixture-worker-alpha",
  created_at: "2026-09-30T12:01:04Z",
  started_at: "2026-09-30T12:01:05Z",
  finished_at: null,
  timeout_at: "2026-09-30T12:01:50Z",
  failure_class: null,
  error_code: null,
  error_message: null,
  retry_delay_ms: null,
  retry_at: null,
};

export const healthyWorker: Worker = {
  id: WORKER_ID,
  name: "fixture-worker-alpha",
  status: "HEALTHY",
  worker_group: "fixture-group",
  hostname: "fixture-host-alpha",
  concurrency_limit: 8,
  capabilities: ["fixture-cap-smtp"],
  supported_job_types: ["fixture.send_email"],
  registered_at: "2026-09-30T11:00:00Z",
  last_heartbeat_at: "2026-09-30T12:04:58Z",
  ended_at: null,
  heartbeat_age_seconds: 2.4,
  active_leases: 3,
};

export const crashedWorker: Worker = {
  id: CRASHED_WORKER_ID,
  name: "fixture-worker-crashed",
  status: "UNHEALTHY",
  worker_group: "fixture-group",
  hostname: "fixture-host-crashed",
  concurrency_limit: 4,
  capabilities: [],
  supported_job_types: ["fixture.send_email"],
  registered_at: "2026-09-30T10:00:00Z",
  last_heartbeat_at: "2026-09-30T12:00:30Z",
  ended_at: "2026-09-30T12:00:50Z",
  heartbeat_age_seconds: 268,
  active_leases: 0,
};

export const replacedWorker: Worker = {
  ...crashedWorker,
  id: "3c4d5e6f-7081-4923-8a4b-5c6d7e8f90a1",
  name: "fixture-worker-replaced",
  status: "OFFLINE",
  hostname: "fixture-host-replaced",
};

export const workerPage: WorkerPage = {
  workers: [healthyWorker, crashedWorker, replacedWorker],
};

export const queueList: QueueList = {
  queues: [
    {
      name: "fixture-queue-emails",
      worker_group: "fixture-group",
      max_concurrency: 64,
      depth: { PENDING: 5, QUEUED: 11, LEASED: 2, RUNNING: 3, RETRY_WAIT: 7, CANCEL_REQUESTED: 1 },
    },
  ],
};

export const dlqEntry: DLQEntry = {
  id: "4d5e6f70-8192-4a3b-9c4d-5e6f708192a3",
  job_id: REPLAYED_FROM_JOB_ID,
  queue: "fixture-queue-emails",
  job_type: "fixture.send_email",
  priority: 70,
  max_attempts: 4,
  reason: "ATTEMPTS_EXHAUSTED",
  created_at: "2026-09-30T11:30:00Z",
  terminal_attempt_id: "5e6f7081-92a3-4b4c-8d5e-6f708192a3b4",
  attempt_number: 4,
  attempt_status: "FAILED",
  failure_class: "RETRYABLE",
  error_code: "fixture_smtp_refused",
  error_message: "fixture: smtp refused the connection",
  replay_count: 2,
};

export const dlqPage: DLQPage = { entries: [dlqEntry], next_cursor: "fixture-cursor-dlq-2" };

export function errorBody(code: string, message: string): ErrorBody {
  return { error: { code, message, request_id: FIXTURE_REQUEST_ID } };
}

/** Every fixture above, for hardcoded.test.ts to walk. */
export const ALL_FIXTURES: readonly unknown[] = [
  job,
  jobPage,
  failedAttempt,
  runningAttempt,
  workerPage,
  queueList,
  dlqPage,
  TEST_API_KEY,
  FIXTURE_REQUEST_ID,
];
