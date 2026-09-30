// Hand-written against api/openapi.yaml's component schemas, deliberately not
// generated: a reviewer can diff each interface below against its schema
// directly. types.test.ts fails if the two drift -- a field added, removed, or
// changed between required and optional on either side, or an enum value
// changed.
//
// Every interface is paired with a FieldSpec constant. The type checker forces
// each constant to name exactly its interface's fields, each marked the way the
// interface declares it -- optional or required, nullable or not; the drift
// test compares those constants with the OpenAPI document. Together they close
// the loop without a code generator.

type Presence<T, K extends keyof T> = object extends Pick<T, K> ? "optional" : "required";
type Nullability<T, K extends keyof T> = null extends T[K] ? "|null" : "";

/**
 * One entry per field of T, spelled exactly as T declares it: "required" or
 * "optional" (the OpenAPI required list), with "|null" when null is a value
 * (the OpenAPI type array includes 'null').
 */
export type FieldSpec<T> = {
  readonly [K in keyof T]-?: `${Presence<T, K>}${Nullability<T, K>}`;
};

export const JOB_STATUSES = [
  "PENDING",
  "QUEUED",
  "LEASED",
  "RUNNING",
  "RETRY_WAIT",
  "CANCEL_REQUESTED",
  "SUCCEEDED",
  "CANCELED",
  "DEAD_LETTERED",
] as const;
export type JobStatus = (typeof JOB_STATUSES)[number];

/** The statuses a queue's depth counts. Terminal statuses are absent by design. */
export const NON_TERMINAL_JOB_STATUSES = [
  "PENDING",
  "QUEUED",
  "LEASED",
  "RUNNING",
  "RETRY_WAIT",
  "CANCEL_REQUESTED",
] as const satisfies readonly JobStatus[];
export type NonTerminalJobStatus = (typeof NON_TERMINAL_JOB_STATUSES)[number];

export const ATTEMPT_STATUSES = [
  "LEASED",
  "RUNNING",
  "SUCCEEDED",
  "FAILED",
  "TIMED_OUT",
  "CANCELED",
  "ABANDONED",
] as const;
export type AttemptStatus = (typeof ATTEMPT_STATUSES)[number];

export const FAILURE_CLASSES = [
  "RETRYABLE",
  "PERMANENT",
  "TIMED_OUT",
  "CANCELED",
  "ABANDONED",
] as const;
export type FailureClass = (typeof FAILURE_CLASSES)[number];

export const WORKER_STATUSES = ["STARTING", "HEALTHY", "DRAINING", "UNHEALTHY", "OFFLINE"] as const;
export type WorkerStatus = (typeof WORKER_STATUSES)[number];

export const DLQ_REASONS = ["PERMANENT_FAILURE", "ATTEMPTS_EXHAUSTED"] as const;
export type DLQReason = (typeof DLQ_REASONS)[number];

/** components.schemas.Job -- GET /v1/jobs/{job_id}. */
export interface Job {
  id: string;
  queue: string;
  job_type: string;
  /** The submitted payload, verbatim. Rendered as text, never interpreted. */
  payload: { [key: string]: unknown };
  status: JobStatus;
  priority: number;
  max_attempts: number;
  timeout_seconds: number;
  required_capabilities: string[];
  scheduled_at: string | null;
  available_at: string;
  cancel_requested_at: string | null;
  replayed_from_job_id: string | null;
  created_at: string;
  updated_at: string;
}

/** components.schemas.JobSummary -- every Job field except payload. */
export type JobSummary = Omit<Job, "payload">;

/** components.schemas.JobPage -- GET /v1/jobs. next_cursor is absent on the last page. */
export interface JobPage {
  jobs: JobSummary[];
  next_cursor?: string;
}

/** components.schemas.Attempt. */
export interface Attempt {
  id: string;
  attempt_number: number;
  status: AttemptStatus;
  worker_id: string;
  worker_name: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  timeout_at: string | null;
  failure_class: FailureClass | null;
  error_code: string | null;
  error_message: string | null;
  retry_delay_ms: number | null;
  retry_at: string | null;
}

/** components.schemas.AttemptList -- oldest first, unpaginated. */
export interface AttemptList {
  attempts: Attempt[];
}

/** components.schemas.Worker -- a logical worker joined to its most recent session. */
export interface Worker {
  id: string;
  name: string;
  status: WorkerStatus;
  worker_group: string;
  hostname: string;
  concurrency_limit: number;
  capabilities: string[];
  supported_job_types: string[];
  registered_at: string;
  last_heartbeat_at: string;
  ended_at: string | null;
  heartbeat_age_seconds: number;
  active_leases: number;
}

/** components.schemas.WorkerPage -- GET /v1/workers. */
export interface WorkerPage {
  workers: Worker[];
  next_cursor?: string;
}

/** components.schemas.Queue.depth -- this scope's count per non-terminal status. */
export type QueueDepth = Record<NonTerminalJobStatus, number>;

/** components.schemas.Queue. */
export interface Queue {
  name: string;
  worker_group: string;
  /** Queue-wide, across every scope. Not comparable with depth. */
  max_concurrency: number;
  depth: QueueDepth;
}

/** components.schemas.QueueList -- GET /v1/queues, unpaginated. */
export interface QueueList {
  queues: Queue[];
}

/** components.schemas.DLQEntry. */
export interface DLQEntry {
  id: string;
  job_id: string;
  queue: string;
  job_type: string;
  priority: number;
  max_attempts: number;
  reason: DLQReason;
  created_at: string;
  terminal_attempt_id?: string | null;
  attempt_number?: number | null;
  attempt_status?: string | null;
  failure_class?: string | null;
  error_code?: string | null;
  error_message?: string | null;
  replay_count: number;
}

/** components.schemas.DLQPage -- GET /v1/dlq. */
export interface DLQPage {
  entries: DLQEntry[];
  next_cursor?: string;
}

/** components.schemas.Error.error -- the one error shape every route returns. */
export interface ErrorDetail {
  code: string;
  message: string;
  details?: { field?: string; message?: string }[];
  /** The thread back to the server's logs for this exact request. */
  request_id?: string;
}

/** components.schemas.Error. */
export interface ErrorBody {
  error: ErrorDetail;
}

export const JOB_FIELDS: FieldSpec<Job> = {
  id: "required",
  queue: "required",
  job_type: "required",
  payload: "required",
  status: "required",
  priority: "required",
  max_attempts: "required",
  timeout_seconds: "required",
  required_capabilities: "required",
  scheduled_at: "required|null",
  available_at: "required",
  cancel_requested_at: "required|null",
  replayed_from_job_id: "required|null",
  created_at: "required",
  updated_at: "required",
};

export const JOB_SUMMARY_FIELDS: FieldSpec<JobSummary> = {
  id: "required",
  queue: "required",
  job_type: "required",
  status: "required",
  priority: "required",
  max_attempts: "required",
  timeout_seconds: "required",
  required_capabilities: "required",
  scheduled_at: "required|null",
  available_at: "required",
  cancel_requested_at: "required|null",
  replayed_from_job_id: "required|null",
  created_at: "required",
  updated_at: "required",
};

export const JOB_PAGE_FIELDS: FieldSpec<JobPage> = {
  jobs: "required",
  next_cursor: "optional",
};

export const ATTEMPT_FIELDS: FieldSpec<Attempt> = {
  id: "required",
  attempt_number: "required",
  status: "required",
  worker_id: "required",
  worker_name: "required",
  created_at: "required",
  started_at: "required|null",
  finished_at: "required|null",
  timeout_at: "required|null",
  failure_class: "required|null",
  error_code: "required|null",
  error_message: "required|null",
  retry_delay_ms: "required|null",
  retry_at: "required|null",
};

export const ATTEMPT_LIST_FIELDS: FieldSpec<AttemptList> = {
  attempts: "required",
};

export const WORKER_FIELDS: FieldSpec<Worker> = {
  id: "required",
  name: "required",
  status: "required",
  worker_group: "required",
  hostname: "required",
  concurrency_limit: "required",
  capabilities: "required",
  supported_job_types: "required",
  registered_at: "required",
  last_heartbeat_at: "required",
  ended_at: "required|null",
  heartbeat_age_seconds: "required",
  active_leases: "required",
};

export const WORKER_PAGE_FIELDS: FieldSpec<WorkerPage> = {
  workers: "required",
  next_cursor: "optional",
};

export const QUEUE_FIELDS: FieldSpec<Queue> = {
  name: "required",
  worker_group: "required",
  max_concurrency: "required",
  depth: "required",
};

export const QUEUE_DEPTH_FIELDS: FieldSpec<QueueDepth> = {
  PENDING: "required",
  QUEUED: "required",
  LEASED: "required",
  RUNNING: "required",
  RETRY_WAIT: "required",
  CANCEL_REQUESTED: "required",
};

export const QUEUE_LIST_FIELDS: FieldSpec<QueueList> = {
  queues: "required",
};

export const DLQ_ENTRY_FIELDS: FieldSpec<DLQEntry> = {
  id: "required",
  job_id: "required",
  queue: "required",
  job_type: "required",
  priority: "required",
  max_attempts: "required",
  reason: "required",
  created_at: "required",
  terminal_attempt_id: "optional|null",
  attempt_number: "optional|null",
  attempt_status: "optional|null",
  failure_class: "optional|null",
  error_code: "optional|null",
  error_message: "optional|null",
  replay_count: "required",
};

export const DLQ_PAGE_FIELDS: FieldSpec<DLQPage> = {
  entries: "required",
  next_cursor: "optional",
};

export const ERROR_BODY_FIELDS: FieldSpec<ErrorBody> = {
  error: "required",
};

export const ERROR_DETAIL_FIELDS: FieldSpec<ErrorDetail> = {
  code: "required",
  message: "required",
  details: "optional",
  request_id: "optional",
};
