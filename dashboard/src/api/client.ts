// The dashboard's only way to reach data: six read-only calls to the public /v1
// routes M6A shipped, each presenting the operator's own API key. There is no
// write method here, by design -- see docs/adr/0017.

import type {
  AttemptList,
  DLQPage,
  ErrorBody,
  Job,
  JobPage,
  JobStatus,
  QueueList,
  WorkerPage,
} from "./types";

/** Echoed on every response by taskforge-api, and included in error bodies. */
export const REQUEST_ID_HEADER = "X-Request-Id";

/** Every /v1 route's answer to a missing, unknown, or revoked key. */
export const HTTP_UNAUTHORIZED = 401;

/**
 * A request that did not produce the expected response.
 *
 * `status` is null when taskforge-api was never reached (the connection
 * failed) and a number whenever it answered. `code` is the server's stable
 * machine-readable code when it sent its structured error body, and null when
 * it did not -- this client never invents a code that could be mistaken for
 * one of the server's. `requestId` is the thread back to the server's logs.
 */
export class ApiError extends Error {
  readonly status: number | null;
  readonly code: string | null;
  readonly requestId: string | null;

  constructor(
    status: number | null,
    code: string | null,
    message: string,
    requestId: string | null,
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.requestId = requestId;
  }
}

export interface PageQuery {
  limit?: number;
  cursor?: string;
}

export interface JobQuery extends PageQuery {
  status?: JobStatus;
  queue?: string;
}

export interface TaskForgeClient {
  listJobs(query: JobQuery, signal?: AbortSignal): Promise<JobPage>;
  getJob(jobId: string, signal?: AbortSignal): Promise<Job>;
  listAttempts(jobId: string, signal?: AbortSignal): Promise<AttemptList>;
  listWorkers(query: PageQuery, signal?: AbortSignal): Promise<WorkerPage>;
  listQueues(signal?: AbortSignal): Promise<QueueList>;
  listDLQ(query: PageQuery, signal?: AbortSignal): Promise<DLQPage>;
}

export interface ClientOptions {
  apiKey: string;
  /** Injected by tests. Defaults to the browser's fetch. */
  fetch?: typeof fetch;
  /** Empty by default: same-origin, which is what makes CORS irrelevant. */
  baseUrl?: string;
}

export function createClient(options: ClientOptions): TaskForgeClient {
  const doFetch = options.fetch ?? globalThis.fetch.bind(globalThis);
  const baseUrl = options.baseUrl ?? "";

  async function get<T>(
    path: string,
    params: Record<string, string | number | undefined>,
    signal: AbortSignal | undefined,
  ): Promise<T> {
    const search = new URLSearchParams();
    for (const [name, value] of Object.entries(params)) {
      if (value !== undefined && value !== "") {
        search.set(name, String(value));
      }
    }
    const query = search.toString();
    const url = `${baseUrl}${path}${query === "" ? "" : `?${query}`}`;

    let response: Response;
    try {
      response = await doFetch(url, {
        method: "GET",
        headers: { Authorization: `Bearer ${options.apiKey}`, Accept: "application/json" },
        // The key is presented explicitly above; no cookie is ever involved.
        credentials: "omit",
        // Every view has an explicit refresh control; a cached read would make
        // that control lie.
        cache: "no-store",
        ...(signal === undefined ? {} : { signal }),
      });
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new ApiError(null, null, "taskforge-api could not be reached.", null);
    }

    const headerRequestId = response.headers.get(REQUEST_ID_HEADER);
    if (!response.ok) {
      const body = await readErrorBody(response);
      if (body !== null) {
        throw new ApiError(
          response.status,
          body.error.code,
          body.error.message,
          body.error.request_id ?? headerRequestId,
        );
      }
      throw new ApiError(
        response.status,
        null,
        `HTTP ${response.status} without a TaskForge error body.`,
        headerRequestId,
      );
    }

    try {
      return (await response.json()) as T;
    } catch (error) {
      if (isAbort(error)) {
        throw error;
      }
      throw new ApiError(
        response.status,
        null,
        "taskforge-api answered with a body that is not JSON.",
        headerRequestId,
      );
    }
  }

  return {
    listJobs: (query, signal) =>
      get<JobPage>(
        "/v1/jobs",
        { status: query.status, queue: query.queue, limit: query.limit, cursor: query.cursor },
        signal,
      ),
    getJob: (jobId, signal) => get<Job>(`/v1/jobs/${encodeURIComponent(jobId)}`, {}, signal),
    listAttempts: (jobId, signal) =>
      get<AttemptList>(`/v1/jobs/${encodeURIComponent(jobId)}/attempts`, {}, signal),
    listWorkers: (query, signal) =>
      get<WorkerPage>("/v1/workers", { limit: query.limit, cursor: query.cursor }, signal),
    listQueues: (signal) => get<QueueList>("/v1/queues", {}, signal),
    listDLQ: (query, signal) =>
      get<DLQPage>("/v1/dlq", { limit: query.limit, cursor: query.cursor }, signal),
  };
}

/** The structured error body, or null when the response did not carry one. */
async function readErrorBody(response: Response): Promise<ErrorBody | null> {
  let parsed: unknown;
  try {
    parsed = await response.json();
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null || !("error" in parsed)) {
    return null;
  }
  const detail = (parsed as { error: unknown }).error;
  if (typeof detail !== "object" || detail === null) {
    return null;
  }
  const { code, message, request_id: requestId } = detail as Record<string, unknown>;
  if (typeof code !== "string" || typeof message !== "string") {
    return null;
  }
  return {
    error: {
      code,
      message,
      ...(typeof requestId === "string" && requestId !== "" ? { request_id: requestId } : {}),
    },
  };
}

export function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}
