import { describe, expect, it } from "vitest";
import { ApiError, createClient } from "./client";

type Call = { url: string; init: RequestInit };

function clientReturning(response: () => Promise<Response> | Response) {
  const calls: Call[] = [];
  const client = createClient({
    apiKey: "tf_test_key",
    fetch: async (input, init = {}) => {
      calls.push({ url: String(input), init });
      return response();
    },
  });
  return { client, calls };
}

async function rejection(promise: Promise<unknown>): Promise<ApiError> {
  try {
    await promise;
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    return error as ApiError;
  }
  throw new Error("expected the call to reject");
}

describe("createClient", () => {
  it("sends a same-origin GET with the key as a bearer token and no cookies", async () => {
    const { client, calls } = clientReturning(() => Response.json({ queues: [] }));
    await client.listQueues();
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe("/v1/queues");
    expect(calls[0]?.init.method).toBe("GET");
    expect(new Headers(calls[0]?.init.headers).get("Authorization")).toBe("Bearer tf_test_key");
    expect(calls[0]?.init.credentials).toBe("omit");
    expect(calls[0]?.init.cache).toBe("no-store");
  });

  it("encodes query parameters and omits unset ones", async () => {
    const { client, calls } = clientReturning(() => Response.json({ jobs: [] }));
    await client.listJobs({ status: "QUEUED", queue: "a b&c", limit: 25 });
    await client.listJobs({});
    expect(calls[0]?.url).toBe("/v1/jobs?status=QUEUED&queue=a+b%26c&limit=25");
    expect(calls[1]?.url).toBe("/v1/jobs");
  });

  it("encodes a job id as one path segment", async () => {
    const { client, calls } = clientReturning(() => Response.json({ attempts: [] }));
    await client.listAttempts("../queues");
    expect(calls[0]?.url).toBe("/v1/jobs/..%2Fqueues/attempts");
  });

  it("surfaces the structured error body, including its request id", async () => {
    const { client } = clientReturning(() =>
      Response.json(
        { error: { code: "not_found", message: "job not found", request_id: "req-body" } },
        { status: 404, headers: { "X-Request-Id": "req-header" } },
      ),
    );
    const error = await rejection(client.getJob("x"));
    expect(error.status).toBe(404);
    expect(error.code).toBe("not_found");
    expect(error.message).toBe("job not found");
    expect(error.requestId).toBe("req-body");
  });

  it("falls back to the X-Request-Id header when the body has no request id", async () => {
    const { client } = clientReturning(() =>
      Response.json(
        { error: { code: "internal_error", message: "internal error" } },
        { status: 500, headers: { "X-Request-Id": "req-header" } },
      ),
    );
    expect((await rejection(client.listQueues())).requestId).toBe("req-header");
  });

  it("never invents a server code for a response without the TaskForge error shape", async () => {
    const { client } = clientReturning(
      () => new Response("<html>bad gateway</html>", { status: 502 }),
    );
    const error = await rejection(client.listQueues());
    expect(error.status).toBe(502);
    expect(error.code).toBeNull();
    expect(error.requestId).toBeNull();
    expect(error.message).toContain("502");
  });

  it("reports an unreachable API with no status at all", async () => {
    const { client } = clientReturning(() => {
      throw new TypeError("Failed to fetch");
    });
    const error = await rejection(client.listQueues());
    expect(error.status).toBeNull();
    expect(error.code).toBeNull();
    expect(error.message).toBe("taskforge-api could not be reached.");
  });

  it("reports a 200 whose body is not JSON as a failure, not as data", async () => {
    const { client } = clientReturning(() => new Response("not json", { status: 200 }));
    const error = await rejection(client.listQueues());
    expect(error.status).toBe(200);
    expect(error.code).toBeNull();
  });

  it("lets an abort through untouched, so a superseded read is dropped silently", async () => {
    const { client } = clientReturning(() => {
      throw new DOMException("aborted", "AbortError");
    });
    await expect(client.listQueues()).rejects.toMatchObject({ name: "AbortError" });
  });
});
