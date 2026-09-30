// @vitest-environment node
// types.ts is hand-written against api/openapi.yaml. This test is what keeps
// the two from drifting: it reads the real document -- the same file the Go
// contract tests read -- and compares every field's presence and nullability,
// every enum, and every route and query parameter the client uses.

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";
import {
  ATTEMPT_FIELDS,
  ATTEMPT_LIST_FIELDS,
  ATTEMPT_STATUSES,
  DLQ_ENTRY_FIELDS,
  DLQ_PAGE_FIELDS,
  DLQ_REASONS,
  ERROR_BODY_FIELDS,
  ERROR_DETAIL_FIELDS,
  FAILURE_CLASSES,
  JOB_FIELDS,
  JOB_PAGE_FIELDS,
  JOB_STATUSES,
  JOB_SUMMARY_FIELDS,
  NON_TERMINAL_JOB_STATUSES,
  QUEUE_DEPTH_FIELDS,
  QUEUE_FIELDS,
  QUEUE_LIST_FIELDS,
  WORKER_FIELDS,
  WORKER_PAGE_FIELDS,
  WORKER_STATUSES,
} from "./types";

interface Schema {
  type?: string | string[];
  required?: string[];
  properties?: Record<string, Schema>;
  enum?: unknown[];
}

interface Operation {
  parameters?: { name: string; in: string }[];
}

interface OpenAPIDocument {
  paths: Record<string, Record<string, Operation>>;
  components: { schemas: Record<string, Schema> };
}

const OPENAPI_PATH = fileURLToPath(new URL("../../../api/openapi.yaml", import.meta.url));
const doc = parse(readFileSync(OPENAPI_PATH, "utf8")) as OpenAPIDocument;
const schemas = doc.components.schemas;

function schema(name: string): Schema {
  const found = schemas[name];
  if (found === undefined) {
    throw new Error(`api/openapi.yaml has no schema ${name}`);
  }
  return found;
}

function property(parent: Schema, name: string): Schema {
  const found = parent.properties?.[name];
  if (found === undefined) {
    throw new Error(`schema has no property ${name}`);
  }
  return found;
}

/** The FieldSpec the OpenAPI schema implies, in the same spelling as types.ts. */
function specFromSchema(from: Schema): Record<string, string> {
  const required = new Set(from.required ?? []);
  for (const name of required) {
    if (from.properties?.[name] === undefined) {
      throw new Error(`required field ${name} has no property definition`);
    }
  }
  return Object.fromEntries(
    Object.entries(from.properties ?? {}).map(([name, prop]) => {
      const nullable = Array.isArray(prop.type) && prop.type.includes("null");
      return [name, `${required.has(name) ? "required" : "optional"}${nullable ? "|null" : ""}`];
    }),
  );
}

describe("types.ts agrees with api/openapi.yaml", () => {
  it.each([
    ["Job", JOB_FIELDS],
    ["JobSummary", JOB_SUMMARY_FIELDS],
    ["JobPage", JOB_PAGE_FIELDS],
    ["Attempt", ATTEMPT_FIELDS],
    ["AttemptList", ATTEMPT_LIST_FIELDS],
    ["Worker", WORKER_FIELDS],
    ["WorkerPage", WORKER_PAGE_FIELDS],
    ["Queue", QUEUE_FIELDS],
    ["QueueList", QUEUE_LIST_FIELDS],
    ["DLQEntry", DLQ_ENTRY_FIELDS],
    ["DLQPage", DLQ_PAGE_FIELDS],
    ["Error", ERROR_BODY_FIELDS],
  ] as const)("%s: same fields, same presence, same nullability", (name, spec) => {
    expect(spec).toEqual(specFromSchema(schema(name)));
  });

  it("Queue.depth and Error.error, the two nested objects the dashboard reads", () => {
    expect(QUEUE_DEPTH_FIELDS).toEqual(specFromSchema(property(schema("Queue"), "depth")));
    expect(ERROR_DETAIL_FIELDS).toEqual(specFromSchema(property(schema("Error"), "error")));
  });

  it("every enum the dashboard labels or filters by", () => {
    expect(JOB_STATUSES).toEqual(property(schema("Job"), "status").enum);
    expect(JOB_STATUSES).toEqual(property(schema("JobSummary"), "status").enum);
    expect(ATTEMPT_STATUSES).toEqual(property(schema("Attempt"), "status").enum);
    expect(FAILURE_CLASSES).toEqual(
      property(schema("Attempt"), "failure_class").enum?.filter((value) => value !== null),
    );
    expect(WORKER_STATUSES).toEqual(property(schema("Worker"), "status").enum);
    expect(DLQ_REASONS).toEqual(property(schema("DLQEntry"), "reason").enum);
    // Queue depth is keyed by exactly the non-terminal statuses, in order.
    expect(NON_TERMINAL_JOB_STATUSES).toEqual(property(schema("Queue"), "depth").required);
  });

  it("every route and query parameter the client sends is a documented GET", () => {
    const reads: Record<string, string[]> = {
      "/v1/jobs": ["status", "queue", "limit", "cursor"],
      "/v1/jobs/{job_id}": ["job_id"],
      "/v1/jobs/{job_id}/attempts": ["job_id"],
      "/v1/workers": ["limit", "cursor"],
      "/v1/queues": [],
      "/v1/dlq": ["limit", "cursor"],
    };
    for (const [path, parameters] of Object.entries(reads)) {
      const operation = doc.paths[path]?.get;
      expect(operation, `GET ${path}`).toBeDefined();
      const documented = (operation?.parameters ?? [])
        .filter((parameter) => parameter.in === "query" || parameter.in === "path")
        .map((parameter) => parameter.name);
      for (const parameter of parameters) {
        expect(documented, `GET ${path} ?${parameter}`).toContain(parameter);
      }
    }
  });

  // The comparison must be able to fail, or every assertion above is vacuous.
  it("detects a drifted field, a flipped presence, and a flipped nullability", () => {
    const job = schema("Job");
    const withoutPayload = {
      ...job,
      required: (job.required ?? []).filter((field) => field !== "payload"),
      properties: { ...job.properties },
    };
    delete withoutPayload.properties.payload;
    expect(JOB_FIELDS).not.toEqual(specFromSchema(withoutPayload));

    // A required field with no definition is itself a malformed document.
    expect(() => specFromSchema({ ...job, properties: {} })).toThrow(/no property definition/);

    const payloadOptional = {
      ...job,
      required: (job.required ?? []).filter((field) => field !== "payload"),
    };
    expect(JOB_FIELDS).not.toEqual(specFromSchema(payloadOptional));

    const updatedNullable = {
      ...job,
      properties: { ...job.properties, updated_at: { type: ["string", "null"] } },
    };
    expect(JOB_FIELDS).not.toEqual(specFromSchema(updatedNullable));
  });
});
