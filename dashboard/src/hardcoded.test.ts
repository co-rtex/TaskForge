// @vitest-environment node
// "Nothing is hardcoded" (PROJECT_SPEC.md section 5), as a check a reviewer
// can run instead of a claim to trust. It scans every non-test source file for
// the shapes a canned value takes -- an id, a timestamp, a value copied from a
// test fixture -- and forbids numeric literals in views and components
// outright, so any number the dashboard shows came from the API.

import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  ATTEMPT_STATUSES,
  DLQ_REASONS,
  FAILURE_CLASSES,
  JOB_STATUSES,
  WORKER_STATUSES,
} from "./api/types";
import { ALL_FIXTURES } from "./test/fixtures";

const SRC = fileURLToPath(new URL("./", import.meta.url));

function walk(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    return entry.isDirectory() ? walk(path) : [path];
  });
}

/** Everything that ships: source, not tests and not the test support directory. */
const sources = walk(SRC)
  .map((path) => relative(SRC, path))
  .filter((path) => /\.(ts|tsx)$/.test(path))
  .filter((path) => !/\.test\.tsx?$/.test(path) && !path.startsWith("test/"))
  .map((path) => ({ path, text: readFileSync(join(SRC, path), "utf8") }));

const rendering = sources.filter(
  (file) => file.path.startsWith("views/") || file.path.startsWith("components/"),
);

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
const TIMESTAMP = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/;

/** Source with comments and string/template literals blanked out. */
function code(text: string): string {
  return text
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/\/\/[^\n]*/g, " ")
    .replace(/"(?:\\.|[^"\\\n])*"/g, '""')
    .replace(/'(?:\\.|[^'\\\n])*'/g, "''")
    .replace(/`(?:\\.|[^`\\])*`/g, "``");
}

function strings(value: unknown): string[] {
  if (typeof value === "string") {
    return [value];
  }
  if (Array.isArray(value)) {
    return value.flatMap(strings);
  }
  if (typeof value === "object" && value !== null) {
    return Object.values(value).flatMap(strings);
  }
  return [];
}

// Schema vocabulary, which source legitimately names.
const ENUM_VALUES = new Set<string>([
  ...JOB_STATUSES,
  ...ATTEMPT_STATUSES,
  ...FAILURE_CLASSES,
  ...WORKER_STATUSES,
  ...DLQ_REASONS,
]);

describe("no hardcoded data in shipped source", () => {
  it("scans a real, non-trivial set of files", () => {
    // Guards the guard: a broken walk would make every test below vacuous.
    expect(sources.length).toBeGreaterThan(10);
    expect(rendering.map((file) => file.path)).toContain("views/Overview.tsx");
    expect(rendering.map((file) => file.path)).toContain("components/AttemptTimeline.tsx");
  });

  it("contains no job, worker, attempt, or entry id", () => {
    for (const file of sources) {
      expect(file.text, file.path).not.toMatch(UUID);
    }
  });

  it("contains no timestamp", () => {
    for (const file of sources) {
      expect(file.text, file.path).not.toMatch(TIMESTAMP);
    }
  });

  it("contains no value from any test fixture", () => {
    const values = [...new Set(strings(ALL_FIXTURES))].filter((value) => !ENUM_VALUES.has(value));
    expect(values.length).toBeGreaterThan(20);
    for (const file of sources) {
      for (const value of values) {
        expect(file.text.includes(value), `${file.path} contains fixture value "${value}"`).toBe(
          false,
        );
      }
    }
  });

  it("views and components contain no numeric literal but 0 and 1", () => {
    for (const file of rendering) {
      const literals = code(file.text).match(/(?<![\w.])\d+(?:\.\d+)?(?![\w.])/g) ?? [];
      const unexpected = literals.filter((literal) => literal !== "0" && literal !== "1");
      expect(unexpected, file.path).toEqual([]);
    }
  });

  it("every view reads through the client, and nothing renders from a direct fetch", () => {
    for (const file of rendering) {
      expect(code(file.text), file.path).not.toMatch(/\bfetch\s*\(/);
      expect(code(file.text), file.path).not.toMatch(/\bXMLHttpRequest\b/);
    }
    for (const file of rendering.filter((candidate) => candidate.path.startsWith("views/"))) {
      expect(file.text, file.path).toContain('from "../api/ClientContext"');
      expect(file.text, file.path).toMatch(/useClient\(\)/);
    }
  });
});
