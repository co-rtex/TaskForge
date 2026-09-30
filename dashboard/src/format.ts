// Presentation constants and formatting. Views import these rather than
// writing numbers inline, which is what lets hardcoded.test.ts forbid numeric
// literals in views outright: any number a view shows came from the API.

/** Rows per listing page. The API's own default is 25 and its maximum 100. */
export const PAGE_SIZE = 25;

/** How many workers the overview reads to summarize worker health. */
export const OVERVIEW_WORKER_LIMIT = 100;

/** How many of the most recent jobs the overview lists. */
export const OVERVIEW_JOB_LIMIT = 10;

const MILLIS_PER_SECOND = 1000;
const PAYLOAD_INDENT = 2;
const SECONDS_PER_MINUTE = 60;
const SECONDS_PER_HOUR = 3600;

/** An RFC 3339 instant from the API, rendered unambiguously in UTC. */
export function formatInstant(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

/** A duration in seconds, rounded to what a human reads at a glance. */
export function formatSeconds(seconds: number): string {
  if (seconds < SECONDS_PER_MINUTE) {
    return `${seconds.toFixed(1)}s`;
  }
  if (seconds < SECONDS_PER_HOUR) {
    return `${Math.floor(seconds / SECONDS_PER_MINUTE)}m ${Math.floor(seconds % SECONDS_PER_MINUTE)}s`;
  }
  const hours = Math.floor(seconds / SECONDS_PER_HOUR);
  return `${hours}h ${Math.floor((seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE)}m`;
}

export function formatMillis(milliseconds: number): string {
  return formatSeconds(milliseconds / MILLIS_PER_SECOND);
}

/** Sums a record of counts, e.g. a queue's depth per status. */
export function total(counts: Record<string, number>): number {
  return Object.values(counts).reduce((sum, count) => sum + count, 0);
}

/** Counts items by a key, preserving the order the keys are given in. */
export function countBy<T, K extends string>(
  items: readonly T[],
  keys: readonly K[],
  keyOf: (item: T) => K,
): Record<K, number> {
  const counts = Object.fromEntries(keys.map((key) => [key, 0])) as Record<K, number>;
  for (const item of items) {
    counts[keyOf(item)] += 1;
  }
  return counts;
}

/** A job payload as indented JSON text. Displayed, never interpreted. */
export function formatPayload(payload: unknown): string {
  return JSON.stringify(payload, null, PAYLOAD_INDENT);
}
