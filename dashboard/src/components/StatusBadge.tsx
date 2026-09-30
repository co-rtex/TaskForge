/**
 * A status as text first. Color is a secondary cue keyed by class, so a
 * status is never conveyed by color alone.
 */
export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`badge badge-${status.toLowerCase().replaceAll("_", "-")}`}>{status}</span>
  );
}
