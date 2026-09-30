import { formatInstant } from "../format";

/** An instant from the API, or an explicit dash when the API sent null. */
export function Instant({ value }: { value: string | null | undefined }) {
  if (value === null || value === undefined) {
    return <span className="muted">—</span>;
  }
  return (
    <time dateTime={value} title={value}>
      {formatInstant(value)}
    </time>
  );
}
