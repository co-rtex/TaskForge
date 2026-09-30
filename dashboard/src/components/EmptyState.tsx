import type { ReactNode } from "react";

/**
 * A successful response with nothing in it. Visually and semantically distinct
 * from an error: an empty queue is a fact about the system, not a failure.
 */
export function EmptyState({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="state state-empty" data-state="empty">
      <p className="state-title">{title}</p>
      {children}
    </div>
  );
}
