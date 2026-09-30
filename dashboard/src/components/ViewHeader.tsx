import type { ReactNode } from "react";

/**
 * A view's title and its refresh control. There is no auto-refresh: the data
 * on screen is exactly what the last explicit read returned.
 */
export function ViewHeader({
  title,
  onRefresh,
  children,
}: {
  title: string;
  onRefresh: () => void;
  children?: ReactNode;
}) {
  return (
    <header className="view-header">
      <h1 id="view-title">{title}</h1>
      <div className="view-actions">
        {children}
        <button type="button" onClick={onRefresh}>
          Refresh
        </button>
      </div>
    </header>
  );
}
