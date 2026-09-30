import { useCallback, useState } from "react";

/**
 * Keyset pagination state. The API hands out a cursor for the next page only,
 * so "previous" is the stack of cursors already followed. A full page is not
 * proof that more exist; the presence of next_cursor is, and that alone
 * enables "Next".
 */
export function useCursorStack() {
  const [stack, setStack] = useState<string[]>([]);
  const cursor = stack.at(-1);
  const next = useCallback((nextCursor: string) => setStack((s) => [...s, nextCursor]), []);
  const previous = useCallback(() => setStack((s) => s.slice(0, -1)), []);
  const reset = useCallback(() => setStack([]), []);
  return { cursor, page: stack.length, next, previous, reset };
}

export function Pager({
  page,
  nextCursor,
  onNext,
  onPrevious,
}: {
  page: number;
  nextCursor: string | undefined;
  onNext: (cursor: string) => void;
  onPrevious: () => void;
}) {
  return (
    <nav className="pager" aria-label="Pagination">
      <button type="button" onClick={onPrevious} disabled={page === 0}>
        Previous
      </button>
      <span>Page {page + 1}</span>
      <button
        type="button"
        onClick={() => nextCursor !== undefined && onNext(nextCursor)}
        disabled={nextCursor === undefined}
      >
        Next
      </button>
    </nav>
  );
}
