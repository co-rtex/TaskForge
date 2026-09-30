export function LoadingState({ what }: { what: string }) {
  return (
    <p className="state state-loading" role="status" aria-live="polite" data-state="loading">
      Loading {what}…
    </p>
  );
}
