import { useCallback, useEffect, useState } from "react";
import { ApiError, isAbort } from "./client";

/**
 * The three request states a view can be in. "Empty" is deliberately not one
 * of them: a 200 with zero rows is a successful load, and each view decides
 * what empty means for its own data.
 */
export type Resource<T> =
  | { state: "loading" }
  | { state: "error"; error: ApiError }
  | { state: "loaded"; data: T };

/**
 * Runs `load` whenever it changes (callers memoize it with useCallback, so
 * that is whenever its inputs change) and again whenever `reload` is called.
 * A superseded request is aborted, so a slow earlier response can never
 * overwrite a newer one.
 */
export function useResource<T>(
  load: (signal: AbortSignal) => Promise<T>,
): [Resource<T>, () => void] {
  const [resource, setResource] = useState<Resource<T>>({ state: "loading" });
  const [generation, setGeneration] = useState(0);

  // biome-ignore lint/correctness/useExhaustiveDependencies: generation is the reload trigger.
  useEffect(() => {
    const controller = new AbortController();
    setResource({ state: "loading" });
    load(controller.signal).then(
      (data) => {
        if (!controller.signal.aborted) {
          setResource({ state: "loaded", data });
        }
      },
      (error: unknown) => {
        if (controller.signal.aborted || isAbort(error)) {
          return;
        }
        setResource({ state: "error", error: toApiError(error) });
      },
    );
    return () => controller.abort();
  }, [load, generation]);

  const reload = useCallback(() => setGeneration((value) => value + 1), []);
  return [resource, reload];
}

function toApiError(error: unknown): ApiError {
  if (error instanceof ApiError) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  return new ApiError(null, null, `Unexpected failure: ${message}`, null);
}
