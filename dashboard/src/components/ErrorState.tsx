import { type ApiError, HTTP_UNAUTHORIZED } from "../api/client";

/**
 * A failed request, rendered with everything taskforge-api said about it. The
 * request id is shown whenever the server sent one: it is the field that makes
 * a failure an operator reports findable in the server's logs.
 */
export function ErrorState({ error, onRetry }: { error: ApiError; onRetry?: () => void }) {
  return (
    <div className="state state-error" role="alert" data-state="error">
      <p className="state-title">
        {error.status === null ? "taskforge-api could not be reached" : "The request failed"}
      </p>
      <dl className="error-fields">
        {error.status !== null && (
          <>
            <dt>HTTP status</dt>
            <dd>{error.status}</dd>
          </>
        )}
        {error.code !== null && (
          <>
            <dt>Code</dt>
            <dd>
              <code data-testid="error-code">{error.code}</code>
            </dd>
          </>
        )}
        <dt>Message</dt>
        <dd data-testid="error-message">{error.message}</dd>
        {error.requestId !== null && (
          <>
            <dt>Request id</dt>
            <dd>
              <code data-testid="request-id">{error.requestId}</code>
            </dd>
          </>
        )}
      </dl>
      {error.status === HTTP_UNAUTHORIZED && (
        <p>The API key was not accepted. Check it, or forget it and enter another.</p>
      )}
      {onRetry !== undefined && (
        <button type="button" onClick={onRetry}>
          Try again
        </button>
      )}
    </div>
  );
}
