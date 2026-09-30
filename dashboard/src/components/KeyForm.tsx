import { type FormEvent, useId } from "react";

/**
 * Where the operator supplies their own API key. The dashboard has no
 * credential of its own and cannot mint one; the key is created exactly as it
 * is for the CLI or the SDK.
 */
export function KeyForm({ onSubmit }: { onSubmit: (key: string) => void }) {
  const inputId = useId();
  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const key = String(new FormData(event.currentTarget).get("apiKey") ?? "").trim();
    if (key !== "") {
      onSubmit(key);
    }
  };
  return (
    <section className="panel key-form" aria-labelledby="key-form-title">
      <h1 id="key-form-title">Enter an API key</h1>
      <p>
        Every view reads the authenticated <code>/v1</code> API with your own key, exactly as{" "}
        <code>taskforge-cli</code> does. It is kept in this tab's session storage only and is
        forgotten when the tab closes.
      </p>
      <p>
        Create one with{" "}
        <code>taskforge-cli api-keys create --scope &lt;scope&gt; --name &lt;name&gt;</code>. The
        dashboard shows only that scope's jobs.
      </p>
      <form onSubmit={handleSubmit}>
        <label htmlFor={inputId}>API key</label>
        <input
          id={inputId}
          name="apiKey"
          type="password"
          autoComplete="off"
          spellCheck={false}
          required
        />
        <button type="submit">Use this key</button>
      </form>
    </section>
  );
}
