import { useMemo, useState } from "react";
import { ClientProvider } from "./api/ClientContext";
import { createClient, type TaskForgeClient } from "./api/client";
import { clearKey, readKey, saveKey } from "./auth/key";
import { EmptyState } from "./components/EmptyState";
import { KeyForm } from "./components/KeyForm";
import { hrefFor, Link, parseRoute, type Route, usePathname } from "./router";
import { DLQ } from "./views/DLQ";
import { JobDetail } from "./views/JobDetail";
import { Jobs } from "./views/Jobs";
import { Overview } from "./views/Overview";
import { Queues } from "./views/Queues";
import { Workers } from "./views/Workers";

const NAV: { route: Exclude<Route, { name: "not_found" | "job" }>; label: string }[] = [
  { route: { name: "overview" }, label: "Overview" },
  { route: { name: "jobs" }, label: "Jobs" },
  { route: { name: "workers" }, label: "Workers" },
  { route: { name: "queues" }, label: "Queues" },
  { route: { name: "dlq" }, label: "DLQ" },
];

// Module-level, so its identity is stable. A default-parameter arrow would be a
// new function every render, rebuilding the client and re-running every read.
const sameOriginClient = (apiKey: string) => createClient({ apiKey });

export function App({
  makeClient = sameOriginClient,
}: {
  /** Injected by tests; production builds the same-origin fetch client. */
  makeClient?: (apiKey: string) => TaskForgeClient;
}) {
  const [apiKey, setApiKey] = useState<string | null>(() => readKey());
  const client = useMemo(() => (apiKey === null ? null : makeClient(apiKey)), [apiKey, makeClient]);
  const route = parseRoute(usePathname());

  const acceptKey = (key: string) => {
    saveKey(key);
    setApiKey(key);
  };
  const forgetKey = () => {
    clearKey();
    setApiKey(null);
  };

  return (
    <div className="app">
      <header className="app-header">
        <span className="brand">TaskForge</span>
        <nav aria-label="Dashboard views">
          <ul>
            {NAV.map(({ route: target, label }) => (
              <li key={target.name}>
                <Link
                  href={hrefFor(target)}
                  aria-current={
                    route.name === target.name || (route.name === "job" && target.name === "jobs")
                      ? "page"
                      : undefined
                  }
                >
                  {label}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
        {client !== null && (
          <button type="button" className="forget-key" onClick={forgetKey}>
            Forget API key
          </button>
        )}
      </header>
      <main>
        {client === null ? (
          <KeyForm onSubmit={acceptKey} />
        ) : (
          <ClientProvider client={client}>
            <View route={route} />
          </ClientProvider>
        )}
      </main>
    </div>
  );
}

function View({ route }: { route: Route }) {
  switch (route.name) {
    case "overview":
      return <Overview />;
    case "jobs":
      return <Jobs />;
    case "job":
      // Keyed so that following a link to another job starts a fresh read.
      return <JobDetail key={route.jobId} jobId={route.jobId} />;
    case "workers":
      return <Workers />;
    case "queues":
      return <Queues />;
    case "dlq":
      return <DLQ />;
    case "not_found":
      return (
        <EmptyState title="There is no dashboard view at this address.">
          <Link href={hrefFor({ name: "overview" })}>Go to the overview</Link>
        </EmptyState>
      );
  }
}
