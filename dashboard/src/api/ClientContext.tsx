import { createContext, type ReactNode, useContext } from "react";
import type { TaskForgeClient } from "./client";

const ClientContext = createContext<TaskForgeClient | null>(null);

export function ClientProvider({
  client,
  children,
}: {
  client: TaskForgeClient;
  children: ReactNode;
}) {
  return <ClientContext.Provider value={client}>{children}</ClientContext.Provider>;
}

/** Every view reads data through this, and only through this. */
export function useClient(): TaskForgeClient {
  const client = useContext(ClientContext);
  if (client === null) {
    throw new Error("useClient must be used inside a ClientProvider");
  }
  return client;
}
