// Drives views through the REAL client with a stubbed fetch, so every view
// test also exercises URL building, the Authorization header, and error-body
// parsing -- not a hand-rolled fake that could agree with the view by accident.

import { cleanup, render } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach } from "vitest";
import { ClientProvider } from "../api/ClientContext";
import { createClient, REQUEST_ID_HEADER } from "../api/client";
import { FIXTURE_REQUEST_ID, TEST_API_KEY } from "./fixtures";

afterEach(() => {
  cleanup();
});

export type Handler = (url: URL, init: RequestInit) => Promise<Response>;

export interface RecordedCall {
  url: URL;
  authorization: string | null;
}

export function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", [REQUEST_ID_HEADER]: FIXTURE_REQUEST_ID },
  });
}

/** A request that never settles, which holds a view in its loading state. */
export function pending(): Promise<Response> {
  return new Promise<Response>(() => {});
}

/** Routes by exact pathname; anything unrouted fails the test loudly. */
export function routes(table: Record<string, () => Promise<Response> | Response>): Handler {
  return async (url) => {
    const route = table[url.pathname];
    if (route === undefined) {
      throw new Error(`unexpected request to ${url.pathname}`);
    }
    return route();
  };
}

export function renderWithClient(ui: ReactElement, handler: Handler) {
  const calls: RecordedCall[] = [];
  const client = createClient({
    apiKey: TEST_API_KEY,
    fetch: async (input, init = {}) => {
      const url = new URL(String(input), "http://dashboard.test");
      calls.push({ url, authorization: new Headers(init.headers).get("Authorization") });
      return handler(url, init);
    },
  });
  const result = render(<ClientProvider client={client}>{ui}</ClientProvider>);
  return { ...result, calls };
}
