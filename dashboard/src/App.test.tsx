import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { App } from "./App";
import { createClient } from "./api/client";
import { KEY_STORAGE_NAME } from "./auth/key";
import { navigate, parseRoute } from "./router";
import { queueList, TEST_API_KEY } from "./test/fixtures";

function recordingClient() {
  const seen: string[] = [];
  const makeClient = (apiKey: string) =>
    createClient({
      apiKey,
      fetch: async (input, init = {}) => {
        seen.push(new Headers(init.headers).get("Authorization") ?? "");
        const path = new URL(String(input), "http://dashboard.test").pathname;
        if (path === "/v1/queues") {
          return Response.json(queueList);
        }
        return Response.json({ jobs: [], workers: [], entries: [] });
      },
    });
  return { makeClient, seen };
}

beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
  window.history.replaceState(null, "", "/dashboard/");
});

afterEach(() => {
  cleanup();
});

describe("App", () => {
  it("asks for a key before making any request", () => {
    const { makeClient, seen } = recordingClient();
    render(<App makeClient={makeClient} />);
    expect(screen.getByRole("heading", { name: "Enter an API key" })).toBeTruthy();
    expect(seen).toEqual([]);
  });

  it("keeps the key in sessionStorage only, and presents it on every read", async () => {
    const { makeClient, seen } = recordingClient();
    render(<App makeClient={makeClient} />);
    fireEvent.change(screen.getByLabelText("API key"), { target: { value: TEST_API_KEY } });
    fireEvent.click(screen.getByRole("button", { name: "Use this key" }));

    await screen.findByRole("heading", { name: "Overview" });
    expect(sessionStorage.getItem(KEY_STORAGE_NAME)).toBe(TEST_API_KEY);
    expect(localStorage.length).toBe(0);
    expect(document.cookie).toBe("");
    await screen.findByText("1 queue, 29 non-terminal jobs visible to this key.");
    expect(seen.length).toBeGreaterThan(0);
    expect(new Set(seen)).toEqual(new Set([`Bearer ${TEST_API_KEY}`]));
  });

  it("forgets the key on request", async () => {
    sessionStorage.setItem(KEY_STORAGE_NAME, TEST_API_KEY);
    const { makeClient } = recordingClient();
    render(<App makeClient={makeClient} />);
    fireEvent.click(await screen.findByRole("button", { name: "Forget API key" }));
    expect(sessionStorage.getItem(KEY_STORAGE_NAME)).toBeNull();
    expect(screen.getByRole("heading", { name: "Enter an API key" })).toBeTruthy();
  });

  it("routes between views without a page load, and marks the current one", async () => {
    sessionStorage.setItem(KEY_STORAGE_NAME, TEST_API_KEY);
    const { makeClient } = recordingClient();
    render(<App makeClient={makeClient} />);
    await screen.findByRole("heading", { name: "Overview" });

    const nav = screen.getByRole("navigation", { name: "Dashboard views" });
    fireEvent.click(within(nav).getByRole("link", { name: "Queues" }));
    await screen.findByRole("heading", { name: "Queues", level: 1 });
    expect(window.location.pathname).toBe("/dashboard/queues");
    expect(within(nav).getByRole("link", { name: "Queues" }).getAttribute("aria-current")).toBe(
      "page",
    );
    expect(
      within(nav).getByRole("link", { name: "Overview" }).getAttribute("aria-current"),
    ).toBeNull();

    act(() => navigate("/dashboard/no-such-view"));
    expect(screen.getByText("There is no dashboard view at this address.")).toBeTruthy();
  });
});

describe("parseRoute", () => {
  it.each([
    ["/dashboard/", { name: "overview" }],
    ["/dashboard", { name: "not_found" }],
    ["/dashboard/jobs", { name: "jobs" }],
    ["/dashboard/jobs/", { name: "jobs" }],
    ["/dashboard/jobs/abc", { name: "job", jobId: "abc" }],
    ["/dashboard/jobs/abc/attempts", { name: "not_found" }],
    ["/dashboard/workers", { name: "workers" }],
    ["/dashboard/queues", { name: "queues" }],
    ["/dashboard/dlq", { name: "dlq" }],
    ["/dashboard/dlq/x", { name: "not_found" }],
    ["/dashboard/elsewhere", { name: "not_found" }],
    ["/v1/jobs", { name: "not_found" }],
  ])("%s", (path, route) => {
    expect(parseRoute(path)).toEqual(route);
  });
});
