import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { OVERVIEW_JOB_LIMIT, OVERVIEW_WORKER_LIMIT } from "../format";
import { errorBody, jobPage, queueList, TEST_API_KEY, workerPage } from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectError, expectLoaded, expectLoading } from "../test/states";
import { Overview } from "./Overview";

const loadedRoutes = routes({
  "/v1/queues": () => json(queueList),
  "/v1/workers": () => json(workerPage),
  "/v1/jobs": () => json(jobPage),
});

describe("Overview", () => {
  it("renders a loading state while any of its three reads is outstanding", async () => {
    const { container } = renderWithClient(
      <Overview />,
      routes({
        "/v1/queues": () => json(queueList),
        "/v1/workers": pending,
        "/v1/jobs": () => json(jobPage),
      }),
    );
    await expectLoading(container);
  });

  it("renders an empty state per section for a system with nothing in it", async () => {
    const { container } = renderWithClient(
      <Overview />,
      routes({
        "/v1/queues": () => json({ queues: [] }),
        "/v1/workers": () => json({ workers: [] }),
        "/v1/jobs": () => json({ jobs: [] }),
      }),
    );
    const loaded = await expectLoaded(container);
    // A successful read of nothing: three empty sections, not an error and not
    // a spinner that never resolves.
    expect(loaded.querySelectorAll('[data-state="empty"]')).toHaveLength(3);
    expect(screen.getByText("No queues exist.")).toBeTruthy();
    expect(screen.getByText("No workers have registered.")).toBeTruthy();
    expect(screen.getByText("No jobs have been submitted.")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("renders the error state, with its request id, when any read fails", async () => {
    const { container } = renderWithClient(
      <Overview />,
      routes({
        "/v1/queues": () => json(queueList),
        "/v1/workers": () => json(errorBody("unauthorized", "a valid API key is required"), 401),
        "/v1/jobs": () => json(jobPage),
      }),
    );
    await expectError(container, 401, "unauthorized", "a valid API key is required");
  });

  it("summarizes exactly the rows the API returned", async () => {
    const { container, calls } = renderWithClient(<Overview />, loadedRoutes);
    const loaded = await expectLoaded(container);

    // Queue depth summed across queues, per non-terminal status: 5+11+2+3+7+1.
    expect(loaded.textContent).toContain("1 queue, 29 non-terminal jobs visible to this key.");
    // Workers counted by status from the page actually read.
    expect(loaded.textContent).toContain("3 workers, by status.");
    expect(screen.getByText("fixture.send_email")).toBeTruthy();

    const workers = calls.find((call) => call.url.pathname === "/v1/workers");
    expect(workers?.url.searchParams.get("limit")).toBe(String(OVERVIEW_WORKER_LIMIT));
    const jobs = calls.find((call) => call.url.pathname === "/v1/jobs");
    expect(jobs?.url.searchParams.get("limit")).toBe(String(OVERVIEW_JOB_LIMIT));
    for (const call of calls) {
      expect(call.authorization).toBe(`Bearer ${TEST_API_KEY}`);
    }
  });

  it("counts a worker status it does not recognize instead of dropping it", async () => {
    const { container } = renderWithClient(
      <Overview />,
      routes({
        "/v1/queues": () => json(queueList),
        // A status a newer server might add. The type says it cannot happen;
        // a rolling deploy says it can.
        "/v1/workers": () =>
          json({
            workers: [...workerPage.workers, { ...workerPage.workers[0], status: "RETIRED" }],
          }),
        "/v1/jobs": () => json(jobPage),
      }),
    );
    const loaded = await expectLoaded(container);
    expect(loaded.textContent).toContain(
      "4 workers, by status. 1 with a status this dashboard does not recognize.",
    );
    expect(loaded.textContent).not.toContain("NaN");
  });

  it("says so when the worker summary covers only the first page", async () => {
    const { container } = renderWithClient(
      <Overview />,
      routes({
        "/v1/queues": () => json(queueList),
        "/v1/workers": () => json({ ...workerPage, next_cursor: "fixture-cursor-workers-2" }),
        "/v1/jobs": () => json(jobPage),
      }),
    );
    const loaded = await expectLoaded(container);
    expect(loaded.textContent).toContain(
      "The first 3 workers, by status. More exist; see Workers.",
    );
  });
});
