import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  errorBody,
  failedAttempt,
  JOB_ID,
  job,
  REPLAYED_FROM_JOB_ID,
  runningAttempt,
} from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectError, expectLoaded, expectLoading } from "../test/states";
import { JobDetail } from "./JobDetail";

const jobPath = `/v1/jobs/${JOB_ID}`;
const attemptsPath = `/v1/jobs/${JOB_ID}/attempts`;

describe("JobDetail", () => {
  it("renders a loading state until both the job and its attempts arrive", async () => {
    const { container } = renderWithClient(
      <JobDetail jobId={JOB_ID} />,
      routes({ [jobPath]: () => json(job), [attemptsPath]: pending }),
    );
    await expectLoading(container);
  });

  it("renders an empty attempt timeline for a job no worker has claimed", async () => {
    const { container } = renderWithClient(
      <JobDetail jobId={JOB_ID} />,
      routes({
        [jobPath]: () => json({ ...job, status: "QUEUED" }),
        [attemptsPath]: () => json({ attempts: [] }),
      }),
    );
    const loaded = await expectLoaded(container);
    const empty = loaded.querySelector('[data-state="empty"]');
    expect(empty?.textContent).toContain("No attempts yet.");
    expect(loaded.querySelector("ol.timeline")).toBeNull();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("renders the error state with the request id for an unknown job", async () => {
    const { container } = renderWithClient(
      <JobDetail jobId={JOB_ID} />,
      routes({
        [jobPath]: () => json(errorBody("not_found", "job not found"), 404),
        [attemptsPath]: () => json(errorBody("not_found", "job not found"), 404),
      }),
    );
    await expectError(container, 404, "not_found", "job not found");
  });

  it("renders the job, its payload as text, and every attempt oldest first", async () => {
    const { container, calls } = renderWithClient(
      <JobDetail jobId={JOB_ID} />,
      routes({
        [jobPath]: () => json(job),
        [attemptsPath]: () => json({ attempts: [failedAttempt, runningAttempt] }),
      }),
    );
    const loaded = await expectLoaded(container);

    expect(loaded.querySelector("pre.payload")?.textContent).toBe(
      JSON.stringify(job.payload, null, 2),
    );
    const replayLink = within(loaded).getByText(REPLAYED_FROM_JOB_ID).closest("a");
    expect(replayLink?.getAttribute("href")).toBe(`/dashboard/jobs/${REPLAYED_FROM_JOB_ID}`);

    const entries = loaded.querySelectorAll("ol.timeline > li");
    expect(entries).toHaveLength(2);
    // The abandoned attempt comes first and says why it ended.
    expect(entries[0]?.textContent).toContain("Attempt 1");
    expect(entries[0]?.textContent).toContain("ABANDONED");
    expect(entries[0]?.textContent).toContain(failedAttempt.worker_name);
    expect(entries[0]?.textContent).toContain(failedAttempt.error_message);
    expect(entries[0]?.textContent).toContain("2.5s");
    expect(entries[1]?.textContent).toContain("Attempt 2");
    expect(entries[1]?.textContent).toContain("RUNNING");
    expect(entries[1]?.textContent).not.toContain("Failure class");

    expect(calls.map((call) => call.url.pathname).sort()).toEqual([jobPath, attemptsPath].sort());
  });
});
