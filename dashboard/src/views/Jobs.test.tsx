import { fireEvent, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PAGE_SIZE } from "../format";
import { errorBody, JOB_ID, jobPage, jobSummary } from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectEmpty, expectError, expectLoaded, expectLoading } from "../test/states";
import { Jobs } from "./Jobs";

describe("Jobs", () => {
  it("renders a loading state", async () => {
    const { container } = renderWithClient(<Jobs />, routes({ "/v1/jobs": pending }));
    await expectLoading(container);
  });

  it("renders an empty state for a successful read of zero jobs", async () => {
    const { container } = renderWithClient(
      <Jobs />,
      routes({ "/v1/jobs": () => json({ jobs: [] }) }),
    );
    await expectEmpty(container, "No jobs have been submitted.");
  });

  it("renders the error state with the request id", async () => {
    const { container } = renderWithClient(
      <Jobs />,
      routes({ "/v1/jobs": () => json(errorBody("internal_error", "internal error"), 500) }),
    );
    await expectError(container, 500, "internal_error", "internal error");
  });

  it("renders each job the API returned, linked to its detail view", async () => {
    const { container, calls } = renderWithClient(
      <Jobs />,
      routes({ "/v1/jobs": () => json(jobPage) }),
    );
    const loaded = await expectLoaded(container);
    const row = within(loaded).getByText(JOB_ID).closest("tr");
    expect(row?.textContent).toContain(jobSummary.queue);
    expect(row?.textContent).toContain(jobSummary.job_type);
    expect(row?.textContent).toContain("RETRY_WAIT");
    expect(within(loaded).getByText(JOB_ID).closest("a")?.getAttribute("href")).toBe(
      `/dashboard/jobs/${JOB_ID}`,
    );
    expect(calls[0]?.url.searchParams.get("limit")).toBe(String(PAGE_SIZE));
    expect(calls[0]?.url.searchParams.has("cursor")).toBe(false);
  });

  it("follows next_cursor, and only offers Next when the API sent one", async () => {
    let served = 0;
    const { container, calls } = renderWithClient(
      <Jobs />,
      routes({
        "/v1/jobs": () => {
          served += 1;
          // The second page is the last: no next_cursor.
          return json(served === 1 ? jobPage : { jobs: [jobSummary] });
        },
      }),
    );
    await expectLoaded(container);
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    await screen.findByText("Page 2");
    await expectLoaded(container);
    expect(calls[1]?.url.searchParams.get("cursor")).toBe(jobPage.next_cursor);
    expect((screen.getByRole("button", { name: "Next" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("sends the chosen filters and says an empty filtered result is filtered", async () => {
    const { container, calls } = renderWithClient(
      <Jobs />,
      routes({ "/v1/jobs": () => json({ jobs: [] }) }),
    );
    await expectEmpty(container, "No jobs have been submitted.");
    fireEvent.change(screen.getByLabelText("Status"), { target: { value: "DEAD_LETTERED" } });
    fireEvent.change(screen.getByLabelText("Queue"), { target: { value: "fixture-queue-emails" } });
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    await expectEmpty(container, "No jobs match these filters.");
    const last = calls.at(-1);
    expect(last?.url.searchParams.get("status")).toBe("DEAD_LETTERED");
    expect(last?.url.searchParams.get("queue")).toBe("fixture-queue-emails");
  });
});
