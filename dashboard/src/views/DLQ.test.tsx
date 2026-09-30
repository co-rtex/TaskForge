import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { dlqEntry, dlqPage, errorBody } from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectEmpty, expectError, expectLoaded, expectLoading } from "../test/states";
import { DLQ } from "./DLQ";

describe("DLQ", () => {
  it("renders a loading state", async () => {
    const { container } = renderWithClient(<DLQ />, routes({ "/v1/dlq": pending }));
    await expectLoading(container);
  });

  it("renders an empty state for an empty dead-letter queue", async () => {
    const { container } = renderWithClient(
      <DLQ />,
      routes({ "/v1/dlq": () => json({ entries: [] }) }),
    );
    await expectEmpty(container, "The dead-letter queue is empty.");
  });

  it("renders the error state with the request id", async () => {
    const { container } = renderWithClient(
      <DLQ />,
      routes({
        "/v1/dlq": () => json(errorBody("invalid_cursor", "the cursor is not valid"), 400),
      }),
    );
    await expectError(container, 400, "invalid_cursor", "the cursor is not valid");
  });

  it("renders each entry, linked to its job, and offers no write action", async () => {
    const { container } = renderWithClient(<DLQ />, routes({ "/v1/dlq": () => json(dlqPage) }));
    const loaded = await expectLoaded(container);
    const row = within(loaded).getByText(dlqEntry.job_id).closest("tr");
    expect(row?.textContent).toContain("ATTEMPTS_EXHAUSTED");
    expect(row?.textContent).toContain("#4 of 4");
    expect(row?.textContent).toContain(dlqEntry.error_code);
    expect(row?.textContent).toContain(dlqEntry.error_message);
    expect(within(loaded).getByText(dlqEntry.job_id).closest("a")?.getAttribute("href")).toBe(
      `/dashboard/jobs/${dlqEntry.job_id}`,
    );
    // Read-only: the only buttons are pagination and refresh.
    const labels = screen.getAllByRole("button").map((button) => button.textContent);
    expect(labels.sort()).toEqual(["Next", "Previous", "Refresh"]);
  });
});
