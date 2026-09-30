import { within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { errorBody, queueList } from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectEmpty, expectError, expectLoaded, expectLoading } from "../test/states";
import { Queues } from "./Queues";

describe("Queues", () => {
  it("renders a loading state", async () => {
    const { container } = renderWithClient(<Queues />, routes({ "/v1/queues": pending }));
    await expectLoading(container);
  });

  it("renders an empty state for zero queues", async () => {
    const { container } = renderWithClient(
      <Queues />,
      routes({ "/v1/queues": () => json({ queues: [] }) }),
    );
    await expectEmpty(container, "No queues exist.");
  });

  it("renders the error state with the request id", async () => {
    const { container } = renderWithClient(
      <Queues />,
      routes({
        "/v1/queues": () => json(errorBody("unauthorized", "a valid API key is required"), 401),
      }),
    );
    await expectError(container, 401, "unauthorized", "a valid API key is required");
    expect(container.textContent).toContain("The API key was not accepted.");
  });

  it("keeps scope-local depth and queue-wide concurrency in separate, labeled columns", async () => {
    const { container } = renderWithClient(
      <Queues />,
      routes({ "/v1/queues": () => json(queueList) }),
    );
    const loaded = await expectLoaded(container);
    const headers = [...loaded.querySelectorAll("thead th")].map((th) => th.textContent);
    expect(headers).toContain("Depth (this key)");
    expect(headers).toContain("Max concurrency (queue-wide, all scopes)");

    const row = within(loaded).getByText("fixture-queue-emails").closest("tr");
    const cells = [...(row?.querySelectorAll("td") ?? [])].map((td) => td.textContent);
    // worker group, six depth columns in schema order, their total, then the limit.
    expect(cells).toEqual(["fixture-group", "5", "11", "2", "3", "7", "1", "29", "64"]);
    // Never rendered as a ratio: that would be a utilization the API never reported.
    expect(loaded.textContent).not.toMatch(/29\s*\/\s*64/);
  });
});
