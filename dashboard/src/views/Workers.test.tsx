import { within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  crashedWorker,
  errorBody,
  healthyWorker,
  replacedWorker,
  workerPage,
} from "../test/fixtures";
import { json, pending, renderWithClient, routes } from "../test/harness";
import { expectEmpty, expectError, expectLoaded, expectLoading } from "../test/states";
import { Workers } from "./Workers";

describe("Workers", () => {
  it("renders a loading state", async () => {
    const { container } = renderWithClient(<Workers />, routes({ "/v1/workers": pending }));
    await expectLoading(container);
  });

  it("renders an empty state when no worker has registered", async () => {
    const { container } = renderWithClient(
      <Workers />,
      routes({ "/v1/workers": () => json({ workers: [] }) }),
    );
    await expectEmpty(container, "No workers have registered.");
  });

  it("renders the error state with the request id", async () => {
    const { container } = renderWithClient(
      <Workers />,
      routes({
        "/v1/workers": () =>
          json(errorBody("service_unavailable", "request deadline elapsed"), 503),
      }),
    );
    await expectError(container, 503, "service_unavailable", "request deadline elapsed");
  });

  it("lists a crashed worker as UNHEALTHY and a replaced one by its newest session", async () => {
    const { container } = renderWithClient(
      <Workers />,
      routes({ "/v1/workers": () => json(workerPage) }),
    );
    const loaded = await expectLoaded(container);
    const rows = loaded.querySelectorAll("tbody tr");
    expect(rows).toHaveLength(3);

    const rowFor = (name: string) => within(loaded).getByText(name).closest("tr");
    // A badge class, not a substring: "UNHEALTHY" contains "HEALTHY".
    expect(rowFor(healthyWorker.name)?.querySelector(".badge-healthy")).not.toBeNull();
    expect(rowFor(healthyWorker.name)?.textContent).toContain("3 / 8");
    // A crashed process is the row an operator opens this page to find. It is
    // listed, and says so in text, not only in color.
    expect(rowFor(crashedWorker.name)?.textContent).toContain("UNHEALTHY");
    expect(rowFor(crashedWorker.name)?.querySelector(".badge-unhealthy")).not.toBeNull();
    // Heartbeat age as the API measured it, formatted, not judged.
    expect(rowFor(crashedWorker.name)?.textContent).toContain("4m 28s");
    // M6A reports a replaced worker's newest session, so it shows that
    // session's status and limit, not the OFFLINE one it replaced.
    expect(rowFor(replacedWorker.name)?.querySelector(".badge-healthy")).not.toBeNull();
    expect(rowFor(replacedWorker.name)?.querySelector(".badge-unhealthy")).toBeNull();
    expect(rowFor(replacedWorker.name)?.textContent).toContain("1 / 6");
  });

  // Rendering only. OFFLINE is in the Worker.status enum, but M6A never returns
  // it as a worker's latest status today (see the replacedWorker fixture). This
  // pins that the value would still be labeled in text if it ever did.
  it("labels OFFLINE in text, as a rendering-only case", async () => {
    const { container } = renderWithClient(
      <Workers />,
      routes({
        "/v1/workers": () => json({ workers: [{ ...crashedWorker, status: "OFFLINE" }] }),
      }),
    );
    const loaded = await expectLoaded(container);
    const row = within(loaded).getByText(crashedWorker.name).closest("tr");
    expect(row?.textContent).toContain("OFFLINE");
    expect(row?.querySelector(".badge-offline")).not.toBeNull();
  });
});
