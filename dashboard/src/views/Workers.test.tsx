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

  it("lists crashed and replaced workers, each labeled by its own status", async () => {
    const { container } = renderWithClient(
      <Workers />,
      routes({ "/v1/workers": () => json(workerPage) }),
    );
    const loaded = await expectLoaded(container);
    const rows = loaded.querySelectorAll("tbody tr");
    expect(rows).toHaveLength(3);

    const rowFor = (name: string) => within(loaded).getByText(name).closest("tr");
    expect(rowFor(healthyWorker.name)?.textContent).toContain("HEALTHY");
    expect(rowFor(healthyWorker.name)?.textContent).toContain("3 / 8");
    // UNHEALTHY and OFFLINE are the rows an operator opens this page to find.
    // They are listed, and each says so in text, not only in color.
    expect(rowFor(crashedWorker.name)?.textContent).toContain("UNHEALTHY");
    expect(rowFor(crashedWorker.name)?.querySelector(".badge-unhealthy")).not.toBeNull();
    expect(rowFor(replacedWorker.name)?.textContent).toContain("OFFLINE");
    expect(rowFor(replacedWorker.name)?.querySelector(".badge-offline")).not.toBeNull();
    // Heartbeat age as the API measured it, formatted, not judged.
    expect(rowFor(crashedWorker.name)?.textContent).toContain("4m 28s");
  });
});
