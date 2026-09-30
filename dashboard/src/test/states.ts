// Assertions for the four states every view must render distinctly: loading,
// empty, error, and loaded. Each also asserts the other three are absent, so a
// spinner can never pass for an empty state or an error for a loaded one.

import { screen, within } from "@testing-library/react";
import { expect } from "vitest";
import { FIXTURE_REQUEST_ID } from "./fixtures";

type State = "loading" | "empty" | "error" | "loaded";
const STATES: readonly State[] = ["loading", "empty", "error", "loaded"];

function expectOnly(container: HTMLElement, state: State): void {
  for (const other of STATES) {
    const present = [...container.querySelectorAll(`[data-state="${other}"]`)].some(
      // A section of a loaded view may itself be empty -- an overview with no
      // workers, a job with no attempts. That is part of "loaded", not a
      // second state competing with it.
      (element) =>
        !(state === "loaded" && other === "empty" && element.closest('[data-state="loaded"]')),
    );
    expect(present, `data-state="${other}"`).toBe(other === state);
  }
}

export async function expectLoading(container: HTMLElement): Promise<void> {
  const status = await screen.findByRole("status");
  expect(status.textContent).toMatch(/^Loading /);
  expectOnly(container, "loading");
}

export async function expectEmpty(container: HTMLElement, title: string): Promise<void> {
  await screen.findByText(title);
  const empty = container.querySelector('[data-state="empty"]');
  expect(empty?.textContent).toContain(title);
  expectOnly(container, "empty");
}

/**
 * The error state renders the server's code, its message, and -- the
 * assertion this milestone exists to make -- the request id, which is the only
 * way an operator's report of a failure can be found in the server's logs.
 */
export async function expectError(
  container: HTMLElement,
  status: number,
  code: string,
  message: string,
): Promise<void> {
  const alert = await screen.findByRole("alert");
  expect(within(alert).getByText(String(status))).toBeTruthy();
  expect(within(alert).getByTestId("error-code").textContent).toBe(code);
  expect(within(alert).getByTestId("error-message").textContent).toBe(message);
  expect(within(alert).getByTestId("request-id").textContent).toBe(FIXTURE_REQUEST_ID);
  expectOnly(container, "error");
}

export async function expectLoaded(container: HTMLElement): Promise<HTMLElement> {
  await screen.findAllByText((_, element) => element?.getAttribute("data-state") === "loaded");
  expectOnly(container, "loaded");
  const loaded = container.querySelector<HTMLElement>('[data-state="loaded"]');
  if (loaded === null) {
    throw new Error("no loaded region");
  }
  return loaded;
}
