// A deliberately small client-side router: six views, one of them
// parameterized, is not enough to justify a routing dependency. taskforge-api
// answers every non-file path under /dashboard/ with the entry document, so a
// reload or a pasted link lands here and is resolved by parseRoute.

import { type AnchorHTMLAttributes, type MouseEvent, useEffect, useState } from "react";
import { BASE_PATH } from "./basePath";

export type Route =
  | { name: "overview" }
  | { name: "jobs" }
  | { name: "job"; jobId: string }
  | { name: "workers" }
  | { name: "queues" }
  | { name: "dlq" }
  | { name: "not_found" };

export function parseRoute(pathname: string): Route {
  if (!pathname.startsWith(BASE_PATH)) {
    return { name: "not_found" };
  }
  const segments = pathname.slice(BASE_PATH.length).split("/").filter(Boolean);
  const [first, second, ...rest] = segments;
  if (first === undefined) {
    return { name: "overview" };
  }
  if (rest.length > 0) {
    return { name: "not_found" };
  }
  if (first === "jobs" && second !== undefined) {
    const jobId = decodeSegment(second);
    return jobId === null ? { name: "not_found" } : { name: "job", jobId };
  }
  if (second !== undefined) {
    return { name: "not_found" };
  }
  switch (first) {
    case "jobs":
      return { name: "jobs" };
    case "workers":
      return { name: "workers" };
    case "queues":
      return { name: "queues" };
    case "dlq":
      return { name: "dlq" };
    default:
      return { name: "not_found" };
  }
}

/**
 * decodeURIComponent throws URIError on a malformed escape, and a throw during
 * render would blank the whole app. taskforge-api already refuses the obvious
 * cases (an invalid escape is a 400, invalid UTF-8 a JSON 404), but the router
 * must not depend on that: history navigation never reaches the server.
 */
function decodeSegment(segment: string): string | null {
  try {
    return decodeURIComponent(segment);
  } catch {
    return null;
  }
}

export function hrefFor(route: Exclude<Route, { name: "not_found" }>): string {
  switch (route.name) {
    case "overview":
      return BASE_PATH;
    case "job":
      return `${BASE_PATH}jobs/${encodeURIComponent(route.jobId)}`;
    default:
      return `${BASE_PATH}${route.name}`;
  }
}

const NAVIGATE_EVENT = "taskforge:navigate";

export function navigate(href: string): void {
  window.history.pushState(null, "", href);
  window.dispatchEvent(new Event(NAVIGATE_EVENT));
}

export function usePathname(): string {
  const [pathname, setPathname] = useState(() => window.location.pathname);
  useEffect(() => {
    const update = () => setPathname(window.location.pathname);
    window.addEventListener("popstate", update);
    window.addEventListener(NAVIGATE_EVENT, update);
    return () => {
      window.removeEventListener("popstate", update);
      window.removeEventListener(NAVIGATE_EVENT, update);
    };
  }, []);
  return pathname;
}

type LinkProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "href"> & { href: string };

/** A real anchor, so it can be opened in a new tab; a plain click stays in-app. */
export function Link({ href, onClick, ...rest }: LinkProps) {
  const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
    onClick?.(event);
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.metaKey ||
      event.ctrlKey ||
      event.shiftKey ||
      event.altKey
    ) {
      return;
    }
    event.preventDefault();
    navigate(href);
  };
  return <a href={href} onClick={handleClick} {...rest} />;
}
