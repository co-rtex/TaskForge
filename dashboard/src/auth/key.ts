// The operator's API key lives in sessionStorage and nowhere else: never
// localStorage, never a cookie, never the URL. sessionStorage is scoped to one
// tab and cleared when it closes, so the credential's lifetime in the browser
// is bounded by the tab. The trade-off -- a script running in this origin can
// read it -- and what mitigates it are recorded in docs/adr/0017.

const STORAGE_KEY = "taskforge.dashboard.apiKey";

function storage(): Storage | null {
  try {
    return globalThis.sessionStorage ?? null;
  } catch {
    // Some privacy modes throw on access rather than returning null.
    return null;
  }
}

export function readKey(): string | null {
  try {
    const value = storage()?.getItem(STORAGE_KEY) ?? null;
    return value === "" ? null : value;
  } catch {
    return null;
  }
}

/** Returns false when the browser refused to store it; the key then lasts only for this page. */
export function saveKey(key: string): boolean {
  try {
    const store = storage();
    if (store === null) {
      return false;
    }
    store.setItem(STORAGE_KEY, key);
    return true;
  } catch {
    return false;
  }
}

export function clearKey(): void {
  try {
    storage()?.removeItem(STORAGE_KEY);
  } catch {
    // Nothing stored, or nothing storable; either way nothing to clear.
  }
}

export const KEY_STORAGE_NAME = STORAGE_KEY;
