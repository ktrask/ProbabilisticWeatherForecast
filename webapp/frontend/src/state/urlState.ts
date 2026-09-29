// The URL is the only place the view's state lives - place, product, variant,
// days - so every view is a link, and back/forward walk through places.
import { useCallback, useMemo, useSyncExternalStore } from "react";

export type Variant = "ensemble" | "hres";

export interface ViewState {
  lat: number | null;
  lon: number | null;
  name: string | null;
  product: string | null; // null: the API's default
  variant: Variant;
  days: number;
}

export const DEFAULT_DAYS = 5;
export const MAX_DAYS = 16;

function number(raw: string | null, min: number, max: number): number | null {
  if (raw === null || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) && value >= min && value <= max ? value : null;
}

export function parseState(search: string): ViewState {
  const params = new URLSearchParams(search);
  const lat = number(params.get("lat"), -90, 90);
  const lon = number(params.get("lon"), -180, 180);
  const located = lat !== null && lon !== null;
  const days = number(params.get("days"), 1, MAX_DAYS);
  return {
    lat: located ? lat : null,
    lon: located ? lon : null,
    name: located ? params.get("name") || null : null,
    product: params.get("product") || null,
    variant: params.get("variant") === "hres" ? "hres" : "ensemble",
    days: days === null ? DEFAULT_DAYS : Math.round(days),
  };
}

/** The query string for `state`, leaving out what is the default anyway.
 *
 * Coordinates are written back exactly as they are: rounding them here would
 * turn a link's place into a slightly different one on the first adjustment,
 * and that is a new forecast to fetch. Whoever picks a place rounds it. */
export function formatState(state: ViewState, extra: Record<string, string> = {}): string {
  const params = new URLSearchParams();
  if (state.lat !== null && state.lon !== null) {
    params.set("lat", String(state.lat));
    params.set("lon", String(state.lon));
    if (state.name) params.set("name", state.name);
  }
  if (state.product) params.set("product", state.product);
  if (state.variant !== "ensemble") params.set("variant", state.variant);
  if (state.days !== DEFAULT_DAYS) params.set("days", String(state.days));
  for (const [key, value] of Object.entries(extra)) params.set(key, value);
  const text = params.toString();
  return text ? `?${text}` : "";
}

// Parameters the view does not own but must not drop, such as ?lang=.
const PASSED_THROUGH = ["lang"];

const listeners = new Set<() => void>();

function subscribe(listener: () => void) {
  listeners.add(listener);
  window.addEventListener("popstate", listener);
  return () => {
    listeners.delete(listener);
    window.removeEventListener("popstate", listener);
  };
}

const snapshot = () => window.location.search;

export interface Navigate {
  (patch: Partial<ViewState>, options?: { replace?: boolean }): void;
}

/** [state, navigate]. A new place pushes a history entry; `replace` is for
 * adjustments like the number of days, which should not fill the history. */
export function useUrlState(): [ViewState, Navigate] {
  const search = useSyncExternalStore(subscribe, snapshot, () => "");
  const state = useMemo(() => parseState(search), [search]);
  const navigate = useCallback<Navigate>((patch, options = {}) => {
    const current = new URLSearchParams(window.location.search);
    const extra: Record<string, string> = {};
    for (const key of PASSED_THROUGH) {
      const value = current.get(key);
      if (value !== null) extra[key] = value;
    }
    const next = { ...parseState(window.location.search), ...patch };
    const url = `${window.location.pathname}${formatState(next, extra)}`;
    if (options.replace) window.history.replaceState(null, "", url);
    else window.history.pushState(null, "", url);
    listeners.forEach((listener) => listener());
  }, []);
  return [state, navigate];
}
