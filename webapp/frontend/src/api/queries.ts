// Server state through TanStack Query: caching, de-duplication, and loading
// and error states, so "reactive" needs no store of its own.
import { keepPreviousData, useQuery } from "@tanstack/react-query";

import { ApiError, api, type ForecastQuery } from "./client";
import type { Lang } from "../i18n";

// Retry only what retrying can fix: not a 4xx, which will fail the same way.
function retry(failures: number, error: Error) {
  if (error instanceof ApiError && error.status < 500) return false;
  return failures < 2;
}

export function useForecast(query: ForecastQuery | null) {
  return useQuery({
    queryKey: ["forecast", query],
    queryFn: ({ signal }) => api.forecast(query as ForecastQuery, signal),
    enabled: query !== null,
    // The backend caches upstream answers for an hour; five minutes here keeps
    // switching back and forth between places instant.
    staleTime: 5 * 60_000,
    retry,
  });
}

export function useProducts() {
  return useQuery({ queryKey: ["products"], queryFn: ({ signal }) => api.products(signal), staleTime: Infinity, retry });
}

export function useSchemes() {
  return useQuery({ queryKey: ["schemes"], queryFn: ({ signal }) => api.schemes(signal), staleTime: Infinity, retry });
}

export const MIN_QUERY_LENGTH = 2;

export function useGeocode(text: string, lang: Lang) {
  const q = text.trim();
  return useQuery({
    queryKey: ["geocode", q.toLowerCase(), lang],
    queryFn: ({ signal }) => api.geocode({ q, lang, count: 6 }, signal),
    enabled: q.length >= MIN_QUERY_LENGTH,
    staleTime: 60 * 60_000,
    // Keep showing the last suggestions while the next ones load.
    placeholderData: keepPreviousData,
    retry,
  });
}
