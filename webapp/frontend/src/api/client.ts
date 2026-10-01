// Typed access to the backend. Every type comes from schema.d.ts, which is
// generated from the backend's checked-in OpenAPI contract, so a field the
// API no longer has is a compile error here rather than `undefined` at runtime.
import type { components, operations } from "./schema";

type Schemas = components["schemas"];
export type Forecast = Schemas["ForecastOut"];
export type VariableSeries = Schemas["VariableSeries"];
export type PictogramItem = Schemas["PictogramItem"];
export type PictogramSeries = Schemas["PictogramSeries"];
export type Place = Schemas["Place"];
export type Product = Schemas["ProductOut"];
export type Scheme = Schemas["SchemeOut"];
export type Schemes = Schemas["SchemesOut"];

export type ForecastQuery = operations["forecast_api_forecast_get"]["parameters"]["query"];
export type GeocodeQuery = operations["geocode_api_geocode_get"]["parameters"]["query"];

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/** The human-readable part of an error body: {"detail": "..."} or FastAPI's
 * [{"loc": [...], "msg": "..."}] for invalid parameters. */
export function errorMessage(status: number, body: unknown): string {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      const messages = detail
        .map((item) => (item && typeof item === "object" && "msg" in item ? String(item.msg) : ""))
        .filter(Boolean);
      if (messages.length) return messages.join("; ");
    }
  }
  return `HTTP ${status}`;
}

async function getJson<T>(path: string, params: Record<string, unknown>, signal?: AbortSignal): Promise<T> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") query.set(key, String(value));
  }
  const url = query.size ? `${path}?${query}` : path;
  const response = await fetch(url, { signal, headers: { Accept: "application/json" } });
  if (!response.ok) {
    let body: unknown = null;
    try {
      body = await response.json();
    } catch {
      // not JSON - fall back to the status
    }
    throw new ApiError(response.status, errorMessage(response.status, body));
  }
  return (await response.json()) as T;
}

export const api = {
  forecast: (query: ForecastQuery, signal?: AbortSignal) => getJson<Forecast>("/api/forecast", query, signal),
  geocode: (query: GeocodeQuery, signal?: AbortSignal) =>
    getJson<Schemas["GeocodeOut"]>("/api/geocode", query, signal),
  products: (signal?: AbortSignal) => getJson<Schemas["ProductsOut"]>("/api/products", {}, signal),
  schemes: (signal?: AbortSignal) => getJson<Schemes>("/api/schemes", {}, signal),
};
