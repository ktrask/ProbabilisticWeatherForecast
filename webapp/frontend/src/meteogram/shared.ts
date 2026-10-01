// What both layouts of the meteogram share: which pictogram lanes there are and
// in which order, the temperature scale, and when a pictogram belongs in time.
//
// Instant variables (clouds, wind, temperature) belong to their step's time.
// Precipitation is the total over [t, t + window); its pictogram stands at t,
// the start of that window, in one column with the step's other pictograms -
// the user's call of 2026-10-01: in the middle of the window, between two
// instants, the rain row looked misaligned. The crosshair shows the window.
import type { Forecast, PictogramSeries, VariableSeries } from "../api/client";
import { extent } from "./layout";
import { HOUR_MS } from "./time";

// The temperature band, from the widest spread to the narrowest.
export const BANDS: [string, string, string][] = [
  ["p0", "p100", "band-outer"],
  ["p10", "p90", "band-middle"],
  ["p25", "p75", "band-inner"],
];
// Pictogram lanes before and after the temperature band: top to bottom in the
// row layout, left to right in the column layout.
export const BEFORE = ["cloud_cover", "precipitation"];
export const AFTER = ["wind_speed_10m"];
export const PICTOGRAM = { min: 14, max: 46 };

export interface Lane {
  variable: string;
  series: PictogramSeries;
  windowMs: number | null; // for totals: the window each pictogram covers
}

export function lanes(forecast: Forecast, variables: string[]): Lane[] {
  const result: Lane[] = [];
  for (const variable of variables) {
    const series = forecast.pictograms[variable];
    if (!series) continue;
    const hours = forecast.variables[variable]?.window_hours;
    result.push({ variable, series, windowMs: hours ? hours * HOUR_MS : null });
  }
  return result;
}

/** Where on the time axis a pictogram goes: its step - or null for a total
 * whose window runs past `end`, which is not drawn at all. */
export function pictogramTime(t: Date, windowMs: number | null, end: Date): Date | null {
  if (windowMs !== null && t.getTime() + windowMs > end.getTime()) return null;
  return t;
}

/** The temperature axis for the whole window, so that 5 degrees look like 5
 * degrees wherever the reader looks. */
export function temperatureDomain(series: VariableSeries, from: number, to: number): [number, number] {
  const q = series.quantiles;
  const outer = BANDS.find(([lo, hi]) => q[lo] && q[hi]);
  const [lo, hi] = extent(outer ? [q[outer[0]] as number[], q[outer[1]] as number[]] : [q.p50 ?? []], from, to);
  // Room on both sides for the extreme markers.
  return [lo - 1.5, hi + 1.5];
}
