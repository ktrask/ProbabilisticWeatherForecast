// Small synthetic API answers for component tests; the end-to-end tests use
// the real backend on recorded forecasts instead.
import type { Forecast, Scheme, Schemes } from "../api/client";

const H = 3_600_000;
export const START = new Date("2026-09-28T22:00:00Z"); // local midnight in Berlin

function series(n: number, base: number, spread: number) {
  const levels: Record<string, number> = { p0: -2, p10: -1.3, p25: -0.6, p50: 0, p75: 0.6, p90: 1.3, p100: 2 };
  return Object.fromEntries(
    Object.entries(levels).map(([name, k]) => [name, Array.from({ length: n }, (_, i) => base + i + k * spread)]),
  );
}

export function forecast(n = 12): Forecast {
  const steps = Array.from({ length: n }, (_, k) => new Date(START.getTime() + k * 6 * H).toISOString());
  const items = (pictogram: string, cls: string, level: number) =>
    Array.from({ length: n }, () => ({ pictogram, level, class: cls }));
  return {
    location: { lat: 52.25, lon: 10.5, elevation_m: 80, timezone: "Europe/Berlin", name: "Braunschweig" },
    run: { source: "test", init_time: null, members: 51 },
    deterministic_run: null,
    steps,
    step_hours: 6,
    variables: {
      temperature_2m: { unit: "degC", kind: "instant", window_hours: null, quantiles: series(n, 10, 2), deterministic: null },
      precipitation: { unit: "mm", kind: "sum", window_hours: 6, quantiles: series(n, 3, 1), deterministic: null },
      cloud_cover: { unit: "percent", kind: "instant", window_hours: null, quantiles: series(n, 50, 10), deterministic: null },
      wind_speed_10m: { unit: "m/s", kind: "instant", window_hours: null, quantiles: series(n, 4, 1), deterministic: null },
    },
    pictograms: {
      cloud_cover: { scheme: "cloud-vsup", items: items("cloud/step3_sunny.svg", "clear", 3) },
      precipitation: { scheme: "precipitation-vsup", items: items("rain/step2_mostly_dry.svg", "none+light", 2) },
      wind_speed_10m: { scheme: "wind-vsup", items: items("wind/step1_v2.svg", "calm+light+strong+storm", 1) },
    },
  };
}

export const rainScheme: Scheme = {
  name: "precipitation-vsup",
  description: null,
  mode: "tree",
  variable: "precipitation",
  unit: "mm",
  declared_unit: "mm",
  window_hours: 6,
  classes: [
    { id: "none", below: 0.1 },
    { id: "light", below: 1 },
    { id: "medium", below: 2 },
    { id: "heavy", below: null },
  ],
  outcomes: [
    { pictogram: "rain/dry.svg", level: 3, class: "none", condition: "p10..p90 in one group" },
    { pictogram: "rain/light.svg", level: 3, class: "light", condition: "p10..p90 in one group" },
    { pictogram: "rain/medium.svg", level: 3, class: "medium", condition: "p10..p90 in one group" },
    { pictogram: "rain/heavy.svg", level: 3, class: "heavy", condition: "p10..p90 in one group" },
    { pictogram: "rain/mostly-dry.svg", level: 2, class: "none+light", condition: "p25..p75 in one group" },
    { pictogram: "rain/mostly-wet.svg", level: 2, class: "medium+heavy", condition: "p25..p75 in one group" },
    { pictogram: "rain/unknown.svg", level: 1, class: "none+light+medium+heavy", condition: "always" },
  ],
};

export const windRules: Scheme = {
  name: "wind-legacy",
  description: "getVSUPWindCoordinate",
  mode: "rules",
  variable: "wind_speed_10m",
  unit: "m/s",
  declared_unit: "m/s",
  window_hours: null,
  classes: null,
  outcomes: [
    { pictogram: "wind/calm.png", level: 3, class: "calm", condition: "p90 < 3" },
    { pictogram: "wind/strong.png", level: 2, class: "strong+storm", condition: "p10 > 10" },
    { pictogram: "wind/strong.png", level: 2, class: "strong+storm", condition: "p50 > 10" },
    { pictogram: "wind/any.png", level: 1, class: "calm+light+strong+storm", condition: "default" },
  ],
};

export const schemes: Schemes = {
  version: "abc123",
  pictogram_base: "/pictograms/abc123/",
  quantiles: [0, 10, 25, 50, 75, 90, 100],
  schemes: [rainScheme, windRules],
};
