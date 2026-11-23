// The start page's short overview of a place: a few days, each as its 6-hour
// steps from local midnight - clouds and rain as the forecast's own VSUP
// pictograms, so a card is as vague as the ensemble is - and the range of the
// median temperature over those steps.
import type { Forecast, PictogramItem } from "../api/client";
import { pictogramTime } from "../meteogram/shared";
import { HOUR_MS, dayKey } from "../meteogram/time";

export interface Slot {
  time: Date;
  cloud: PictogramItem | null;
  rain: PictogramItem | null; // the total over the step; null where its window runs past the forecast
  past: boolean; // the step's window lies wholly before now
}

export interface Day {
  key: string; // "2026-11-23", local
  start: Date; // the day's first step
  today: boolean;
  slots: Slot[];
  temperature: [number, number] | null; // lowest and highest median of its steps
}

export interface Overview {
  days: Day[];
  timeZone: string;
  stale: boolean; // the forecast does not reach today; shown from its first day
}

export function overview(forecast: Forecast, now: Date, count = 3): Overview {
  const timeZone = forecast.location.timezone ?? "UTC";
  const steps = forecast.steps.map((s) => new Date(s));
  const stepMs = forecast.step_hours * HOUR_MS;
  const last = steps[steps.length - 1];
  // As in the meteogram: a total is drawn only if its whole window fits.
  const end = last ? new Date(last.getTime() + stepMs / 2) : new Date(0);
  const rainWindow = forecast.variables.precipitation?.window_hours;
  const rainMs = rainWindow ? rainWindow * HOUR_MS : null;
  const median = forecast.variables.temperature_2m?.quantiles.p50;

  const groups: { key: string; indices: number[] }[] = [];
  steps.forEach((step, i) => {
    const key = dayKey(step, timeZone);
    const group = groups[groups.length - 1];
    if (group && group.key === key) group.indices.push(i);
    else groups.push({ key, indices: [i] });
  });

  const today = dayKey(now, timeZone);
  const first = groups.findIndex((g) => g.key >= today);
  const stale = first === -1;
  const days = groups.slice(stale ? 0 : first, (stale ? 0 : first) + count).map((group): Day => {
    const slots = group.indices.map((i): Slot => {
      const time = steps[i] as Date;
      const rain = pictogramTime(time, rainMs, end) ? forecast.pictograms.precipitation?.items[i] ?? null : null;
      return {
        time,
        cloud: forecast.pictograms.cloud_cover?.items[i] ?? null,
        rain,
        past: time.getTime() + stepMs <= now.getTime(),
      };
    });
    const values = median ? group.indices.map((i) => median[i]).filter((v): v is number => v !== undefined) : [];
    return {
      key: group.key,
      start: steps[group.indices[0] as number] as Date,
      today: group.key === today,
      slots,
      temperature: values.length ? [Math.min(...values), Math.max(...values)] : null,
    };
  });
  return { days, timeZone, stale };
}
