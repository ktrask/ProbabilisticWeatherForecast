// What to draw where, without React: the visible window of the forecast, the
// day bands, and the daily extremes. Kept pure so it can be tested directly.
import { HOUR_MS, dayKey, midnightsBetween } from "./time";

export interface Window {
  from: number; // first visible step
  to: number; // one past the last
  stale: boolean; // the forecast ends before now
}

/** The step to start at: the one nearest to now. A forecast that ends before
 * now is shown from its beginning and flagged stale rather than shown empty. */
export function startIndex(steps: Date[], stepHours: number, now: Date): { index: number; stale: boolean } {
  const first = steps[0];
  const last = steps[steps.length - 1];
  if (!first || !last) return { index: 0, stale: false };
  const stepMs = stepHours * HOUR_MS;
  if (now.getTime() >= last.getTime() + stepMs / 2) return { index: 0, stale: true };
  if (now.getTime() <= first.getTime()) return { index: 0, stale: false };
  return { index: Math.round((now.getTime() - first.getTime()) / stepMs), stale: false };
}

/** Whole days the forecast still covers from step `from`. */
export function daysAvailable(stepCount: number, from: number, stepHours: number): number {
  return Math.max(1, Math.floor(((stepCount - from) * stepHours) / 24));
}

export function visibleWindow(steps: Date[], stepHours: number, now: Date, days: number): Window {
  const { index, stale } = startIndex(steps, stepHours, now);
  const span = Math.max(2, Math.round((days * 24) / stepHours));
  const to = Math.min(steps.length, index + span);
  return { from: Math.max(0, Math.min(index, to - 2)), to, stale };
}

export interface DayBand {
  key: string; // local date, "2026-09-29"
  start: Date;
  end: Date;
  shaded: boolean; // every other day, stable while scrolling
}

/** The local days between `start` and `end`, cut at local midnight. */
export function dayBands(start: Date, end: Date, timeZone: string): DayBand[] {
  const cuts = [start, ...midnightsBetween(start, end, timeZone), end];
  const bands: DayBand[] = [];
  for (let i = 0; i + 1 < cuts.length; i++) {
    const from = cuts[i] as Date;
    const key = dayKey(new Date(from.getTime() + 1), timeZone);
    const [y, m, d] = key.split("-").map(Number) as [number, number, number];
    bands.push({ key, start: from, end: cuts[i + 1] as Date, shaded: Math.floor(Date.UTC(y, m - 1, d) / 86_400_000) % 2 === 0 });
  }
  return bands;
}

export interface Extreme {
  index: number; // step index
  value: number;
  kind: "max" | "min";
}

/** The warmest and coldest step of each local day, by the median.
 * Days with fewer than `minSteps` visible steps are skipped: a day cut off at
 * 18:00 would otherwise claim its evening as the day's minimum. */
export function dailyExtremes(
  steps: Date[],
  median: number[],
  from: number,
  to: number,
  timeZone: string,
  minSteps = 3,
): Extreme[] {
  const days = new Map<string, number[]>();
  for (let i = from; i < to; i++) {
    const key = dayKey(steps[i] as Date, timeZone);
    const list = days.get(key) ?? [];
    list.push(i);
    days.set(key, list);
  }
  const extremes: Extreme[] = [];
  for (const indices of days.values()) {
    if (indices.length < minSteps) continue;
    let hi = indices[0] as number;
    let lo = hi;
    for (const i of indices) {
      if ((median[i] as number) > (median[hi] as number)) hi = i;
      if ((median[i] as number) < (median[lo] as number)) lo = i;
    }
    extremes.push({ index: hi, value: median[hi] as number, kind: "max" });
    if (lo !== hi) extremes.push({ index: lo, value: median[lo] as number, kind: "min" });
  }
  return extremes;
}

/** The lowest and highest value any of `series` takes in [from, to). */
export function extent(series: number[][], from: number, to: number): [number, number] {
  let lo = Infinity;
  let hi = -Infinity;
  for (const values of series) {
    for (let i = from; i < to; i++) {
      const v = values[i] as number;
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    }
  }
  return [lo, hi];
}

export interface Section {
  first: number; // step index
  last: number; // step index, inclusive
}

/** Splits the steps [from, to) into sections of at most `maxSteps` steps each,
 * to be drawn one below the other when a single row would make the pictograms
 * too small.
 *
 * Sections are cut where a local day begins, as evenly as the days allow.
 * Neighbours share the step they are cut at: the temperature line runs on,
 * and the total that begins at the cut is drawn in the later section - were
 * they not to overlap, the total spanning the cut would fit in neither.
 */
export function sections(steps: Date[], from: number, to: number, maxSteps: number, timeZone: string): Section[] {
  const last = to - 1;
  const span = last - from; // intervals between the first and the last step
  const room = Math.max(1, maxSteps - 1); // intervals a section can hold
  if (span <= room) return [{ first: from, last }];

  const dayStarts: number[] = [];
  for (let i = from + 1; i < last; i++) {
    if (dayKey(steps[i] as Date, timeZone) !== dayKey(steps[i - 1] as Date, timeZone)) dayStarts.push(i);
  }

  // Even: as few sections as fit, each cut at the day start nearest its share.
  const count = Math.ceil(span / room);
  const even: number[] = [];
  let previous = from;
  for (let k = 1; k < count; k++) {
    const target = from + (span * k) / count;
    const options = dayStarts.filter((c) => c > previous && c - previous <= room);
    if (!options.length) break;
    const cut = options.reduce((best, c) => (Math.abs(c - target) < Math.abs(best - target) ? c : best));
    even.push(cut);
    previous = cut;
  }
  const cuts = even.length === count - 1 && last - previous <= room ? even : greedy(from, last, room, dayStarts);

  const bounds = [from, ...cuts, last];
  return bounds.slice(0, -1).map((first, i) => ({ first, last: bounds[i + 1] as number }));
}

/** As many whole days per section as fit; mid-day only where a day alone is too long. */
function greedy(from: number, last: number, room: number, dayStarts: number[]): number[] {
  const cuts: number[] = [];
  let start = from;
  while (last - start > room) {
    const fitting = dayStarts.filter((c) => c > start && c - start <= room);
    const cut = fitting.length ? (fitting[fitting.length - 1] as number) : start + room;
    cuts.push(cut);
    start = cut;
  }
  return cuts;
}
