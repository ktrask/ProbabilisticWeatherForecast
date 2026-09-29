// Local time in an arbitrary IANA time zone, from nothing but Intl.
//
// The forecast's steps are UTC. Where days begin and which hour a step falls
// on depend on the location's time zone - not the browser's - including its
// daylight-saving changes, which can happen inside a 15-day forecast.

export const HOUR_MS = 3_600_000;
export const DAY_MS = 24 * HOUR_MS;

export interface LocalTime {
  year: number;
  month: number; // 1-12
  day: number;
  hour: number;
  minute: number;
}

const formats = new Map<string, Intl.DateTimeFormat>();

function format(timeZone: string) {
  let f = formats.get(timeZone);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hourCycle: "h23",
      year: "numeric",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "numeric",
    });
    formats.set(timeZone, f);
  }
  return f;
}

export function localTime(date: Date, timeZone: string): LocalTime {
  const parts: Record<string, number> = {};
  for (const part of format(timeZone).formatToParts(date)) {
    if (part.type !== "literal") parts[part.type] = Number(part.value);
  }
  return {
    year: parts.year ?? 0,
    month: parts.month ?? 1,
    day: parts.day ?? 1,
    hour: parts.hour ?? 0,
    minute: parts.minute ?? 0,
  };
}

/** Local time minus UTC, in milliseconds, at `date`. */
export function offsetMs(date: Date, timeZone: string): number {
  const l = localTime(date, timeZone);
  const asUtc = Date.UTC(l.year, l.month - 1, l.day, l.hour, l.minute);
  return asUtc - Math.floor(date.getTime() / 60_000) * 60_000;
}

/** "2026-09-29" - the local calendar day. */
export function dayKey(date: Date, timeZone: string): string {
  const l = localTime(date, timeZone);
  return `${l.year}-${String(l.month).padStart(2, "0")}-${String(l.day).padStart(2, "0")}`;
}

/** The instant a local calendar day begins. */
export function localMidnight(year: number, month: number, day: number, timeZone: string): Date {
  const wall = Date.UTC(year, month - 1, day);
  // The offset at the guess can differ from the offset at midnight itself when
  // a DST change lies in between; a second pass settles it.
  let guess = new Date(wall - offsetMs(new Date(wall), timeZone));
  guess = new Date(wall - offsetMs(guess, timeZone));
  return guess;
}

/** Every local midnight strictly between `start` and `end`. */
export function midnightsBetween(start: Date, end: Date, timeZone: string): Date[] {
  const result: Date[] = [];
  const first = localTime(start, timeZone);
  for (let i = 1; ; i++) {
    const date = new Date(Date.UTC(first.year, first.month - 1, first.day + i));
    const midnight = localMidnight(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate(), timeZone);
    if (midnight.getTime() >= end.getTime()) return result;
    if (midnight.getTime() > start.getTime()) result.push(midnight);
  }
}
