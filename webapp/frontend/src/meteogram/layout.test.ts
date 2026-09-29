import { describe, expect, it } from "vitest";

import { dailyExtremes, dayBands, daysAvailable, extent, sections, startIndex, visibleWindow } from "./layout";
import { dayKey, localMidnight, localTime, midnightsBetween, offsetMs } from "./time";

const H = 3_600_000;
const utc = (text: string) => new Date(`${text}Z`);
// 6-hourly steps from local midnight in Berlin (22:00 UTC in summer time).
const steps = (start: string, count: number) =>
  Array.from({ length: count }, (_, k) => new Date(utc(start).getTime() + k * 6 * H));

describe("time zones", () => {
  it("reads local time in the forecast's zone, not the browser's", () => {
    expect(localTime(utc("2026-09-28T22:00:00"), "Europe/Berlin")).toMatchObject({ day: 29, hour: 0 });
    expect(localTime(utc("2026-09-28T14:30:00"), "Australia/Darwin")).toMatchObject({ day: 29, hour: 0 });
    expect(localTime(utc("2026-09-29T00:00:00"), "Atlantic/Reykjavik")).toMatchObject({ day: 29, hour: 0 });
  });

  it("knows the offset on both sides of a DST change", () => {
    expect(offsetMs(utc("2026-10-24T12:00:00"), "Europe/Berlin")).toBe(2 * H);
    expect(offsetMs(utc("2026-10-26T12:00:00"), "Europe/Berlin")).toBe(1 * H);
  });

  it("finds local midnight, including right after the clocks go back", () => {
    expect(localMidnight(2026, 9, 29, "Europe/Berlin")).toEqual(utc("2026-09-28T22:00:00"));
    expect(localMidnight(2026, 10, 26, "Europe/Berlin")).toEqual(utc("2026-10-25T23:00:00"));
    expect(localMidnight(2026, 9, 29, "Australia/Darwin")).toEqual(utc("2026-09-28T14:30:00"));
  });

  it("lists the midnights inside a range", () => {
    const found = midnightsBetween(utc("2026-10-24T12:00:00"), utc("2026-10-27T12:00:00"), "Europe/Berlin");
    expect(found).toEqual([utc("2026-10-24T22:00:00"), utc("2026-10-25T23:00:00"), utc("2026-10-26T23:00:00")]);
  });

  it("names the local day", () => {
    expect(dayKey(utc("2026-09-28T22:30:00"), "Europe/Berlin")).toBe("2026-09-29");
    expect(dayKey(utc("2026-09-28T22:30:00"), "Atlantic/Reykjavik")).toBe("2026-09-28");
  });
});

describe("where the view starts", () => {
  const s = steps("2026-09-28T22:00:00", 56);

  it("starts at the step nearest to now", () => {
    expect(startIndex(s, 6, utc("2026-09-29T09:00:00"))).toEqual({ index: 2, stale: false }); // 11:00 local
    expect(startIndex(s, 6, utc("2026-09-29T11:00:00"))).toEqual({ index: 2, stale: false });
    expect(startIndex(s, 6, utc("2026-09-29T13:00:00"))).toEqual({ index: 3, stale: false });
  });

  it("starts at the beginning when now is before the forecast", () => {
    expect(startIndex(s, 6, utc("2026-09-01T00:00:00"))).toEqual({ index: 0, stale: false });
  });

  it("flags a forecast that is over", () => {
    expect(startIndex(s, 6, utc("2026-12-01T00:00:00"))).toEqual({ index: 0, stale: true });
  });

  it("cuts the window to the requested days", () => {
    expect(visibleWindow(s, 6, utc("2026-09-29T09:00:00"), 3)).toEqual({ from: 2, to: 14, stale: false });
  });

  it("stops at the end of the forecast", () => {
    expect(visibleWindow(s, 6, utc("2026-10-10T00:00:00"), 7)).toMatchObject({ to: 56 });
  });

  it("counts the whole days left", () => {
    expect(daysAvailable(56, 0, 6)).toBe(14);
    expect(daysAvailable(56, 2, 6)).toBe(13);
    expect(daysAvailable(56, 55, 6)).toBe(1);
  });
});

describe("day bands", () => {
  it("cuts at local midnight and alternates the shading", () => {
    const bands = dayBands(utc("2026-09-28T19:00:00"), utc("2026-09-30T12:00:00"), "Europe/Berlin");
    expect(bands.map((b) => b.key)).toEqual(["2026-09-28", "2026-09-29", "2026-09-30"]);
    expect(bands[1]?.start).toEqual(utc("2026-09-28T22:00:00"));
    expect(bands[1]?.end).toEqual(utc("2026-09-29T22:00:00"));
    expect(bands.map((b) => b.shaded)).toEqual([bands[0]?.shaded, !bands[0]?.shaded, bands[0]?.shaded]);
  });

  it("makes the day the clocks go back 25 hours long", () => {
    const bands = dayBands(utc("2026-10-24T12:00:00"), utc("2026-10-27T12:00:00"), "Europe/Berlin");
    const sunday = bands.find((b) => b.key === "2026-10-25");
    expect(sunday && (sunday.end.getTime() - sunday.start.getTime()) / H).toBe(25);
  });
});

describe("daily extremes", () => {
  it("finds the warmest and coldest step of each local day", () => {
    const s = steps("2026-09-28T22:00:00", 8); // two local days, 00/06/12/18
    const median = [10, 8, 18, 14, 9, 7, 16, 12];
    expect(dailyExtremes(s, median, 0, 8, "Europe/Berlin")).toEqual([
      { index: 2, value: 18, kind: "max" },
      { index: 1, value: 8, kind: "min" },
      { index: 6, value: 16, kind: "max" },
      { index: 5, value: 7, kind: "min" },
    ]);
  });

  it("skips days of which too little is visible", () => {
    const s = steps("2026-09-28T22:00:00", 6); // second day has 2 steps
    expect(dailyExtremes(s, [1, 2, 3, 4, 5, 6], 0, 6, "Europe/Berlin").map((e) => e.index)).toEqual([3, 0]);
  });
});

it("extent spans every series in the window", () => {
  expect(extent([[5, 1, 9], [7, 0, 20]], 0, 2)).toEqual([0, 7]);
});

describe("sections", () => {
  const s = steps("2026-09-28T22:00:00", 58); // Berlin: every 4th step is local midnight
  const TZ = "Europe/Berlin";

  it("keeps a window that fits in one piece", () => {
    expect(sections(s, 2, 30, 41, TZ)).toEqual([{ first: 2, last: 29 }]);
  });

  it("cuts at local midnight, neighbours sharing the cut", () => {
    // Noon today to 06:00 in 3 days on a phone: 9 steps (two days) per section at most.
    const parts = sections(s, 2, 15, 9, TZ);
    expect(parts).toEqual([
      { first: 2, last: 8 },
      { first: 8, last: 14 },
    ]);
    for (const part of parts.slice(1)) expect(localTime(s[part.first] as Date, TZ).hour).toBe(0);
  });

  it("spreads the days evenly rather than leaving a stub", () => {
    // 14 days on a desktop: 10 fit in a row, but 7 + 7 reads better than 10 + 4.
    const parts = sections(s, 0, 57, 41, TZ);
    expect(parts).toEqual([
      { first: 0, last: 28 },
      { first: 28, last: 56 },
    ]);
  });

  it("covers every step, and no section is too long", () => {
    for (const [from, to, max] of [[2, 58, 9], [0, 58, 13], [3, 40, 5], [0, 58, 29]] as const) {
      const parts = sections(s, from, to, max, TZ);
      expect(parts[0]?.first).toBe(from);
      expect(parts.at(-1)?.last).toBe(to - 1);
      parts.forEach((p, i) => {
        expect(p.last - p.first + 1).toBeLessThanOrEqual(max);
        if (i > 0) expect(p.first).toBe(parts[i - 1]?.last);
      });
    }
  });

  it("cuts mid-day only when a single day does not fit", () => {
    const parts = sections(s, 0, 9, 3, TZ);
    expect(parts.every((p) => p.last - p.first + 1 <= 3)).toBe(true);
    expect(parts.at(-1)?.last).toBe(8);
  });

  it("cuts at the first step of a day when DST moves the grid off midnight", () => {
    // After the clocks go back, the 6-hour grid lands on 23/05/11/17 local.
    const late = steps("2026-10-23T22:00:00", 20);
    const parts = sections(late, 0, 20, 9, TZ);
    for (const part of parts.slice(1)) {
      const here = dayKey(late[part.first] as Date, TZ);
      const before = dayKey(late[part.first - 1] as Date, TZ);
      expect(here).not.toBe(before);
    }
  });
});
