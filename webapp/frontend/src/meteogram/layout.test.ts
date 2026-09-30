import { describe, expect, it } from "vitest";

import { dailyExtremes, dayBands, daysAvailable, extent, startIndex, visibleWindow } from "./layout";
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
