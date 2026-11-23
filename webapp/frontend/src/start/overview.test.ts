import { describe, expect, it } from "vitest";

import { START, forecast } from "../test/fixtures";
import { overview } from "./overview";

const H = 3_600_000;

describe("overview", () => {
  it("groups the steps into local days, four 6-hour steps each", () => {
    const result = overview(forecast(16), new Date(START.getTime() + 13 * H));
    expect(result.stale).toBe(false);
    expect(result.days.map((d) => d.key)).toEqual(["2026-09-29", "2026-09-30", "2026-10-01"]);
    expect(result.days.map((d) => d.slots.length)).toEqual([4, 4, 4]);
    expect(result.days[0]?.today).toBe(true);
    expect(result.days[1]?.today).toBe(false);
  });

  it("starts at today, not at the forecast's first day", () => {
    const result = overview(forecast(16), new Date(START.getTime() + 30 * H));
    expect(result.days.map((d) => d.key)).toEqual(["2026-09-30", "2026-10-01", "2026-10-02"]);
  });

  it("marks the steps that are over", () => {
    // 13:00 local: the 00-06 and 06-12 windows are over, 12-18 is running.
    const day = overview(forecast(16), new Date(START.getTime() + 13 * H)).days[0];
    expect(day?.slots.map((s) => s.past)).toEqual([true, true, false, false]);
  });

  it("gives the range of the median temperature over the day's steps", () => {
    // The synthetic median is 10 + step index.
    const days = overview(forecast(16), START).days;
    expect(days.map((d) => d.temperature)).toEqual([
      [10, 13],
      [14, 17],
      [18, 21],
    ]);
  });

  it("leaves out the last step's total, whose window runs past the forecast", () => {
    const days = overview(forecast(12), START).days;
    const rain = days.flatMap((d) => d.slots.map((s) => s.rain !== null));
    expect(rain).toEqual([...Array(11).fill(true), false]);
    expect(days[2]?.slots[3]?.cloud).not.toBeNull();
  });

  it("shows an outdated forecast from its first day, and says so", () => {
    const result = overview(forecast(16), new Date(START.getTime() + 30 * 24 * H));
    expect(result.stale).toBe(true);
    expect(result.days[0]?.key).toBe("2026-09-29");
    expect(result.days[0]?.today).toBe(false);
  });

  it("is in the place's time zone, not the browser's", () => {
    const f = forecast(16);
    f.location.timezone = "Asia/Tokyo"; // 22:00 UTC is 07:00 the next day there
    const days = overview(f, START).days;
    expect(days[0]?.key).toBe("2026-09-29");
    expect(days[0]?.slots.map((s) => s.time.toISOString())).toEqual([
      "2026-09-28T22:00:00.000Z",
      "2026-09-29T04:00:00.000Z",
      "2026-09-29T10:00:00.000Z",
    ]);
  });
});
