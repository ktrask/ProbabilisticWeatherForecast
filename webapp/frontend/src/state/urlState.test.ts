import { act, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { DEFAULT_DAYS, formatState, parseState, useUrlState } from "./urlState";

describe("parseState", () => {
  it("reads a full view", () => {
    expect(parseState("?lat=52.26&lon=10.52&name=Braunschweig&product=ecmwf&variant=hres&days=7")).toEqual({
      lat: 52.26,
      lon: 10.52,
      name: "Braunschweig",
      product: "ecmwf",
      variant: "hres",
      days: 7,
    });
  });

  it("falls back to defaults", () => {
    expect(parseState("")).toEqual({
      lat: null, lon: null, name: null, product: null, variant: "ensemble", days: DEFAULT_DAYS,
    });
  });

  it.each([
    ["?lat=91&lon=10", "latitude out of range"],
    ["?lat=52&lon=-181", "longitude out of range"],
    ["?lat=north&lon=10", "not a number"],
    ["?lat=52", "half a coordinate"],
    ["?lat=&lon=10", "empty"],
  ])("drops a place that is not one: %s (%s)", (search) => {
    const state = parseState(search);
    expect([state.lat, state.lon, state.name]).toEqual([null, null, null]);
  });

  it("clamps and rounds days, ignores unknown variants", () => {
    expect(parseState("?days=99").days).toBe(DEFAULT_DAYS);
    expect(parseState("?days=3.6").days).toBe(4);
    expect(parseState("?variant=deterministic").variant).toBe("ensemble");
  });
});

describe("formatState", () => {
  it("leaves out defaults", () => {
    expect(formatState(parseState("?lat=52.26&lon=10.52&variant=ensemble&days=5"))).toBe("?lat=52.26&lon=10.52");
  });

  it("keeps coordinates exactly - an adjustment must not move the place", () => {
    const state = parseState("?lat=52.2646577&lon=10.5236066&name=Braunschweig");
    expect(formatState({ ...state, days: 6 })).toBe("?lat=52.2646577&lon=10.5236066&name=Braunschweig&days=6");
  });

  it("round-trips", () => {
    const search = "?lat=64.15&lon=-21.94&name=Reykjav%C3%ADk&product=ecmwf&days=10";
    expect(formatState(parseState(search))).toBe(search);
  });
});

describe("useUrlState", () => {
  afterEach(() => window.history.replaceState(null, "", "/"));

  it("pushes a new place and replaces an adjustment", () => {
    window.history.replaceState(null, "", "/?lang=de");
    const { result } = renderHook(() => useUrlState());
    const before = window.history.length;
    act(() => result.current[1]({ lat: 52.26, lon: 10.52, name: "Braunschweig" }));
    expect(window.history.length).toBe(before + 1);
    expect(result.current[0].name).toBe("Braunschweig");
    // ?lang= is not part of the view but must survive navigation.
    expect(window.location.search).toBe("?lat=52.26&lon=10.52&name=Braunschweig&lang=de");

    act(() => result.current[1]({ days: 9 }, { replace: true }));
    expect(window.history.length).toBe(before + 1);
    expect(result.current[0].days).toBe(9);
    expect(result.current[0].name).toBe("Braunschweig");
  });

  it("follows the back button", () => {
    const { result } = renderHook(() => useUrlState());
    act(() => result.current[1]({ lat: 1, lon: 2, name: "A" }));
    act(() => {
      window.history.replaceState(null, "", "/?lat=3&lon=4&name=B");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });
    expect(result.current[0].name).toBe("B");
  });
});
