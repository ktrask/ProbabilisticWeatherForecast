import { describe, expect, it } from "vitest";

import type { Product } from "../api/client";
import { automaticChoice, covers, offered } from "./products";

const product = (id: string, area: Product["area"], grid_km = 2, horizon_days = 2, automatic = true): Product => ({
  id, label: id, default: id === "ecmwf", variants: ["ensemble"], variables: [], schemes: [],
  members: 20, grid_km, horizon_days, automatic, area,
});
// As in config/sources.yaml.
const ecmwf = product("ecmwf", null, 25, 15);
const icon = product("icon", null, 26, 7.5);
const eu = product("icon-eu", { south: 29.5, north: 70.5, west: -23.5, east: 62.5 }, 13, 5);
const d2 = product("icon-d2", { south: 43.18, north: 58.06, west: -3.94, east: 20.32 }, 2, 2, false);
const alps = product("meteoswiss", { south: 42.58, north: 49.79, west: 1.23, east: 16.85 }, 2, 5);
const shipped = [ecmwf, icon, eu, d2, alps];

describe("products for a place", () => {
  it("a global model covers everywhere, a regional one its area", () => {
    expect(covers(ecmwf, 1.35, 103.82)).toBe(true);
    expect(covers(d2, 52.26, 10.52)).toBe(true);
    expect(covers(d2, 1.35, 103.82)).toBe(false);
  });

  it("offers what covers the place", () => {
    expect(offered([ecmwf, d2, alps], 52.26, 10.52, null).map((p) => p.id)).toEqual(["ecmwf", "icon-d2"]);
    expect(offered([ecmwf, d2, alps], 46.02, 7.75, null).map((p) => p.id)).toEqual(["ecmwf", "icon-d2", "meteoswiss"]);
  });

  it("keeps the chosen one in the list, so the picker shows it", () => {
    expect(offered([ecmwf, d2, alps], 52.26, 10.52, "meteoswiss").map((p) => p.id)).toContain("meteoswiss");
  });
});

describe("the automatic choice", () => {
  // The same cases as the API's tests/test_choice.py.
  it.each([
    [52.26, 10.52, 1, "icon-eu"], // ICON-D2 is finer, but not automatic before hourly steps
    [52.26, 10.52, 5, "icon-eu"],
    [52.26, 10.52, 6, "ecmwf"],
    [52.26, 10.52, undefined, "ecmwf"],
    [46.02, 7.75, 4, "meteoswiss"],
    [46.02, 7.75, 6, "ecmwf"],
    [64.15, -21.94, 3, "icon-eu"],
    [1.35, 103.82, 1, "ecmwf"],
  ])("at %s, %s for %s days: %s", (lat, lon, days, id) => {
    expect(automaticChoice(shipped, lat, lon, days)?.id).toBe(id);
  });
});
