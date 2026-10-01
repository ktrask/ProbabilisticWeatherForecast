import { describe, expect, it } from "vitest";

import type { Product } from "../api/client";
import { covers, offered } from "./products";

const product = (id: string, area: Product["area"]): Product => ({
  id, label: id, default: id === "ecmwf", variants: ["ensemble"], variables: [], schemes: [],
  members: 20, grid_km: 2, horizon_days: 2, area,
});
const ecmwf = product("ecmwf", null);
const d2 = product("icon-d2", { south: 43.18, north: 58.06, west: -3.94, east: 20.32 });
const alps = product("meteoswiss", { south: 42.58, north: 49.79, west: 1.23, east: 16.85 });

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
