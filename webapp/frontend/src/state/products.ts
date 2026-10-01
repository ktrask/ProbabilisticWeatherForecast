// Which products can draw a place. A regional model computes only inside its
// area; the API sends the box around it. Inside a rotated grid's box the API
// has the last word (a 404), so this is a filter for what to offer, not a promise.
import type { Product } from "../api/client";

export function covers(product: Product, lat: number, lon: number): boolean {
  const area = product.area;
  if (!area) return true;
  return area.south <= lat && lat <= area.north && area.west <= lon && lon <= area.east;
}

/** The products to offer for a place: those covering it, and the chosen one
 * even if it does not, so the picker can show what is selected. */
export function offered(products: Product[], lat: number | null, lon: number | null, chosen: string | null): Product[] {
  if (lat === null || lon === null) return products;
  return products.filter((p) => p.id === chosen || covers(p, lat, lon));
}

/** The product the automatic choice takes: the finest automatic one that covers
 * the place and reaches `days` (without: as far as the default), file order on a
 * tie, else the default - the rule of the API's sources/choice.py. The API also
 * steps past a model that turns out not to cover the place at the edge of a
 * rotated grid, which only it can see; this is for knowing when a change of days
 * or place changes the model and so needs a new request. */
export function automaticChoice(products: Product[], lat: number, lon: number, days?: number): Product | undefined {
  const fallback = products.find((p) => p.default);
  const reach = days ?? fallback?.horizon_days ?? 0;
  const candidates = products.filter((p) => p.automatic && p.horizon_days >= reach && covers(p, lat, lon));
  candidates.sort((a, b) => a.grid_km - b.grid_km || products.indexOf(a) - products.indexOf(b));
  return candidates[0] ?? fallback;
}
