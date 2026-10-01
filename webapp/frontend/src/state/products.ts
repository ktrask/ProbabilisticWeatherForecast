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
