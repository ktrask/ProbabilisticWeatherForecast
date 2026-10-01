"""Which product draws a place when the reader has not chosen one.

The finest model that covers the place and reaches the days asked for:
products marked `automatic` in sources.yaml whose area holds the location
and whose horizon is at least `days`, finest grid first, file order on a
tie. The default product (the first) closes the list, so there is always
an answer. Without `days`, a product has to reach as far as the default.

A rotated regional grid fills its area only partly, so the area cannot
promise a forecast. The API tries the list in order and takes the first
product that does not answer NotCovered - which is why this returns the
whole ranking and not one product.

frontend/src/state/products.ts applies the same rule, to know when a
change of days or place changes the model and needs a new request.
"""


def ranked(catalog, lat, lon, days=None):
    """The products to try, best first, the default last."""
    default = catalog.default
    reach = days if days is not None else default.horizon_days
    candidates = [
        p for p in catalog.products.values()
        if p.automatic and p.horizon_days >= reach and (p.area is None or p.area.contains(lat, lon))
    ]
    order = list(catalog.products)
    candidates.sort(key=lambda p: (p.grid_km, order.index(p.id)))
    if default not in candidates:
        candidates.append(default)
    return candidates[:candidates.index(default) + 1]
