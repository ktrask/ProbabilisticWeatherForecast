"""The FastAPI application: the JSON API, the pictograms, and the built frontend.

    uvicorn api.app:create_app --factory             # development
    gunicorn -k uvicorn_worker.UvicornWorker 'api.app:create_app()'   # the container

Status codes, all with {"detail": ...}:
    422  a parameter is invalid - including an unknown product
    404  the source has no data for that place
    409  the product does not offer that variant
    502  the upstream source failed or sent unusable data
    504  the upstream source did not answer in time
"""
import logging
from typing import Literal

from fastapi import FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api.cache import TTLCache
from api.models import (
    ClassOut,
    ErrorOut,
    GeocodeOut,
    HealthOut,
    LevelOut,
    OutcomeOut,
    ProductOut,
    ProductsOut,
    SchemeOut,
    SchemesOut,
)
from api.settings import Settings
from core.model import Forecast, Location
from core.pipeline import build_forecast
from core.reduce import DataGap
from core.variables import variable
from sources import config as sources_config
from sources.base import NoData, SourceError, SourceTimeout
from sources.geocode import OpenMeteoGeocoder
from vsup import config as vsup_config
from vsup.classify import classify

# Coordinates are rounded to this many decimals (about a kilometre) before
# anything is fetched or cached. The ensemble's grid is 25 km, so nothing is
# lost, and nearby requests share one cache entry.
COORDINATE_DECIMALS = 2

API_CACHE = "public, max-age=300"
PICTOGRAM_CACHE = "public, max-age=31536000, immutable"
# Vite puts a content hash into every file name under assets/; index.html
# must be revalidated so a new build is picked up.
FRONTEND_ASSET_CACHE = PICTOGRAM_CACHE
FRONTEND_PAGE_CACHE = "no-cache"

log = logging.getLogger(__name__)

ERRORS = {
    404: {"model": ErrorOut, "description": "The source has no data for this place."},
    502: {"model": ErrorOut, "description": "The upstream source failed or sent unusable data."},
    504: {"model": ErrorOut, "description": "The upstream source did not answer in time."},
}


class _Pictograms(StaticFiles):
    """Served under a path that contains the config's version, so a changed
    pictogram gets a new URL and the old one may be cached for good."""

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            response.headers["Cache-Control"] = PICTOGRAM_CACHE
        return response


class _Frontend(StaticFiles):
    """The built frontend, mounted last so it only answers what no route took."""

    async def get_response(self, path, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            hashed = path.startswith("assets/")
            response.headers["Cache-Control"] = FRONTEND_ASSET_CACHE if hashed else FRONTEND_PAGE_CACHE
        return response


def create_app(settings=None, *, catalog=None, geocoder=None):
    """Build the app. Loads and cross-checks both configs; a ConfigError here
    means the service must not start.

    `catalog` and `geocoder` replace what the settings would build - for tests.
    """
    settings = settings or Settings.from_env()
    schemes = vsup_config.load(settings.vsup_config)
    catalog = catalog or sources_config.load(settings.sources_config, schemes)
    geocoder = geocoder or OpenMeteoGeocoder(timeout_s=settings.geocoder_timeout_s)
    forecasts = TTLCache(settings.forecast_cache_ttl_s, settings.forecast_cache_size)
    places = TTLCache(settings.geocode_cache_ttl_s, settings.geocode_cache_size)
    pictogram_base = f"/pictograms/{schemes.version}/"

    app = FastAPI(
        title="VSUP meteogram API",
        version="1",
        description="Probabilistic forecasts as quantiles plus VSUP pictograms. Read-only.",
        openapi_url="/api/openapi.json",
        docs_url="/api/docs",
        redoc_url=None,
    )
    app.state.forecast_cache = forecasts

    def error(status):
        async def handler(request: Request, exc: Exception):
            return JSONResponse({"detail": str(exc)}, status_code=status)

        return handler

    # Most specific first is not needed: Starlette looks handlers up along the
    # exception's class hierarchy.
    app.add_exception_handler(NoData, error(404))
    app.add_exception_handler(SourceTimeout, error(504))
    app.add_exception_handler(SourceError, error(502))
    app.add_exception_handler(DataGap, error(502))

    def product_or_422(key):
        if key is None:
            return catalog.default
        try:
            return catalog.product(key)
        except KeyError as exc:
            raise RequestValidationError([{
                "type": "value_error", "loc": ("query", "product"), "msg": exc.args[0], "input": key,
            }]) from None

    @app.get("/api/products", response_model=ProductsOut, tags=["meta"])
    async def products(response: Response):
        """The products on offer and what each can do. The first is the default."""
        response.headers["Cache-Control"] = API_CACHE
        return ProductsOut(products=[
            ProductOut(
                id=p.id, label=p.label, default=p is catalog.default, variants=list(p.variants),
                variables=sorted(p.variables), schemes=list(p.schemes),
            )
            for p in catalog.products.values()
        ])

    @app.get("/api/schemes", response_model=SchemesOut, tags=["meta"])
    async def list_schemes(response: Response):
        """Every loaded VSUP scheme with every pictogram it can choose - enough
        to draw a legend that always matches the rules."""
        response.headers["Cache-Control"] = API_CACHE
        return SchemesOut(
            version=schemes.version,
            pictogram_base=pictogram_base,
            quantiles=list(schemes.quantiles),
            schemes=[_scheme_out(scheme) for scheme in schemes.schemes.values()],
        )

    @app.get("/api/forecast", response_model=Forecast, tags=["forecast"],
             responses={**ERRORS, 409: {"model": ErrorOut, "description": "The product lacks this variant."}})
    async def forecast(
        response: Response,
        lat: float = Query(ge=-90, le=90, description=f"Rounded to {COORDINATE_DECIMALS} decimals."),
        lon: float = Query(ge=-180, le=180, description=f"Rounded to {COORDINATE_DECIMALS} decimals."),
        product: str | None = Query(None, description="A product id from /api/products; default: the first."),
        variant: Literal["ensemble", "hres"] = "ensemble",
        name: str | None = Query(None, max_length=200, description="Put into location.name as given."),
    ):
        """The whole forecast the source has - the client picks the days it shows -
        as quantiles per step plus the product's pictograms."""
        chosen = product_or_422(product)
        if variant not in chosen.variants:
            return JSONResponse(
                {"detail": f"{chosen.label} has no {variant!r} variant; it offers {', '.join(chosen.variants)}"},
                status_code=409,
            )
        lat, lon = round(lat, COORDINATE_DECIMALS), round(lon, COORDINATE_DECIMALS)

        async def fetch():
            result = await chosen.source.fetch(Location(lat=lat, lon=lon), set(chosen.variables))
            return build_forecast(result, quantile_levels=schemes.quantiles)

        base = await forecasts.get((chosen.source_key, chosen.variables, lat, lon), fetch)
        out = classify(base, schemes, chosen.schemes)
        if name:
            out = out.model_copy(update={"location": out.location.model_copy(update={"name": name})})
        response.headers["Cache-Control"] = API_CACHE
        return out

    @app.get("/api/geocode", response_model=GeocodeOut, tags=["forecast"], responses=ERRORS)
    async def geocode(
        response: Response,
        q: str = Query(min_length=1, max_length=100, description="Place name; fewer than 2 characters find nothing."),
        lang: Literal["de", "en"] = "en",
        count: int = Query(5, ge=1, le=20),
    ):
        """Places matching `q`, best first; an empty list rather than an error
        when nothing matches. Fine to call while the user types."""
        key = (" ".join(q.split()).casefold(), lang, count)
        results = await places.get(key, lambda: geocoder.search(q, count=count, language=lang))
        response.headers["Cache-Control"] = API_CACHE
        return GeocodeOut(query=q, results=results)

    @app.get("/api/health", response_model=HealthOut, tags=["meta"],
             responses={503: {"model": HealthOut, "description": "deep=true and a source is failing."}})
    async def health(response: Response, deep: bool = False):
        """Liveness. With deep=true, also fetches from every source, uncached."""
        response.headers["Cache-Control"] = "no-store"
        upstream = None
        if deep:
            upstream = {}
            probe = Location(lat=settings.probe_lat, lon=settings.probe_lon)
            for key, source in catalog.sources.items():
                try:
                    await source.fetch(probe, {min(source.variables)})
                    upstream[key] = "ok"
                except SourceError as exc:
                    upstream[key] = f"failing: {exc}"
        status = "degraded" if upstream and any(v != "ok" for v in upstream.values()) else "ok"
        if status != "ok":
            response.status_code = 503
        return HealthOut(
            status=status, vsup_version=schemes.version, products=list(catalog.products),
            forecast_cache=forecasts.stats(), upstream=upstream,
        )

    app.mount(pictogram_base.rstrip("/"), _Pictograms(directory=schemes.pictogram_root), name="pictograms")
    if (settings.frontend_dist / "index.html").is_file():
        app.mount("/", _Frontend(directory=settings.frontend_dist, html=True), name="frontend")
    else:
        log.warning("no built frontend in %s - serving the API only", settings.frontend_dist)
    return app


def _scheme_out(scheme):
    canonical = variable(scheme.variable).unit
    classes = levels = None
    if scheme.mode == "tree":
        bounds = list(scheme.bounds) + [None]
        classes = [ClassOut(id=class_id, below=below) for class_id, below in zip(scheme.class_ids, bounds)]
        levels = [
            LevelOut(level=level.level, interval=list(level.interval) if level.interval else None)
            for level in scheme.levels
        ]
    return SchemeOut(
        name=scheme.name,
        description=scheme.description,
        mode=scheme.mode,
        variable=scheme.variable,
        unit=canonical,
        declared_unit=scheme.unit,
        window_hours=scheme.window_hours,
        outcomes=[
            OutcomeOut(pictogram=choice.pictogram, level=choice.level, class_=choice.class_, condition=text)
            for choice, text in scheme.outcomes()
        ],
        classes=classes,
        levels=levels,
    )
