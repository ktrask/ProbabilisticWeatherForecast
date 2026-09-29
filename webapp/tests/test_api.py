"""The HTTP API, offline: config/sources.fixtures.yaml serves the recorded
forecasts, a MockTransport stands in for the geocoder, and fake sources play
the failing upstreams.

Requests go through httpx.ASGITransport straight into the app - no server,
no network.
"""
import asyncio
import dataclasses
import json

import httpx
import pytest
from fastapi.routing import APIRoute

from api import __main__ as cli
from api.app import create_app
from api.cache import TTLCache
from api.settings import Settings
from core.model import Forecast
from core.reduce import DataGap
from sources import config as sources_config
from sources.base import SourceError, SourceTimeout
from sources.fixture import FixtureSource
from sources.geocode import OpenMeteoGeocoder
from vsup import config as vsup_config

OFFLINE = Settings(sources_config=sources_config.DEFAULT_SOURCES.parent / "sources.fixtures.yaml")
BRAUNSCHWEIG = "lat=52.26&lon=10.52"


class Recorded:
    """The fixture source, counting fetches, optionally slow or failing."""

    def __init__(self, delay=0.0, raises=None):
        self.inner = FixtureSource()
        self.delay = delay
        self.raises = raises
        self.calls = 0
        for name in ("id", "kind", "variables", "quantile_levels", "native_step", "max_lead"):
            setattr(self, name, getattr(self.inner, name))

    async def fetch(self, location, variables):
        self.calls += 1
        await asyncio.sleep(self.delay)
        if self.raises is not None:
            raise self.raises
        return await self.inner.fetch(location, variables)


def catalog_with(source):
    catalog = sources_config.load(OFFLINE.sources_config, vsup_config.load())
    product = dataclasses.replace(catalog.default, source=source)
    return dataclasses.replace(catalog, sources={product.source_key: source}, products={product.id: product})


def geocoder_answering(payload=None, status=200, seen=None, raises=None):
    def handler(request):
        if seen is not None:
            seen.append(request)
        if raises is not None:
            raise raises
        return httpx.Response(status, json=payload if payload is not None else {})

    return OpenMeteoGeocoder(client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


def app_with(source=None, geocoder=None):
    return create_app(
        OFFLINE,
        catalog=catalog_with(source) if source is not None else None,
        geocoder=geocoder or geocoder_answering({"results": []}),
    )


async def _get_all(app, urls):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await asyncio.gather(*(client.get(url) for url in urls))


def get(app, url):
    return asyncio.run(_get_all(app, [url]))[0]


def get_concurrently(app, urls):
    return asyncio.run(_get_all(app, urls))


@pytest.fixture(scope="module")
def app():
    return app_with()


class TestProducts:
    def test_lists_the_default_product(self, app):
        response = get(app, "/api/products")
        assert response.status_code == 200
        (product,) = response.json()["products"]
        assert product["id"] == "ecmwf"
        assert product["default"] is True
        assert product["variants"] == ["ensemble"]
        assert product["schemes"] == ["cloud-vsup", "precipitation-vsup", "wind-vsup"]
        assert response.headers["cache-control"] == "public, max-age=300"


class TestSchemes:
    def test_every_scheme_with_every_outcome(self, app):
        body = get(app, "/api/schemes").json()
        assert body["version"] == vsup_config.load().version
        assert body["pictogram_base"] == f"/pictograms/{body['version']}/"
        names = [s["name"] for s in body["schemes"]]
        assert "wind-vsup" in names and "precipitation-legacy" in names
        wind = next(s for s in body["schemes"] if s["name"] == "wind-vsup")
        assert wind["unit"] == wind["declared_unit"] == "m/s"
        assert [c["id"] for c in wind["classes"]] == ["calm", "light", "strong", "storm"]
        assert [c["below"] for c in wind["classes"]] == [3, 10, 17.2, None]
        assert len(wind["outcomes"]) == 7
        rain = next(s for s in body["schemes"] if s["name"] == "precipitation-legacy")
        assert rain["outcomes"][0] == {
            "pictogram": "rain/Stufe3_KeinRegen.png", "level": 3, "class": "none", "condition": "p90 < 0.1",
        }
        assert rain["classes"] is None

    def test_every_pictogram_is_served_and_cacheable_for_good(self, app):
        body = get(app, "/api/schemes").json()
        pictures = {o["pictogram"] for s in body["schemes"] for o in s["outcomes"]}
        for picture in sorted(pictures):
            response = get(app, body["pictogram_base"] + picture)
            assert response.status_code == 200, picture
            assert response.headers["cache-control"] == "public, max-age=31536000, immutable"
            assert response.headers["content-type"] in ("image/svg+xml", "image/png")

    def test_an_old_version_is_gone(self, app):
        assert get(app, "/pictograms/000000000000/rain/step3_dry.svg").status_code == 404

    def test_nothing_outside_the_pictograms(self, app):
        base = get(app, "/api/schemes").json()["pictogram_base"]
        assert get(app, base + "../../config.py").status_code == 404
        assert get(app, base + "%2e%2e/%2e%2e/config.py").status_code == 404


class TestForecast:
    def test_a_valid_forecast_with_the_products_pictograms(self, app):
        response = get(app, f"/api/forecast?{BRAUNSCHWEIG}")
        assert response.status_code == 200
        forecast = Forecast.model_validate(response.json())
        assert len(forecast.steps) == 56
        assert {name: s.scheme for name, s in forecast.pictograms.items()} == {
            "cloud_cover": "cloud-vsup", "precipitation": "precipitation-vsup", "wind_speed_10m": "wind-vsup",
        }
        assert set(forecast.variables) == {"temperature_2m", "precipitation", "cloud_cover", "wind_speed_10m"}
        assert response.json()["steps"][0].endswith("Z")
        assert response.headers["cache-control"] == "public, max-age=300"

    def test_name_is_passed_through(self, app):
        body = get(app, f"/api/forecast?{BRAUNSCHWEIG}&name=Zuhause").json()
        assert body["location"]["name"] == "Zuhause"

    def test_explicit_product_and_variant(self, app):
        assert get(app, f"/api/forecast?{BRAUNSCHWEIG}&product=ecmwf&variant=ensemble").status_code == 200

    @pytest.mark.parametrize("query,field", [
        ("lat=91&lon=10", "lat"),
        ("lat=52&lon=-181", "lon"),
        ("lat=52", "lon"),
        ("lat=north&lon=10", "lat"),
        (f"{BRAUNSCHWEIG}&variant=deterministic", "variant"),
        (f"{BRAUNSCHWEIG}&name={'x' * 201}", "name"),
    ])
    def test_invalid_parameters_are_422_naming_the_field(self, app, query, field):
        response = get(app, f"/api/forecast?{query}")
        assert response.status_code == 422
        assert [e["loc"] for e in response.json()["detail"]] == [["query", field]]

    def test_unknown_product_is_422(self, app):
        response = get(app, f"/api/forecast?{BRAUNSCHWEIG}&product=gfs")
        assert response.status_code == 422
        (error,) = response.json()["detail"]
        assert error["loc"] == ["query", "product"]
        assert "no product 'gfs'; there are ecmwf" in error["msg"]

    def test_missing_variant_is_409(self, app):
        response = get(app, f"/api/forecast?{BRAUNSCHWEIG}&variant=hres")
        assert response.status_code == 409
        assert "has no 'hres' variant" in response.json()["detail"]

    def test_no_data_for_the_place_is_404(self, app):
        response = get(app, "/api/forecast?lat=0&lon=0")
        assert response.status_code == 404
        assert "no fixture within" in response.json()["detail"]

    @pytest.mark.parametrize("failure,status", [
        (SourceTimeout("open-meteo did not answer within 30s"), 504),
        (SourceError("open-meteo answered 500: boom"), 502),
        (DataGap("data is missing from row 3 but resumes at row 5"), 502),
    ])
    def test_upstream_failures(self, failure, status):
        response = get(app_with(Recorded(raises=failure)), f"/api/forecast?{BRAUNSCHWEIG}")
        assert response.status_code == status
        assert response.json() == {"detail": str(failure)}


class TestForecastCache:
    def test_a_second_request_is_served_from_the_cache(self):
        source = Recorded()
        app = app_with(source)
        get(app, f"/api/forecast?{BRAUNSCHWEIG}")
        get(app, f"/api/forecast?{BRAUNSCHWEIG}&name=again")
        assert source.calls == 1

    def test_nearby_coordinates_share_an_entry(self):
        source = Recorded()
        app = app_with(source)
        get(app, "/api/forecast?lat=52.2601&lon=10.5199")
        get(app, "/api/forecast?lat=52.2649&lon=10.5249")
        assert source.calls == 1

    def test_simultaneous_requests_share_one_fetch(self):
        source = Recorded(delay=0.05)
        responses = get_concurrently(app_with(source), [f"/api/forecast?{BRAUNSCHWEIG}"] * 8)
        assert {r.status_code for r in responses} == {200}
        assert source.calls == 1

    def test_failures_are_not_cached(self):
        source = Recorded(raises=SourceTimeout("slow"))
        app = app_with(source)
        assert get(app, f"/api/forecast?{BRAUNSCHWEIG}").status_code == 504
        source.raises = None
        assert get(app, f"/api/forecast?{BRAUNSCHWEIG}").status_code == 200
        assert source.calls == 2


PARIS = {"results": [
    {"name": "Paris", "latitude": 48.85341, "longitude": 2.3488, "admin1": "Île-de-France",
     "country": "France", "country_code": "FR", "timezone": "Europe/Paris", "elevation": 42.0},
    {"name": "Paris", "latitude": 33.66094, "longitude": -95.55551, "country_code": "US"},
]}


class TestGeocode:
    def test_places_best_first(self):
        seen = []
        response = get(app_with(geocoder=geocoder_answering(PARIS, seen=seen)), "/api/geocode?q=Paris&lang=de&count=2")
        assert response.status_code == 200
        body = response.json()
        assert body["query"] == "Paris"
        assert [(r["name"], r["country_code"]) for r in body["results"]] == [("Paris", "FR"), ("Paris", "US")]
        assert body["results"][0]["timezone"] == "Europe/Paris"
        assert seen[0].url.params["name"] == "Paris"
        assert seen[0].url.params["language"] == "de"
        assert seen[0].url.params["count"] == "2"

    def test_nothing_found_is_an_empty_list(self):
        response = get(app_with(geocoder=geocoder_answering({"generationtime_ms": 0.1})), "/api/geocode?q=xqzvw")
        assert response.status_code == 200
        assert response.json()["results"] == []

    def test_one_character_asks_nobody(self):
        seen = []
        response = get(app_with(geocoder=geocoder_answering(PARIS, seen=seen)), "/api/geocode?q=P")
        assert response.json()["results"] == []
        assert seen == []

    def test_the_same_query_is_asked_once(self):
        """Typing makes the same queries over and over."""
        seen = []
        app = app_with(geocoder=geocoder_answering(PARIS, seen=seen))
        for q in ("Paris", "paris", " Paris  "):
            get(app, f"/api/geocode?q={q}")
        assert len(seen) == 1

    @pytest.mark.parametrize("query", ["", "q=", f"q={'x' * 101}", "q=Paris&lang=fr", "q=Paris&count=0"])
    def test_invalid_parameters(self, query):
        assert get(app_with(), f"/api/geocode?{query}").status_code == 422

    def test_upstream_error_is_502(self):
        app = app_with(geocoder=geocoder_answering({"error": True, "reason": "down"}, status=500))
        response = get(app, "/api/geocode?q=Paris")
        assert response.status_code == 502
        assert "answered 500: down" in response.json()["detail"]

    def test_upstream_timeout_is_504(self):
        response = get(app_with(geocoder=geocoder_answering(raises=httpx.ReadTimeout("slow"))), "/api/geocode?q=Paris")
        assert response.status_code == 504

    def test_malformed_answer_is_502(self):
        response = get(app_with(geocoder=geocoder_answering({"results": [{"name": "Paris"}]})), "/api/geocode?q=Paris")
        assert response.status_code == 502
        assert "not a list of places" in response.json()["detail"]


class TestHealth:
    def test_liveness(self, app):
        response = get(app, "/api/health")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ok"
        assert body["upstream"] is None
        assert response.headers["cache-control"] == "no-store"

    def test_deep_asks_every_source(self):
        source = Recorded()
        response = get(app_with(source), "/api/health?deep=true")
        assert response.status_code == 200
        assert response.json()["upstream"] == {"recorded": "ok"}
        assert source.calls == 1

    def test_deep_with_a_failing_source_is_503(self):
        response = get(app_with(Recorded(raises=SourceError("answered 500"))), "/api/health?deep=true")
        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "degraded"
        assert body["upstream"] == {"recorded": "failing: answered 500"}


class TestContract:
    def test_every_route_is_read_only(self, app):
        """Carried over from test_deployment.py: there is no session, no CSRF
        token and no state to change. A route taking POST/PUT/PATCH/DELETE is
        the signal to revisit that."""
        routes = [r for r in app.routes if isinstance(r, APIRoute)]
        assert routes, "no API routes found"
        for route in routes:
            assert route.methods <= {"GET", "HEAD"}, f"{route.path} accepts {sorted(route.methods)}"

    def test_openapi_schema_is_checked_in(self):
        """The contract with the frontend. Regenerate with
        `python -m api openapi -o api/openapi.json` and review the diff."""
        assert cli.OPENAPI_FILE.read_text() == cli.openapi_text()

    def test_openapi_is_served(self, app):
        assert get(app, "/api/openapi.json").json() == json.loads(cli.OPENAPI_FILE.read_text())

    def test_start_up_refuses_a_broken_config(self, tmp_path):
        from core.configfile import ConfigError

        broken = tmp_path / "sources.yaml"
        broken.write_text(OFFLINE.sources_config.read_text().replace("wind-vsup", "wind-vsupp"))
        with pytest.raises(ConfigError, match="no scheme 'wind-vsupp'"):
            create_app(Settings(sources_config=broken))


class TestSettings:
    def test_defaults(self):
        assert Settings.from_env({}) == Settings()

    def test_from_environment(self, tmp_path):
        settings = Settings.from_env({"SOURCES_CONFIG": str(tmp_path), "FORECAST_CACHE_TTL_S": "60",
                                      "FORECAST_CACHE_SIZE": "8"})
        assert settings.sources_config == tmp_path
        assert settings.forecast_cache_ttl_s == 60.0
        assert settings.forecast_cache_size == 8

    def test_bad_value(self):
        with pytest.raises(ValueError, match="FORECAST_CACHE_SIZE='many' is not a valid int"):
            Settings.from_env({"FORECAST_CACHE_SIZE": "many"})


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def run(coroutine):
    return asyncio.run(coroutine)


class TestTTLCache:
    def test_expires(self):
        clock, calls = FakeClock(), []
        cache = TTLCache(ttl_s=10, max_entries=4, clock=clock)

        async def factory():
            calls.append(clock.now)
            return len(calls)

        async def scenario():
            assert await cache.get("k", factory) == 1
            clock.now = 9.9
            assert await cache.get("k", factory) == 1
            clock.now = 10.1
            assert await cache.get("k", factory) == 2

        run(scenario())
        assert cache.stats() == {"entries": 1, "hits": 1, "misses": 2}

    def test_evicts_the_least_recently_used(self):
        cache = TTLCache(ttl_s=100, max_entries=2)

        async def value(v):
            return v

        async def scenario():
            await cache.get("a", lambda: value(1))
            await cache.get("b", lambda: value(2))
            await cache.get("a", lambda: value(99))  # a is now the most recent
            await cache.get("c", lambda: value(3))  # evicts b
            assert await cache.get("a", lambda: value(99)) == 1
            assert await cache.get("b", lambda: value(22)) == 22

        run(scenario())

    def test_a_cancelled_caller_does_not_cancel_the_fetch(self):
        """A client hanging up must not waste the fetch for everyone else."""
        cache = TTLCache(ttl_s=100, max_entries=4)
        started = []

        async def slow():
            started.append(True)
            await asyncio.sleep(0.05)
            return "done"

        async def scenario():
            impatient = asyncio.ensure_future(cache.get("k", slow))
            await asyncio.sleep(0.01)
            patient = asyncio.ensure_future(cache.get("k", slow))
            impatient.cancel()
            assert await patient == "done"
            assert await cache.get("k", slow) == "done"

        run(scenario())
        assert len(started) == 1

    def test_a_failure_reaches_every_waiter_and_is_forgotten(self):
        cache = TTLCache(ttl_s=100, max_entries=4)

        async def failing():
            await asyncio.sleep(0.01)
            raise SourceTimeout("slow")

        async def scenario():
            results = await asyncio.gather(*(cache.get("k", failing) for _ in range(3)), return_exceptions=True)
            assert all(isinstance(r, SourceTimeout) for r in results)
            assert cache.stats()["entries"] == 0

        run(scenario())
