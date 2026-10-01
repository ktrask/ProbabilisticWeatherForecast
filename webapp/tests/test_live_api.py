"""Live tests: does Open-Meteo still hand back what the adapters expect?

These are the only tests that touch the network, and nothing here is cached,
so an upstream change cannot hide behind a stored answer.

    pytest -m live          # just these
    pytest -m "not live"    # everything else

They skip themselves (rather than fail) when the network is unreachable.
"""
import asyncio

import httpx
import pytest

from api.models import ForecastOut
from core.model import Forecast, Location
from core.pipeline import build_forecast
from sources.base import SourceError
from sources.fixture import FixtureSource
from sources.geocode import OpenMeteoGeocoder
from sources.open_meteo import OpenMeteoEnsemble

pytestmark = pytest.mark.live

ENSEMBLE_MEMBERS = 51  # ecmwf_ifs025

# Somewhere none of the offline fixtures use.
PARIS = Location(lat=48.8566, lon=2.3522, name="Paris")

# The canonical units, as the JSON API spells them. The adapter asks for them
# and checks the flatbuffers' own unit field; this checks the other spelling
# of the same request.
EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "wind_speed_10m": "m/s",
    "cloud_cover": "%",
}


def skip_if_offline(exc):
    pytest.skip(f"Open-Meteo unreachable: {type(exc).__name__}: {exc}")


def fetch(source, location, variables):
    try:
        return asyncio.run(source.fetch(location, variables))
    except SourceError as exc:
        if isinstance(exc.__cause__, httpx.TransportError):
            skip_if_offline(exc)
        raise


@pytest.fixture(scope="module")
def raw_response():
    """The adapter's own request, two days of it, as JSON instead of flatbuffers."""
    source = OpenMeteoEnsemble(forecast_days=2)
    params = {**source.params(PARIS, source.variables), "format": "json"}
    try:
        response = httpx.get(source.url, params=params, timeout=60)
    except httpx.TransportError as exc:
        skip_if_offline(exc)
    assert response.status_code == 200, f"{source.url} returned {response.status_code}: {response.text[:300]}"
    return response.json()


def test_model_is_still_available(raw_response):
    assert "error" not in raw_response, raw_response.get("reason")
    assert "hourly" in raw_response, f"no hourly block: {sorted(raw_response)}"


def test_units_have_not_changed(raw_response):
    """A silent unit switch upstream would quietly corrupt every pictogram."""
    units = raw_response["hourly_units"]
    for variable, expected in EXPECTED_UNITS.items():
        assert units.get(variable) == expected, f"{variable} is now in {units.get(variable)!r}, expected {expected!r}"


def test_still_51_ensemble_members(raw_response):
    hourly = raw_response["hourly"]
    for variable in EXPECTED_UNITS:
        members = [key for key in hourly if key == variable or key.startswith(f"{variable}_member")]
        assert len(members) == ENSEMBLE_MEMBERS, f"{variable}: {len(members)} members, expected {ENSEMBLE_MEMBERS}"


def test_hourly_interval_is_still_one_hour(raw_response):
    """The reduction builds 6-hour steps out of hourly rows."""
    times = raw_response["hourly"]["time"]
    assert len(times) >= 24
    assert times[0].endswith(":00"), f"unexpected timestamp format: {times[0]!r}"


def shipped_products():
    from sources.config import DEFAULT_SOURCES, load
    from vsup.config import load as load_vsup

    return list(load(DEFAULT_SOURCES, load_vsup()).products.values())


@pytest.mark.parametrize("product", shipped_products(), ids=lambda p: p.id)
def test_each_product_is_what_it_claims(product):
    """What sources.yaml tells the reader - members and range - against a
    live run, and the product's schemes apply to it."""
    from vsup.classify import classify
    from vsup.config import load

    # Paris, or for a regional model the middle of its area.
    place = PARIS if product.area is None else Location(lat=product.area.center[0], lon=product.area.center[1])
    result = fetch(product.source, place, product.variables)
    assert result.member_count == product.members
    forecast = build_forecast(result)
    span_days = (forecast.steps[-1] - forecast.steps[0]).total_seconds() / 86400
    # Counted from local midnight, so a run started later that day can reach a
    # little past its nominal range. And the newest run is not always the
    # longest: ECMWF's 06 and 18 UTC runs stop at 6 days, and then the end
    # comes from an older run (13.2 days on 2026-10-01, 09 UTC; 14.5 right
    # after a 00 UTC run).
    assert product.horizon_days - 2 <= span_days <= product.horizon_days + 1, f"{span_days:.1f} days"
    classify(forecast, load(), list(product.schemes))


def test_a_regional_model_says_where_it_has_no_grid():
    """The reason Open-Meteo gives - the adapter turns exactly that into NotCovered."""
    from sources.base import NotCovered

    with pytest.raises(NotCovered):
        fetch(OpenMeteoEnsemble("icon_d2_eps", forecast_days=2), Location(lat=1.35, lon=103.82), {"temperature_2m"})


def test_live_forecast_matches_the_fixtures():
    """The offline suite and the offline configuration stand in for this; they
    have to look the same."""
    source = OpenMeteoEnsemble()
    live = build_forecast(fetch(source, PARIS, source.variables))
    recorded = build_forecast(FixtureSource().load("braunschweig"))
    assert set(live.variables) == set(recorded.variables)
    for name, series in live.variables.items():
        assert list(series.quantiles) == list(recorded.variables[name].quantiles)
        assert (series.unit, series.kind, series.window_hours) == (
            recorded.variables[name].unit,
            recorded.variables[name].kind,
            recorded.variables[name].window_hours,
        )


def test_geocoder_finds_a_place():
    try:
        places = asyncio.run(OpenMeteoGeocoder().search("Braunschweig", language="de"))
    except SourceError as exc:
        if isinstance(exc.__cause__, httpx.TransportError):
            skip_if_offline(exc)
        raise
    first = places[0]
    assert (first.name, first.country_code, first.timezone) == ("Braunschweig", "DE", "Europe/Berlin")
    assert abs(first.lat - 52.26) < 0.1 and abs(first.lon - 10.52) < 0.1


def test_open_meteo_adapter_delivers_a_valid_forecast():
    """The adapter end to end: fetch, reduce to the Forecast model, classify
    with the shipped VSUP schemes."""
    from vsup.classify import classify
    from vsup.config import load

    source = OpenMeteoEnsemble()
    result = fetch(source, PARIS, source.variables)
    assert result.member_count == ENSEMBLE_MEMBERS
    forecast = build_forecast(result)
    # 15 days are requested; the run ends earlier and the NaN padding after it
    # is dropped - 13 to 14.5 days, depending on the run (see above).
    assert len(forecast.steps) > 13 * 4, f"only {len(forecast.steps)} steps"
    assert forecast.location.timezone == "Europe/Paris"
    classify(forecast, load(), ["cloud-vsup", "precipitation-vsup", "wind-vsup"])


def test_api_end_to_end():
    """The API with its real sources: geocode a place, then fetch its forecast."""
    from api.app import create_app
    from api.settings import Settings
    from tests.test_api import get

    app = create_app(Settings())

    def ok(url):
        response = get(app, url)
        if response.status_code in (502, 504) and "could not be reached" in response.text:
            skip_if_offline(RuntimeError(response.json()["detail"]))
        assert response.status_code == 200, f"{url}: {response.status_code} {response.text[:300]}"
        return response.json()

    places = ok("/api/geocode?q=Paris&lang=en")["results"]
    paris = next(p for p in places if p["country_code"] == "FR")
    body = ok(f"/api/forecast?lat={paris['lat']}&lon={paris['lon']}&name=Paris")
    forecast = ForecastOut.model_validate(body)
    assert forecast.location.name == "Paris"
    assert forecast.location.timezone == "Europe/Paris"
    assert set(forecast.pictograms) == {"cloud_cover", "precipitation", "wind_speed_10m"}
    assert set(ok("/api/health?deep=true")["upstream"].values()) == {"ok"}
