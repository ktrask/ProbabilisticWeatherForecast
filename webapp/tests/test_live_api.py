"""Live tests: does Open-Meteo still hand back the shape we expect?

These are the only tests that touch the network. They deliberately bypass the
requests_cache session so a cached response cannot mask an upstream change.

    pytest -m live          # just these
    pytest -m "not live"    # everything else

They skip themselves (rather than fail) when the network is unreachable.
"""
import socket

import pytest
import requests

from meteogram import downloadJsonData
from tests.schema import ENSEMBLE_MEMBERS, assert_meteogram_schema

pytestmark = pytest.mark.live

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
MODEL = "ecmwf_ifs025"

# Somewhere none of the offline fixtures use, so nothing can come from a warm cache.
LIVE_LATITUDE = 48.8566
LIVE_LONGITUDE = 2.3522
LIVE_ALTITUDE = 35

# The units downloadJsonData's thresholds and ranges are calibrated against today.
EXPECTED_UNITS = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "wind_speed_10m": "km/h",
    "cloud_cover": "%",
}


def skip_if_offline(exc):
    pytest.skip(f"Open-Meteo unreachable: {type(exc).__name__}: {exc}")


@pytest.fixture(scope="module")
def raw_response():
    """One uncached JSON call, shared by the tests that inspect the raw payload."""
    params = {
        "latitude": LIVE_LATITUDE,
        "longitude": LIVE_LONGITUDE,
        "models": MODEL,
        "hourly": "temperature_2m,precipitation,wind_speed_10m,cloud_cover",
        "forecast_days": 2,
        "format": "json",
    }
    try:
        response = requests.get(ENSEMBLE_URL, params=params, timeout=60)
    except (requests.RequestException, socket.error) as exc:
        skip_if_offline(exc)
    assert response.status_code == 200, (
        f"{ENSEMBLE_URL} returned {response.status_code}: {response.text[:300]}"
    )
    return response.json()


@pytest.fixture(scope="module")
def live_meteogram_data():
    """downloadJsonData.getData against the real API, with caching disabled."""
    import openmeteo_requests

    uncached = openmeteo_requests.Client()
    original = downloadJsonData.openmeteo
    downloadJsonData.openmeteo = uncached
    try:
        return downloadJsonData.getData(
            LIVE_LONGITUDE, LIVE_LATITUDE, LIVE_ALTITUDE, writeToFile=False
        )
    except (requests.RequestException, socket.error) as exc:
        skip_if_offline(exc)
    finally:
        downloadJsonData.openmeteo = original


def test_live_data_matches_the_offline_contract(live_meteogram_data):
    """The headline check: the live pipeline output is interchangeable with the
    fixtures the rest of the suite is built on."""
    assert_meteogram_schema(live_meteogram_data)


def test_model_is_still_available(raw_response):
    assert "error" not in raw_response, raw_response.get("reason")
    assert "hourly" in raw_response, f"no hourly block: {sorted(raw_response)}"


def test_units_have_not_changed(raw_response):
    """A silent unit switch upstream would quietly corrupt every pictogram."""
    units = raw_response["hourly_units"]
    for variable, expected in EXPECTED_UNITS.items():
        assert units.get(variable) == expected, (
            f"{variable} is now in {units.get(variable)!r}, expected {expected!r}"
        )


def test_still_51_ensemble_members(raw_response):
    """calculate_percentiles hardcodes range(51); fewer members means a KeyError
    and more means silently dropped data."""
    hourly = raw_response["hourly"]
    for variable in EXPECTED_UNITS:
        members = [
            key
            for key in hourly
            if key == variable or key.startswith(f"{variable}_member")
        ]
        assert len(members) == ENSEMBLE_MEMBERS, (
            f"{variable}: found {len(members)} members, expected {ENSEMBLE_MEMBERS}"
        )


def test_hourly_interval_is_still_one_hour(raw_response):
    """create_dictionary derives its step size from the first two rows and
    downloadJsonData subsamples every 6th one to get 6-hourly steps."""
    times = raw_response["hourly"]["time"]
    assert len(times) >= 24
    assert times[0].endswith(":00"), f"unexpected timestamp format: {times[0]!r}"


def test_live_and_fixture_variables_agree(live_meteogram_data, braunschweig):
    assert set(live_meteogram_data) == set(braunschweig)
    for name in braunschweig:
        assert set(live_meteogram_data[name]) == set(braunschweig[name])
        assert set(live_meteogram_data[name][name]) == set(braunschweig[name][name])
