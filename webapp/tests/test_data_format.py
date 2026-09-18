"""Offline tests: the stored fixtures still match the allMeteogramData contract.

These never touch the network. They pin the format that plotMeteogram.py consumes,
so a change to downloadJsonData.py that breaks the contract fails here immediately.
"""
from datetime import datetime

import pytest

from tests.conftest import LOCATION_KEYS, load_fixture
from tests.schema import (
    EXPECTED_VARIABLES,
    PERCENTILE_KEYS,
    assert_meteogram_schema,
)


def test_all_fixtures_present():
    for key in LOCATION_KEYS:
        assert load_fixture(key), f"fixture {key} is empty"


def test_fixture_matches_schema(meteogram_data):
    assert_meteogram_schema(meteogram_data)


def test_locations_index_covers_every_fixture(locations):
    assert set(locations) == set(LOCATION_KEYS)
    for key, loc in locations.items():
        assert -90 <= loc["latitude"] <= 90, f"{key}: bad latitude"
        assert -180 <= loc["longitude"] <= 180, f"{key}: bad longitude"
        assert loc["name"] and loc["timezone"]


def test_forecast_covers_about_two_weeks(meteogram_data):
    """forecast_days=14 at 6-hourly steps -> 56 steps spanning 330 hours."""
    steps = meteogram_data["2t"]["2t"]["steps"]
    span_hours = int(steps[-1]) - int(steps[0])
    assert 13 * 24 <= span_hours <= 14 * 24, (
        f"forecast spans {span_hours}h, expected roughly 14 days"
    )


def test_all_variables_share_one_reference_time(meteogram_data):
    """plotMeteogram derives the x-axis from 2t's date/time alone, so the other
    variables have to be on the same clock or the panels silently desynchronise."""
    stamps = {
        name: (meteogram_data[name]["date"], meteogram_data[name]["time"])
        for name in EXPECTED_VARIABLES
    }
    assert len(set(stamps.values())) == 1, f"variables disagree on start time: {stamps}"


def test_reference_time_is_parseable(meteogram_data):
    """getTimeFrame and plotTemperature slice date/time by character offset."""
    entry = meteogram_data["2t"]
    parsed = datetime(
        int(entry["date"][0:4]),
        int(entry["date"][4:6]),
        int(entry["date"][6:8]),
        int(entry["time"][0:2]),
    )
    assert datetime(2018, 1, 1) < parsed < datetime(2100, 1, 1)


def test_precipitation_is_never_negative(meteogram_data):
    for key in PERCENTILE_KEYS:
        assert min(meteogram_data["tp"]["tp"][key]) >= 0.0


def test_cloud_cover_spans_the_full_percent_scale():
    """Cloud cover is a percentage (0-100); the pictogram thresholds are stated in
    the same unit. Braunschweig reaches full overcast in this fixture."""
    values = load_fixture("braunschweig")["tcc"]["tcc"]["max"]
    assert max(values) > 1.0, "cloud cover looks like a 0-1 fraction, not percent"
    assert max(values) <= 100.0


@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_fixture_has_real_spread(key):
    """A fixture where every member agrees would make the uncertainty pictograms
    meaningless and would silently weaken every downstream test."""
    series = load_fixture(key)["2t"]["2t"]
    spread = [hi - lo for lo, hi in zip(series["min"], series["max"])]
    assert max(spread) > 0.5, (
        f"{key}: ensemble spread never exceeds {max(spread):.2f} degC"
    )
