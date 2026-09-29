"""Offline tests: the recorded fixtures are usable, whichever format they are in.

The fixtures are Forecasts written by generate_fixtures.py; the one sample left
from the removed legacy pipeline (fixtures/legacy/) is held to the
allMeteogramData contract (tests/schema.py). Either way the fixture source has
to turn them into a valid Forecast with plausible values, which is what the
rest of the suite and the offline configuration build on.
"""
import json

import pytest

from core import legacy
from core.model import Forecast
from core.pipeline import build_forecast
from sources.fixture import FixtureSource, is_forecast
from tests.conftest import FIXTURE_DIR, LOCATION_KEYS, load_fixture
from tests.schema import EXPECTED_VARIABLES, assert_meteogram_schema

# The plausible range of each variable, in its canonical unit.
RANGES = {legacy.VARIABLES[key]: (spec["low"], spec["high"]) for key, spec in EXPECTED_VARIABLES.items()}


def forecast(key):
    return build_forecast(FixtureSource().load(key))


def test_the_legacy_sample_still_matches_its_contract():
    """The one file kept in the removed pipeline's format (fixtures/legacy/)."""
    with open(FIXTURE_DIR / "legacy" / "braunschweig.json") as fp:
        assert_meteogram_schema(json.load(fp))


def test_all_fixtures_present():
    for key in LOCATION_KEYS:
        assert load_fixture(key), f"fixture {key} is empty"


def test_locations_index_covers_every_fixture(locations):
    assert set(locations) == set(LOCATION_KEYS)
    for key, loc in locations.items():
        assert -90 <= loc["latitude"] <= 90, f"{key}: bad latitude"
        assert -180 <= loc["longitude"] <= 180, f"{key}: bad longitude"
        assert loc["name"] and loc["timezone"]


@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_fixture_matches_its_format(key):
    data = load_fixture(key)
    if is_forecast(data):
        Forecast.model_validate(data)
    else:
        assert_meteogram_schema(data)


@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_forecast_covers_about_two_weeks(key):
    """Legacy recordings asked for 14 days, the adapter asks for 15 and keeps
    what the model run fills - about 14 days of 6-hour steps either way."""
    steps = forecast(key).steps
    span_hours = (steps[-1] - steps[0]).total_seconds() / 3600
    assert 13 * 24 <= span_hours <= 15 * 24, f"forecast spans {span_hours}h"


@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_values_are_plausible_in_canonical_units(key):
    """Out-of-range values mean a unit crept in that is not the canonical one."""
    for name, series in forecast(key).variables.items():
        low, high = RANGES[name]
        for quantile, values in series.quantiles.items():
            assert low <= min(values) and max(values) <= high, f"{key} {name} {quantile} outside [{low}, {high}]"


def test_cloud_cover_spans_the_full_percent_scale():
    """Cloud cover is a percentage (0-100); a 0-1 fraction would pass the range
    check and pin every step to the clearest pictogram."""
    values = forecast("braunschweig").variables["cloud_cover"].quantiles["p100"]
    assert 1.0 < max(values) <= 100.0


@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_fixture_has_real_spread(key):
    """A fixture where every member agrees would make the uncertainty pictograms
    meaningless and would silently weaken every downstream test."""
    q = forecast(key).variables["temperature_2m"].quantiles
    spread = [hi - lo for lo, hi in zip(q["p0"], q["p100"])]
    assert max(spread) > 0.5, f"{key}: ensemble spread never exceeds {max(spread):.2f} degC"
