"""Offline tests for the percentile -> pictogram mapping.

The coordinate functions return an index into a positionally-coupled filename list
(see CLAUDE.md). These tests pin both halves of that coupling.
"""
import os

import pytest

from plotMeteogram import (
    getVSUPCloudCoordinate,
    getVSUPWindCoordinate,
    getVSUPrainCoordinate,
)
from tests.conftest import LOCATION_KEYS, load_fixture

# The ensemble filename lists exactly as plot*VSUP() spells them, so a reordering
# there without a matching change here is caught.
VSUP_FILES = {
    "cloud": (
        "pictogram/cloud/",
        [
            "step1.png",
            "step2_mostly_clear.png",
            "step2_mostly_cloudy.png",
            "step3_sunny.png",
            "step3_light_clouds.png",
            "step3_medium_cloudy.png",
            "step3_cloud_max.png",
        ],
    ),
    "rain": (
        "pictogram/rain/",
        [
            "step1_v2.png",
            "Stufe2_KaumRegen.png",
            "Stufe2_Regen.png",
            "Stufe3_KeinRegen.png",
            "Stufe3_leichterRegen.png",
            "Stufe3_MittlererRegen.png",
            "Stufe3_Starkregen.png",
        ],
    ),
    "wind": (
        "pictogram/wind/",
        [
            "step1_v2.png",
            "Stufe2_kaumWind.png",
            "Stufe2_vielWind.png",
            "Stufe3_Windstille.png",
            "Stufe3_leichterWind.png",
            "Stufe3_starkerWind.png",
            "Stufe3_Sturm.png",
        ],
    ),
}

COORDINATE_FUNCTIONS = {
    "tcc": getVSUPCloudCoordinate,
    "tp": getVSUPrainCoordinate,
    "ws": getVSUPWindCoordinate,
}


def percentiles_at(data, variable, index):
    series = data[variable][variable]
    return {key: series[key][index] for key in series if key != "steps"}


@pytest.mark.parametrize("group", sorted(VSUP_FILES))
def test_every_referenced_pictogram_exists(group):
    """A missing file only surfaces as a crash deep inside rendering."""
    path, filenames = VSUP_FILES[group]
    missing = [f for f in filenames if not os.path.exists(path + f)]
    assert not missing, f"{group}: missing pictograms {missing} under {path}"


@pytest.mark.parametrize("variable", sorted(COORDINATE_FUNCTIONS))
@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_coordinates_stay_in_range(key, variable):
    """Every index must address the 7-entry ensemble filename list."""
    data = load_fixture(key)
    fn = COORDINATE_FUNCTIONS[variable]
    n = len(data[variable][variable]["steps"])
    for i in range(n):
        index = fn(percentiles_at(data, variable, i))
        assert 0 <= index < 7, (
            f"{key} {variable} step {i}: index {index} is out of range"
        )


class TestThresholds:
    """Boundary cases stated in the units each function was written for."""

    def test_confident_calm_wind(self):
        # ninety < 3 -> "no wind"
        assert getVSUPWindCoordinate(
            {"min": 0.0, "ten": 0.2, "twenty_five": 0.5, "median": 1.0,
             "seventy_five": 1.5, "ninety": 2.0, "max": 2.5}
        ) == 3

    def test_confident_storm(self):
        # ten > 17.2 m/s -> "storm"
        assert getVSUPWindCoordinate(
            {"min": 18.0, "ten": 19.0, "twenty_five": 20.0, "median": 22.0,
             "seventy_five": 24.0, "ninety": 26.0, "max": 30.0}
        ) == 6

    def test_uncertain_wind_falls_back_to_the_vaguest_pictogram(self):
        # Wide spread straddling every threshold -> index 0, the "no idea" glyph.
        assert getVSUPWindCoordinate(
            {"min": 0.0, "ten": 1.0, "twenty_five": 4.0, "median": 9.0,
             "seventy_five": 14.0, "ninety": 20.0, "max": 25.0}
        ) == 0

    def test_confident_dry(self):
        # ninety < 1e-4 m -> "no rain"
        assert getVSUPrainCoordinate(
            {"min": 0.0, "ten": 0.0, "twenty_five": 0.0, "median": 0.0,
             "seventy_five": 0.0, "ninety": 0.0, "max": 0.0}
        ) == 3

    def test_confident_clear_sky(self):
        # ninety < 10 percent -> "no cloud"
        assert getVSUPCloudCoordinate(
            {"min": 0.0, "ten": 0.0, "twenty_five": 2.0, "median": 4.0,
             "seventy_five": 6.0, "ninety": 8.0, "max": 20.0}
        ) == 3

    def test_confident_overcast(self):
        # ten > 90 percent -> "all cloudy"
        assert getVSUPCloudCoordinate(
            {"min": 90.0, "ten": 93.0, "twenty_five": 95.0, "median": 97.0,
             "seventy_five": 99.0, "ninety": 100.0, "max": 100.0}
        ) == 6

    def test_confident_light_cloud(self):
        # ninety < 50 percent, but not clear -> "light clouds"
        assert getVSUPCloudCoordinate(
            {"min": 5.0, "ten": 12.0, "twenty_five": 20.0, "median": 30.0,
             "seventy_five": 38.0, "ninety": 45.0, "max": 55.0}
        ) == 4

    def test_confident_heavy_cloud(self):
        # ten > 50 percent but not a confident overcast -> "lot of clouds"
        assert getVSUPCloudCoordinate(
            {"min": 45.0, "ten": 55.0, "twenty_five": 65.0, "median": 75.0,
             "seventy_five": 85.0, "ninety": 92.0, "max": 98.0}
        ) == 5


class TestCloudCoverUnits:
    """Regression cover for the percent-vs-fraction fix.

    getVSUPCloudCoordinate's thresholds were written for the old grib pipeline's
    0-1 fraction; Open-Meteo delivers percent, which pinned almost every step to
    the "all cloudy" glyph. The thresholds are now stated in percent.
    """

    def test_cloud_cover_arrives_as_percent(self):
        values = load_fixture("braunschweig")["tcc"]["tcc"]["median"]
        assert max(values) > 1.0, "cloud cover looks like a 0-1 fraction again"

    def test_every_cloud_pictogram_is_reachable(self):
        """All seven glyphs must be selectable across the fixture climates; if the
        thresholds drift back to fractions this collapses to {6} again."""
        seen = set()
        for key in LOCATION_KEYS:
            data = load_fixture(key)
            n = len(data["tcc"]["tcc"]["steps"])
            for i in range(n):
                seen.add(getVSUPCloudCoordinate(percentiles_at(data, "tcc", i)))
        assert seen == set(range(7)), (
            f"only pictograms {sorted(seen)} are ever selected"
        )

    def test_a_clear_sky_location_is_not_reported_as_overcast(self):
        """Alice Springs is arid; it must not sit on the overcast glyph."""
        data = load_fixture("alice_springs")
        n = len(data["tcc"]["tcc"]["steps"])
        indices = [
            getVSUPCloudCoordinate(percentiles_at(data, "tcc", i)) for i in range(n)
        ]
        assert indices.count(6) < n / 2, "arid location is mostly overcast"


class TestUnitMismatch:
    """Precipitation and wind still carry the unit mismatch that cloud cover had.

    downloadJsonData delivers precipitation in mm and wind in km/h, while these
    coordinate functions were written for the grib pipeline's metres and m/s. The
    xfail below documents the consequence; it will XPASS once the units are
    reconciled the way cloud cover now is.
    """

    @pytest.mark.xfail(
        reason="tp arrives in mm, so any measurable rain clears the 2e-3 'strong "
        "rain' threshold and the light/medium pictograms (index 4 and 5) are "
        "unreachable",
        strict=True,
    )
    def test_light_and_medium_rain_pictograms_are_reachable(self):
        seen = set()
        for key in LOCATION_KEYS:
            data = load_fixture(key)
            n = len(data["tp"]["tp"]["steps"])
            for i in range(n):
                seen.add(getVSUPrainCoordinate(percentiles_at(data, "tp", i)))
        assert {4, 5} <= seen, f"only pictograms {sorted(seen)} are ever selected"
