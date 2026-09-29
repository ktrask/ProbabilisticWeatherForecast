"""Offline tests for the percentile -> pictogram mapping.

The coordinate functions return an index into a positionally-coupled filename list
(see CLAUDE.md). These tests pin both halves of that coupling.
"""
import os

import pytest

from meteogram.plotMeteogram import PICTOGRAM_DIR
from meteogram.plotMeteogram import (
    getVSUPCloudCoordinate,
    getVSUPWindCoordinate,
    getVSUPrainCoordinate,
)
from tests.conftest import LOCATION_KEYS, load_fixture

# The ensemble filename lists exactly as plot*VSUP() spells them, so a reordering
# there without a matching change here is caught.
VSUP_FILES = {
    "cloud": (
        os.path.join(PICTOGRAM_DIR, "cloud") + os.sep,
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
        os.path.join(PICTOGRAM_DIR, "rain") + os.sep,
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
        os.path.join(PICTOGRAM_DIR, "wind") + os.sep,
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

    def test_confident_light_wind(self):
        # 3 <= ninety < 10 m/s -> "light wind"; a 4.8 m/s breeze is Beaufort 3.
        # It was the storm glyph while wind still arrived in km/h (17.3).
        assert getVSUPWindCoordinate(
            {"min": 3.5, "ten": 4.0, "twenty_five": 4.4, "median": 4.8,
             "seventy_five": 5.2, "ninety": 5.6, "max": 6.5}
        ) == 4

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
        # ninety < 0.1 mm over the step -> "no rain"
        assert getVSUPrainCoordinate(
            {"min": 0.0, "ten": 0.0, "twenty_five": 0.0, "median": 0.0,
             "seventy_five": 0.0, "ninety": 0.0, "max": 0.0}
        ) == 3

    def test_confident_light_rain(self):
        # ninety < 1 mm, but not dry -> "light rain"
        assert getVSUPrainCoordinate(
            {"min": 0.1, "ten": 0.2, "twenty_five": 0.3, "median": 0.5,
             "seventy_five": 0.7, "ninety": 0.9, "max": 1.4}
        ) == 4

    def test_confident_medium_rain(self):
        # 1 mm < ten, ninety < 2 mm -> "medium rain"; needs a tight ensemble,
        # which is why the fixtures rarely land here.
        assert getVSUPrainCoordinate(
            {"min": 1.0, "ten": 1.2, "twenty_five": 1.4, "median": 1.6,
             "seventy_five": 1.8, "ninety": 1.9, "max": 2.4}
        ) == 5

    def test_confident_heavy_rain(self):
        # ten > 2 mm over the step -> "strong rain"
        assert getVSUPrainCoordinate(
            {"min": 2.0, "ten": 2.5, "twenty_five": 3.5, "median": 5.0,
             "seventy_five": 7.0, "ninety": 9.0, "max": 14.0}
        ) == 6

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

    def test_mid_range_cloud_pictograms_are_reachable(self):
        """With fraction thresholds this collapsed to {6} for whole locations.
        Only the mid-range glyphs are asserted: which of the extremes show up
        depends on the weather in the current fixtures."""
        seen = set()
        for key in LOCATION_KEYS:
            data = load_fixture(key)
            n = len(data["tcc"]["tcc"]["steps"])
            for i in range(n):
                seen.add(getVSUPCloudCoordinate(percentiles_at(data, "tcc", i)))
        assert {4, 5} <= seen, f"only pictograms {sorted(seen)} are ever selected"
        assert len(seen) >= 5, f"cloud glyphs barely vary: {sorted(seen)}"

    def test_a_clear_sky_location_is_not_reported_as_overcast(self):
        """Alice Springs is arid; it must not sit on the overcast glyph."""
        data = load_fixture("alice_springs")
        n = len(data["tcc"]["tcc"]["steps"])
        indices = [
            getVSUPCloudCoordinate(percentiles_at(data, "tcc", i)) for i in range(n)
        ]
        assert indices.count(6) < n / 2, "arid location is mostly overcast"


class TestPrecipitationUnits:
    """Regression cover for the mm-vs-metres fix and the 6-hourly accumulation.

    tp used to be an hour's rain sampled every six hours and compared against
    thresholds in metres, so any measurable rain cleared the "strong rain" line
    while roughly five sixths of the rainfall was discarded. It is now the
    millimetres accumulated across the step, compared in millimetres.
    """

    def test_light_rain_pictogram_is_reachable(self):
        """Unreachable before the fix: every wet step went straight to "strong"."""
        seen = set()
        for key in LOCATION_KEYS:
            data = load_fixture(key)
            n = len(data["tp"]["tp"]["steps"])
            for i in range(n):
                seen.add(getVSUPrainCoordinate(percentiles_at(data, "tp", i)))
        assert 4 in seen, f"only pictograms {sorted(seen)} are ever selected"
        assert len(seen) >= 4, f"rain glyphs barely vary: {sorted(seen)}"

    def test_a_wet_location_is_not_permanently_at_strong_rain(self):
        data = load_fixture("reykjavik")
        n = len(data["tp"]["tp"]["steps"])
        indices = [
            getVSUPrainCoordinate(percentiles_at(data, "tp", i)) for i in range(n)
        ]
        assert indices.count(6) < n / 2, "wet location is permanently at strong rain"

    def test_accumulation_is_large_enough_to_be_a_six_hour_total(self):
        """An hourly sample would rarely clear a few mm; a 6-hour total in a wet
        climate reaches double digits."""
        wettest = max(max(load_fixture(k)["tp"]["tp"]["max"]) for k in LOCATION_KEYS)
        assert wettest > 5.0, (
            f"wettest 6h bucket across fixtures is only {wettest:.2f} mm - "
            f"precipitation may have gone back to being subsampled"
        )


class TestWindUnits:
    """Regression cover for the km/h-vs-m/s fix.

    Open-Meteo's default wind unit is km/h, but getVSUPWindCoordinate is written
    in m/s, so every threshold used to fire 3.6x too early and a 4.8 m/s breeze
    cleared the 17.2 storm line. getData now asks for m/s and refuses anything
    else (test_downloader.py::TestUnits); this checks what it does to the glyphs.
    """

    def test_a_temperate_location_is_not_mostly_undecided(self):
        """Everyday winds of 2-5 m/s are 7-18 km/h, which straddles the 3 and 10
        thresholds, so in km/h the ensemble looked hopelessly uncertain: on the
        fixtures of 2026-09-28, 35 of Braunschweig's 56 steps got the vaguest
        glyph. In m/s it gets none."""
        data = load_fixture("braunschweig")
        n = len(data["ws"]["ws"]["steps"])
        indices = [
            getVSUPWindCoordinate(percentiles_at(data, "ws", i)) for i in range(n)
        ]
        assert indices.count(0) < n / 2, (
            f"vaguest wind glyph on {indices.count(0)} of {n} steps - is wind "
            f"arriving in km/h again?"
        )


class TestPictogramCache:
    """Each panel calls imscatter() once per timestep, so the same few PNGs were
    being decoded from disk over a hundred times per meteogram."""

    def test_repeated_reads_hit_the_cache(self):
        from meteogram.plotMeteogram import readPictogram

        path = os.path.join(PICTOGRAM_DIR, "cloud", "step1.png")
        readPictogram.cache_clear()
        first = readPictogram(path)
        before = readPictogram.cache_info().hits
        for _ in range(20):
            readPictogram(path)
        info = readPictogram.cache_info()
        assert info.hits - before == 20
        assert info.misses == 1
        assert readPictogram(path) is first, "the same array should be handed back"

    def test_cached_images_are_read_only(self):
        """Callers share one array, so a stray write would corrupt every later
        meteogram in the process."""
        from meteogram.plotMeteogram import readPictogram

        image = readPictogram(os.path.join(PICTOGRAM_DIR, "cloud", "step1.png"))
        assert not image.flags.writeable
        with pytest.raises(ValueError):
            image[0][0] = 0

    def test_a_whole_render_decodes_each_file_once(self, braunschweig=None):
        """The regression this exists for, measured end to end."""
        import io
        import contextlib
        from datetime import datetime, timedelta

        import matplotlib.pyplot as plt

        from meteogram import plotMeteogram as pm

        data = load_fixture("braunschweig")
        entry = data["2t"]
        start = datetime(
            int(entry["date"][:4]), int(entry["date"][4:6]),
            int(entry["date"][6:8]), int(entry["time"][:2]),
        )
        decodes = {"n": 0}
        real = plt.imread

        def counting(path, *args, **kwargs):
            decodes["n"] += 1
            return real(path, *args, **kwargs)

        pm.readPictogram.cache_clear()
        plt.imread = counting
        try:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                fromIndex, toIndex = pm.getTimeFrame(
                    data, start, start + timedelta(days=14)
                )
                fig = pm.plotMeteogram(data, fromIndex, toIndex, "Europe/Berlin", "ensemble")
                plt.close(fig)
        finally:
            plt.imread = real

        steps = toIndex - 1
        assert steps > 20, "fixture should cover a long enough forecast to matter"
        assert decodes["n"] < steps, (
            f"{decodes['n']} decodes for {steps} timesteps x 3 panels - the cache "
            f"is not being used"
        )
        assert decodes["n"] == pm.readPictogram.cache_info().currsize
