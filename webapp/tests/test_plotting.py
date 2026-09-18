"""Offline rendering tests: fixtures survive the full matplotlib path.

Dates are derived from each fixture's own reference time rather than "now", so
these stay deterministic as the fixtures age.
"""
from datetime import datetime, timedelta

import matplotlib
import pytest

from meteogram.plotMeteogram import HresDataUnavailable, getTimeFrame, plotMeteogram
from tests.conftest import LOCATION_KEYS, load_fixture


def reference_time(data):
    entry = data["2t"]
    return datetime(
        int(entry["date"][0:4]),
        int(entry["date"][4:6]),
        int(entry["date"][6:8]),
        int(entry["time"][0:2]),
    )


def test_agg_backend_is_active():
    """plotMeteogram sets Agg at import; Tk is not thread-safe under Flask."""
    assert matplotlib.get_backend().lower() == "agg"


class TestGetTimeFrame:
    def test_full_range_from_reference_time(self, braunschweig):
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=14)
        )
        assert fromIndex == 0
        assert toIndex == len(braunschweig["2t"]["2t"]["steps"])

    def test_window_shrinks_with_the_requested_span(self, braunschweig):
        start = reference_time(braunschweig)
        _, three_days = getTimeFrame(braunschweig, start, start + timedelta(days=3))
        _, ten_days = getTimeFrame(braunschweig, start, start + timedelta(days=10))
        assert 0 < three_days < ten_days

    def test_three_day_window_is_four_steps_per_day(self, braunschweig):
        start = reference_time(braunschweig)
        _, toIndex = getTimeFrame(braunschweig, start, start + timedelta(days=3))
        assert toIndex == pytest.approx(3 * 4, abs=1)

    def test_later_start_advances_the_first_index(self, braunschweig):
        start = reference_time(braunschweig)
        fromIndex, _ = getTimeFrame(
            braunschweig, start + timedelta(days=2), start + timedelta(days=5)
        )
        assert fromIndex == pytest.approx(2 * 4, abs=1)


class TestRendering:
    @pytest.mark.parametrize("key", LOCATION_KEYS)
    def test_renders_every_fixture(self, key, locations):
        data = load_fixture(key)
        start = reference_time(data)
        fromIndex, toIndex = getTimeFrame(data, start, start + timedelta(days=3))
        fig = plotMeteogram(
            data, fromIndex, toIndex, locations[key]["timezone"], "ensemble"
        )
        try:
            assert len(fig.axes) == 4, "expected cloud/rain/temperature/wind panels"
            assert fig.get_size_inches() == pytest.approx([14, 6])
        finally:
            matplotlib.pyplot.close(fig)

    def test_writes_a_readable_png(self, braunschweig, tmp_path):
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=3)
        )
        fig = plotMeteogram(
            braunschweig, fromIndex, toIndex, "Europe/Berlin", "ensemble"
        )
        target = tmp_path / "forecast.png"
        try:
            fig.savefig(target, dpi=100, bbox_inches="tight")
        finally:
            matplotlib.pyplot.close(fig)
        assert target.stat().st_size > 10_000
        with open(target, "rb") as fp:
            assert fp.read(8) == b"\x89PNG\r\n\x1a\n"

    def test_unknown_timezone_does_not_crash(self, braunschweig):
        """plotTemperature swallows bad timezone names and falls back to UTC."""
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=2)
        )
        fig = plotMeteogram(braunschweig, fromIndex, toIndex, "Not/AZone", "ensemble")
        matplotlib.pyplot.close(fig)

    def test_unknown_plot_type_is_rejected(self, braunschweig):
        """Used to fall through both branches and die on an unbound localMinima."""
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=2)
        )
        with pytest.raises(ValueError, match="unknown plotType"):
            plotMeteogram(braunschweig, fromIndex, toIndex, "Europe/Berlin", "evil")

    def test_enhanced_hres_explains_itself(self, braunschweig):
        """A valid plot type the current data source cannot serve should say so,
        not raise KeyError('hres') from somewhere deep in plotTemperature."""
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=3)
        )
        with pytest.raises(HresDataUnavailable, match="ensemble percentiles only"):
            plotMeteogram(
                braunschweig, fromIndex, toIndex, "Europe/Berlin", "enhanced-hres"
            )

    @pytest.mark.xfail(
        reason="the Open-Meteo downloader produces no 'hres' key, so the "
        "HRES-enhanced plot cannot be rendered (see CLAUDE.md, Known gaps)",
        raises=HresDataUnavailable,
        strict=True,
    )
    def test_enhanced_hres_plot_type(self, braunschweig):
        start = reference_time(braunschweig)
        fromIndex, toIndex = getTimeFrame(
            braunschweig, start, start + timedelta(days=3)
        )
        fig = plotMeteogram(
            braunschweig, fromIndex, toIndex, "Europe/Berlin", "enhanced-hres"
        )
        matplotlib.pyplot.close(fig)
