"""Offline tests for the source adapters.

The Open-Meteo adapter is fed a recorded response (tests/fixtures/open_meteo/,
written by generate_fixtures.py) through httpx.MockTransport, so the real
flatbuffers decoding runs without a network. The same bytes also go through
the legacy getData, which is how the new reduction is held to the old one.
"""
import asyncio
from datetime import timedelta, timezone

import httpx
import numpy as np
import pytest

from core import legacy
from core.model import Location
from core.pipeline import build_forecast
from meteogram import downloadJsonData
from sources import open_meteo
from sources.base import SourceError, SourceTimeout
from sources.fixture import FixtureNotFound, FixtureSource
from sources.open_meteo import OpenMeteoEnsemble, decode, messages
from tests.conftest import FIXTURE_DIR, LOCATION_KEYS, load_fixture

# Reykjavik, because it rains there: the precipitation windows need rain to differ.
RAW = (FIXTURE_DIR / "open_meteo" / "reykjavik_3d.fb").read_bytes()
REYKJAVIK = Location(lat=64.1466, lon=-21.9426, name="Reykjavik, Iceland")
VARIABLES = OpenMeteoEnsemble.variables


def run(coroutine):
    return asyncio.run(coroutine)


def serving(content=RAW, status=200, seen=None, raises=None):
    """An AsyncClient whose every request is answered by a handler, not the network."""

    def handler(request):
        if seen is not None:
            seen.append(request)
        if raises is not None:
            raise raises
        return httpx.Response(status, content=content)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestFixtureSource:
    @pytest.mark.parametrize("key", LOCATION_KEYS)
    def test_every_fixture_becomes_a_valid_forecast(self, key):
        f = build_forecast(FixtureSource().load(key))
        assert len(f.steps) == len(load_fixture(key)["2t"]["2t"]["steps"])
        assert f.step_hours == 6
        assert set(f.variables) == set(legacy.VARIABLES.values())

    def test_values_come_through_unchanged(self, braunschweig):
        f = build_forecast(FixtureSource().load("braunschweig"))
        for key, name in legacy.VARIABLES.items():
            for legacy_name, quantile in legacy.QUANTILES.items():
                assert f.variables[name].quantiles[quantile] == braunschweig[key][key][legacy_name]

    def test_first_step_is_the_fixtures_reference_time(self, braunschweig):
        f = build_forecast(FixtureSource().load("braunschweig"))
        assert f.steps[0] == legacy.reference_time(braunschweig["2t"])
        assert f.steps[0].tzinfo == timezone.utc

    def test_location_metadata(self):
        location = FixtureSource().location("zermatt")
        assert (location.timezone, location.elevation_m) == ("Europe/Zurich", 1608)

    def test_fetch_finds_the_nearest_fixture(self):
        near = Location(lat=52.3, lon=10.6)
        result = run(FixtureSource().fetch(near, {"precipitation"}))
        assert result.source == "fixture:braunschweig"
        assert set(result.quantiles) == {"precipitation"}

    def test_fetch_far_from_any_fixture(self):
        with pytest.raises(FixtureNotFound):
            run(FixtureSource().fetch(Location(lat=0.0, lon=0.0), {"precipitation"}))

    def test_unknown_key(self):
        with pytest.raises(FixtureNotFound, match="there are alice_springs"):
            FixtureSource().load("atlantis")


class TestOpenMeteoRequest:
    def test_asks_for_every_unit_in_canonical_form(self):
        seen = []
        run(OpenMeteoEnsemble(client=serving(seen=seen)).fetch(REYKJAVIK, VARIABLES))
        params = seen[0].url.params
        assert params["wind_speed_unit"] == "ms"
        assert params["temperature_unit"] == "celsius"
        assert params["precipitation_unit"] == "mm"
        assert params["format"] == "flatbuffers"
        assert params["models"] == "ecmwf_ifs025"
        assert params["hourly"] == "cloud_cover,precipitation,temperature_2m,wind_speed_10m"

    def test_every_request_carries_the_adapters_timeout(self):
        """Even on a shared client configured without one."""
        seen = []
        client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: seen.append(request) or httpx.Response(200, content=RAW)), timeout=None)
        run(OpenMeteoEnsemble(timeout_s=7, client=client).fetch(REYKJAVIK, VARIABLES))
        assert seen[0].extensions["timeout"]["read"] == 7

    def test_refuses_variables_it_cannot_deliver(self):
        with pytest.raises(ValueError, match="snow_depth"):
            run(OpenMeteoEnsemble(client=serving()).fetch(REYKJAVIK, {"snow_depth"}))


class TestOpenMeteoFailures:
    def test_timeout(self):
        client = serving(raises=httpx.ReadTimeout("slow"))
        with pytest.raises(SourceTimeout, match="did not answer within 30.0s"):
            run(OpenMeteoEnsemble(client=client).fetch(REYKJAVIK, VARIABLES))

    def test_unreachable(self):
        client = serving(raises=httpx.ConnectError("refused"))
        with pytest.raises(SourceError, match="could not be reached: ConnectError"):
            run(OpenMeteoEnsemble(client=client).fetch(REYKJAVIK, VARIABLES))

    def test_refusal_carries_open_meteos_reason(self):
        client = serving(content=b'{"error": true, "reason": "Latitude must be in range"}', status=400)
        with pytest.raises(SourceError, match="400: Latitude must be in range"):
            run(OpenMeteoEnsemble(client=client).fetch(REYKJAVIK, VARIABLES))

    def test_truncated_response(self):
        with pytest.raises(SourceError, match="truncated"):
            list(messages(RAW[:1000]))

    def test_missing_variable(self):
        with pytest.raises(SourceError, match="lacks snow"):
            decode(RAW, "test", VARIABLES | {"snow"})


@pytest.fixture(scope="module")
def result():
    return decode(RAW, "open-meteo:ecmwf_ifs025", VARIABLES, name="Reykjavik, Iceland")


@pytest.fixture(scope="module")
def both():
    """The recorded bytes through the legacy getData and through the new pipeline."""

    class Replay:
        def weather_api(self, url, params):
            return list(messages(RAW))

    original = downloadJsonData.openmeteo
    downloadJsonData.openmeteo = Replay()
    try:
        old = downloadJsonData.getData(-21.94, 64.15, 61, writeToFile=False)
    finally:
        downloadJsonData.openmeteo = original
    return old, build_forecast(decode(RAW, "t", VARIABLES))


class TestOpenMeteoDecoding:
    def test_members_and_rows(self, result):
        assert result.member_count == 51
        assert {v.shape for v in result.members.values()} == {(51, 72)}

    def test_time_and_place(self, result):
        assert result.start.tzinfo == timezone.utc
        assert result.location.timezone == "Atlantic/Reykjavik"
        assert result.location.name == "Reykjavik, Iceland"
        assert abs(result.location.lat - 64.15) < 0.2

    def test_builds_a_valid_forecast(self, result):
        f = build_forecast(result)
        # 72 hourly rows: 11 complete 6-hour totals, so 11 steps for everyone.
        assert len(f.steps) == 11
        assert f.variables["wind_speed_10m"].unit == "m/s"

    def test_a_reported_non_canonical_unit_is_converted(self, monkeypatch):
        """Were the response in km/h, the adapter converts rather than passing it on."""
        plain = decode(RAW, "t", VARIABLES).members["wind_speed_10m"]
        monkeypatch.setitem(open_meteo.SDK_UNITS, open_meteo.Unit.metre_per_second, "km/h")
        converted = decode(RAW, "t", VARIABLES).members["wind_speed_10m"]
        np.testing.assert_allclose(converted, plain / 3.6)

    def test_an_unknown_unit_is_refused(self, monkeypatch):
        monkeypatch.delitem(open_meteo.SDK_UNITS, open_meteo.Unit.metre_per_second)
        with pytest.raises(SourceError, match="wind_speed_10m arrived in metre_per_second, a unit"):
            decode(RAW, "t", VARIABLES)

    def test_an_unconvertible_unit_is_refused(self, monkeypatch):
        monkeypatch.setitem(open_meteo.SDK_UNITS, open_meteo.Unit.millimetre, "percent")
        with pytest.raises(SourceError, match="precipitation arrived in 'percent'"):
            decode(RAW, "t", VARIABLES)


class TestParityWithLegacyGetData:
    """The same recorded bytes through both pipelines."""

    def test_same_start(self, both):
        old, new = both
        assert new.steps[0] == legacy.reference_time(old["2t"])

    @pytest.mark.parametrize("key", ["2t", "tcc", "ws"])
    def test_instant_quantiles_agree(self, both, key):
        old, new = both
        name = legacy.VARIABLES[key]
        for legacy_name, quantile in legacy.QUANTILES.items():
            ours = new.variables[name].quantiles[quantile]
            theirs = old[key][key][legacy_name][: len(ours)]
            # The legacy code works in float32, this in float64.
            np.testing.assert_allclose(ours, theirs, rtol=1e-6, atol=1e-5, err_msg=f"{key} {legacy_name}")

    @pytest.mark.parametrize("legacy_name,quantile", list(legacy.QUANTILES.items()))
    def test_precipitation_windows_are_an_hour_later_than_legacy(self, both, legacy_name, quantile):
        """The one intended difference. Open-Meteo's hourly value is the
        preceding hour's total, so the window starting at t is rows t+1..t+6;
        the legacy code took rows t..t+5. Both checked against the raw members."""
        old, new = both
        members = decode(RAW, "t", VARIABLES).members["precipitation"]
        level = int(quantile[1:])
        ours = new.variables["precipitation"].quantiles[quantile]
        steps = range(len(ours))
        following = [np.percentile(members[:, 6 * k + 1 : 6 * k + 7].sum(axis=1), level) for k in steps]
        shifted = [np.percentile(members[:, 6 * k : 6 * k + 6].sum(axis=1), level) for k in steps]
        np.testing.assert_allclose(ours, following, rtol=1e-9)
        np.testing.assert_allclose(old["tp"]["tp"][legacy_name][: len(ours)], shifted, rtol=1e-5, atol=1e-5)

    def test_the_window_shift_is_visible_in_this_recording(self, both):
        """Guards the test above against a dry recording, where both would be 0."""
        old, new = both
        ours = np.array(new.variables["precipitation"].quantiles["p50"])
        theirs = np.array(old["tp"]["tp"]["median"][: len(ours)])
        assert np.abs(ours - theirs).max() > 0.2
