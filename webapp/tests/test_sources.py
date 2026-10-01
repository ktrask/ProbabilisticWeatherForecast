"""Offline tests for the source adapters.

The Open-Meteo adapter is fed a recorded response (tests/fixtures/open_meteo/,
written by generate_fixtures.py) through httpx.MockTransport, so the real
flatbuffers decoding runs without a network, and its reduction is checked
against numpy on the raw members.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx
import numpy as np
import pytest

from core import legacy
from core.model import Location
from core.pipeline import build_forecast
from sources import open_meteo
from sources.base import Area, NotCovered, SourceError, SourceTimeout
from sources.fixture import FixtureNotFound, FixtureSource, is_forecast
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


def recorded(key):
    """What a fixture file says, read straight from its JSON in either format:
    (first step, {variable: {quantile: [values]}})."""
    data = load_fixture(key)
    if is_forecast(data):
        first = datetime.fromisoformat(data["steps"][0])
        return first, {name: series["quantiles"] for name, series in data["variables"].items()}
    first = legacy.reference_time(data["2t"])
    return first, {
        name: {q: data[k][k][legacy_name] for legacy_name, q in legacy.QUANTILES.items()}
        for k, name in legacy.VARIABLES.items()
    }


class TestFixtureSource:
    @pytest.mark.parametrize("key", LOCATION_KEYS)
    def test_every_fixture_becomes_a_valid_forecast(self, key):
        f = build_forecast(FixtureSource().load(key))
        _, quantiles = recorded(key)
        assert len(f.steps) == len(quantiles["temperature_2m"]["p50"])
        assert f.step_hours == 6
        assert set(f.variables) == set(legacy.VARIABLES.values())

    @pytest.mark.parametrize("key", LOCATION_KEYS)
    def test_values_come_through_unchanged(self, key):
        f = build_forecast(FixtureSource().load(key))
        _, quantiles = recorded(key)
        for name, by_quantile in quantiles.items():
            for quantile, values in by_quantile.items():
                assert f.variables[name].quantiles[quantile] == values, f"{key} {name} {quantile}"

    @pytest.mark.parametrize("key", LOCATION_KEYS)
    def test_first_step_is_the_recorded_one(self, key):
        f = build_forecast(FixtureSource().load(key))
        assert f.steps[0] == recorded(key)[0]
        assert f.steps[0].tzinfo == timezone.utc

    def test_still_reads_the_legacy_format(self, tmp_path):
        """tests/fixtures/legacy/ keeps one file recorded by the removed
        getData(), so the reader for that format stays tested."""
        sample = FIXTURE_DIR / "legacy" / "braunschweig.json"
        data = json.loads(sample.read_text())
        assert not is_forecast(data)
        (tmp_path / "braunschweig.json").write_text(sample.read_text())
        (tmp_path / "locations.json").write_text(json.dumps(
            {"braunschweig": json.loads((FIXTURE_DIR / "locations.json").read_text())["braunschweig"]}
        ))
        source = FixtureSource(tmp_path)
        # The legacy format has seven fixed quantiles - not the 17th and 83rd.
        assert source.quantile_levels == (0, 10, 25, 50, 75, 90, 100)
        f = build_forecast(source.load("braunschweig"), quantile_levels=source.quantile_levels)
        assert f.steps[0] == legacy.reference_time(data["2t"])
        assert f.variables["precipitation"].quantiles["p90"] == data["tp"]["tp"]["ninety"]
        assert f.variables["wind_speed_10m"].quantiles["p50"] == data["ws"]["ws"]["median"]

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

    def test_an_area_refuses_like_the_live_source(self):
        alps = FixtureSource(area=Area(42.58, 49.79, 1.23, 16.85))
        with pytest.raises(NotCovered):
            run(alps.fetch(Location(lat=52.3, lon=10.6), {"precipitation"}))
        assert run(alps.fetch(Location(lat=46.0, lon=7.7), {"precipitation"})).source == "fixture:zermatt"

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

    def test_a_place_outside_a_regional_grid_is_not_covered(self):
        """Open-Meteo's answer for a point its regional model has no grid for."""
        reason = b'{"error": true, "reason": "No data is available for this location"}'
        with pytest.raises(NotCovered, match="does not cover 64.1466, -21.9426"):
            run(OpenMeteoEnsemble("icon_d2_eps", client=serving(content=reason, status=400)).fetch(REYKJAVIK, VARIABLES))

    def test_outside_the_area_nothing_is_asked(self):
        seen = []
        d2 = OpenMeteoEnsemble("icon_d2_eps", area=Area(43.18, 58.06, -3.94, 20.32), client=serving(seen=seen))
        with pytest.raises(NotCovered):
            run(d2.fetch(REYKJAVIK, VARIABLES))
        assert seen == []

    def test_a_health_check_asks_inside_the_area(self):
        d2 = OpenMeteoEnsemble("icon_d2_eps", area=Area(43.18, 58.06, -3.94, 20.32))
        inside = Location(lat=52.26, lon=10.52)
        assert d2.probe(inside) == inside
        assert (d2.probe(REYKJAVIK).lat, d2.probe(REYKJAVIK).lon) == pytest.approx((50.62, 8.19))

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
def reduced(result):
    return build_forecast(result)


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


class TestReductionOfTheRecording:
    """The pipeline on real bytes, checked against numpy on the raw members.

    Until the legacy app was removed, these bytes also went through its
    getData(): instant quantiles agreed to float32 precision, and its
    precipitation windows were an hour early (rows t..t+5 instead of t+1..t+6).
    """

    @pytest.mark.parametrize("name", ["temperature_2m", "cloud_cover", "wind_speed_10m"])
    def test_instants_are_the_members_at_each_step(self, result, reduced, name):
        members = result.members[name]
        for quantile, values in reduced.variables[name].quantiles.items():
            level = int(quantile[1:])
            expected = [np.percentile(members[:, 6 * k], level) for k in range(len(values))]
            np.testing.assert_allclose(values, expected, rtol=1e-12, err_msg=f"{name} {quantile}")

    @pytest.mark.parametrize("quantile", list(legacy.QUANTILES.values()))
    def test_totals_cover_the_six_hours_after_the_step(self, result, reduced, quantile):
        """Open-Meteo's hourly value is the preceding hour's total, so the
        window starting at t is made of the rows t+1 ... t+6."""
        members = result.members["precipitation"]
        level = int(quantile[1:])
        ours = reduced.variables["precipitation"].quantiles[quantile]
        following = [np.percentile(members[:, 6 * k + 1 : 6 * k + 7].sum(axis=1), level) for k in range(len(ours))]
        np.testing.assert_allclose(ours, following, rtol=1e-9)

    def test_this_recording_has_rain(self, reduced):
        """Guards the test above against a dry recording, where every total is 0."""
        assert max(reduced.variables["precipitation"].quantiles["p50"]) > 1.0
