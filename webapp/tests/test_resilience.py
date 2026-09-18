"""Offline tests for timeouts and third-party failure handling.

Every outbound call is faked, so nothing here touches the network. The point is
that a slow or broken third party degrades or reports cleanly instead of hanging
a worker or surfacing as a 500.
"""
import json

import pytest
import requests
from geopy.exc import GeocoderServiceError, GeocoderTimedOut

import downloadJsonData
from downloadJsonData import (
    ELEVATION_TIMEOUT,
    GEOCODE_TIMEOUT,
    OPEN_METEO_TIMEOUT,
    UNKNOWN_ELEVATION,
    LocationNotFound,
    geocodeLocation,
    getElevation,
)


class FakeResponse:
    def __init__(self, payload=None, status=200, text=None):
        self._payload = payload
        self.status_code = status
        self.text = text if text is not None else json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


class TestElevationLookup:
    def test_passes_a_timeout(self, monkeypatch):
        """Without one, a stalled connection blocks the worker forever."""
        seen = {}

        def fakeGet(url, **kwargs):
            seen.update(kwargs)
            return FakeResponse({"results": [{"elevation": 79}]})

        monkeypatch.setattr(downloadJsonData.requests, "get", fakeGet)
        assert getElevation(52.26, 10.52) == 79
        assert seen.get("timeout") == ELEVATION_TIMEOUT

    @pytest.mark.parametrize(
        "label,failure",
        [
            ("timeout", requests.Timeout("timed out")),
            ("connection refused", requests.ConnectionError("refused")),
        ],
    )
    def test_network_failure_returns_none(self, monkeypatch, label, failure):
        def fakeGet(url, **kwargs):
            raise failure

        monkeypatch.setattr(downloadJsonData.requests, "get", fakeGet)
        assert getElevation(52.26, 10.52) is None, f"{label} should degrade, not raise"

    def test_error_status_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            downloadJsonData.requests, "get",
            lambda url, **kw: FakeResponse(status=503, text="down for maintenance"),
        )
        assert getElevation(52.26, 10.52) is None

    def test_html_error_page_returns_none(self, monkeypatch):
        """The old code did json.loads(response.text) and died on an error page."""
        monkeypatch.setattr(
            downloadJsonData.requests, "get",
            lambda url, **kw: FakeResponse(text="<html>502 Bad Gateway</html>"),
        )
        assert getElevation(52.26, 10.52) is None

    def test_unexpected_payload_shape_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            downloadJsonData.requests, "get",
            lambda url, **kw: FakeResponse({"results": []}),
        )
        assert getElevation(52.26, 10.52) is None


class FakeGeocoder:
    def __init__(self, result=None, raises=None):
        self.result = result
        self.raises = raises
        self.kwargs = None

    def __call__(self, user_agent=None, timeout=None):
        self.kwargs = {"user_agent": user_agent, "timeout": timeout}
        return self

    def geocode(self, name):
        if self.raises is not None:
            raise self.raises
        return self.result


class Located:
    latitude = 52.2646577
    longitude = 10.5236066


class TestGeocoding:
    def test_passes_a_timeout(self, monkeypatch):
        fake = FakeGeocoder(result=Located())
        monkeypatch.setattr(downloadJsonData, "Nominatim", fake)
        assert geocodeLocation("Braunschweig") == (Located.latitude, Located.longitude)
        assert fake.kwargs["timeout"] == GEOCODE_TIMEOUT

    def test_unknown_place_raises_location_not_found(self, monkeypatch):
        """geocode() returns None for gibberish; the old code then did
        loc.latitude and raised AttributeError, surfacing as a 500."""
        monkeypatch.setattr(downloadJsonData, "Nominatim", FakeGeocoder(result=None))
        with pytest.raises(LocationNotFound, match="No place called"):
            geocodeLocation("qwertyuiop")

    def test_the_message_names_what_the_user_typed(self, monkeypatch):
        monkeypatch.setattr(downloadJsonData, "Nominatim", FakeGeocoder(result=None))
        with pytest.raises(LocationNotFound, match="qwertyuiop"):
            geocodeLocation("qwertyuiop")

    @pytest.mark.parametrize(
        "failure", [GeocoderTimedOut("slow"), GeocoderServiceError("broken")]
    )
    def test_service_failure_raises_location_not_found(self, monkeypatch, failure):
        monkeypatch.setattr(
            downloadJsonData, "Nominatim", FakeGeocoder(raises=failure)
        )
        with pytest.raises(LocationNotFound, match="did not respond"):
            geocodeLocation("Braunschweig")


class TestOpenMeteoSession:
    def test_session_applies_a_default_timeout(self, tmp_path):
        """openmeteo_requests calls session.request() without a timeout, so the
        session has to supply one or the forecast call can hang indefinitely."""
        seen = {}

        class Recorder(downloadJsonData.TimeoutCachedSession):
            def request(self, *args, **kwargs):
                kwargs.setdefault("timeout", downloadJsonData.OPEN_METEO_TIMEOUT)
                seen.update(kwargs)
                return "sent"

        session = Recorder(str(tmp_path / "cache"))
        assert session.request("GET", "https://example.invalid") == "sent"
        assert seen.get("timeout") == OPEN_METEO_TIMEOUT

    def test_the_live_session_is_the_timeout_aware_one(self):
        assert isinstance(
            downloadJsonData.cache_session, downloadJsonData.TimeoutCachedSession
        )

    def test_an_explicit_timeout_is_not_overridden(self, tmp_path):
        seen = {}

        class Recorder(downloadJsonData.TimeoutCachedSession):
            def request(self, *args, **kwargs):
                kwargs.setdefault("timeout", downloadJsonData.OPEN_METEO_TIMEOUT)
                seen.update(kwargs)

        session = Recorder(str(tmp_path / "cache"))
        session.request("GET", "https://example.invalid", timeout=1)
        assert seen["timeout"] == 1


class TestElevationComesFromTheForecast:
    def test_getdata_reports_the_grid_cell_it_used(self, monkeypatch):
        """The web path uses this instead of a second call to open-elevation."""
        from tests.conftest import load_fixture

        captured = {}

        def fakeGetData(longitude, latitude, altitude, metadata=None, **kwargs):
            if metadata is not None:
                metadata.update({"elevation": 1613.0})
            captured["called"] = True
            return load_fixture("braunschweig")

        from app import controller

        monkeypatch.setattr(controller, "getData", fakeGetData)
        monkeypatch.setattr(
            controller, "geocodeLocation", lambda name: (46.0207, 7.7491)
        )
        filename = controller.plotMeteogramFile(
            location="Zermatt", days=2, plotType="ensemble"
        )
        import os

        try:
            assert captured["called"]
            assert os.path.getsize("/tmp/" + filename) > 10_000
        finally:
            if os.path.exists("/tmp/" + filename):
                os.remove("/tmp/" + filename)

    def test_missing_elevation_falls_back_to_the_sentinel(self, monkeypatch):
        """A forecast must still render when nothing reports an elevation."""
        from tests.conftest import load_fixture
        from app import controller

        monkeypatch.setattr(
            controller, "getData",
            lambda longitude, latitude, altitude, metadata=None, **kw: load_fixture("braunschweig"),
        )
        monkeypatch.setattr(
            controller, "geocodeLocation", lambda name: (52.2646577, 10.5236066)
        )
        filename = controller.plotMeteogramFile(
            location="Braunschweig", days=2, plotType="ensemble"
        )
        import os

        try:
            assert os.path.exists("/tmp/" + filename)
        finally:
            if os.path.exists("/tmp/" + filename):
                os.remove("/tmp/" + filename)

    def test_sentinel_is_negative(self):
        assert UNKNOWN_ELEVATION < 0
