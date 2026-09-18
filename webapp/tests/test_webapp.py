"""Offline Flask tests: the view -> controller -> plot -> base64 path.

Network calls in the controller are monkeypatched to fixture data, so these run
without touching Open-Meteo or Nominatim.
"""
import pytest

from tests.conftest import load_fixture


@pytest.fixture
def client():
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client


@pytest.fixture
def offline_controller(monkeypatch):
    """Serve the Braunschweig fixture instead of calling the API."""
    from app import controller

    monkeypatch.setattr(
        controller, "getData", lambda *a, **kw: load_fixture("braunschweig")
    )
    monkeypatch.setattr(controller, "getElevation", lambda lat, lon: 79)
    return controller


def test_index_renders_the_search_form(client):
    response = client.get("/")
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "VSUP - Meteogram" in body
    for field in ("search", "lat", "lon", "days", "plotType"):
        assert f'name="{field}"' in body, f"missing form field {field}"


def test_search_returns_an_inline_png(client, offline_controller):
    response = client.get(
        "/search",
        query_string={
            "search": "",
            "lat": "52.2646577",
            "lon": "10.5236066",
            "days": "3",
            "plotType": "ensemble",
        },
    )
    assert response.status_code == 200
    body = response.get_data(as_text=True)
    assert "data:image/png;base64," in body, "meteogram image was not embedded"


def test_search_cleans_up_its_temporary_file(client, offline_controller, monkeypatch):
    """views.search writes the PNG to /tmp, inlines it, then removes it."""
    import os

    removed = []
    real_remove = os.remove
    monkeypatch.setattr(
        os, "remove", lambda path: (removed.append(path), real_remove(path))[1]
    )
    client.get(
        "/search",
        query_string={
            "search": "",
            "lat": "52.2646577",
            "lon": "10.5236066",
            "days": "3",
            "plotType": "ensemble",
        },
    )
    assert removed, "temporary meteogram file was never deleted"
    assert not os.path.exists(removed[0])


def test_controller_returns_a_filename(offline_controller):
    filename = offline_controller.plotMeteogramFile(
        latitude=52.2646577,
        longitude=10.5236066,
        location="",
        days=3,
        plotType="ensemble",
    )
    import os

    try:
        assert filename.endswith("forecast.png")
        assert os.path.getsize("/tmp/" + filename) > 10_000
    finally:
        if os.path.exists("/tmp/" + filename):
            os.remove("/tmp/" + filename)
