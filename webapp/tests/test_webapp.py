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

    def fakeGetData(longitude, latitude, altitude, metadata=None, **kwargs):
        #Mirror what the real call reports back about the grid cell it used.
        if metadata is not None:
            metadata.update(
                {"elevation": 79.0, "latitude": latitude, "longitude": longitude}
            )
        return load_fixture("braunschweig")

    monkeypatch.setattr(controller, "getData", fakeGetData)
    monkeypatch.setattr(
        controller, "geocodeLocation", lambda name: (52.2646577, 10.5236066)
    )
    return controller


VALID_QUERY = {
    "search": "",
    "lat": "52.2646577",
    "lon": "10.5236066",
    "days": "3",
    "plotType": "ensemble",
}


def query(**overrides):
    """VALID_QUERY with overrides applied; a None value drops the parameter."""
    q = dict(VALID_QUERY)
    q.update(overrides)
    return {k: v for k, v in q.items() if v is not None}


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


class TestInputValidation:
    """Bad query strings must be 400s with the form redisplayed, not 500s.

    The form is submitted with method="get", so validate_on_submit() is always
    False. The view binds the form to request.args instead, which is what makes
    the field validators run at all.
    """

    def test_valid_request_still_renders(self, client, offline_controller):
        response = client.get("/search", query_string=query())
        assert response.status_code == 200
        assert "data:image/png;base64," in response.get_data(as_text=True)

    @pytest.mark.parametrize(
        "label,overrides",
        [
            ("unknown plot type", {"plotType": "evil"}),
            ("non-numeric days", {"days": "abc"}),
            ("latitude out of range", {"lat": "999"}),
            ("longitude out of range", {"lon": "-200"}),
            ("non-numeric latitude", {"lat": "north"}),
            ("no location given", {"search": None, "lat": None, "lon": None}),
            ("no parameters at all", dict.fromkeys(VALID_QUERY)),
        ],
    )
    def test_bad_input_is_a_400(self, client, offline_controller, label, overrides):
        response = client.get("/search", query_string=query(**overrides))
        assert response.status_code == 400, f"{label} should be rejected"
        assert "data:image/png;base64," not in response.get_data(as_text=True)

    def test_an_unknown_plot_type_says_why(self, client, offline_controller):
        """quick_form does not render RadioField errors, so the view surfaces
        them as a form-level alert instead of a silently blank rejection."""
        response = client.get("/search", query_string=query(plotType="evil"))
        assert "Not a valid choice" in response.get_data(as_text=True)

    def test_an_out_of_range_latitude_says_why(self, client, offline_controller):
        response = client.get("/search", query_string=query(lat="999"))
        assert "Number must be between -90 and 90" in response.get_data(as_text=True)

    def test_a_missing_location_says_why(self, client, offline_controller):
        response = client.get(
            "/search", query_string=query(search=None, lat=None, lon=None)
        )
        assert "Enter a place name" in response.get_data(as_text=True)

    def test_a_rejected_request_redisplays_the_form(self, client, offline_controller):
        """A 400 has to be usable, not a dead end - the user needs the form back."""
        response = client.get("/search", query_string=query(plotType="evil"))
        body = response.get_data(as_text=True)
        for field in ("search", "lat", "lon", "days", "plotType"):
            assert f'name="{field}"' in body, f"missing form field {field}"

    def test_missing_optional_parameters_use_defaults(self, client, offline_controller):
        """Absent is not the same as invalid: no days given means the default."""
        response = client.get("/search", query_string=query(days=None))
        assert response.status_code == 200

    def test_place_name_alone_is_enough(self, client, offline_controller):
        response = client.get(
            "/search", query_string=query(search="Braunschweig", lat=None, lon=None)
        )
        assert response.status_code == 200

    @pytest.mark.parametrize("days", ["0", "-5", "99", "365"])
    def test_out_of_range_days_is_clamped_not_rejected(
        self, client, offline_controller, days
    ):
        """The intent is unambiguous, we just run out of forecast."""
        response = client.get("/search", query_string=query(days=days))
        assert response.status_code == 200
        assert "data:image/png;base64," in response.get_data(as_text=True)

    def test_form_choices_match_the_renderer(self):
        """The radio buttons and plotMeteogram's whitelist must not drift apart."""
        from meteogram.plotMeteogram import PLOT_TYPES
        from app.views import searchForm

        assert tuple(value for value, _ in searchForm.plotType.kwargs["choices"]) == PLOT_TYPES


class TestUnavailableData:
    def test_enhanced_hres_is_a_503_with_an_explanation(self, client, offline_controller):
        """A valid choice the form offers that this data source cannot serve.
        The request is fine, so it is not a 400, and it must not be a traceback."""
        response = client.get("/search", query_string=query(plotType="enhanced-hres"))
        assert response.status_code == 503
        body = response.get_data(as_text=True)
        assert "ensemble percentiles only" in body, "the reason should be shown"


class TestUnresolvableLocation:
    def test_an_unknown_place_name_is_a_400_naming_it(self, client, monkeypatch):
        """geocode() returning None used to surface as AttributeError -> 500."""
        from app import controller
        from meteogram.downloadJsonData import LocationNotFound

        def cannotResolve(name):
            raise LocationNotFound(f"No place called {name!r} was found.")

        monkeypatch.setattr(controller, "geocodeLocation", cannotResolve)
        response = client.get("/search", query_string=query(search="qwertyuiop"))
        assert response.status_code == 400
        body = response.get_data(as_text=True)
        assert "qwertyuiop" in body, "the message should name what was typed"
        assert "data:image/png;base64," not in body
