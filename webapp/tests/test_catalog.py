"""config/sources.yaml: products are checked against the VSUP schemes at load.

Everything here would otherwise fail on some later request; the point is that
it fails at start-up, with the line to fix.
"""
import json

import pytest

from core.configfile import ConfigError
from sources import __main__ as cli
from sources.config import DEFAULT_SOURCES, json_schema, load
from sources.fixture import FixtureSource
from sources.open_meteo import OpenMeteoEnsemble
from tests.conftest import FIXTURE_DIR
from vsup import config as vsup

BASE = f"""\
version: 1
sources:
  recorded:
    adapter: fixture
    directory: {FIXTURE_DIR}
  live:
    adapter: open_meteo_ensemble
    model: ecmwf_ifs025
products:
  main:
    label: Main
    ensemble: recorded
    schemes: [cloud-vsup, precipitation-vsup, wind-vsup]
"""
ENSEMBLE_LINE = 12
SCHEMES_LINE = 13


@pytest.fixture(scope="module")
def schemes():
    return vsup.load()


def write(tmp_path, text, name="sources.yaml"):
    path = tmp_path / name
    path.write_text(text)
    return path


def broken(tmp_path, schemes, old, new, text=BASE):
    assert old in text, f"{old!r} is not in the config"
    with pytest.raises(ConfigError) as info:
        load(write(tmp_path, text.replace(old, new, 1)), schemes)
    return info.value.issues


def one(issues, fragment):
    matching = [i for i in issues if fragment in i.message]
    assert len(matching) == 1, f"expected one issue about {fragment!r}, got {issues}"
    return matching[0]


class TestShippedConfigs:
    def test_sources_yaml(self, schemes):
        catalog = load(DEFAULT_SOURCES, schemes)
        assert list(catalog.products) == ["ecmwf"]
        assert isinstance(catalog.default.source, OpenMeteoEnsemble)
        assert catalog.default.variants == ("ensemble",)

    def test_offline_twin_offers_the_same_products(self, schemes):
        live = load(DEFAULT_SOURCES, schemes)
        offline = load(DEFAULT_SOURCES.parent / "sources.fixtures.yaml", schemes)
        assert list(offline.products) == list(live.products)
        assert isinstance(offline.default.source, FixtureSource)
        for key in live.products:
            assert offline.products[key].schemes == live.products[key].schemes

    def test_json_schema_is_up_to_date(self):
        """Regenerate with `python -m sources schema -o config/sources.schema.json`."""
        checked_in = json.loads((DEFAULT_SOURCES.parent / "sources.schema.json").read_text())
        assert checked_in == json_schema()

    def test_cli(self, tmp_path, capsys):
        assert cli.main(["check"]) == 0
        assert "ecmwf: ECMWF ensemble <- ecmwf-ens (open-meteo:ecmwf_ifs025)  (default)" in capsys.readouterr().out
        bad = write(tmp_path, BASE.replace("ensemble: recorded", "ensemble: nowhere"))
        assert cli.main(["check", str(bad)]) == 1
        assert f"sources.yaml:{ENSEMBLE_LINE}: products.main.ensemble: no source 'nowhere'" in capsys.readouterr().err


class TestProducts:
    def test_base(self, tmp_path, schemes):
        catalog = load(write(tmp_path, BASE), schemes)
        assert catalog.default.id == "main"
        assert catalog.default.variables == FixtureSource.variables

    def test_unknown_source(self, tmp_path, schemes):
        one(broken(tmp_path, schemes, "ensemble: recorded", "ensemble: nowhere"), "no source 'nowhere'")

    def test_unknown_scheme(self, tmp_path, schemes):
        issue = one(broken(tmp_path, schemes, "wind-vsup]", "wind-vsupp]"), "no scheme 'wind-vsupp' in vsup.yaml")
        assert issue.line == SCHEMES_LINE
        assert issue.where == "products.main.schemes[2]"

    def test_two_schemes_for_one_variable(self, tmp_path, schemes):
        one(broken(tmp_path, schemes, "wind-vsup]", "wind-vsup, wind-legacy]"), "both draw wind_speed_10m")

    def test_scheme_for_a_variable_that_is_not_fetched(self, tmp_path, schemes):
        issues = broken(tmp_path, schemes, "    ensemble: recorded\n",
                        "    ensemble: recorded\n    variables: [cloud_cover, precipitation]\n")
        one(issues, "wind-vsup draws wind_speed_10m, which this product does not fetch")

    def test_unknown_variable(self, tmp_path, schemes):
        one(broken(tmp_path, schemes, "    ensemble: recorded\n",
                   "    ensemble: recorded\n    variables: [snow_depth]\n"), "Input should be")

    def test_window_must_match_the_step(self, tmp_path, schemes):
        text = vsup.DEFAULT_CONFIG.read_text().replace("window_hours: 6", "window_hours: 12")
        text = text.replace("pictogram_root: ../pictograms", f"pictogram_root: {schemes.pictogram_root}")
        twelve = vsup.load(write(tmp_path, text, "vsup.yaml"))
        issues = broken(tmp_path, twelve, "", "")
        one(issues, "precipitation-vsup is written for 12-hour totals, but forecasts are built in 6-hour steps")

    def test_deterministic_schemes_need_a_deterministic_source(self, tmp_path, schemes):
        text = vsup.DEFAULT_CONFIG.read_text().replace('when: "p90 < 3"', 'when: "deterministic < 3"')
        text = text.replace("pictogram_root: ../pictograms", f"pictogram_root: {schemes.pictogram_root}")
        needs_hres = vsup.load(write(tmp_path, text, "vsup.yaml"))
        issues = broken(tmp_path, needs_hres, "wind-vsup]", "wind-legacy]")
        one(issues, "wind-legacy reads the deterministic run")


class TestSources:
    def test_recorded_quantiles_must_cover_the_computed_ones(self, tmp_path, schemes):
        text = vsup.DEFAULT_CONFIG.read_text().replace("83, 90, 100]", "83, 90, 95, 100]")
        text = text.replace("pictogram_root: ../pictograms", f"pictogram_root: {schemes.pictogram_root}")
        more = vsup.load(write(tmp_path, text, "vsup.yaml"))
        issue = one(broken(tmp_path, more, "", ""), "computes p95 too")
        assert issue.where == "sources.recorded"

    def test_missing_fixture_directory(self, tmp_path, schemes):
        one(broken(tmp_path, schemes, f"directory: {FIXTURE_DIR}", "directory: nowhere"),
            "cannot read recorded forecasts")

    def test_unknown_adapter(self, tmp_path, schemes):
        issue = one(broken(tmp_path, schemes, "adapter: open_meteo_ensemble", "adapter: carrier_pigeon"),
                    "Input tag 'carrier_pigeon'")
        assert issue.where == "sources.live.adapter"

    def test_adapter_options_are_checked(self, tmp_path, schemes):
        issue = one(broken(tmp_path, schemes, "    model: ecmwf_ifs025\n",
                           "    model: ecmwf_ifs025\n    forecast_days: 0\n"), "greater than or equal to 1")
        assert issue.where == "sources.live.forecast_days"

    def test_misspelt_option(self, tmp_path, schemes):
        one(broken(tmp_path, schemes, "    model: ecmwf_ifs025\n", "    modle: ecmwf_ifs025\n"),
            "Extra inputs are not permitted")
