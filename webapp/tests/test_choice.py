"""The automatic choice (sources/choice.py): the finest model that covers the
place and reaches the days, the default last. On the shipped sources.yaml,
so the table below is also what the reader gets."""
import dataclasses

import pytest

from sources import choice
from sources.config import DEFAULT_SOURCES, load
from vsup import config as vsup


@pytest.fixture(scope="module")
def catalog():
    return load(DEFAULT_SOURCES, vsup.load())


BRAUNSCHWEIG = (52.26, 10.52)
ZERMATT = (46.02, 7.75)
SINGAPORE = (1.35, 103.82)
REYKJAVIK = (64.15, -21.94)
# Inside MeteoSwiss's box, outside its rotated grid (Open-Meteo: "No data").
ADRIATIC = (42.7, 16.7)


@pytest.mark.parametrize("place, days, first", [
    (BRAUNSCHWEIG, 1, "icon-d2"),  # 2 km, hourly
    (BRAUNSCHWEIG, 2, "icon-d2"),
    (BRAUNSCHWEIG, 3, "icon-eu"),
    (BRAUNSCHWEIG, 5, "icon-eu"),
    (BRAUNSCHWEIG, 6, "ecmwf"),  # ICON-EU stops at 5; ICON reaches 7.5, but ECMWF's grid is finer
    (BRAUNSCHWEIG, None, "ecmwf"),  # as far as the default reaches
    (ZERMATT, 2, "icon-d2"),  # as fine as MeteoSwiss, and first in the file
    (ZERMATT, 4, "meteoswiss"),
    (ZERMATT, 5, "meteoswiss"),
    (ZERMATT, 6, "ecmwf"),
    (REYKJAVIK, 3, "icon-eu"),
    (SINGAPORE, 1, "ecmwf"),
    (SINGAPORE, 7, "ecmwf"),
])
def test_the_finest_model_for_place_and_days(catalog, place, days, first):
    assert choice.ranked(catalog, *place, days)[0].id == first


def test_the_list_ends_with_the_default(catalog):
    assert [p.id for p in choice.ranked(catalog, *ZERMATT, 2)] == ["icon-d2", "meteoswiss", "icon-eu", "ecmwf"]
    assert [p.id for p in choice.ranked(catalog, *ZERMATT, 3)] == ["meteoswiss", "icon-eu", "ecmwf"]
    assert [p.id for p in choice.ranked(catalog, *SINGAPORE, 3)] == ["ecmwf"]


def test_a_product_not_marked_automatic_is_never_chosen(catalog):
    manual = dataclasses.replace(catalog.products["icon-d2"], automatic=False)
    without = dataclasses.replace(catalog, products={**catalog.products, "icon-d2": manual})
    for days in (1, 2):
        assert "icon-d2" not in [p.id for p in choice.ranked(without, *BRAUNSCHWEIG, days)]


def test_at_the_edge_of_a_rotated_grid_there_is_a_next_one(catalog):
    """The box holds the point, the grid may not: the API tries the next."""
    assert [p.id for p in choice.ranked(catalog, *ADRIATIC, 3)] == ["meteoswiss", "icon-eu", "ecmwf"]


def test_the_models_chosen_by_hand_never_come_automatically(catalog):
    """GFS, AI-GEFS and GEM reach 16 days, one more than ECMWF: they would take long views over."""
    for place in (BRAUNSCHWEIG, SINGAPORE, ZERMATT):
        for days in (1, 5, 15, 16):
            assert not {"aifs", "gfs", "aigefs", "gem"} & {p.id for p in choice.ranked(catalog, *place, days)}
