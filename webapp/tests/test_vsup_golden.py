"""Golden tests: the *-legacy schemes in config/vsup.yaml choose exactly what the
old getVSUP*Coordinate() functions chose - kept, frozen, in tests/legacy_reference.py
since the old app was removed.

Checked two ways. First, every step of every fixture, through the whole new
path (fixture source -> pipeline -> classifier). Second, exhaustively: the
legacy functions do nothing but compare quantiles with constants, so their
answer depends only on where each quantile lies relative to each constant -
below, on, or above. A grid of values just below, on and just above every
constant, plus one beyond either end, contains every such arrangement. Agreeing
on all ordered combinations from that grid therefore means agreeing on every
possible input, not just on the weather the fixtures happen to hold.

The constants are read from the legacy functions' own source, so a typo in the
YAML (1.6 for 1.5, > for >=) cannot hide by also being in the test.

The fixtures are read through the fixture source, so this works on the
recorded legacy-format files and on ones regenerated in the new format alike.
"""
import ast
import inspect
from itertools import combinations_with_replacement

import pytest

from core import legacy
from core.pipeline import build_forecast
from sources.fixture import FixtureSource
from tests.conftest import LOCATION_KEYS
from tests.legacy_reference import (
    LEVEL as LEGACY_LEVEL,
    VSUP_FILES,
    getVSUPCloudCoordinate,
    getVSUPrainCoordinate,
    getVSUPWindCoordinate,
)
from vsup import expr
from vsup.classify import classify_series
from vsup.config import load

# scheme -> (legacy function, pictogram directory)
LEGACY = {
    "cloud-legacy": (getVSUPCloudCoordinate, "cloud"),
    "precipitation-legacy": (getVSUPrainCoordinate, "rain"),
    "wind-legacy": (getVSUPWindCoordinate, "wind"),
}

# The quantiles the legacy functions read; p0 and p100 are never consulted.
READ = ("ten", "twenty_five", "median", "seventy_five", "ninety")

EPSILON = 1e-6


@pytest.fixture(scope="module")
def config():
    return load()


def legacy_pictogram(scheme, percentiles):
    function, directory = LEGACY[scheme]
    index = function(percentiles)
    return f"{directory}/{VSUP_FILES[directory][index]}", LEGACY_LEVEL[index]


def constants_in_source(function):
    """Every number a function compares against."""
    tree = ast.parse(inspect.getsource(function))
    return {
        operand.value
        for compare in ast.walk(tree)
        if isinstance(compare, ast.Compare)
        for operand in [compare.left, *compare.comparators]
        if isinstance(operand, ast.Constant) and isinstance(operand.value, (int, float))
    }


def constants_in_scheme(scheme):
    found = set()

    def visit(node):
        if isinstance(node, expr.Num):
            found.add(node.value)
        elif isinstance(node, expr.Compare):
            visit(node.left)
            visit(node.right)
        elif isinstance(node, (expr.And, expr.Or)):
            for term in node.terms:
                visit(term)
        elif isinstance(node, expr.Not):
            visit(node.term)

    for rule in scheme.rules:
        if rule.condition is not None:
            visit(rule.condition)
    return found


def test_the_thresholds_are_the_ones_expected():
    """A sanity check on constants_in_source, which the grid is built from."""
    assert constants_in_source(getVSUPCloudCoordinate) == {10, 30, 50, 70, 90}
    assert constants_in_source(getVSUPrainCoordinate) == {0.1, 1, 1.5, 2}
    assert constants_in_source(getVSUPWindCoordinate) == {3, 10, 17.2}


@pytest.mark.parametrize("scheme", sorted(LEGACY))
def test_scheme_uses_the_legacy_thresholds(config, scheme):
    function = LEGACY[scheme][0]
    assert constants_in_scheme(config.scheme(scheme)) == constants_in_source(function)


@pytest.mark.parametrize("scheme", sorted(LEGACY))
@pytest.mark.parametrize("key", LOCATION_KEYS)
def test_same_pictogram_for_every_fixture_step(config, key, scheme):
    compiled = config.scheme(scheme)
    series = build_forecast(FixtureSource().load(key)).variables[compiled.variable]
    choices = classify_series(compiled, series)
    for i, choice in enumerate(choices):
        percentiles = legacy.percentiles(series.at(i))
        expected, level = legacy_pictogram(scheme, percentiles)
        assert (choice.pictogram, choice.level) == (expected, level), (
            f"{key} step {i} ({percentiles}): {choice.outcome} gives {choice.pictogram}, "
            f"legacy gives {expected}"
        )


def grid(thresholds):
    values = {0.0, 2 * max(thresholds)}
    for t in thresholds:
        values |= {t - EPSILON, t, t + EPSILON}
    return sorted(values)


@pytest.mark.parametrize("scheme", sorted(LEGACY))
def test_same_pictogram_for_every_arrangement_around_the_thresholds(config, scheme):
    compiled = config.scheme(scheme)
    thresholds = constants_in_source(LEGACY[scheme][0]) | constants_in_scheme(compiled)
    checked = 0
    for values in combinations_with_replacement(grid(thresholds), len(READ)):
        percentiles = dict(zip(READ, values))
        percentiles["min"], percentiles["max"] = values[0], values[-1]
        expected, level = legacy_pictogram(scheme, percentiles)
        choice = compiled.classify({legacy.QUANTILES[k]: v for k, v in percentiles.items()})
        assert (choice.pictogram, choice.level) == (expected, level), (
            f"{percentiles}: {choice.outcome} gives {choice.pictogram}, legacy gives {expected}"
        )
        checked += 1
    assert checked > 1000
