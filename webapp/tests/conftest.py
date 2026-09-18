"""Shared pytest setup.

plotMeteogram.py resolves './pictogram/...' and 'output/' against the process CWD
(and creates 'output/' at import time), so every test run has to happen from
webapp/ regardless of where pytest was invoked. Doing the chdir at conftest module
level means it is in effect before pytest imports any test module.
"""
import json
import os
import sys
from pathlib import Path

import pytest

WEBAPP_DIR = Path(__file__).resolve().parent.parent
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"

os.chdir(WEBAPP_DIR)
sys.path.insert(0, str(WEBAPP_DIR))

LOCATION_KEYS = ["braunschweig", "reykjavik", "singapore", "zermatt", "alice_springs"]


def load_fixture(key):
    with open(FIXTURE_DIR / f"{key}.json") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def locations():
    with open(FIXTURE_DIR / "locations.json") as fp:
        return json.load(fp)


@pytest.fixture(params=LOCATION_KEYS)
def meteogram_data(request):
    """Every offline fixture in turn, so contract tests run against all regimes."""
    return load_fixture(request.param)


@pytest.fixture
def braunschweig():
    """The project's default location - a single stable fixture for focused tests."""
    return load_fixture("braunschweig")
