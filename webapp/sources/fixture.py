"""Serves the recorded forecasts in tests/fixtures/ as if they were a live source.

For tests, offline development and demos - the successor of replaying
allmeteogramdata.json. The fixtures are verbatim legacy getData() output, so
this is a quantile source: the 51 members were reduced when the fixture was
recorded, and precipitation is already totalled per 6-hour step, with the
legacy code's window (an hour early, see core.reduce).
"""
import json
from datetime import timedelta
from pathlib import Path

from core import legacy
from core.model import Location, quantile_level
from core.pipeline import SourceResult
from sources.base import NoData

DEFAULT_DIRECTORY = Path(__file__).resolve().parent.parent / "tests" / "fixtures"

# How far a requested location may be from a fixture's and still get it.
MAX_DISTANCE_DEG = 0.5


class FixtureNotFound(NoData):
    pass


class FixtureSource:
    id = "fixture"
    kind = "quantiles"
    variables = frozenset(legacy.VARIABLES.values())
    native_step = timedelta(hours=6)
    max_lead = timedelta(days=14)
    quantile_levels = tuple(quantile_level(name) for name in legacy.QUANTILES.values())

    def __init__(self, directory=DEFAULT_DIRECTORY):
        self.directory = Path(directory)
        with open(self.directory / "locations.json") as fp:
            self._locations = json.load(fp)

    def keys(self):
        return sorted(self._locations)

    def location(self, key):
        meta = self._locations[key]
        return Location(
            lat=meta["latitude"],
            lon=meta["longitude"],
            elevation_m=meta.get("altitude"),
            timezone=meta.get("timezone"),
            name=meta.get("name"),
        )

    def load(self, key, variables=None):
        """The fixture called `key` ("braunschweig") as a SourceResult."""
        if key not in self._locations:
            raise FixtureNotFound(f"no fixture {key!r}; there are {', '.join(self.keys())}")
        with open(self.directory / f"{key}.json") as fp:
            start, step_hours, quantiles = legacy.read(json.load(fp))
        if variables is not None:
            unknown = set(variables) - self.variables
            if unknown:
                raise ValueError(f"fixtures have no {', '.join(sorted(unknown))}")
            quantiles = {name: quantiles[name] for name in variables}
        return SourceResult(
            source=f"{self.id}:{key}",
            kind="quantiles",
            location=self.location(key),
            start=start,
            native_step_hours=step_hours,
            quantiles=quantiles,
        )

    def nearest(self, location):
        def distance(key):
            here = self.location(key)
            return max(abs(here.lat - location.lat), abs(here.lon - location.lon))

        key = min(self.keys(), key=distance)
        if distance(key) > MAX_DISTANCE_DEG:
            raise FixtureNotFound(
                f"no fixture within {MAX_DISTANCE_DEG} degrees of {location.lat}, {location.lon}"
            )
        return key

    async def fetch(self, location, variables):
        return self.load(self.nearest(location), variables)
