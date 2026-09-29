"""Serves the recorded forecasts in tests/fixtures/ as if they were a live source.

For tests, offline development and demos. It is a quantile source: the 51
members were reduced to quantiles when the forecast was recorded. Two formats
are read, told apart by their content:

- a Forecast, as tests/generate_fixtures.py writes it now - the new pipeline's
  own output, precipitation totalled over [t, t + 6 h);
- the legacy allMeteogramData dict that the removed downloadJsonData.getData()
  produced. The files recorded that way are kept as they are; their
  precipitation window is the legacy one, an hour early (see core.reduce).
"""
import json
from datetime import timedelta
from pathlib import Path

import numpy as np

from core import legacy
from core.model import Forecast, Location, quantile_level
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
    max_lead = timedelta(days=15)

    def __init__(self, directory=DEFAULT_DIRECTORY):
        self.directory = Path(directory)
        with open(self.directory / "locations.json") as fp:
            self._locations = json.load(fp)
        # The quantile levels every recording offers - what a configuration
        # may ask of this source. Legacy files have the seven fixed ones.
        levels = None
        for key in self.keys():
            with open(self.directory / f"{key}.json") as fp:
                offered = set(read(json.load(fp))[2]["temperature_2m"])
            levels = offered if levels is None else levels & offered
        self.quantile_levels = tuple(sorted(quantile_level(name) for name in levels or ()))

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
            start, step_hours, quantiles = read(json.load(fp))
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


def is_forecast(data):
    """Whether a recorded file is in the new format rather than the legacy one."""
    return isinstance(data, dict) and "steps" in data and "variables" in data


def read(data):
    """A recorded file, in either format -> (start, step_hours, {variable: {"p10": array}})."""
    if not is_forecast(data):
        return legacy.read(data)
    forecast = Forecast.model_validate(data)
    quantiles = {
        name: {q: np.asarray(values, dtype=np.float64) for q, values in series.quantiles.items()}
        for name, series in forecast.variables.items()
    }
    return forecast.steps[0], forecast.step_hours, quantiles
