"""Serves the recorded forecasts in tests/fixtures/ as if they were a live source.

For tests, offline development and demos. It is a quantile source: the 51
members were reduced to quantiles when the forecast was recorded. Two formats
are read, told apart by their content:

- a Forecast, as tests/generate_fixtures.py writes it now - the new pipeline's
  own output, precipitation totalled over [t, t + 6 h);
- the legacy allMeteogramData dict that the removed downloadJsonData.getData()
  produced, with its precipitation window an hour early (see core.reduce).
  Only a sample is left in that format (tests/fixtures/legacy/), which keeps
  this reader tested.
"""
import json
from datetime import timedelta
from pathlib import Path

import numpy as np

from core import legacy
from core.model import Forecast, Location, quantile_level
from core.pipeline import SourceResult
from sources.base import NoData, NotCovered

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

    def __init__(self, directory=DEFAULT_DIRECTORY, area=None):
        """area: as the live source it stands in for (sources.base.Area), so a
        place outside it is refused the same way."""
        self.directory = Path(directory)
        self.area = area
        with open(self.directory / "locations.json") as fp:
            self._locations = json.load(fp)
        # Each place is recorded in 6-hour steps (<key>.json) and may be in
        # others too (<key>.1h.json). What every place has in every recording -
        # step widths and quantile levels - is what a configuration may ask for.
        # Legacy files have the seven fixed levels.
        levels = None
        steps = None
        self._files = {}  # (key, step_hours) -> path
        for key in self.keys():
            here = set()
            for path in sorted(self.directory.glob(f"{key}.*json")):
                if path.name != f"{key}.json" and not path.name.endswith("h.json"):
                    continue
                with open(path) as fp:
                    _, step_hours, quantiles = read(json.load(fp))
                self._files[(key, step_hours)] = path
                here.add(step_hours)
                offered = set(quantiles["temperature_2m"])
                levels = offered if levels is None else levels & offered
            steps = here if steps is None else steps & here
        self.quantile_levels = tuple(sorted(quantile_level(name) for name in levels or ()))
        self.steps = frozenset(steps or ())

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

    def load(self, key, variables=None, step_hours=None):
        """The fixture called `key` ("braunschweig") as a SourceResult, in
        `step_hours` steps if recorded so (default: <key>.json, 6 hours)."""
        if key not in self._locations:
            raise FixtureNotFound(f"no fixture {key!r}; there are {', '.join(self.keys())}")
        path = self.directory / f"{key}.json" if step_hours is None else self._files.get((key, step_hours))
        if path is None:
            raise FixtureNotFound(f"no recording of {key!r} in {step_hours}-hour steps")
        with open(path) as fp:
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

    def probe(self, location):
        """Where a health check asks: `location` if a recording is near it,
        otherwise the first recording."""
        try:
            self.nearest(location)
            return location
        except FixtureNotFound:
            return self.location(self.keys()[0])

    async def fetch(self, location, variables, step_hours=None):
        if self.area is not None and not self.area.contains(location.lat, location.lon):
            raise NotCovered(f"{self.id} does not cover {location.lat}, {location.lon}")
        return self.load(self.nearest(location), variables, step_hours)


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
