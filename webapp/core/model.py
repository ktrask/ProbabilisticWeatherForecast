"""The forecast as it leaves the backend - successor of allMeteogramData.

The legacy dict left the important things implicit. Here every variable says
which unit it is in and whether it is a sample or a total over a window; times
are ISO 8601 in UTC instead of "YYYYMMDD"/"HHMM" plus hour offsets as strings;
quantiles are named by level (p10) rather than by word (ten); and there is no
doubled nesting. Conversion to local time is the client's job.

The validators are the contract. Anything that builds a Forecast - the pipeline,
a test, a future source - gets the same checks, and a Forecast that exists is
consistent: units canonical, steps evenly spaced, quantiles ordered and finite.
"""
import math
import re
from datetime import timedelta
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from core.variables import VARIABLES

_QUANTILE_NAME = re.compile(r"p(100|[1-9]?[0-9])")

# The name of a variable's deterministic series, alongside its quantiles.
DETERMINISTIC = "deterministic"


def quantile_name(level):
    """10 -> "p10"."""
    if not (isinstance(level, int) and 0 <= level <= 100):
        raise ValueError(f"quantile levels are integers from 0 to 100, got {level!r}")
    return f"p{level}"


def quantile_level(name):
    """"p10" -> 10."""
    match = _QUANTILE_NAME.fullmatch(name)
    if match is None:
        raise ValueError(f"{name!r} is not a quantile name like p10 or p90")
    return int(match.group(1))


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    elevation_m: float | None = None
    timezone: str | None = None
    name: str | None = None


class Run(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str
    # When the model run started. Open-Meteo does not report it, so it is often
    # unknown - and steps[0] is the first forecast time, not the run's start.
    init_time: AwareDatetime | None = None
    members: int | None = Field(default=None, ge=1)


class VariableSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    unit: str
    kind: Literal["instant", "sum"]
    # kind=sum only: each step's value is the total over [step, step + window).
    window_hours: int | None = Field(default=None, ge=1)
    quantiles: dict[str, list[float]]
    deterministic: list[float] | None = None

    @field_validator("quantiles")
    @classmethod
    def _named_in_ascending_order(cls, quantiles):
        if not quantiles:
            raise ValueError("at least one quantile is required")
        levels = [quantile_level(name) for name in quantiles]
        if levels != sorted(set(levels)):
            raise ValueError(f"quantiles must be listed in ascending order, got {list(quantiles)}")
        return quantiles

    @model_validator(mode="after")
    def _consistent(self):
        if (self.kind == "sum") != (self.window_hours is not None):
            raise ValueError("window_hours is required for kind 'sum' and not allowed for 'instant'")
        lengths = {name: len(values) for name, values in self.quantiles.items()}
        if self.deterministic is not None:
            lengths["deterministic"] = len(self.deterministic)
        if len(set(lengths.values())) != 1:
            raise ValueError(f"series have different lengths: {lengths}")
        for name, values in self._series():
            if not all(math.isfinite(v) for v in values):
                raise ValueError(f"{name} contains values that are not finite")
        names = list(self.quantiles)
        for lower, upper in zip(names, names[1:]):
            for i, (lo, hi) in enumerate(zip(self.quantiles[lower], self.quantiles[upper])):
                if lo > hi:
                    raise ValueError(f"step {i}: {lower}={lo} > {upper}={hi}")
        return self

    def _series(self):
        yield from self.quantiles.items()
        if self.deterministic is not None:
            yield DETERMINISTIC, self.deterministic

    def __len__(self):
        return len(next(iter(self.quantiles.values())))

    def at(self, index):
        """Everything known about one step: {"p10": ..., "deterministic": ...}."""
        return {name: values[index] for name, values in self._series()}


class PictogramItem(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_by_name=True, serialize_by_alias=True)

    pictogram: str
    level: int = Field(ge=1)
    class_: str = Field(alias="class")


class PictogramSeries(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scheme: str
    items: list[PictogramItem]


class Forecast(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: Location
    run: Run
    deterministic_run: Run | None = None
    steps: list[AwareDatetime]
    step_hours: int = Field(ge=1)
    variables: dict[str, VariableSeries]
    pictograms: dict[str, PictogramSeries] = {}

    @model_validator(mode="after")
    def _consistent(self):
        if not self.steps:
            raise ValueError("a forecast needs at least one step")
        for t in self.steps:
            if t.utcoffset() != timedelta(0):
                raise ValueError(f"steps must be in UTC, got {t.isoformat()}")
        spacing = timedelta(hours=self.step_hours)
        for i, (a, b) in enumerate(zip(self.steps, self.steps[1:])):
            if b - a != spacing:
                raise ValueError(f"steps {i} and {i + 1} are {b - a} apart, expected {spacing}")

        for name, series in self.variables.items():
            spec = VARIABLES.get(name)
            if spec is None:
                raise ValueError(f"unknown variable {name!r}")
            if series.unit != spec.unit:
                raise ValueError(
                    f"{name} is in {series.unit!r}, but its canonical unit is {spec.unit!r}"
                )
            if series.kind != spec.kind:
                raise ValueError(f"{name} is a {spec.kind!r} variable, not {series.kind!r}")
            if len(series) != len(self.steps):
                raise ValueError(f"{name} has {len(series)} values for {len(self.steps)} steps")
            if series.deterministic is not None and self.deterministic_run is None:
                raise ValueError(f"{name} has a deterministic series but there is no deterministic_run")

        for name, series in self.pictograms.items():
            if name not in self.variables:
                raise ValueError(f"pictograms for {name!r}, which is not among the variables")
            if len(series.items) != len(self.steps):
                raise ValueError(
                    f"{name} has {len(series.items)} pictograms for {len(self.steps)} steps"
                )
        return self
