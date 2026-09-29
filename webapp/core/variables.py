"""The variables a forecast can carry, each with its one canonical unit.

Every source adapter has to deliver these units and every VSUP scheme is
checked against them when it is loaded. The legacy pipeline had no such table,
which is how cloud cover, precipitation and wind each ended up compared against
thresholds written in a different unit than the data - and none of those
mismatches raised anything, they just picked the wrong pictograms.
"""
from dataclasses import dataclass
from typing import Literal

Kind = Literal["instant", "sum"]


@dataclass(frozen=True)
class VariableSpec:
    name: str
    unit: str
    # instant: a value at the step's time, sampled.
    # sum: a total over a window, accumulated per member before quantiles are
    # taken - the 90th percentile of 6-hour totals is not the sum of the hourly
    # 90th percentiles.
    kind: Kind
    description: str


VARIABLES = {
    spec.name: spec
    for spec in (
        VariableSpec("temperature_2m", "degC", "instant", "air temperature 2 m above ground"),
        VariableSpec("precipitation", "mm", "sum", "total precipitation (rain, showers, snow)"),
        VariableSpec("cloud_cover", "percent", "instant", "total cloud cover"),
        VariableSpec("wind_speed_10m", "m/s", "instant", "wind speed 10 m above ground"),
    )
}


class UnknownVariable(KeyError):
    def __str__(self):
        return f"unknown variable {self.args[0]!r}; known: {', '.join(sorted(VARIABLES))}"


def variable(name):
    try:
        return VARIABLES[name]
    except KeyError:
        raise UnknownVariable(name) from None
