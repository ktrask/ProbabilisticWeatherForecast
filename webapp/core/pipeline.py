"""From what a source delivers to a validated Forecast.

Sources differ in what they hand over - raw ensemble members, or quantiles that
someone else already computed - but not in units or time zones: a SourceResult
is always in canonical units and UTC, so everything after the adapter is the
same code for every source.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

import numpy as np

from core import reduce
from core.model import Forecast, Location, Run, VariableSeries, quantile_name
from core.variables import variable

STEP_HOURS = 6
# The levels config/vsup.yaml computes: the three bands of the temperature
# chart (0-100, 10-90, 25-75), the median, and 17-83 - the middle two thirds,
# which decide when a pictogram counts as certain.
DEFAULT_QUANTILES = (0, 10, 17, 25, 50, 75, 83, 90, 100)


@dataclass(frozen=True)
class SourceResult:
    """What an adapter returns. Values in canonical units, times in UTC.

    kind="ensemble": `members` maps each variable to a (members, rows) array on
      the native grid; sum variables hold the total of the native step ending at
      each row (see core.reduce).
    kind="quantiles": `quantiles` maps each variable to {"p10": array, ...},
      already on the step grid, sums already totalled over the step. Quantiles
      cannot be re-gridded, so the native step has to be the step asked for.
    """

    source: str
    kind: Literal["ensemble", "quantiles"]
    location: Location
    start: datetime
    native_step_hours: int
    members: dict = field(default_factory=dict)
    quantiles: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.start.utcoffset() != timedelta(0):
            raise ValueError(f"start must be in UTC, got {self.start.isoformat()}")
        if self.kind == "ensemble" and (not self.members or self.quantiles):
            raise ValueError("an ensemble result carries members and nothing else")
        if self.kind == "quantiles" and (not self.quantiles or self.members):
            raise ValueError("a quantile result carries quantiles and nothing else")

    @property
    def variables(self):
        return sorted(self.members or self.quantiles)

    @property
    def member_count(self):
        if not self.members:
            return None
        return next(iter(self.members.values())).shape[0]


def build_forecast(result, *, step_hours=STEP_HOURS, quantile_levels=DEFAULT_QUANTILES):
    """Reduce `result` to quantiles on a `step_hours` grid. No pictograms yet."""
    if step_hours % result.native_step_hours:
        raise ValueError(
            f"{step_hours}-hour steps cannot be built from {result.native_step_hours}-hour data"
        )
    if result.kind == "ensemble":
        n_steps, variables = _from_members(result, step_hours, quantile_levels)
    else:
        n_steps, variables = _from_quantiles(result, step_hours, quantile_levels)
    return Forecast(
        location=result.location,
        run=Run(source=result.source, members=result.member_count),
        steps=[result.start + timedelta(hours=step_hours * k) for k in range(n_steps)],
        step_hours=step_hours,
        variables=variables,
    )


def _series(name, step_hours, quantiles):
    spec = variable(name)
    return VariableSeries(
        unit=spec.unit,
        kind=spec.kind,
        window_hours=step_hours if spec.kind == "sum" else None,
        quantiles=quantiles,
    )


def _from_members(result, step_hours, levels):
    rows_per_step = step_hours // result.native_step_hours
    names = result.variables
    n_rows = reduce.complete_rows(result.members[name] for name in names)
    n_steps = reduce.step_count(n_rows, rows_per_step, [variable(name).kind for name in names])
    if n_steps == 0:
        raise reduce.DataGap(f"{n_rows} complete rows are not enough for one {step_hours}-hour step")
    variables = {}
    for name in names:
        values = result.members[name][:, :n_rows]
        on_grid = reduce.to_steps(values, variable(name).kind, n_steps, rows_per_step)
        variables[name] = _series(name, step_hours, reduce.quantiles(on_grid, levels))
    return n_steps, variables


def _from_quantiles(result, step_hours, levels):
    if result.native_step_hours != step_hours:
        raise ValueError(
            f"{result.source} delivers quantiles every {result.native_step_hours} hours; "
            f"quantiles cannot be re-gridded to {step_hours}-hour steps"
        )
    wanted = [quantile_name(level) for level in levels]
    arrays = []
    for name in result.variables:
        missing = [q for q in wanted if q not in result.quantiles[name]]
        if missing:
            raise ValueError(f"{result.source} does not provide {', '.join(missing)} for {name}")
        arrays.extend(np.asarray(result.quantiles[name][q], dtype=np.float64)[np.newaxis] for q in wanted)
    n_steps = reduce.complete_rows(arrays)
    variables = {
        name: _series(
            name,
            step_hours,
            {q: np.asarray(result.quantiles[name][q], dtype=np.float64)[:n_steps].tolist() for q in wanted},
        )
        for name in result.variables
    }
    return n_steps, variables
