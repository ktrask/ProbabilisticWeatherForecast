"""Reading the legacy allMeteogramData format.

Only needed while both pipelines exist: the offline fixtures are recorded in
the legacy format, and the golden tests feed the same data to the old
getVSUP*Coordinate() functions and to the new classifier.

The legacy format carries no units. Since the wind fix it is degC, mm per step,
percent and m/s (tests/schema.py pins that), which happen to be the canonical
units - older dumps have wind in km/h and cannot be told apart by looking.
"""
from datetime import datetime, timedelta, timezone

import numpy as np

# allMeteogramData key -> variable name
VARIABLES = {
    "2t": "temperature_2m",
    "tp": "precipitation",
    "tcc": "cloud_cover",
    "ws": "wind_speed_10m",
}

# legacy percentile key -> quantile name
QUANTILES = {
    "min": "p0",
    "ten": "p10",
    "twenty_five": "p25",
    "median": "p50",
    "seventy_five": "p75",
    "ninety": "p90",
    "max": "p100",
}


def reference_time(entry):
    """The UTC datetime of an entry's first step ("YYYYMMDD", "HHMM")."""
    return datetime.strptime(entry["date"] + entry["time"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def read(data):
    """allMeteogramData -> (start, step_hours, {variable: {"p10": array, ...}})."""
    starts, spacings, quantiles = set(), set(), {}
    for key, name in VARIABLES.items():
        entry = data[key]
        series = entry[key]
        hours = [int(step) for step in series["steps"]]
        starts.add(reference_time(entry) + timedelta(hours=hours[0]))
        spacings.update(b - a for a, b in zip(hours, hours[1:]))
        quantiles[name] = {
            QUANTILES[legacy]: np.asarray(series[legacy], dtype=np.float64) for legacy in QUANTILES
        }
    if len(starts) != 1 or len(spacings) != 1:
        raise ValueError(f"variables disagree on their time axis: starts {starts}, spacings {spacings}")
    return starts.pop(), spacings.pop(), quantiles


def percentiles(quantiles):
    """{"p10": ..., ...} -> {"ten": ..., ...}, the dict getVSUP*Coordinate() takes."""
    return {legacy: quantiles[name] for legacy, name in QUANTILES.items()}
