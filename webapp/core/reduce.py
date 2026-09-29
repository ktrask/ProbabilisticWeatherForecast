"""Ensemble members on the source's native grid -> quantiles on the step grid.

Arrays are (members, rows): one row per native time step (an hour for
Open-Meteo), one line per ensemble member, already in canonical units.

Timing convention. An instant variable's row is its value at the row's time.
A sum variable's row is the total over the native step *ending* at the row's
time - Open-Meteo documents precipitation as the "preceding hour sum". A step t
of the output covers [t, t + step), so its total is made of rows t+1 ... t+step.
The legacy accumulate_over_steps summed rows t ... t+step-1 instead, i.e. the
window from an hour before t to an hour before the next step.
"""
import numpy as np

from core.model import quantile_name


class DataGap(ValueError):
    """The data has holes this reduction will not paper over."""


def complete_rows(arrays):
    """How many leading rows are finite in every member of every array.

    Sources pad the end of their horizon with NaN (Open-Meteo does once a model
    run ends); those rows are dropped. A NaN followed by real data again is a
    hole in the middle, which would silently shift or distort windows, so it is
    refused.
    """
    arrays = list(arrays)
    if not arrays:
        raise DataGap("no data")
    finite = np.logical_and.reduce([np.isfinite(a).all(axis=0) for a in arrays])
    if finite.all():
        return len(finite)
    first_gap = int(np.argmin(finite))
    if finite[first_gap:].any():
        resumes = first_gap + int(np.argmax(finite[first_gap:]))
        raise DataGap(f"data is missing from row {first_gap} but resumes at row {resumes}")
    return first_gap


def step_count(n_rows, rows_per_step, kinds):
    """The number of whole steps every variable can fill from `n_rows` rows.

    An instant step needs its own row; a sum step needs the `rows_per_step` rows
    after it. Steps that only some variables could fill are dropped, so all
    variables share one time axis.
    """
    if n_rows == 0:
        return 0
    counts = [
        (n_rows - 1) // rows_per_step if kind == "sum" else (n_rows - 1) // rows_per_step + 1
        for kind in kinds
    ]
    return min(counts)


def to_steps(values, kind, n_steps, rows_per_step):
    """(members, rows) -> (members, n_steps): sampled for instants, summed for sums.

    Sums are taken per member - before any quantile - because a quantile of
    totals is not the total of quantiles.
    """
    values = np.asarray(values, dtype=np.float64)
    end = n_steps * rows_per_step
    if kind == "instant":
        if values.shape[1] < end - rows_per_step + 1:
            raise DataGap(f"{values.shape[1]} rows cannot fill {n_steps} steps")
        return values[:, 0:end:rows_per_step]
    if values.shape[1] < end + 1:
        raise DataGap(f"{values.shape[1]} rows cannot fill {n_steps} summed steps")
    windows = values[:, 1 : end + 1].reshape(values.shape[0], n_steps, rows_per_step)
    return windows.sum(axis=2)


def quantiles(values, levels):
    """(members, steps) -> {"p10": [...], ...}, linear interpolation like the legacy code."""
    result = np.percentile(values, list(levels), axis=0)
    return {quantile_name(level): result[i].tolist() for i, level in enumerate(levels)}
