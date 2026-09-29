"""Applying schemes to a forecast: one pictogram per variable and step."""
from core.model import PictogramItem, PictogramSeries
from vsup.expr import DETERMINISTIC


class SchemeMismatch(ValueError):
    """A valid scheme that cannot be applied to this particular forecast."""


def check_applicable(scheme, forecast):
    series = forecast.variables.get(scheme.variable)
    if series is None:
        raise SchemeMismatch(f"{scheme.name} needs {scheme.variable}, which this forecast does not have")
    if series.window_hours != scheme.window_hours:
        # Thresholds for 6-hour totals say nothing about 12-hour totals.
        raise SchemeMismatch(
            f"{scheme.name} is written for {scheme.window_hours}-hour totals, "
            f"but {scheme.variable} here is totalled over {series.window_hours} hours"
        )
    missing = sorted(scheme.reads - set(series.quantiles) - {DETERMINISTIC})
    if missing:
        raise SchemeMismatch(f"{scheme.name} reads {', '.join(missing)}, which this forecast does not have")
    if DETERMINISTIC in scheme.reads and series.deterministic is None:
        raise SchemeMismatch(f"{scheme.name} needs a deterministic run for {scheme.variable}")
    return series


def classify_series(scheme, series):
    """The Choice for every step of one variable."""
    return [scheme.classify(series.at(i)) for i in range(len(series))]


def classify(forecast, config, scheme_names):
    """A copy of `forecast` with pictograms from the named schemes, at most one per variable."""
    pictograms = {}
    for name in scheme_names:
        scheme = config.scheme(name)
        if scheme.variable in pictograms:
            raise SchemeMismatch(
                f"{name} and {pictograms[scheme.variable].scheme} both draw {scheme.variable}"
            )
        series = check_applicable(scheme, forecast)
        pictograms[scheme.variable] = PictogramSeries(
            scheme=name,
            items=[
                PictogramItem(pictogram=choice.pictogram, level=choice.level, class_=choice.class_)
                for choice in classify_series(scheme, series)
            ],
        )
    return forecast.model_copy(update={"pictograms": {**forecast.pictograms, **pictograms}})
