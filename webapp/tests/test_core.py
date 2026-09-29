"""Offline tests for core/: units, the Forecast contract, and the reduction.

Synthetic arrays throughout, so the arithmetic is pinned exactly rather than
depending on the weather in the fixtures.
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from pydantic import ValidationError

from core import reduce
from core.model import Forecast, Location, Run, VariableSeries, quantile_level, quantile_name
from core.pipeline import DEFAULT_QUANTILES, SourceResult, build_forecast
from core.units import UnitError, converter, to_canonical
from core.variables import VARIABLES, UnknownVariable, variable

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
MEMBERS = 51


class TestUnits:
    @pytest.mark.parametrize(
        "value,unit,name,expected",
        [
            (17.28, "km/h", "wind_speed_10m", 4.8),
            (10.0, "kn", "wind_speed_10m", 5.144444),
            (212.0, "degF", "temperature_2m", 100.0),
            (273.15, "K", "temperature_2m", 0.0),
            (1.0, "inch", "precipitation", 25.4),
            (0.5, "fraction", "cloud_cover", 50.0),
            (4.8, "m/s", "wind_speed_10m", 4.8),
        ],
    )
    def test_converts_to_the_canonical_unit(self, value, unit, name, expected):
        assert to_canonical(value, unit, name) == pytest.approx(expected, rel=1e-6)

    def test_an_unconvertible_unit_is_refused(self):
        with pytest.raises(UnitError, match="precipitation arrived in 'm/s'"):
            to_canonical([1.0], "m/s", "precipitation")

    def test_an_unknown_unit_is_refused(self):
        with pytest.raises(UnitError):
            converter("furlongs/fortnight", "m/s")

    def test_every_conversion_preserves_order(self):
        """vsup converts thresholds once instead of data every time; that is
        only sound if a < b stays a < b in the other unit."""
        from core.units import _CONVERSIONS

        for (source, target), (scale, _) in _CONVERSIONS.items():
            convert = converter(source, target)
            assert convert(1.0) < convert(2.0), f"{source} -> {target} is not increasing"

    def test_every_variable_has_a_convertible_canonical_unit(self):
        for spec in VARIABLES.values():
            assert converter(spec.unit, spec.unit)(3.0) == 3.0

    def test_unknown_variable(self):
        with pytest.raises(UnknownVariable, match="snow_depth"):
            variable("snow_depth")


class TestQuantileNames:
    def test_round_trip(self):
        for level in DEFAULT_QUANTILES:
            assert quantile_level(quantile_name(level)) == level

    @pytest.mark.parametrize("name", ["p101", "p", "P10", "p010", "ten", "p-1"])
    def test_rejects_malformed_names(self, name):
        with pytest.raises(ValueError):
            quantile_level(name)


def series(unit="m/s", kind="instant", n=3, window_hours=None, **overrides):
    quantiles = {
        "p10": [1.0 + i for i in range(n)],
        "p50": [2.0 + i for i in range(n)],
        "p90": [3.0 + i for i in range(n)],
    }
    return {"unit": unit, "kind": kind, "window_hours": window_hours, "quantiles": quantiles, **overrides}


def forecast(**overrides):
    data = {
        "location": {"lat": 52.25, "lon": 10.5},
        "run": {"source": "test"},
        "steps": [START + timedelta(hours=6 * k) for k in range(3)],
        "step_hours": 6,
        "variables": {"wind_speed_10m": series()},
    }
    data.update(overrides)
    return Forecast.model_validate(data)


class TestForecastContract:
    def test_a_consistent_forecast_is_accepted(self):
        f = forecast()
        assert f.variables["wind_speed_10m"].at(1) == {"p10": 2.0, "p50": 3.0, "p90": 4.0}

    def test_json_round_trip(self):
        f = forecast()
        again = Forecast.model_validate_json(f.model_dump_json())
        assert again == f
        assert '"2026-01-01T00:00:00Z"' in f.model_dump_json(), "steps should serialise as UTC"

    def test_steps_must_be_utc(self):
        cet = timezone(timedelta(hours=1))
        with pytest.raises(ValidationError, match="UTC"):
            forecast(steps=[START.astimezone(cet) + timedelta(hours=6 * k) for k in range(3)])

    def test_naive_steps_are_refused(self):
        with pytest.raises(ValidationError):
            forecast(steps=[datetime(2026, 1, 1, 6 * k) for k in range(3)])

    def test_steps_must_be_evenly_spaced(self):
        with pytest.raises(ValidationError, match="apart"):
            forecast(steps=[START, START + timedelta(hours=6), START + timedelta(hours=18)])

    def test_unit_must_be_canonical(self):
        """The whole point: wind in km/h must not get past the model."""
        with pytest.raises(ValidationError, match="canonical unit is 'm/s'"):
            forecast(variables={"wind_speed_10m": series(unit="km/h")})

    def test_kind_must_match_the_variable(self):
        with pytest.raises(ValidationError, match="'sum' variable"):
            forecast(variables={"precipitation": series(unit="mm", kind="instant")})

    def test_sums_say_their_window(self):
        with pytest.raises(ValidationError, match="window_hours is required"):
            forecast(variables={"precipitation": series(unit="mm", kind="sum")})
        forecast(variables={"precipitation": series(unit="mm", kind="sum", window_hours=6)})

    def test_quantiles_must_be_ordered(self):
        bad = series()
        bad["quantiles"]["p50"][1] = 99.0
        with pytest.raises(ValidationError, match="step 1: p50=99.0 > p90=4.0"):
            forecast(variables={"wind_speed_10m": bad})

    def test_quantiles_must_be_listed_in_ascending_order(self):
        bad = series()
        bad["quantiles"] = {"p90": [3.0] * 3, "p10": [1.0] * 3}
        with pytest.raises(ValidationError, match="ascending"):
            forecast(variables={"wind_speed_10m": bad})

    def test_values_must_be_finite(self):
        bad = series()
        bad["quantiles"]["p10"][0] = float("nan")
        with pytest.raises(ValidationError, match="not finite"):
            forecast(variables={"wind_speed_10m": bad})

    def test_every_variable_covers_every_step(self):
        with pytest.raises(ValidationError, match="2 values for 3 steps"):
            forecast(variables={"wind_speed_10m": series(n=2)})

    def test_unknown_variables_are_refused(self):
        with pytest.raises(ValidationError, match="unknown variable"):
            forecast(variables={"snow_depth": series()})

    def test_deterministic_needs_a_deterministic_run(self):
        with pytest.raises(ValidationError, match="deterministic_run"):
            forecast(variables={"wind_speed_10m": series(deterministic=[1.0, 2.0, 3.0])})
        forecast(
            variables={"wind_speed_10m": series(deterministic=[1.0, 2.0, 3.0])},
            deterministic_run={"source": "hres"},
        )

    def test_pictograms_cover_every_step(self):
        items = [{"pictogram": "wind/x.svg", "level": 3, "class": "calm"}] * 2
        with pytest.raises(ValidationError, match="2 pictograms for 3 steps"):
            forecast(pictograms={"wind_speed_10m": {"scheme": "s", "items": items}})

    def test_pictogram_class_serialises_under_its_json_name(self):
        items = [{"pictogram": "wind/x.svg", "level": 3, "class": "calm"}] * 3
        f = forecast(pictograms={"wind_speed_10m": {"scheme": "s", "items": items}})
        assert '"class":"calm"' in f.model_dump_json()


class TestCompleteRows:
    def test_all_finite(self):
        assert reduce.complete_rows([np.ones((2, 5))]) == 5

    def test_trailing_padding_is_dropped(self):
        """Open-Meteo pads past the end of the model run with NaN."""
        a = np.ones((2, 6))
        a[1, 4:] = np.nan
        assert reduce.complete_rows([a, np.ones((3, 6))]) == 4

    def test_a_hole_in_the_middle_is_refused(self):
        a = np.ones((2, 6))
        a[0, 2] = np.nan
        with pytest.raises(reduce.DataGap, match="missing from row 2 but resumes at row 3"):
            reduce.complete_rows([a])


class TestStepGrid:
    def test_instants_are_sampled_at_the_step(self):
        values = np.arange(13, dtype=float)[np.newaxis]
        assert reduce.to_steps(values, "instant", 3, 6).tolist() == [[0.0, 6.0, 12.0]]

    def test_sums_cover_the_hours_after_the_step(self):
        """Open-Meteo's hourly precipitation is the preceding hour's total, so
        the step starting at hour 0 is made of the rows at hours 1 to 6."""
        rain = np.zeros((1, 13))
        rain[0, 0] = 100.0  # fell before the forecast window starts
        rain[0, 1] = 1.0
        rain[0, 6] = 2.0  # still inside the first window: 05:00-06:00
        rain[0, 7] = 4.0  # second window
        assert reduce.to_steps(rain, "sum", 2, 6).tolist() == [[3.0, 4.0]]

    def test_sums_need_a_complete_window(self):
        """An instant step needs one row, a sum step the six after it."""
        assert reduce.step_count(13, 6, ["instant"]) == 3
        assert reduce.step_count(13, 6, ["sum"]) == 2
        assert reduce.step_count(12, 6, ["instant", "sum"]) == 1
        assert reduce.step_count(0, 6, ["instant"]) == 0

    def test_percentiles_are_taken_after_summing(self):
        """Carried over from the legacy suite: the max of the 6-hour totals is
        not the sum of the hourly maxima when members peak in different hours."""
        values = np.zeros((2, 7))
        values[0, 1] = 4.0
        values[1, 2] = 4.0
        summed = reduce.to_steps(values, "sum", 1, 6)
        assert reduce.quantiles(summed, [100])["p100"] == [4.0]
        assert reduce.quantiles(values[:, 1:], [100])["p100"][:2] == [4.0, 4.0]

    def test_quantiles_interpolate_linearly(self):
        values = np.arange(11, dtype=float)[:, np.newaxis]  # 11 members, 1 step
        assert reduce.quantiles(values, [0, 10, 50, 100]) == {
            "p0": [0.0], "p10": [1.0], "p50": [5.0], "p100": [10.0]
        }


def ensemble(hours=25, **members):
    """A SourceResult with the given (members, hours) arrays."""
    return SourceResult(
        source="synthetic",
        kind="ensemble",
        location=Location(lat=52.25, lon=10.5),
        start=START,
        native_step_hours=1,
        members=members,
    )


class TestBuildForecast:
    def test_members_to_quantiles_on_the_step_grid(self):
        wind = np.tile(np.arange(25, dtype=float), (MEMBERS, 1))
        rain = np.ones((MEMBERS, 25))
        f = build_forecast(ensemble(wind_speed_10m=wind, precipitation=rain))
        assert f.steps == [START + timedelta(hours=6 * k) for k in range(4)]
        assert f.run.members == MEMBERS
        assert f.variables["wind_speed_10m"].quantiles["p50"] == [0.0, 6.0, 12.0, 18.0]
        assert f.variables["precipitation"].quantiles["p50"] == [6.0] * 4
        assert f.variables["precipitation"].window_hours == 6
        assert list(f.variables["wind_speed_10m"].quantiles) == [f"p{q}" for q in DEFAULT_QUANTILES]

    def test_steps_every_variable_can_fill(self):
        """25 hourly rows give 5 instant steps but only 4 complete 6-hour totals;
        all variables share the shorter axis."""
        f = build_forecast(
            ensemble(wind_speed_10m=np.ones((MEMBERS, 25)), precipitation=np.ones((MEMBERS, 25)))
        )
        assert len(f.steps) == 4

    def test_trailing_nan_shortens_the_forecast(self):
        wind = np.ones((MEMBERS, 25))
        wind[:, 13:] = np.nan
        assert len(build_forecast(ensemble(wind_speed_10m=wind)).steps) == 3

    def test_custom_quantile_levels(self):
        f = build_forecast(ensemble(wind_speed_10m=np.ones((MEMBERS, 7))), quantile_levels=(5, 95))
        assert list(f.variables["wind_speed_10m"].quantiles) == ["p5", "p95"]

    def test_too_little_data(self):
        with pytest.raises(reduce.DataGap, match="not enough"):
            build_forecast(ensemble(precipitation=np.ones((MEMBERS, 6))))

    def test_steps_must_be_a_multiple_of_the_native_step(self):
        with pytest.raises(ValueError, match="cannot be built"):
            build_forecast(
                SourceResult(source="s", kind="quantiles", location=Location(lat=0, lon=0), start=START,
                             native_step_hours=4, quantiles={"wind_speed_10m": {"p50": np.ones(3)}})
            )

    def test_quantile_sources_cannot_be_regridded(self):
        result = SourceResult(source="s", kind="quantiles", location=Location(lat=0, lon=0), start=START,
                              native_step_hours=3, quantiles={"wind_speed_10m": {"p50": np.ones(3)}})
        with pytest.raises(ValueError, match="cannot be re-gridded"):
            build_forecast(result)

    def test_quantile_sources_must_provide_the_levels(self):
        result = SourceResult(source="s", kind="quantiles", location=Location(lat=0, lon=0), start=START,
                              native_step_hours=6, quantiles={"wind_speed_10m": {"p50": np.ones(3)}})
        with pytest.raises(ValueError, match="does not provide p0, p10"):
            build_forecast(result)
        assert len(build_forecast(result, quantile_levels=(50,)).steps) == 3

    def test_start_must_be_utc(self):
        with pytest.raises(ValueError, match="UTC"):
            SourceResult(source="s", kind="ensemble", location=Location(lat=0, lon=0),
                         start=datetime(2026, 1, 1), native_step_hours=1,
                         members={"wind_speed_10m": np.ones((2, 7))})
