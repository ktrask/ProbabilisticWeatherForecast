"""Offline unit tests for the downloader.

These use synthetic frames and a fake Open-Meteo client rather than fixtures,
so they pin the arithmetic and the request exactly instead of depending on the
weather.
"""
import numpy as np
import pandas as pd
import pytest
from openmeteo_sdk.Unit import Unit
from openmeteo_sdk.Variable import Variable

from meteogram import downloadJsonData
from meteogram.downloadJsonData import (
    EXPECTED_UNITS,
    STEP_INTERVAL_HOURS,
    UnexpectedUnit,
    accumulate_over_steps,
    calculate_percentiles,
    create_dictionary,
    getData,
)
from meteogram.plotMeteogram import getVSUPWindCoordinate

MEMBERS = 51


def hourly_frame(per_member_hourly, column="precipitation", start="2026-01-01"):
    """Build a frame shaped like getData's hourly_dataframe.

    per_member_hourly: list of per-hour values, reused for every member, or a
    {member_index: [values]} mapping for per-member control.
    """
    if isinstance(per_member_hourly, dict):
        n = len(next(iter(per_member_hourly.values())))
        columns = {
            f"{column}_member{m}": np.array(
                per_member_hourly.get(m, [0.0] * n), dtype=float
            )
            for m in range(MEMBERS)
        }
    else:
        n = len(per_member_hourly)
        columns = {
            f"{column}_member{m}": np.array(per_member_hourly, dtype=float)
            for m in range(MEMBERS)
        }
    data = {"date": pd.date_range(start=start, periods=n, freq="h", tz="UTC")}
    data.update(columns)
    return pd.DataFrame(data)


class TestAccumulateOverSteps:
    def test_sums_each_bucket(self):
        # 12 hours of exactly 1 mm/h -> two buckets of 6 mm.
        df = hourly_frame([1.0] * 12)
        out = accumulate_over_steps(df)
        assert len(out) == 2
        assert out["precipitation_member0"].tolist() == [6.0, 6.0]

    def test_conserves_the_total(self):
        rain = [0.0, 0.3, 1.2, 0.0, 0.0, 4.5, 2.1, 0.0, 0.0, 0.7, 0.0, 0.0]
        df = hourly_frame(rain)
        out = accumulate_over_steps(df)
        assert out["precipitation_member0"].sum() == pytest.approx(sum(rain))

    def test_bucket_covers_the_six_hours_that_follow_its_label(self):
        # Rain only in hour 7, which belongs to the second bucket.
        rain = [0.0] * 12
        rain[7] = 3.0
        out = accumulate_over_steps(hourly_frame(rain))
        assert out["precipitation_member0"].tolist() == [0.0, 3.0]

    def test_keeps_the_bucket_start_date(self):
        out = accumulate_over_steps(hourly_frame([1.0] * 12))
        assert out["date"].iloc[0] == pd.Timestamp("2026-01-01 00:00", tz="UTC")
        assert out["date"].iloc[1] == pd.Timestamp("2026-01-01 06:00", tz="UTC")

    def test_keeps_a_short_trailing_bucket(self):
        """Bucket count must stay equal to the sample count the instantaneous
        variables produce, or the variables desynchronise."""
        df = hourly_frame([1.0] * 15)
        out = accumulate_over_steps(df)
        assert len(out) == len(range(0, 15, STEP_INTERVAL_HOURS)) == 3
        assert out["precipitation_member0"].tolist() == [6.0, 6.0, 3.0]

    def test_keeps_every_member_separate(self):
        df = hourly_frame({0: [1.0] * 6, 1: [2.0] * 6})
        out = accumulate_over_steps(df)
        assert out["precipitation_member0"].iloc[0] == 6.0
        assert out["precipitation_member1"].iloc[0] == 12.0
        assert out["precipitation_member2"].iloc[0] == 0.0

    def test_subsampling_would_have_lost_most_of_the_rain(self):
        """The regression this function exists for: df.iloc[::6] keeps one hour
        out of six, so a burst between samples vanishes entirely."""
        rain = [0.0, 5.0, 5.0, 5.0, 5.0, 0.0]
        df = hourly_frame(rain)
        subsampled = df.iloc[::STEP_INTERVAL_HOURS]["precipitation_member0"].iloc[0]
        accumulated = accumulate_over_steps(df)["precipitation_member0"].iloc[0]
        assert subsampled == 0.0
        assert accumulated == 20.0


class TestPercentilesOfSums:
    def test_percentiles_are_taken_after_summing(self):
        """Order matters: the max of the 6-hour totals is not the sum of the
        hourly maxima when different members peak in different hours."""
        # member0 rains in hour 0, member1 in hour 1; each totals 4 mm.
        df = hourly_frame({0: [4.0, 0.0] + [0.0] * 4, 1: [0.0, 4.0] + [0.0] * 4})
        result = calculate_percentiles(accumulate_over_steps(df), "precipitation")
        assert result["max"].iloc[0] == pytest.approx(4.0)
        # Summing the hourly maxima instead would have given 8 mm.
        hourly = calculate_percentiles(df, "precipitation")
        assert hourly["max"].sum() == pytest.approx(8.0)


class TestCreateDictionary:
    def test_already_bucketed_input_gets_six_hourly_steps(self):
        """Precipitation is passed through with step_interval=1 because
        accumulate_over_steps has already done the 6-hourly grouping."""
        df = hourly_frame([1.0] * 18)
        out = create_dictionary(
            calculate_percentiles(accumulate_over_steps(df), "precipitation"),
            "tp",
            step_interval=1,
        )
        assert out["tp"]["steps"] == ["0", "6", "12"]
        assert out["date"] == "20260101"
        assert out["time"] == "0000"

    def test_instantaneous_input_is_subsampled(self):
        """Temperature and friends still take every 6th hourly row."""
        df = hourly_frame([1.0] * 18, column="temperature_2m")
        out = create_dictionary(
            calculate_percentiles(df, "temperature_2m"), "2t", step_interval=6
        )
        assert out["2t"]["steps"] == ["0", "6", "12"]


# (Variable, altitude) as getData's filters select them, per requested variable.
SDK_VARIABLES = {
    "temperature_2m": (Variable.temperature, 2),
    "precipitation": (Variable.precipitation, 0),
    "wind_speed_10m": (Variable.wind_speed, 10),
    "cloud_cover": (Variable.cloud_cover, 0),
}


class FakeVariable:
    def __init__(self, name, member, values, unit):
        self._variable, self._altitude = SDK_VARIABLES[name]
        self._member = member
        self._values = np.asarray(values, dtype=np.float32)
        self._unit = unit

    def Variable(self):
        return self._variable

    def Altitude(self):
        return self._altitude

    def EnsembleMember(self):
        return self._member

    def ValuesAsNumpy(self):
        return self._values

    def Unit(self):
        return self._unit


class FakeHourly:
    START = 1_767_225_600  # 2026-01-01T00:00Z

    def __init__(self, variables, hours):
        self._variables = variables
        self._hours = hours

    def Variables(self, i):
        return self._variables[i]

    def VariablesLength(self):
        return len(self._variables)

    def Time(self):
        return self.START

    def TimeEnd(self):
        return self.START + 3600 * self._hours

    def Interval(self):
        return 3600


class FakeForecast:
    def __init__(self, hourly):
        self._hourly = hourly

    def Latitude(self):
        return 52.25

    def Longitude(self):
        return 10.5

    def Elevation(self):
        return 80.0

    def Timezone(self):
        return b"Europe/Berlin"

    def TimezoneAbbreviation(self):
        return b"CET"

    def UtcOffsetSeconds(self):
        return 3600

    def Hourly(self):
        return self._hourly


class FakeOpenMeteo:
    """Stands in for openmeteo_requests.Client and records the request.

    `values` are in EXPECTED_UNITS. Like the real API, wind is sent in km/h
    unless wind_speed_unit=ms is asked for. `units` overrides what is sent
    regardless of the request, for an API that ignores the parameter.
    """

    def __init__(self, values=None, units=None, hours=12):
        self.values = {
            "temperature_2m": 10.0,
            "precipitation": 0.0,
            "wind_speed_10m": 4.8,
            "cloud_cover": 50.0,
            **(values or {}),
        }
        self.units = units or {}
        self.hours = hours
        self.params = None

    def weather_api(self, url, params):
        self.params = params
        values = dict(self.values)
        units = dict(EXPECTED_UNITS)
        if params.get("wind_speed_unit") != "ms":
            values["wind_speed_10m"] *= 3.6
            units["wind_speed_10m"] = Unit.kilometres_per_hour
        units.update(self.units)
        variables = [
            FakeVariable(name, member, [values[name]] * self.hours, units[name])
            for name in SDK_VARIABLES
            for member in range(MEMBERS)
        ]
        return [FakeForecast(FakeHourly(variables, self.hours))]


def fetch(monkeypatch, client):
    monkeypatch.setattr(downloadJsonData, "openmeteo", client)
    return getData(10.5, 52.25, 80, writeToFile=False)


class TestUnits:
    """getData has to deliver the units the pictogram thresholds are written in.

    Open-Meteo's default wind unit is km/h while the thresholds are m/s, so every
    wind threshold used to fire 3.6x too early: a 4.8 m/s breeze arrived as
    17.3 "m/s" and cleared the 17.2 storm line.
    """

    def test_asks_for_wind_in_metres_per_second(self, monkeypatch):
        client = FakeOpenMeteo()
        fetch(monkeypatch, client)
        assert client.params["wind_speed_unit"] == "ms"

    def test_every_requested_variable_has_an_expected_unit(self):
        assert set(downloadJsonData.FORECAST_PARAMS["hourly"]) == set(EXPECTED_UNITS)

    def test_a_breeze_is_drawn_as_light_wind(self, monkeypatch):
        """The bug end to end: 4.8 m/s from the API is Beaufort 3, not a storm."""
        data = fetch(monkeypatch, FakeOpenMeteo(values={"wind_speed_10m": 4.8}))
        series = data["ws"]["ws"]
        assert series["median"][0] == pytest.approx(4.8)
        percentiles = {key: series[key][0] for key in series if key != "steps"}
        assert getVSUPWindCoordinate(percentiles) == 4, "expected the light-wind glyph"

    def test_wind_in_kmh_is_refused(self, monkeypatch):
        """If Open-Meteo ever ignores wind_speed_unit, fail loudly rather than
        drawing every breeze as a storm again."""
        client = FakeOpenMeteo(
            values={"wind_speed_10m": 17.28},
            units={"wind_speed_10m": Unit.kilometres_per_hour},
        )
        with pytest.raises(UnexpectedUnit, match="wind_speed_10m in kilometres_per_hour"):
            fetch(monkeypatch, client)

    @pytest.mark.parametrize(
        "name,wrong",
        [
            ("temperature_2m", Unit.fahrenheit),
            ("precipitation", Unit.inch),
            ("cloud_cover", Unit.fraction),
            ("wind_speed_10m", Unit.knots),
        ],
        ids=["temperature_2m", "precipitation", "cloud_cover", "wind_speed_10m"],
    )
    def test_any_unexpected_unit_is_refused(self, monkeypatch, name, wrong):
        with pytest.raises(UnexpectedUnit, match=name):
            fetch(monkeypatch, FakeOpenMeteo(units={name: wrong}))
