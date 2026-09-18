"""Offline unit tests for the downloader's aggregation helpers.

These use synthetic frames rather than fixtures, so they pin the arithmetic
exactly instead of depending on the weather.
"""
import numpy as np
import pandas as pd
import pytest

from downloadJsonData import (
    STEP_INTERVAL_HOURS,
    accumulate_over_steps,
    calculate_percentiles,
    create_dictionary,
)

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
