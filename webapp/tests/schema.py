"""The allMeteogramData contract, in one place.

Both the offline fixture tests and the live API test call assert_meteogram_schema(),
so a change in what Open-Meteo returns shows up as the same failure regardless of
whether the data came from disk or the network.
"""

# Top-level ECMWF-style variable names produced by downloadJsonData.getData, mapped
# to the plausible range of the values Open-Meteo currently returns for them.
#
# NOTE: these ranges describe the units Open-Meteo actually delivers, which are NOT
# the units the pictogram threshold functions in plotMeteogram.py were written for.
# See test_pictograms.py::TestUnitMismatch.
EXPECTED_VARIABLES = {
    "2t": {"unit": "degC", "low": -90.0, "high": 60.0},
    "tp": {"unit": "mm", "low": 0.0, "high": 500.0},
    "tcc": {"unit": "percent", "low": 0.0, "high": 100.0},
    "ws": {"unit": "km/h", "low": 0.0, "high": 400.0},
}

# Ascending order matters: plotTemperature fills between these as nested bands.
PERCENTILE_KEYS = [
    "min",
    "ten",
    "twenty_five",
    "median",
    "seventy_five",
    "ninety",
    "max",
]

ENSEMBLE_MEMBERS = 51  # calculate_percentiles hardcodes range(51)
STEP_INTERVAL_HOURS = 6  # create_dictionary is called with step_interval=6


def assert_meteogram_schema(data):
    """Assert `data` is a well-formed allMeteogramData dict.

    Checks structure, the doubled nesting, percentile ordering and units.
    """
    assert isinstance(data, dict), f"expected a dict, got {type(data).__name__}"
    assert set(data) == set(EXPECTED_VARIABLES), (
        f"top-level variables changed: expected {sorted(EXPECTED_VARIABLES)}, "
        f"got {sorted(data)}"
    )

    step_counts = {}
    for name, spec in EXPECTED_VARIABLES.items():
        entry = data[name]
        assert set(entry) == {name, "date", "time"}, (
            f"{name}: expected keys {{{name!r}, 'date', 'time'}}, got {sorted(entry)}"
        )

        assert_date_time(name, entry["date"], entry["time"])

        # The doubled nesting: data['tp']['tp']['median'].
        series = entry[name]
        assert set(series) == set(PERCENTILE_KEYS) | {"steps"}, (
            f"{name}: percentile keys changed, got {sorted(series)}"
        )

        assert_steps(name, series["steps"])
        step_counts[name] = len(series["steps"])

        n = len(series["steps"])
        for key in PERCENTILE_KEYS:
            values = series[key]
            assert isinstance(values, list), f"{name}.{key} is not a list"
            assert len(values) == n, (
                f"{name}.{key} has {len(values)} values but {n} steps"
            )
            assert all(isinstance(v, (int, float)) for v in values), (
                f"{name}.{key} contains non-numeric values"
            )

        assert_percentiles_ordered(name, series)
        assert_in_range(name, series, spec)

    assert len(set(step_counts.values())) == 1, (
        f"variables disagree on step count: {step_counts}"
    )


def assert_date_time(name, date, time):
    assert isinstance(date, str) and len(date) == 8 and date.isdigit(), (
        f"{name}.date should be 'YYYYMMDD', got {date!r}"
    )
    assert isinstance(time, str) and len(time) == 4 and time.isdigit(), (
        f"{name}.time should be 'HHMM', got {time!r}"
    )
    assert 1 <= int(date[4:6]) <= 12, f"{name}.date has bad month: {date!r}"
    assert 1 <= int(date[6:8]) <= 31, f"{name}.date has bad day: {date!r}"
    assert 0 <= int(time[0:2]) <= 23, f"{name}.time has bad hour: {time!r}"
    assert 0 <= int(time[2:4]) <= 59, f"{name}.time has bad minute: {time!r}"


def assert_steps(name, steps):
    """steps are hour offsets from date/time, as strings, evenly spaced."""
    assert isinstance(steps, list) and steps, f"{name}.steps is empty"
    assert all(isinstance(s, str) for s in steps), (
        f"{name}.steps should be strings, got {[type(s).__name__ for s in steps[:3]]}"
    )
    hours = [int(s) for s in steps]
    assert hours[0] == 0, f"{name}.steps should start at 0, got {hours[0]}"
    deltas = {b - a for a, b in zip(hours, hours[1:])}
    assert deltas == {STEP_INTERVAL_HOURS}, (
        f"{name}.steps should be every {STEP_INTERVAL_HOURS}h, got spacings {sorted(deltas)}"
    )


def assert_percentiles_ordered(name, series):
    """min <= ten <= twenty_five <= median <= seventy_five <= ninety <= max.

    Guards against the percentile labels being wired to the wrong columns in
    calculate_percentiles(), which numpy itself would not catch.
    """
    for lower, upper in zip(PERCENTILE_KEYS, PERCENTILE_KEYS[1:]):
        for i, (lo, hi) in enumerate(zip(series[lower], series[upper])):
            assert lo <= hi, (
                f"{name} step {i}: {lower}={lo} > {upper}={hi} - percentile labels "
                f"are out of order"
            )


def assert_in_range(name, series, spec):
    low, high = spec["low"], spec["high"]
    for key in PERCENTILE_KEYS:
        for i, value in enumerate(series[key]):
            assert value == value, f"{name}.{key} step {i} is NaN"
            assert low <= value <= high, (
                f"{name}.{key} step {i} = {value}, outside the plausible range "
                f"[{low}, {high}] for {spec['unit']} - the API may have changed units"
            )
