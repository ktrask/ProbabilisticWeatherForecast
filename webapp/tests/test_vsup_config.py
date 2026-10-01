"""Loading and validating VSUP configs, the tree mode, and applying schemes.

The validator's job is to stop a broken config at load time, with the line to
fix. Each error class from the plan gets a case here; BASE is a small valid
config, and each case breaks one thing in it.
"""
import json
from datetime import timedelta

import pytest

from core.pipeline import build_forecast
from sources.fixture import FixtureSource
from vsup import __main__ as cli
from vsup.classify import SchemeMismatch, classify
from vsup.config import DEFAULT_CONFIG, ConfigError, json_schema, load

# The shipped pictograms, which the configs below point at.
PICTOGRAM_DIR = load().pictogram_root

BASE = """\
version: 1
quantiles: [0, 10, 25, 50, 75, 90, 100]
pictogram_root: {root}
schemes:
  wind-rules:
    variable: wind_speed_10m
    unit: m/s
    mode: rules
    rules:
      - {{ when: "p90 < 3", pictogram: wind/step3_no_wind.svg, level: 3, class: calm }}
      - {{ default: true, pictogram: wind/step1_v2.svg, level: 1, class: any }}
  rain-tree:
    variable: precipitation
    unit: mm
    window_hours: 6
    mode: tree
    classes:
      - {{ id: dry, below: 0.1 }}
      - {{ id: wet }}
    levels:
      - {{ level: 2, interval: [p10, p90], groups: [[dry], [wet]] }}
      - {{ level: 1, groups: [[dry, wet]] }}
    pictograms:
      "2:dry": rain/step3_dry.svg
      "2:wet": rain/step3_heavy_rain.svg
      "1:dry+wet": rain/step1_v2.svg
""".format(root=PICTOGRAM_DIR)

WIND_RULES_LINE = 5
RULE_1_LINE = 10
RULE_2_LINE = 11
CLASSES_LINE = 18
LEVEL_2_LINE = 21
LEVEL_1_LINE = 22
PICTOGRAMS_LINE = 24


def write(tmp_path, text):
    path = tmp_path / "vsup.yaml"
    path.write_text(text)
    return path


def broken(tmp_path, old, new, text=BASE):
    """Load BASE with `old` replaced by `new`; return the problems it reports."""
    assert old in text, f"{old!r} is not in the config"
    with pytest.raises(ConfigError) as info:
        load(write(tmp_path, text.replace(old, new, 1)))
    return info.value.issues


def one(issues, fragment):
    matching = [i for i in issues if fragment in i.message]
    assert len(matching) == 1, f"expected one issue about {fragment!r}, got {issues}"
    return matching[0]


class TestShippedConfig:
    def test_loads(self):
        config = load()
        assert set(config.schemes) == {
            "cloud-legacy", "precipitation-legacy", "wind-legacy",
            "cloud-vsup", "precipitation-vsup", "precipitation-1h-vsup", "wind-vsup",
        }

    def test_json_schema_is_up_to_date(self):
        """config/vsup.schema.json is what editors validate against; regenerate
        it with `python -m vsup schema -o config/vsup.schema.json`."""
        checked_in = json.loads((DEFAULT_CONFIG.parent / "vsup.schema.json").read_text())
        assert checked_in == json_schema()

    def test_cli_check_passes_and_reports_coverage(self, capsys):
        assert cli.main(["check"]) == 0
        out = capsys.readouterr().out
        assert "OK, 7 scheme(s)" in out
        # The recordings are in 6-hour steps; the hourly scheme says so instead of failing.
        assert "no recordings in 1-hour steps here" in out
        assert "wind/Stufe3_leichterWind.png" in out

    def test_cli_check_fails_with_file_and_line(self, tmp_path, capsys):
        path = write(tmp_path, BASE.replace("wind/step1_v2.svg", "wind/nope.svg"))
        assert cli.main(["check", str(path), "--no-coverage"]) == 1
        assert f"vsup.yaml:{RULE_2_LINE}:" in capsys.readouterr().err


class TestBase:
    def test_base_is_valid(self, tmp_path):
        config = load(write(tmp_path, BASE))
        assert config.scheme("wind-rules").classify({"p90": 2.0}).pictogram == "wind/step3_no_wind.svg"

    def test_unknown_scheme_name(self, tmp_path):
        with pytest.raises(KeyError, match="there are wind-rules, rain-tree"):
            load(write(tmp_path, BASE)).scheme("nope")


class TestPictograms:
    def test_missing_file(self, tmp_path):
        issue = one(broken(tmp_path, "wind/step1_v2.svg", "wind/step1_v3.svg"), "does not exist")
        assert issue.line == RULE_2_LINE
        assert issue.where == "schemes.wind-rules.rules[1].pictogram"

    def test_path_may_not_leave_the_root(self, tmp_path):
        one(broken(tmp_path, "wind/step1_v2.svg", "../pictogram/wind/step1_v2.svg"), "stay inside it")

    def test_only_images(self, tmp_path):
        one(broken(tmp_path, "wind/step1_v2.svg", "wind/step1_v2.txt"), ".svg or .png")

    def test_root_must_exist(self, tmp_path):
        one(broken(tmp_path, f"pictogram_root: {PICTOGRAM_DIR}", "pictogram_root: nowhere"), "not a directory")

    def test_tree_without_a_pictogram_for_a_group(self, tmp_path):
        issue = one(broken(tmp_path, '      "2:wet": rain/step3_heavy_rain.svg\n', ""), "no pictogram for 2:wet")
        assert issue.line == PICTOGRAMS_LINE

    def test_tree_pictogram_for_no_group(self, tmp_path):
        issue = one(
            broken(tmp_path, '"2:wet": rain/step3_heavy_rain.svg', '"2:wet": rain/step3_heavy_rain.svg\n      "2:moist": rain/step3_dry.svg'),
            "'2:moist' is none of this scheme's",
        )
        assert issue.line == PICTOGRAMS_LINE + 2


class TestRules:
    def test_last_rule_must_be_the_default(self, tmp_path):
        issue = one(
            broken(tmp_path, "{ default: true, pictogram", '{ when: "p90 >= 3", pictogram'),
            "has to be `default: true`",
        )
        assert issue.line == RULE_2_LINE

    def test_default_must_be_last(self, tmp_path):
        issues = broken(
            tmp_path,
            '{ when: "p90 < 3", pictogram',
            '{ default: true, pictogram',
        )
        assert one(issues, "has to be the last").line == RULE_1_LINE

    def test_when_and_default_together(self, tmp_path):
        issue = one(broken(tmp_path, "{ default: true,", '{ default: true, when: "p90 > 1",'), "either `when:`")
        assert issue.line == RULE_2_LINE

    def test_syntax_error_names_line_and_column(self, tmp_path):
        issue = one(broken(tmp_path, '"p90 < 3"', '"p90 << 3"'), "column 6")
        assert issue.line == RULE_1_LINE
        assert "'p90 << 3'" in issue.message

    def test_quantile_that_is_not_computed(self, tmp_path):
        issue = one(broken(tmp_path, '"p90 < 3"', '"p95 < 3"'), "uses p95, which is not computed")
        assert issue.line == RULE_1_LINE

    def test_more_quantiles_make_it_valid(self, tmp_path):
        text = BASE.replace("[0, 10, 25, 50, 75, 90, 100]", "[0, 10, 25, 50, 75, 90, 95, 100]")
        load(write(tmp_path, text.replace('"p90 < 3"', '"p95 < 3"')))


class TestUnits:
    def test_incompatible_unit(self, tmp_path):
        issue = one(broken(tmp_path, "unit: mm", "unit: m/s"), "cannot be converted to precipitation's unit 'mm'")
        assert issue.where == "schemes.rain-tree.unit"

    def test_unknown_unit(self, tmp_path):
        one(broken(tmp_path, "unit: mm", "unit: bananas"), "Input should be")

    def test_thresholds_in_another_unit_are_converted_at_load_time(self, tmp_path):
        """3 m/s written as 10.8 km/h: same decisions, no conversion per request."""
        text = BASE.replace("unit: m/s", "unit: km/h").replace('"p90 < 3"', '"p90 < 10.8"')
        scheme = load(write(tmp_path, text)).scheme("wind-rules")
        assert scheme.classify({"p90": 2.99}).outcome == "rule 1"
        assert scheme.classify({"p90": 3.01}).outcome == "rule 2"

    def test_tree_bounds_are_converted_too(self, tmp_path):
        text = BASE.replace("unit: mm", "unit: inch").replace("below: 0.1", "below: 0.1")
        scheme = load(write(tmp_path, text)).scheme("rain-tree")
        assert scheme.bounds == pytest.approx((2.54,))

    def test_a_total_needs_its_window(self, tmp_path):
        issue = one(broken(tmp_path, "    window_hours: 6\n", ""), "say which, e.g. `window_hours: 6`")
        assert issue.where == "schemes.rain-tree.variable"

    def test_an_instant_has_no_window(self, tmp_path):
        one(broken(tmp_path, "    unit: m/s\n", "    unit: m/s\n    window_hours: 6\n"), "is not a total")


class TestTreeStructure:
    def test_bounds_must_increase(self, tmp_path):
        text = BASE.replace("- { id: wet }", "- { id: wet, below: 0.05 }\n      - { id: soaked }")
        text = text.replace("[[dry], [wet]]", "[[dry], [wet], [soaked]]").replace("[[dry, wet]]", "[[dry, wet, soaked]]")
        text = text.replace('"1:dry+wet"', '"2:soaked": rain/step3_dry.svg\n      "1:dry+wet+soaked"')
        issue = one(broken(tmp_path, "", "", text=text), "0.05 is not above the previous bound 0.1")
        assert issue.line == CLASSES_LINE + 1

    def test_last_class_is_open(self, tmp_path):
        one(broken(tmp_path, "- { id: wet }", "- { id: wet, below: 5 }"), "open-ended")

    def test_only_the_last_class_is_open(self, tmp_path):
        issue = one(broken(tmp_path, "{ id: dry, below: 0.1 }", "{ id: dry }"), "needs `below`")
        assert issue.line == CLASSES_LINE

    def test_groups_must_cover_every_class_in_order(self, tmp_path):
        issue = one(broken(tmp_path, "[[dry], [wet]]", "[[wet], [dry]]"), "every class once, in order")
        assert issue.line == LEVEL_2_LINE

    def test_groups_name_known_classes(self, tmp_path):
        one(broken(tmp_path, "[[dry, wet]]", "[[dry, moist]]"), "unknown class(es) moist")

    def test_groups_merge_whole_groups_of_the_level_above(self, tmp_path):
        """[dry, wet] | [soaked] cannot be followed by [dry] | [wet, soaked]:
        that is no longer a tree, and the pictograms stop meaning "less sure"."""
        text = BASE.replace("- { id: wet }", "- { id: wet, below: 5 }\n      - { id: soaked }")
        text = text.replace(
            "      - { level: 2, interval: [p10, p90], groups: [[dry], [wet]] }\n"
            "      - { level: 1, groups: [[dry, wet]] }\n",
            "      - { level: 3, interval: [p10, p90], groups: [[dry, wet], [soaked]] }\n"
            "      - { level: 2, interval: [p25, p75], groups: [[dry], [wet, soaked]] }\n"
            "      - { level: 1, groups: [[dry, wet, soaked]] }\n",
        )
        issue = one(broken(tmp_path, "", "", text=text), "split one of them")
        assert issue.line == LEVEL_1_LINE + 1

    def test_levels_run_from_most_to_least_certain(self, tmp_path):
        issue = one(broken(tmp_path, "{ level: 1, groups", "{ level: 2, groups"), "most to least certain")
        assert issue.line == LEVEL_1_LINE

    def test_last_level_is_unconditional(self, tmp_path):
        issue = one(broken(tmp_path, "{ level: 1, groups", "{ level: 1, interval: [p25, p75], groups"), "the fallback")
        assert issue.line == LEVEL_1_LINE

    def test_only_the_last_level_is_unconditional(self, tmp_path):
        issues = broken(tmp_path, "interval: [p10, p90], ", "")
        one(issues, "only the last level can do without an interval")
        one(issues, "has to be a single group")

    def test_interval_runs_upwards(self, tmp_path):
        one(broken(tmp_path, "[p10, p90]", "[p90, p10]"), "runs backwards")

    def test_interval_quantiles_are_computed(self, tmp_path):
        one(broken(tmp_path, "[p10, p90]", "[p5, p90]"), "uses p5, which is not computed")


class TestFileLevel:
    def test_yaml_syntax_error(self, tmp_path):
        issue = one(broken(tmp_path, "    mode: tree\n", "    mode: tree\n  bad: [\n"), "not valid YAML")
        assert issue.line is not None

    def test_misspelt_key(self, tmp_path):
        issue = one(broken(tmp_path, '{ when: "p90 < 3"', '{ whne: "p90 < 3"'), "Extra inputs are not permitted")
        assert issue.line == RULE_1_LINE

    def test_duplicate_key(self, tmp_path):
        """YAML itself would quietly keep only the second scheme."""
        issues = broken(tmp_path, "  rain-tree:", "  wind-rules:")
        assert one(issues, "duplicate key 'wind-rules'").line == 12

    def test_unknown_variable(self, tmp_path):
        issue = one(broken(tmp_path, "variable: wind_speed_10m", "variable: wind_gusts_10m"), "Input should be")
        assert issue.where == "schemes.wind-rules.variable"

    def test_quantiles_ascend(self, tmp_path):
        one(broken(tmp_path, "[0, 10, 25, 50, 75, 90, 100]", "[0, 50, 10, 100]"), "strictly ascending")

    def test_every_problem_is_reported_at_once(self, tmp_path):
        text = BASE.replace("wind/step1_v2.svg", "wind/gone.svg").replace('"p90 < 3"', '"p95 < 3"')
        with pytest.raises(ConfigError) as info:
            load(write(tmp_path, text.replace("[p10, p90]", "[p90, p10]")))
        assert len(info.value.issues) == 3
        report = str(info.value)
        assert report.startswith(f"{tmp_path / 'vsup.yaml'}: 3 problems")
        assert f"vsup.yaml:{RULE_1_LINE}: schemes.wind-rules.rules[0].when:" in report


class TestTreeClassification:
    @pytest.fixture
    def rain(self):
        return load().scheme("precipitation-vsup")

    @staticmethod
    def q(p17, p25, p75, p83):
        """The quantiles the shipped tree schemes read: p17..p83 for certain,
        p25..p75 for likely."""
        return {"p17": p17, "p25": p25, "p75": p75, "p83": p83}

    def test_class_bounds_are_lower_inclusive(self, rain):
        assert [rain.class_of(v) for v in (0.0, 0.0999, 0.1, 1.99, 2.0, 4.99, 5.0, 50.0)] == [0, 0, 1, 1, 2, 2, 3, 3]

    def test_two_thirds_in_one_class_is_certain(self, rain):
        choice = rain.classify(self.q(2.4, 2.8, 4.2, 4.6))
        assert (choice.level, choice.class_, choice.pictogram) == (3, "medium", "rain/step3_medium_rain.svg")

    def test_spread_over_a_group_drops_a_level(self, rain):
        choice = rain.classify(self.q(0.0, 0.2, 0.8, 1.5))
        assert (choice.level, choice.class_) == (2, "none+light")

    def test_spread_over_groups_drops_to_the_last_level(self, rain):
        choice = rain.classify(self.q(0.0, 0.5, 3.0, 8.0))
        assert (choice.level, choice.class_) == (1, "none+light+medium+heavy")

    def test_a_third_of_the_members_outside_the_class_is_not_certain(self, rain):
        """p25..p75 all light rain, but p17 is dry: two thirds are not in one
        class, so it is only "likely"."""
        choice = rain.classify(self.q(0.05, 0.2, 0.8, 0.9))
        assert (choice.level, choice.class_) == (2, "none+light")

    def test_deliberately_differs_from_legacy(self, rain):
        """p10 dry, p90 light: legacy says confident light rain, the tree says
        it is not even sure it rains."""
        legacy = load().scheme("precipitation-legacy")
        values = {"p10": 0.0, "p17": 0.0, "p25": 0.0, "p50": 0.2, "p75": 0.5, "p83": 0.7, "p90": 0.8}
        assert legacy.classify(values).class_ == "light"
        assert rain.classify(values).class_ == "none+light"


class TestApplyingSchemes:
    @pytest.fixture
    def forecast(self):
        return build_forecast(FixtureSource().load("reykjavik"))

    def test_every_step_gets_a_pictogram(self, forecast):
        out = classify(forecast, load(), ["cloud-vsup", "precipitation-vsup", "wind-legacy"])
        assert set(out.pictograms) == {"cloud_cover", "precipitation", "wind_speed_10m"}
        assert all(len(s.items) == len(out.steps) for s in out.pictograms.values())
        assert out.pictograms["precipitation"].scheme == "precipitation-vsup"
        type(out).model_validate_json(out.model_dump_json())  # still a valid Forecast

    def test_one_scheme_per_variable(self, forecast):
        with pytest.raises(SchemeMismatch, match="both draw precipitation"):
            classify(forecast, load(), ["precipitation-vsup", "precipitation-legacy"])

    def test_window_must_match(self, forecast):
        twelve_hourly = forecast.model_copy(update={"variables": {
            **forecast.variables,
            "precipitation": forecast.variables["precipitation"].model_copy(update={"window_hours": 12}),
        }})
        with pytest.raises(SchemeMismatch, match="written for 6-hour totals"):
            classify(twelve_hourly, load(), ["precipitation-vsup"])

    def test_quantiles_must_be_present(self, forecast):
        thin = build_forecast(FixtureSource().load("reykjavik"), quantile_levels=(50,))
        with pytest.raises(SchemeMismatch, match="reads p17, p25, p75, p83"):
            classify(thin, load(), ["wind-vsup"])

    def test_deterministic_rules_need_a_deterministic_run(self, forecast, tmp_path):
        text = BASE.replace('"p90 < 3"', '"deterministic < 3"')
        with pytest.raises(SchemeMismatch, match="needs a deterministic run"):
            classify(forecast, load(write(tmp_path, text)), ["wind-rules"])

    def test_steps_are_not_touched(self, forecast):
        out = classify(forecast, load(), ["wind-vsup"])
        assert out.steps == forecast.steps
        assert out.steps[1] - out.steps[0] == timedelta(hours=6)
