"""Loading config/vsup.yaml: parse, validate, and compile it into schemes.

Two stages. The Pydantic models below describe the file's shape (and export
the JSON Schema that editors use for completion). Then every scheme is checked
for what a shape cannot express - thresholds in order, pictograms on disk,
quantiles that are actually computed, a unit that converts to the variable's -
and compiled. Every problem found is collected with its line in the file, and
all of them are raised together as one ConfigError, so a broken config is
fixed in one pass rather than one error per restart.
"""
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, NamedTuple

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from core.model import quantile_level, quantile_name
from core.units import UNITS, UnitError, converter
from core.variables import VARIABLES, variable
from vsup import expr

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "vsup.yaml"
PICTOGRAM_SUFFIXES = (".svg", ".png")


# --- The file's shape -------------------------------------------------------

class _Strict(BaseModel):
    # extra="forbid": a misspelt key is an error, not silently ignored.
    model_config = ConfigDict(extra="forbid", validate_by_name=True, serialize_by_alias=True)


class RuleSpec(_Strict):
    when: str | None = Field(
        None, description="Condition on the quantiles, e.g. 'p10 > 1 and p90 < 2'. The first rule that holds wins."
    )
    default: Literal[True] | None = Field(None, description="The catch-all. Exactly the last rule has it.")
    pictogram: str = Field(description="Path relative to pictogram_root.")
    level: int = Field(ge=1, description="Certainty: higher is more certain.")
    class_: str = Field(alias="class", description="Which intensity class or group the pictogram stands for.")

    @model_validator(mode="after")
    def _when_or_default(self):
        if (self.when is None) == (self.default is None):
            raise ValueError("a rule has either `when:` or `default: true`")
        return self


class ClassSpec(_Strict):
    id: str
    below: float | None = Field(None, description="Upper bound, exclusive. The last class has none.")


class LevelSpec(_Strict):
    level: int = Field(ge=1)
    interval: tuple[str, str] | None = Field(
        None,
        description="Two quantiles, e.g. [p10, p90]. The level applies when both fall into the same "
        "group. The last level has none and always applies.",
    )
    groups: list[list[str]] = Field(min_length=1, description="The classes, merged into groups, in order.")


class _SchemeSpec(_Strict):
    description: str | None = None
    variable: Literal[tuple(VARIABLES)]
    unit: Literal[tuple(sorted(UNITS))] = Field(
        description="The unit the thresholds are written in. Anything but the variable's canonical "
        "unit is converted once, at load time."
    )
    window_hours: int | None = Field(
        None, ge=1, description="For totals (precipitation): the window the thresholds refer to."
    )


class RulesSchemeSpec(_SchemeSpec):
    mode: Literal["rules"]
    rules: list[RuleSpec] = Field(min_length=1)


class TreeSchemeSpec(_SchemeSpec):
    mode: Literal["tree"]
    classes: list[ClassSpec] = Field(min_length=2)
    levels: list[LevelSpec] = Field(min_length=1)
    pictograms: dict[str, str] = Field(
        description='One per level and group, keyed "level:group", e.g. "2:none+light".'
    )


class VsupFile(_Strict):
    version: Literal[1]
    quantiles: list[int] = Field(min_length=1, description="The quantile levels to compute, 0-100.")
    pictogram_root: str = Field("pictograms", description="Directory of the pictograms, relative to this file.")
    schemes: dict[str, Annotated[RulesSchemeSpec | TreeSchemeSpec, Field(discriminator="mode")]] = Field(
        min_length=1
    )

    @field_validator("quantiles")
    @classmethod
    def _ascending_levels(cls, levels):
        if any(not 0 <= q <= 100 for q in levels):
            raise ValueError("quantile levels lie between 0 and 100")
        if levels != sorted(set(levels)):
            raise ValueError("quantile levels must be strictly ascending")
        return levels


def json_schema():
    schema = VsupFile.model_json_schema(by_alias=True)
    schema["title"] = "VSUP schemes"
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return schema


# --- Compiled schemes -------------------------------------------------------

@dataclass(frozen=True)
class Choice:
    pictogram: str
    level: int
    class_: str
    # Which rule or branch produced this choice; the coverage table counts these.
    outcome: str


@dataclass(frozen=True)
class _Rule:
    condition: object  # an expr node, or None for the default
    text: str
    choice: Choice


@dataclass(frozen=True)
class RulesScheme:
    name: str
    variable: str
    window_hours: int | None
    reads: frozenset  # quantile names, and maybe "deterministic"
    rules: tuple

    mode = "rules"

    def classify(self, values):
        for rule in self.rules:
            if rule.condition is None or expr.evaluate(rule.condition, values):
                return rule.choice
        raise AssertionError("unreachable: the loader requires a default rule")

    def outcomes(self):
        return [(rule.choice, rule.text) for rule in self.rules]


@dataclass(frozen=True)
class _Level:
    level: int
    interval: tuple | None  # (quantile, quantile)
    groups: tuple  # of tuples of class indices
    choices: tuple  # one Choice per group


@dataclass(frozen=True)
class TreeScheme:
    name: str
    variable: str
    window_hours: int | None
    reads: frozenset
    class_ids: tuple
    bounds: tuple  # in canonical units; class i is bounds[i-1] <= v < bounds[i]
    levels: tuple

    mode = "tree"

    def class_of(self, value):
        return bisect_right(self.bounds, value)

    def classify(self, values):
        for level in self.levels:
            if level.interval is None:
                return level.choices[0]
            low = self.class_of(values[level.interval[0]])
            high = self.class_of(values[level.interval[1]])
            for group, choice in zip(level.groups, level.choices):
                if low in group:
                    if high in group:
                        return choice
                    break
        raise AssertionError("unreachable: the loader requires an unconditional last level")

    def outcomes(self):
        return [
            (choice, "always" if level.interval is None else f"{level.interval[0]}..{level.interval[1]} in one group")
            for level in self.levels
            for choice in level.choices
        ]


@dataclass(frozen=True)
class VsupConfig:
    path: Path
    quantiles: tuple
    pictogram_root: Path
    schemes: dict

    def scheme(self, name):
        try:
            return self.schemes[name]
        except KeyError:
            raise KeyError(f"no scheme {name!r} in {self.path}; there are {', '.join(self.schemes)}") from None


# --- Errors with line numbers -----------------------------------------------

class ConfigIssue(NamedTuple):
    line: int | None
    where: str
    message: str


class ConfigError(ValueError):
    def __init__(self, path, issues):
        super().__init__(path, issues)
        self.path = Path(path)
        self.issues = list(issues)

    def __str__(self):
        lines = [f"{self.path}: {len(self.issues)} problem{'s' if len(self.issues) != 1 else ''}"]
        for issue in sorted(self.issues, key=lambda i: (i.line or 0, i.where)):
            position = f"{self.path.name}:{issue.line}" if issue.line else self.path.name
            where = f" {issue.where}:" if issue.where else ""
            lines.append(f"  {position}:{where} {issue.message}")
        return "\n".join(lines)


class _Locator:
    """Maps a path like ("schemes", "wind", "rules", 3, "when") to its line."""

    def __init__(self, root):
        self.root = root

    def find(self, loc):
        node, parts = self.root, []
        for part in loc:
            child = self._child(node, part)
            if child is not None:  # skip what is not in the file, e.g. Pydantic's union tags
                node = child
                parts.append(part)
        return node, parts

    def line(self, loc):
        node, _ = self.find(loc)
        return node.start_mark.line + 1 if node is not None else None

    def where(self, loc):
        _, parts = self.find(loc)
        text = ""
        for part in parts:
            text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else str(part))
        return text

    @staticmethod
    def _child(node, part):
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                if isinstance(key, yaml.ScalarNode) and key.value == str(part):
                    return value
        elif isinstance(node, yaml.SequenceNode) and isinstance(part, int) and 0 <= part < len(node.value):
            return node.value[part]
        return None

    def duplicate_keys(self):
        """PyYAML keeps the last of two equal keys without a word; a second
        scheme or pictogram under the same name would simply vanish."""
        found, seen, stack = [], set(), [self.root]
        while stack:
            node = stack.pop()
            if node is None or id(node) in seen:
                continue
            seen.add(id(node))
            if isinstance(node, yaml.MappingNode):
                keys = set()
                for key, value in node.value:
                    if isinstance(key, yaml.ScalarNode) and key.value != "<<":
                        if key.value in keys:
                            found.append(ConfigIssue(key.start_mark.line + 1, "",
                                                     f"duplicate key {key.value!r}; YAML would keep only the last"))
                        keys.add(key.value)
                    stack.append(value)
            elif isinstance(node, yaml.SequenceNode):
                stack.extend(node.value)
        return found


# --- Loading ----------------------------------------------------------------

_MODES = ("rules", "tree")


def _without_union_tag(loc):
    """Pydantic puts the chosen `mode` into the error path of a scheme:
    ("schemes", "wind", "rules", "variable"). That tag is not in the file, and
    "rules" is also a real key, so left in, it would send the line lookup to
    the rules list instead of `variable:`."""
    if len(loc) >= 3 and loc[0] == "schemes" and loc[2] in _MODES:
        return loc[:2] + loc[3:]
    return loc


def load(path=DEFAULT_CONFIG):
    """Load and compile `path`. Raises ConfigError listing every problem found."""
    path = Path(path)
    text = path.read_text(encoding="utf-8")
    try:
        root = yaml.compose(text, Loader=yaml.SafeLoader)
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        raise ConfigError(path, [ConfigIssue(mark.line + 1 if mark else None, "",
                                             f"not valid YAML: {getattr(exc, 'problem', None) or exc}")]) from None
    locator = _Locator(root)
    issues = locator.duplicate_keys()
    try:
        spec = VsupFile.model_validate(data)
    except ValidationError as exc:
        for error in exc.errors():
            loc = _without_union_tag(error["loc"])
            issues.append(ConfigIssue(locator.line(loc), locator.where(loc), error["msg"]))
        raise ConfigError(path, issues) from None
    config = _Compiler(spec, path, locator, issues).compile()
    if issues:
        raise ConfigError(path, issues)
    return config


class _Compiler:
    def __init__(self, spec, path, locator, issues):
        self.spec = spec
        self.path = path
        self.locator = locator
        self.issues = issues
        self.root = (path.parent / spec.pictogram_root).resolve()
        self.computed = {quantile_name(level) for level in spec.quantiles}

    def report(self, loc, message):
        self.issues.append(ConfigIssue(self.locator.line(loc), self.locator.where(loc), message))

    def compile(self):
        if not self.root.is_dir():
            self.report(("pictogram_root",), f"{self.spec.pictogram_root!r} is not a directory (resolved to {self.root})")
        schemes = {}
        for name, scheme in self.spec.schemes.items():
            compile_mode = self._rules if scheme.mode == "rules" else self._tree
            schemes[name] = compile_mode(name, scheme, ("schemes", name), self._unit(scheme, ("schemes", name)))
        return VsupConfig(self.path, tuple(self.spec.quantiles), self.root, schemes)

    def _unit(self, scheme, at):
        spec = variable(scheme.variable)
        if spec.kind == "sum" and scheme.window_hours is None:
            self.report(at + ("variable",), f"{spec.name} is a total over a window; say which, e.g. `window_hours: 6`")
        if spec.kind == "instant" and scheme.window_hours is not None:
            self.report(at + ("window_hours",), f"{spec.name} is not a total; window_hours does not apply")
        try:
            return converter(scheme.unit, spec.unit)
        except UnitError:
            self.report(at + ("unit",), f"{scheme.unit!r} cannot be converted to {spec.name}'s unit {spec.unit!r}")
            return lambda value: value

    def _pictogram(self, loc, relative):
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            self.report(loc, f"{relative!r}: pictograms are given relative to pictogram_root and stay inside it")
        elif path.suffix.lower() not in PICTOGRAM_SUFFIXES:
            self.report(loc, f"{relative!r}: pictograms are {' or '.join(PICTOGRAM_SUFFIXES)} files")
        elif not (self.root / path).is_file():
            self.report(loc, f"{relative!r} does not exist in {self.root}")

    def _quantiles(self, loc, names):
        unknown = sorted((n for n in names if n not in self.computed), key=quantile_level)
        if unknown:
            computed = ", ".join(sorted(self.computed, key=quantile_level))
            self.report(loc, f"uses {', '.join(unknown)}, which {'is' if len(unknown) == 1 else 'are'} "
                             f"not computed (quantiles: {computed})")

    def _rules(self, name, scheme, at, convert):
        rules, reads = [], set()
        last = len(scheme.rules) - 1
        for i, rule in enumerate(scheme.rules):
            loc = at + ("rules", i)
            self._pictogram(loc + ("pictogram",), rule.pictogram)
            condition = None
            if rule.default:
                if i != last:
                    self.report(loc + ("default",), f"the default rule has to be the last; "
                                                    f"the {last - i} rule(s) after it can never apply")
            else:
                try:
                    condition = expr.parse(rule.when)
                except expr.ExprError as exc:
                    self.report(loc + ("when",), f"{rule.when!r}: {exc}")
                    continue
                names = expr.refs(condition)
                self._quantiles(loc + ("when",), names - {expr.DETERMINISTIC})
                reads |= names
                condition = expr.map_numbers(condition, convert)
            choice = Choice(rule.pictogram, rule.level, rule.class_, f"rule {i + 1}")
            rules.append(_Rule(condition, rule.when or "default", choice))
        if not scheme.rules[last].default:
            self.report(at + ("rules", last), "the last rule has to be `default: true`, "
                                              "or a step no rule matches would get no pictogram")
        return RulesScheme(name, scheme.variable, scheme.window_hours, frozenset(reads), tuple(rules))

    def _tree(self, name, scheme, at, convert):
        ids = [c.id for c in scheme.classes]
        index = {class_id: i for i, class_id in enumerate(ids)}
        if len(index) != len(ids):
            self.report(at + ("classes",), f"class ids repeat: {ids}")

        bounds = []
        for i, spec in enumerate(scheme.classes):
            loc = at + ("classes", i)
            if i == len(ids) - 1:
                if spec.below is not None:
                    self.report(loc + ("below",), "the last class is open-ended and has no `below`")
            elif spec.below is None:
                self.report(loc, "every class except the last needs `below`")
            else:
                if bounds and spec.below <= bounds[-1]:
                    self.report(loc + ("below",), f"{spec.below} is not above the previous bound {bounds[-1]}; "
                                                  f"bounds must increase strictly")
                bounds.append(spec.below)

        levels, reads, expected = [], set(), []
        previous = None  # (level number, group boundaries)
        last = len(scheme.levels) - 1
        for j, spec in enumerate(scheme.levels):
            loc = at + ("levels", j)
            if previous is not None and spec.level >= previous[0]:
                self.report(loc + ("level",), f"levels are listed from most to least certain; "
                                              f"{spec.level} does not come below {previous[0]}")
            boundaries = self._partition(loc + ("groups",), spec.groups, ids)
            if boundaries is not None and previous is not None and previous[1] is not None:
                if not boundaries <= previous[1]:
                    self.report(loc + ("groups",), "each group has to merge whole groups of the level above; "
                                                   "these groups split one of them")
            previous = (spec.level, boundaries)

            if spec.interval is None:
                if j != last:
                    self.report(loc, "only the last level can do without an interval; "
                                     "the levels after this one could never apply")
                if len(spec.groups) != 1:
                    self.report(loc + ("groups",), "a level without an interval always applies, "
                                                   "so it has to be a single group")
            else:
                low, high = spec.interval
                try:
                    if quantile_level(low) > quantile_level(high):
                        self.report(loc + ("interval",), f"[{low}, {high}] runs backwards")
                    self._quantiles(loc + ("interval",), {low, high})
                    reads |= {low, high}
                except ValueError as exc:
                    self.report(loc + ("interval",), str(exc))
            if j == last and spec.interval is not None:
                self.report(loc + ("interval",), "the last level is the fallback and must not have an interval, "
                                                 "or a step matching no level would get no pictogram")

            choices = []
            for group in spec.groups:
                key = f"{spec.level}:{'+'.join(group)}"
                expected.append(key)
                picture = scheme.pictograms.get(key, "")
                choices.append(Choice(picture, spec.level, "+".join(group), key))
            levels.append(_Level(
                spec.level,
                tuple(spec.interval) if spec.interval else None,
                tuple(tuple(index.get(c, -1) for c in group) for group in spec.groups),
                tuple(choices),
            ))

        missing = [key for key in expected if key not in scheme.pictograms]
        if missing:
            self.report(at + ("pictograms",), f"no pictogram for {', '.join(missing)}")
        for key, picture in scheme.pictograms.items():
            if key not in expected:
                self.report(at + ("pictograms", key), f"{key!r} is none of this scheme's level:group "
                                                      f"combinations ({', '.join(expected)})")
            else:
                self._pictogram(at + ("pictograms", key), picture)

        return TreeScheme(
            name, scheme.variable, scheme.window_hours, frozenset(reads), tuple(ids),
            tuple(convert(b) for b in bounds), tuple(levels),
        )

    def _partition(self, loc, groups, ids):
        """The group boundaries as a set of cut positions, or None if `groups`
        is not every class exactly once, in order, each group non-empty."""
        flat = [c for group in groups for c in group]
        unknown = [c for c in flat if c not in ids]
        if unknown:
            self.report(loc, f"unknown class(es) {', '.join(unknown)}; the classes are {', '.join(ids)}")
            return None
        if flat != ids or any(not group for group in groups):
            self.report(loc, f"groups have to list every class once, in order ({' '.join(ids)}); "
                             f"got {groups}")
            return None
        cuts, position = set(), 0
        for group in groups[:-1]:
            position += len(group)
            cuts.add(position)
        return cuts
