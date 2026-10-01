"""YAML config files whose errors name the line to fix.

Shared by vsup/config.py and sources/config.py. A file is parsed, checked for
duplicate keys and validated against a Pydantic model; the loader then adds
its own semantic checks through report(). Every problem is collected with its
line and raised together by check() as one ConfigError, so a broken file is
fixed in one pass instead of one error per restart.
"""
from pathlib import Path
from typing import NamedTuple

import yaml
from pydantic import ValidationError


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


class Locator:
    """Maps a path like ("schemes", "wind", "rules", 3, "when") to its line."""

    def __init__(self, root):
        self.root = root

    def find(self, loc):
        node, parts = self.root, []
        for part in loc:
            child = self._child(node, part)
            if child is not None:  # skip what is not in the file
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
        entry under the same name would simply vanish."""
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


class ConfigFile:
    """One YAML file being loaded: its validated `spec` and the problems so far.

    Raises ConfigError straight away if the file is not YAML or does not fit
    `model`; otherwise the caller adds its own checks with report() and ends
    with check().

    union_tags: {collection key: tags}. For a discriminated union inside a
    mapping, Pydantic puts the chosen tag into the error path -
    ("schemes", "wind", "rules", "variable"). The tag is not in the file, and
    when it doubles as a real key ("rules") it would send the line lookup to
    the wrong place, so it is taken out.
    """

    def __init__(self, path, model, union_tags=None):
        self.path = Path(path)
        self.issues = []
        self._union_tags = union_tags or {}
        self.text = self.path.read_text(encoding="utf-8")
        try:
            root = yaml.compose(self.text, Loader=yaml.SafeLoader)
            data = yaml.safe_load(self.text)
        except yaml.YAMLError as exc:
            mark = getattr(exc, "problem_mark", None)
            raise ConfigError(self.path, [ConfigIssue(
                mark.line + 1 if mark else None, "",
                f"not valid YAML: {getattr(exc, 'problem', None) or exc}",
            )]) from None
        self.locator = Locator(root)
        self.issues.extend(self.locator.duplicate_keys())
        try:
            self.spec = model.model_validate(data)
        except ValidationError as exc:
            for error in exc.errors():
                self.report(self._untagged(self._at_tag(error)), self._message(error))
            self.check()

    def report(self, loc, message):
        self.issues.append(ConfigIssue(self.locator.line(loc), self.locator.where(loc), message))

    def check(self):
        if self.issues:
            raise ConfigError(self.path, self.issues)

    @staticmethod
    def _message(error):
        """Pydantic's message - except for a missing field, which it calls just
        "Field required": the line points at the entry, so name the field."""
        if error["type"] == "missing" and error["loc"]:
            return f"{error['loc'][-1]!r} is missing"
        return error["msg"]

    @staticmethod
    def _at_tag(error):
        """An unknown or missing tag (`adapter: carrier_pigeon`) is reported on
        the whole entry; point it at the tag's own line instead."""
        loc = error["loc"]
        if error["type"] in ("union_tag_invalid", "union_tag_not_found"):
            return loc + (error["ctx"]["discriminator"].strip("'"),)
        return loc

    def _untagged(self, loc):
        if len(loc) >= 3 and loc[2] in self._union_tags.get(loc[0], ()):
            return loc[:2] + loc[3:]
        return loc
