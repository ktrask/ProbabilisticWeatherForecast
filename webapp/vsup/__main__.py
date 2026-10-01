"""python -m vsup check [CONFIG]   validate, then show which rules fire on the fixtures
python -m vsup schema [-o FILE]  write the JSON Schema for editor support

`check` exits 1 if the config is invalid. The coverage table is information,
not a verdict: a rule that never fires on five recorded forecasts may just be
waiting for the right weather - but a rule that cannot fire is worth a look.
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from core.pipeline import build_forecast
from sources.fixture import DEFAULT_DIRECTORY, FixtureSource
from vsup.classify import SchemeMismatch, check_applicable, classify_series
from vsup.config import DEFAULT_CONFIG, ConfigError, json_schema, load


def coverage(config, fixtures):
    """{scheme name: {fixture key: Counter(outcome -> steps)}} over every fixture."""
    source = FixtureSource(fixtures)
    forecasts = {
        key: build_forecast(source.load(key), quantile_levels=config.quantiles) for key in source.keys()
    }
    table = {}
    for name, scheme in config.schemes.items():
        table[name] = {}
        for key, forecast in forecasts.items():
            try:
                series = check_applicable(scheme, forecast)
            except SchemeMismatch:
                continue  # written for another window than these recordings
            table[name][key] = Counter(choice.outcome for choice in classify_series(scheme, series))
    return table


def print_coverage(config, table, out):
    never = 0
    for name, scheme in config.schemes.items():
        keys = list(table[name])
        out.write(f"\n{name}  ({scheme.mode}, {scheme.variable})\n")
        if not keys:
            out.write(f"  no recordings in {scheme.window_hours}-hour steps here\n")
            continue
        rows = []
        for choice, text in scheme.outcomes():
            counts = [table[name][key][choice.outcome] for key in keys]
            rows.append((choice.outcome, text, choice.pictogram, counts))
        w_outcome = max(len(r[0]) for r in rows)
        w_text = min(max(len(r[1]) for r in rows), 32)
        w_picture = max(len(r[2]) for r in rows)
        header = " ".join(f"{key[:6]:>6}" for key in keys)
        out.write(f"  {'':{w_outcome}}  {'':{w_text}}  {'':{w_picture}}  {header}  {'total':>6}\n")
        for outcome, text, picture, counts in rows:
            total = sum(counts)
            flag = "  never" if total == 0 else ""
            never += total == 0
            cells = " ".join(f"{c:>6}" for c in counts)
            out.write(f"  {outcome:{w_outcome}}  {text[:w_text]:{w_text}}  {picture:{w_picture}}  {cells}  {total:>6}{flag}\n")
    if never:
        out.write(f"\n{never} outcome(s) never chosen on these fixtures. That can be the weather; "
                  f"a rule that cannot fire at all is worth a look.\n")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m vsup", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="validate a VSUP config and show rule coverage")
    check.add_argument("config", nargs="?", type=Path, default=DEFAULT_CONFIG)
    check.add_argument("--fixtures", type=Path, default=DEFAULT_DIRECTORY,
                       help="recorded forecasts for the coverage table (default: tests/fixtures)")
    check.add_argument("--no-coverage", action="store_true", help="validate only")
    schema = commands.add_parser("schema", help="print the JSON Schema of the config format")
    schema.add_argument("-o", "--output", type=Path, help="write to this file instead of stdout")
    args = parser.parse_args(argv)

    if args.command == "schema":
        text = json.dumps(json_schema(), indent=2) + "\n"
        if args.output:
            args.output.write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text)
        return 0

    try:
        config = load(args.config)
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 1
    print(f"{args.config}: OK, {len(config.schemes)} scheme(s)")
    if not args.no_coverage:
        try:
            table = coverage(config, args.fixtures)
        except (ValueError, OSError) as exc:  # includes SchemeMismatch
            print(f"\ncoverage skipped: {exc}")
        else:
            print_coverage(config, table, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
