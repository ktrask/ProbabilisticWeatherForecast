"""python -m sources check [CONFIG] [--vsup VSUP]   validate products against the VSUP schemes
python -m sources schema [-o FILE]                  write the JSON Schema for editor support
"""
import argparse
import json
import sys
from pathlib import Path

from core.configfile import ConfigError
from sources.config import DEFAULT_SOURCES, json_schema, load
from vsup import config as vsup


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m sources", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="validate a sources config")
    check.add_argument("config", nargs="?", type=Path, default=DEFAULT_SOURCES)
    check.add_argument("--vsup", type=Path, default=vsup.DEFAULT_CONFIG, help="the VSUP config it refers to")
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
        catalog = load(args.config, vsup.load(args.vsup))
    except ConfigError as exc:
        print(exc, file=sys.stderr)
        return 1
    print(f"{args.config}: OK")
    for product in catalog.products.values():
        default = "  (default)" if product is catalog.default else ""
        print(f"  {product.id}: {product.label} <- {product.source_key} ({product.source.id}){default}")
        print(f"    variants: {', '.join(product.variants)}; schemes: {', '.join(product.schemes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
