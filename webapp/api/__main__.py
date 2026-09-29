"""python -m api serve [--host H] [--port P] [--reload]   development server
python -m api openapi [-o FILE]                          print the OpenAPI schema

The server binds to 127.0.0.1 unless told otherwise. There is no debug mode to
guard here (unlike run.py's Werkzeug debugger), but a development server still
has no business on a public interface by default.
"""
import argparse
import json
import sys
from pathlib import Path

OPENAPI_FILE = Path(__file__).resolve().parent / "openapi.json"


def openapi_text():
    """The schema as checked in: stable key order, trailing newline."""
    from api.app import create_app

    return json.dumps(create_app().openapi(), indent=2, sort_keys=True) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m api", description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="run the development server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--reload", action="store_true", help="restart when the code changes")
    schema = commands.add_parser("openapi", help="print the OpenAPI schema")
    schema.add_argument("-o", "--output", type=Path, help=f"write to this file, e.g. {OPENAPI_FILE.name}")
    args = parser.parse_args(argv)

    if args.command == "openapi":
        text = openapi_text()
        if args.output:
            args.output.write_text(text, encoding="utf-8")
        else:
            sys.stdout.write(text)
        return 0

    import uvicorn

    uvicorn.run("api.app:create_app", factory=True, host=args.host, port=args.port, reload=args.reload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
