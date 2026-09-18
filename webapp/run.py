#!/usr/bin/env python3
"""Development entry point.

Serves with Flask's built-in server, which is single-threaded and explicitly not
meant for production; the container runs gunicorn instead (see startup.sh).

Both the debug mode and the bind address are opt-in, because their combination
is what turns the Werkzeug debugger into remote code execution for anyone who
can reach the port and provoke a traceback.
"""
import os
import sys

from app import app

TRUTHY = ("1", "true", "yes", "on")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5003

LOOPBACK = ("127.0.0.1", "localhost", "::1")


def envFlag(name):
    return os.environ.get(name, "").strip().lower() in TRUTHY


def serverOptions(environ=None):
    """(host, port, debug) from the environment, with safe defaults.

    Raises SystemExit for the one combination that must never happen by
    accident: the interactive debugger listening on a public interface.
    """
    environ = os.environ if environ is None else environ
    host = environ.get("HOST", DEFAULT_HOST).strip() or DEFAULT_HOST
    port = int(environ.get("PORT", DEFAULT_PORT))
    debug = environ.get("FLASK_DEBUG", "").strip().lower() in TRUTHY

    if debug and host not in LOOPBACK and not envFlag("ALLOW_PUBLIC_DEBUG"):
        raise SystemExit(
            f"Refusing to serve the Werkzeug debugger on {host}: anyone who can "
            f"reach this port could run arbitrary code. Bind to {DEFAULT_HOST} "
            f"instead, or set ALLOW_PUBLIC_DEBUG=1 if this really is what you want."
        )
    return host, port, debug


def main():
    host, port, debug = serverOptions()
    app.run(debug=debug, host=host, port=port)


if __name__ == "__main__":
    sys.exit(main())
