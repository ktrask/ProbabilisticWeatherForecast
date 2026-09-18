#!/bin/bash
#Container entry point. Replaces an earlier `while :; do python3 run.py; done`
#loop, which restarted the development server after every crash - hiding the
#failure, ignoring SIGTERM so `docker stop` always had to escalate to SIGKILL,
#and serving traffic from a server that is single-threaded and not built for it.
#
#gunicorn supervises its own workers, so a crashed worker is replaced without
#swallowing the error, and exec makes it PID 1 so signals reach it directly.
set -euo pipefail

cd /app

#0.0.0.0 is deliberate here and only here: the container has to accept
#connections from outside itself. run.py defaults to loopback.
exec gunicorn \
    --bind "${HOST:-0.0.0.0}:${PORT:-5003}" \
    --workers "${WEB_CONCURRENCY:-2}" \
    --timeout "${GUNICORN_TIMEOUT:-120}" \
    --access-logfile - \
    --error-logfile - \
    app:app
