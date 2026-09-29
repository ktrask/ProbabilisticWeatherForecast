#!/bin/bash
#Container entry point. Replaces an earlier `while :; do python3 run.py; done`
#loop, which restarted the development server after every crash - hiding the
#failure, ignoring SIGTERM so `docker stop` always had to escalate to SIGKILL,
#and serving traffic from a server that is single-threaded and not built for it.
#
#gunicorn supervises its own workers, so a crashed worker is replaced without
#swallowing the error, and exec makes it PID 1 so signals reach it directly.
#
#It serves the new app: the FastAPI backend (api.app), which also delivers the
#built frontend and the pictograms. The app is async, so the workers are
#uvicorn's. Each worker loads and cross-checks config/vsup.yaml and
#config/sources.yaml on start; a broken config fails the boot instead of
#failing requests.
set -euo pipefail

cd /app

#0.0.0.0 is deliberate here and only here: the container has to accept
#connections from outside itself. The development servers default to loopback.
exec gunicorn \
    --bind "${HOST:-0.0.0.0}:${PORT:-5003}" \
    --workers "${WEB_CONCURRENCY:-2}" \
    --worker-class uvicorn_worker.UvicornWorker \
    --timeout "${GUNICORN_TIMEOUT:-120}" \
    --access-logfile - \
    --error-logfile - \
    "api.app:create_app()"
